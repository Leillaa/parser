#!/usr/bin/env python3
"""
Парсер Киномакс: фильмы и сеансы на сегодня по городам/кинотеатрам
+ заполненность зала за N минут до начала каждого сеанса.

Этапы:
  1. /locations, /cinemas-simplified  -> города и кинотеатры
  2. по каждому городу (заголовок Location-Id, иначе API отдаёт Москву):
       /movies                        -> каталог фильмов (обычный прокат)
       /movies?limit=100&listing=kinoteka -> фильмы Кинотеки (отдельный запрос)
       /movies/{id}/sessions          -> в каких кинотеатрах и во сколько идёт фильм
       /cinemas/{id}/sessions         -> контрольный проход по кинотеатрам (мердж по id сеанса)
  3. за 5 минут до начала: /sessions/{id}/hallplan (заголовок Did) -> s=1 свободно, s=2 занято

Запуск:
  python3 kinomax_parser.py                      # все города, расписание + замеры залов
  python3 kinomax_parser.py --city "Москва"      # только Москва и МО
  python3 kinomax_parser.py --schedule-only      # только собрать расписание и выйти
  python3 kinomax_parser.py --now                # сразу снять залы всех будущих сеансов (проверка)
  python3 kinomax_parser.py --daemon --daily-at 03:00   # работать постоянно, новый день в 03:00 МСК

Хранилище:
  * задан DATABASE_URL  -> PostgreSQL (см. db/schema.sql, отчёт — view v_session_report)
  * не задан            -> файлы в ./output/ (schedule_<дата>.json, occupancy_<дата>.csv/.jsonl)
  Режим --now всегда пишет в файлы (occupancy_<дата>_now.*), чтобы не мешать боевым замерам.
Повторный запуск продолжает с того же места: уже снятые сеансы пропускаются.
"""

import argparse
import csv
import heapq
import json
import logging
import os
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

BASE_URL = "https://api.kinomax.ru/rest"
DID = "f2b4a919-f86b-4092-b362-f6617268f8f7"  # нужен для hallplan
HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/130.0 Safari/537.36",
    "Origin": "https://kinomax.ru",
    "Referer": "https://kinomax.ru/",
}
MSK = timezone(timedelta(hours=3))
# Сеанс считается отменённым, только если он пропал из расписания и до его начала ещё больше
# этого запаса: API скрывает уже начавшиеся сеансы, их нельзя принимать за отменённые.
REMOVAL_MARGIN_SEC = 10 * 60
OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")

log = logging.getLogger("kinomax")


# ---------------------------------------------------------------- HTTP

def api_get(path, params=None, did=False, location_id=None, retries=4):
    url = BASE_URL + path
    if params:
        url += "?" + urllib.parse.urlencode(params)
    headers = dict(HEADERS)
    if did:
        headers["Did"] = DID
    if location_id is not None:
        headers["Location-Id"] = str(location_id)  # без него API отдаёт Москву
    delay = 1.0
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            # 4xx (кроме 429) повторять бессмысленно
            if 400 <= e.code < 500 and e.code != 429:
                body = e.read().decode("utf-8", "replace")[:200]
                raise RuntimeError(f"HTTP {e.code} {url}: {body}") from None
            err = f"HTTP {e.code}"
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, ConnectionError) as e:
            err = repr(e)
        if attempt == retries:
            raise RuntimeError(f"{err} {url} (после {retries} попыток)")
        log.debug("retry %s %s: %s", attempt, url, err)
        time.sleep(delay)
        delay *= 2


# ---------------------------------------------------------------- Расписание

CATALOG_SOURCES = {
    "movies": ("/movies", None),                                       # каталог фильмов
    "kinoteka": ("/movies", {"limit": 100, "listing": "kinoteka"}),    # Кинотека
}


def fetch_catalog(location_id):
    """Фильмы города из двух отдельных запросов: каталог /movies и Кинотека.
    Возвращает {movie_id: {..., "sources": ["movies"/"kinoteka"]}}."""
    catalog = {}
    for source, (path, params) in CATALOG_SOURCES.items():
        for m in api_get(path, params, location_id=location_id):
            entry = catalog.setdefault(m["id"], {
                "movie_id": m["id"], "name": m["name"].strip(), "kind": m.get("kind"), "sources": [],
            })
            if source not in entry["sources"]:
                entry["sources"].append(source)
    return catalog


_movie_details_cache = {}   # movie_id -> {"countries": [...]}; живёт весь процесс
_city_offsets = {}          # location_id -> смещение от UTC в минутах (запоминается между сборами)


def parse_countries(raw):
    """' Великобритания, США' -> ['Великобритания', 'США']"""
    if isinstance(raw, list):
        items = raw
    else:
        items = str(raw or "").replace(";", ",").split(",")
    return [c.strip() for c in items if c and c.strip()]


def fetch_movie_details(movie_ids, workers=6):
    """Страны производства из /movies/{id}. Каждый фильм запрашивается один раз за процесс."""
    todo = [m for m in movie_ids if m not in _movie_details_cache]

    def one(mid):
        try:
            d = api_get(f"/movies/{mid}")
            return mid, {"countries": parse_countries(d.get("countries"))}
        except Exception as e:
            log.warning("Детали фильма %s: %s", mid, e)
            return mid, None

    if todo:
        with ThreadPoolExecutor(workers) as ex:
            for mid, info in ex.map(one, todo):
                if info is not None:
                    _movie_details_cache[mid] = info
    return {m: _movie_details_cache.get(m, {}) for m in movie_ids}


def session_record(s, formats=None):
    start = datetime.fromtimestamp(s["epoch"], tz=timezone.utc)
    return {
        "format": (formats or {}).get(s.get("formatId")),
        "session_id": s["id"],
        "time_local": s.get("time"),          # местное время кинотеатра
        "epoch": s["epoch"],
        "start_msk": start.astimezone(MSK).strftime("%Y-%m-%d %H:%M"),
        "price": s.get("price"),
        "format_id": s.get("formatId"),
        "is_passed": s.get("isPassed"),
    }


def utc_offset_minutes(sess):
    """Смещение часового пояса кинотеатра: местное время сеанса минус UTC."""
    try:
        h, m = map(int, sess["time"].split(":"))
        utc = datetime.fromtimestamp(sess["epoch"], tz=timezone.utc)
    except (KeyError, ValueError, TypeError):
        return None
    diff = (h * 60 + m - (utc.hour * 60 + utc.minute)) % 1440
    return diff - 1440 if diff > 840 else diff


def build_schedule(date=None, city_filter=None, workers=6):
    """Возвращает (schedule, coverage).
    coverage — множество (cinema_id, 'YYYY-MM-DD'), для которых расписание получено полностью,
    без ошибок API. Только для них можно делать вывод «сеанса больше нет в расписании»."""
    all_locations = api_get("/locations")
    cinemas = api_get("/cinemas-simplified")
    loc_by_id = {l["id"]: l for l in all_locations}

    def root_loc(loc_id):
        loc = loc_by_id.get(loc_id) or {"id": loc_id, "name": "?", "parentId": None}
        return loc_by_id.get(loc.get("parentId")) or loc

    # города, где есть кинотеатры (Красногорск/Пушкино/Подольск входят в «Москва и МО»)
    cin_by_loc = {}
    for c in cinemas:
        cin_by_loc.setdefault(root_loc(c["locationId"])["id"], []).append(c)
    locations = [loc_by_id[i] for i in cin_by_loc if i in loc_by_id]

    if city_filter:
        f = city_filter.lower()
        locations = [l for l in locations if f in l["name"].lower() or str(l["id"]) == f]
        if not locations:
            sys.exit(f"Город '{city_filter}' не найден. Доступны: "
                     + ", ".join(sorted(loc_by_id[i]["name"] for i in cin_by_loc if i in loc_by_id)))
    log.info("Городов к обработке: %d, кинотеатров: %d",
             len(locations), sum(len(cin_by_loc[l["id"]]) for l in locations))

    cinema_by_id = {c["id"]: c for c in cinemas}
    cinema_city = {c["id"]: root_loc(c["locationId"]) for c in cinemas}
    lock = threading.Lock()
    sessions = {}        # (cinema_id, movie_id) -> {session_id: record}
    movie_meta = {}      # movie_id -> {name, kind}
    movie_sources = {}   # movie_id -> {"movies", "kinoteka"}
    selected_dates = {}

    def add(cinema_id, movie, sess, formats):
        if cinema_id not in cinema_by_id or not sess:
            return
        with lock:
            movie_meta.setdefault(movie["id"], {"name": movie["name"].strip(), "kind": movie.get("kind")})
            bucket = sessions.setdefault((cinema_id, movie["id"]), {})
            for s in sess:
                bucket.setdefault(s["id"], session_record(s, formats))

    def formats_of(data):
        return {f["id"]: f["name"] for f in data.get("sessionFormats") or [] if "id" in f}

    failed_cities = set()   # города, где хоть один запрос упал: для них отмены не вычисляем
    no_date_cities = set()  # города без сеансов, для которых часовой пояс ещё неизвестен

    def safe(fn, *a):
        try:
            return fn(*a)
        except Exception as e:
            log.warning("%s", e)

    def safe_city(loc_id, fn, *a):
        try:
            return fn(*a)
        except Exception as e:
            failed_cities.add(loc_id)
            log.warning("%s", e)

    # Без параметра date API отдаёт ближайшую дату с сеансами (у Кинотеки это может быть
    # через месяц), поэтому дату передаём явно и проверяем selectedDate в ответе.
    city_dates = {}

    def detect_city_date(loc):
        if date:
            return date
        for cin in cin_by_loc[loc["id"]]:
            data = safe(api_get, f"/cinemas/{cin['id']}/sessions", None, False, loc["id"])
            for m in (data or {}).get("movies", []):
                for sess in m.get("sessions", []):
                    off = utc_offset_minutes(sess)
                    if off is not None:
                        _city_offsets[loc["id"]] = off
                        local_now = datetime.now(timezone.utc) + timedelta(minutes=off)
                        return local_now.strftime("%Y-%m-%d")
        off = _city_offsets.get(loc["id"])
        if off is not None:  # сеансов нет, но часовой пояс знаем с прошлого раза
            return (datetime.now(timezone.utc) + timedelta(minutes=off)).strftime("%Y-%m-%d")
        no_date_cities.add(loc["id"])  # берём московскую дату, но отмены не вычисляем
        return datetime.now(MSK).strftime("%Y-%m-%d")

    def load_cinema(cin, loc_id):
        d = city_dates[loc_id]
        data = api_get(f"/cinemas/{cin['id']}/sessions", {"date": d}, location_id=loc_id)
        selected_dates[cin["id"]] = d
        if data.get("selectedDate") != d:
            return
        fm = formats_of(data)
        for m in data.get("movies", []):
            add(cin["id"], m, m.get("sessions", []), fm)

    def load_movie(mid, loc_id):
        d = city_dates[loc_id]
        data = api_get(f"/movies/{mid}/sessions", {"date": d}, location_id=loc_id)
        if data.get("selectedDate") != d:
            return
        fm = formats_of(data)
        for c in data.get("cinemas", []):
            add(c["id"], data["movie"], c.get("sessions", []), fm)

    with ThreadPoolExecutor(workers) as ex:
        # 0) «сегодня» в часовом поясе каждого города
        city_dates.update(zip([l["id"] for l in locations], ex.map(detect_city_date, locations)))
        # 1) каталоги фильмов по каждому городу (с Кинотекой)
        catalogs = dict(zip([l["id"] for l in locations],
                            ex.map(lambda l: safe_city(l["id"], fetch_catalog, l["id"]) or {}, locations)))
        for loc in locations:
            cat = catalogs[loc["id"]]
            for mid, v in cat.items():
                movie_sources.setdefault(mid, set()).update(v["sources"])
            log.info("  %-16s дата %s | /movies: %2d | Кинотека: %2d | всего уникальных: %2d",
                     loc["name"], city_dates[loc["id"]],
                     sum("movies" in v["sources"] for v in cat.values()),
                     sum("kinoteka" in v["sources"] for v in cat.values()), len(cat))
        # 2) сеансы каждого фильма в городе + расписание каждого кинотеатра
        futures = []
        for loc in locations:
            for mid in catalogs[loc["id"]]:
                futures.append(ex.submit(safe_city, loc["id"], load_movie, mid, loc["id"]))
            for cin in cin_by_loc[loc["id"]]:
                futures.append(ex.submit(safe_city, loc["id"], load_cinema, cin, loc["id"]))
        for f in futures:
            f.result()

    details = fetch_movie_details(list(movie_meta))

    wanted_cities = {l["id"] for l in locations}
    tree = {}
    for (cid, mid), sess in sessions.items():
        city = cinema_city[cid]
        if city["id"] not in wanted_cities:
            continue
        mm = movie_meta[mid]
        city_node = tree.setdefault(city["id"], {"location_id": city["id"], "city": city["name"], "cinemas": {}})
        cin_node = city_node["cinemas"].setdefault(cid, {
            "cinema_id": cid, "cinema": cinema_by_id[cid]["name"],
            "date": selected_dates.get(cid) or city_dates.get(city["id"]), "movies": {}})
        cin_node["movies"][mid] = {
            "movie_id": mid,
            "name": mm["name"],
            "kind": mm["kind"],
            "countries": details.get(mid, {}).get("countries"),
            "in_catalog": "movies" in movie_sources.get(mid, ()),
            "source": "+".join(sorted(movie_sources.get(mid) or {"sessions"})),  # movies / kinoteka / kinoteka+movies
            "in_kinoteka": "kinoteka" in movie_sources.get(mid, ()) or mm["kind"] in ("kinoteka", "ownkinoteka"),
            "sessions": sorted(sess.values(), key=lambda s: s["epoch"]),
        }

    result = []
    for city_node in sorted(tree.values(), key=lambda x: x["city"]):
        cins = []
        for cn in sorted(city_node["cinemas"].values(), key=lambda x: x["cinema"]):
            cn["movies"] = sorted(cn["movies"].values(), key=lambda x: x["name"])
            cins.append(cn)
        city_node["cinemas"] = cins
        result.append(city_node)

    coverage = {(cin["id"], city_dates[loc["id"]])
                for loc in locations if loc["id"] not in failed_cities | no_date_cities
                for cin in cin_by_loc[loc["id"]]}
    if failed_cities:
        log.warning("Были ошибки API в городах %s — отмены сеансов там в этот раз не проверяются",
                    sorted(loc_by_id[i]["name"] for i in failed_cities if i in loc_by_id))
    return result, coverage


def flatten(schedule):
    for city in schedule:
        for cin in city["cinemas"]:
            for mv in cin["movies"]:
                for s in mv["sessions"]:
                    yield {
                        "city_id": city["location_id"], "city": city["city"],
                        "cinema_id": cin["cinema_id"], "cinema": cin["cinema"], "session_date": cin["date"],
                        "movie_id": mv["movie_id"], "movie": mv["name"], "kind": mv["kind"],
                        "countries": mv["countries"],
                        "source": mv["source"], "in_catalog": mv["in_catalog"], "in_kinoteka": mv["in_kinoteka"],
                        **s,
                    }


def print_summary(schedule):
    for city in schedule:
        n = sum(len(m["sessions"]) for c in city["cinemas"] for m in c["movies"])
        print(f"\n=== {city['city']} — кинотеатров: {len(city['cinemas'])}, сеансов: {n}")
        for cin in city["cinemas"]:
            print(f"  {cin['cinema']} (id {cin['cinema_id']}, {cin['date']})")
            for mv in cin["movies"]:
                times = ", ".join(s["time_local"] for s in mv["sessions"])
                tag = f" [{mv['source']}; {', '.join(mv['countries'] or []) or '?'}]"
                print(f"    {mv['movie_id']:>5}  {mv['name']}{tag}: {times}")


# ---------------------------------------------------------------- Заполненность

def fetch_occupancy(sess):
    data = api_get(f"/sessions/{sess['session_id']}/hallplan",
                   {"source": "site", "seatDisplay": 1}, did=True)
    seats = (data.get("hallPlan") or {}).get("seats") or []
    free = sum(1 for s in seats if s.get("s") == 1)
    sold = sum(1 for s in seats if s.get("s") == 2)
    other = len(seats) - free - sold
    info = data.get("session") or {}
    return {
        "country": ", ".join(sess.get("countries") or []),
        **{k: sess[k] for k in ("city", "cinema_id", "cinema", "movie_id", "movie",
                                "source", "in_kinoteka", "session_id", "time_local", "start_msk")},
        "hall": info.get("hallName"),
        "format": info.get("formatName") or sess.get("format"),
        "seats_total": len(seats),
        "seats_free": free,
        "seats_sold": sold,
        "seats_other": other,
        "occupancy_pct": round(100 * sold / (free + sold), 1) if free + sold else None,
        "checked_at_msk": datetime.now(MSK).strftime("%Y-%m-%d %H:%M:%S"),
        "minutes_before_start": round((sess["epoch"] - time.time()) / 60, 1),
    }


CSV_FIELDS = ["city", "cinema_id", "cinema", "movie_id", "movie", "country", "source", "in_kinoteka", "session_id",
              "time_local", "start_msk", "hall", "format", "seats_total", "seats_free",
              "seats_sold", "seats_other", "occupancy_pct", "checked_at_msk", "minutes_before_start"]


class ResultWriter:
    """Хранилище в файлах (когда DATABASE_URL не задан)."""

    def __init__(self, tag):
        self.jsonl = os.path.join(OUT_DIR, f"occupancy_{tag}.jsonl")
        self.csv = os.path.join(OUT_DIR, f"occupancy_{tag}.csv")
        self.target = self.csv
        self.lock = threading.Lock()
        self.done = set()
        if os.path.exists(self.jsonl):
            with open(self.jsonl, encoding="utf-8") as f:
                for line in f:
                    try:
                        self.done.add(json.loads(line)["session_id"])
                    except (ValueError, KeyError):
                        pass
        new_csv = not os.path.exists(self.csv)
        if new_csv:
            with open(self.csv, "w", newline="", encoding="utf-8-sig") as f:
                csv.DictWriter(f, CSV_FIELDS, delimiter=";").writeheader()

    def write(self, row):
        with self.lock:
            with open(self.jsonl, "a", encoding="utf-8") as f:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
            with open(self.csv, "a", newline="", encoding="utf-8-sig") as f:
                csv.DictWriter(f, CSV_FIELDS, delimiter=";", extrasaction="ignore").writerow(row)
            self.done.add(row["session_id"])

    def save_schedule(self, flat, coverage=(), margin_sec=0):
        return []  # расписание в файловом режиме пишется в schedule_<дата>.json


def next_daily_ts(hhmm):
    """Ближайший момент hh:mm по Москве (unix time)."""
    h, m = map(int, hhmm.split(":"))
    now = datetime.now(MSK)
    t = now.replace(hour=h, minute=m, second=0, microsecond=0)
    if t <= now:
        t += timedelta(days=1)
    return t.timestamp()


def find_removed(known, fresh_ids, coverage, done, now=None):
    """Сеансы, которые были в расписании, а теперь пропали (отменены).
    Учитываются только кинотеатры/даты из coverage и только сеансы, до которых ещё далеко."""
    now = time.time() if now is None else now
    return {sid for sid, s in known.items()
            if sid not in fresh_ids and sid not in done
            and (s["cinema_id"], s["session_date"]) in coverage
            and s["epoch"] > now + REMOVAL_MARGIN_SEC}


def monitor(flat_sessions, writer, lead_min=5, now_mode=False, workers=8,
            refresh_fn=None, refresh_min=60, daily_at=None, forever=False):
    """Очередь замеров: каждый сеанс снимается за lead_min минут до начала.
    refresh_fn() -> (flat, coverage) — пересобрать расписание (раз в refresh_min минут
    и ежедневно в daily_at МСК); перенесённые сеансы переставляются, отменённые не снимаются.
    forever — не завершаться, когда очередь пуста (режим демона)."""
    lead = lead_min * 60
    queue = []
    known = {}        # session_id -> актуальная запись сеанса
    removed = set()   # отменённые сеансы
    counter = iter(range(10**12))  # чтобы heapq не сравнивал словари

    def enqueue(items, coverage=None):
        now = time.time()
        added = past = moved = 0
        fresh_ids = set()
        for s in items:
            sid = s["session_id"]
            fresh_ids.add(sid)
            old = known.get(sid)
            known[sid] = s
            removed.discard(sid)  # снова появился в расписании
            if sid in writer.done or (old is not None and old["epoch"] == s["epoch"]):
                continue
            if s["epoch"] <= now:
                past += old is None
                continue
            if old is not None:
                moved += 1
                log.info("Сеанс %s (%s, %s) перенесён: %s -> %s МСК", sid, s["cinema"], s["movie"],
                         datetime.fromtimestamp(old["epoch"], MSK).strftime("%H:%M"),
                         datetime.fromtimestamp(s["epoch"], MSK).strftime("%H:%M"))
            # если до начала уже меньше lead — снимаем сразу
            run_at = now if now_mode else max(now, s["epoch"] - lead)
            heapq.heappush(queue, (run_at, next(counter), s))
            added += old is None
        if coverage:
            gone = find_removed(known, fresh_ids, coverage, writer.done, now) - removed
            for sid in gone:
                s = known[sid]
                log.info("Сеанс %s (%s, %s, %s) пропал из расписания — не снимаем",
                         sid, s["cinema"], s["movie"], s["time_local"])
            removed.update(gone)
        return added, past, moved

    added, past, _ = enqueue(flat_sessions)
    log.info("В очереди сеансов: %d (уже начались: %d, уже сняты ранее: %d)",
             added, past, len(writer.done))

    # фоновое обновление расписания: новые сеансы добавляются в очередь
    incoming, incoming_lock, stop = [], threading.Lock(), threading.Event()
    wake = threading.Event()  # будит главный цикл, когда пришло новое расписание

    def refresher():
        next_daily = next_daily_ts(daily_at) if daily_at else None
        next_refresh = time.time() + refresh_min * 60 if refresh_min > 0 else None
        while True:
            targets = [t for t in (next_daily, next_refresh) if t]
            if not targets:
                return
            if next_daily:
                log.info("Следующий ежедневный запуск: %s МСК",
                         datetime.fromtimestamp(next_daily, MSK).strftime("%Y-%m-%d %H:%M"))
            if stop.wait(max(0, min(targets) - time.time())):
                return
            now = time.time()
            if next_daily and now >= next_daily:
                log.info("=== Ежедневный запуск: собираю расписание на новый день ===")
                next_daily = next_daily_ts(daily_at)
            if next_refresh and now >= next_refresh - 1:
                next_refresh = now + refresh_min * 60
            try:
                fresh = refresh_fn()
                with incoming_lock:
                    incoming.append(fresh)
                wake.set()
            except Exception as e:
                log.warning("Не удалось обновить расписание: %s", e)

    if refresh_fn and (refresh_min > 0 or daily_at) and not now_mode:
        threading.Thread(target=refresher, daemon=True).start()

    def job(s):
        try:
            row = fetch_occupancy(s)
            writer.write(row)
            log.info("%-12s | %-28s | %-35s %s | куплено %s / свободно %s (%s%%)",
                     s["city"][:12], s["cinema"][:28], s["movie"][:35], s["time_local"],
                     row["seats_sold"], row["seats_free"], row["occupancy_pct"])
        except Exception as e:
            log.error("Сеанс %s (%s, %s): %s", s["session_id"], s["cinema"], s["movie"], e)

    with ThreadPoolExecutor(workers) as ex:
        while queue or forever:
            if incoming:
                with incoming_lock:
                    batches, incoming[:] = list(incoming), []
                for items, coverage in batches:
                    n, _, mv = enqueue(items, coverage)
                    log.info("Расписание обновлено: новых сеансов %d, перенесено %d, отменено всего %d",
                             n, mv, len(removed))
            if not queue:
                wake.wait(30)
                wake.clear()
                continue
            run_at, _, s = queue[0]
            wait = run_at - time.time()
            if wait > 0:
                wake.wait(min(wait, 30))
                wake.clear()
                continue
            heapq.heappop(queue)
            sid = s["session_id"]
            cur = known.get(sid)
            if sid in writer.done or sid in removed or cur is None or cur["epoch"] != s["epoch"]:
                continue  # уже снят, отменён или перенесён (для нового времени есть своя запись)
            ex.submit(job, s)
    stop.set()
    log.info("Все замеры выполнены. Результаты: %s", writer.target)


# ---------------------------------------------------------------- main

def main():
    ap = argparse.ArgumentParser(description="Парсер сеансов и заполненности залов Киномакс")
    ap.add_argument("--city", default=os.environ.get("CITY") or None,
                    help="фильтр по городу (часть названия или id), напр. 'Москва'")
    ap.add_argument("--date", help="дата YYYY-MM-DD (по умолчанию — сегодня по времени кинотеатра)")
    ap.add_argument("--lead", type=float, default=float(os.environ.get("LEAD_MINUTES", 5)),
                    help="за сколько минут до начала снимать зал (5)")
    ap.add_argument("--schedule-only", action="store_true", help="только расписание, без замеров залов")
    ap.add_argument("--now", action="store_true", help="снять залы всех будущих сеансов прямо сейчас")
    ap.add_argument("--refresh", type=float, default=float(os.environ.get("REFRESH_MINUTES", 60)),
                    help="как часто (мин) перечитывать расписание в поиске новых сеансов, 0 — не перечитывать")
    ap.add_argument("--daemon", action="store_true",
                    help="работать постоянно: сразу собрать сегодняшний день, далее ежедневно в --daily-at")
    ap.add_argument("--daily-at", default=os.environ.get("DAILY_AT", "03:00"),
                    help="время ежедневного запуска по Москве для --daemon (03:00)")
    ap.add_argument("--quiet", action="store_true", help="не печатать расписание в консоль")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()
    if args.daemon and args.date:
        ap.error("--daemon работает с текущей датой, --date с ним не используется")

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s МСК %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    logging.Formatter.converter = lambda *_: datetime.now(MSK).timetuple()  # время в логе — МСК

    db_url = os.environ.get("DATABASE_URL")
    use_db = bool(db_url) and not args.now
    tag = args.date or datetime.now(MSK).strftime("%Y-%m-%d")
    if use_db:
        from db import DbStorage
        storage = DbStorage(db_url)
    else:
        os.makedirs(OUT_DIR, exist_ok=True)
        # --now пишет в отдельный файл, чтобы не мешать «настоящим» замерам за 5 минут
        storage = ResultWriter(tag + ("_now" if args.now else ""))

    def collect():
        schedule, coverage = build_schedule(args.date, args.city)
        flat = list(flatten(schedule))
        storage.save_schedule(flat, coverage, REMOVAL_MARGIN_SEC)
        if not use_db:
            day = args.date or datetime.now(MSK).strftime("%Y-%m-%d")
            path = os.path.join(OUT_DIR, f"schedule_{day}.json")
            with open(path, "w", encoding="utf-8") as f:
                json.dump(schedule, f, ensure_ascii=False, indent=2)
            log.info("Расписание сохранено: %s", path)
        log.info("Сеансов в расписании: %d", len(flat))
        return schedule, flat, coverage

    schedule, flat, _ = collect()
    if not args.quiet:
        print_summary(schedule)
    if args.schedule_only:
        return

    try:
        monitor(flat, storage, lead_min=args.lead, now_mode=args.now,
                refresh_fn=lambda: collect()[1:], refresh_min=args.refresh,
                daily_at=args.daily_at if args.daemon else None, forever=args.daemon)
    except KeyboardInterrupt:
        log.info("Остановлено. Результаты: %s", storage.target)


if __name__ == "__main__":
    main()
