"""
Тесты парсера на подставном API Киномакс и отдельной тестовой БД.

Запуск (из папки проекта):
    docker compose --profile test run --rm --build tests

Проверяется:
  * миграция: старая БД без новых колонок обновляется при старте, данные не теряются;
  * изменения расписания в течение дня: новый фильм, переименование, перенос сеанса,
    отмена сеанса, возврат сеанса — всё правильно отражается в БД и в v_session_report;
  * при ошибках API сеансы не помечаются отменёнными по ошибке;
  * сеансы разных дней не смешиваются;
  * очередь замеров: перенесённый сеанс снимается в новое время, отменённый — не снимается.
"""

import os
import sys
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse, urlunparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg  # noqa: E402

import kinomax_parser as kp  # noqa: E402
from db import DbStorage  # noqa: E402

MSK = kp.MSK
TEST_URL = os.environ.get("TEST_DATABASE_URL", "")


def msk_today(offset_days=0):
    return (datetime.now(MSK) + timedelta(days=offset_days)).strftime("%Y-%m-%d")


# ---------------------------------------------------------------- подставной API

class FakeKinomax:
    """Минимальная копия API Киномакс с поведением, важным для парсера:
    * Location-Id игнорируется (один город — Москва);
    * /cinemas/{id}/sessions не отдаёт фильмы, которые есть только в Кинотеке;
    * уже начавшиеся сеансы скрыты;
    * selectedDate = запрошенная дата (или сегодня)."""

    def __init__(self):
        self.locations = [{"id": 1, "name": "Москва и МО", "parentId": None}]
        self.cinemas = [
            {"id": 42, "name": "Киномакс-Водный Москва", "locationId": 1},
            {"id": 2, "name": "Киномакс-XL Москва", "locationId": 1},
        ]
        self.movies = {}     # id -> {name, kind, countries, listings}
        self.sessions = {}   # (date, session_id) -> {movie_id, cinema_id, epoch}
        self.fail = set()    # пути, на которых API "падает"
        self.calls = []

    def add_movie(self, mid, name, listings=("movies",), countries="США", kind="film"):
        self.movies[mid] = {"name": name, "kind": kind, "countries": countries, "listings": set(listings)}

    def add_session(self, sid, mid, cid, epoch, date=None):
        self.sessions[(date or msk_today(), sid)] = {"movie_id": mid, "cinema_id": cid, "epoch": int(epoch)}

    def remove_session(self, sid):
        for k in [k for k in self.sessions if k[1] == sid]:
            del self.sessions[k]

    def _sess_json(self, sid, s):
        local = datetime.fromtimestamp(s["epoch"], MSK)
        return {"id": sid, "time": local.strftime("%H:%M"), "epoch": s["epoch"],
                "formatId": 1, "price": "500", "isPassed": False}

    def _day(self, date):
        now = time.time()
        return [(sid, s) for (d, sid), s in self.sessions.items() if d == date and s["epoch"] > now]

    def api_get(self, path, params=None, did=False, location_id=None, retries=4):
        self.calls.append(path)
        if path in self.fail:
            raise RuntimeError(f"HTTP 500 {path} (тестовый сбой)")
        params = params or {}
        date = params.get("date") or msk_today()
        fmt = [{"id": 1, "name": "2D"}]
        parts = path.strip("/").split("/")

        if path == "/locations":
            return self.locations
        if path == "/cinemas-simplified":
            return self.cinemas
        if path == "/movies":
            listing = "kinoteka" if params.get("listing") == "kinoteka" else "movies"
            return [{"id": mid, "name": m["name"], "kind": m["kind"]}
                    for mid, m in self.movies.items() if listing in m["listings"]]
        if parts[0] == "movies" and len(parts) == 2:
            m = self.movies[int(parts[1])]
            return {"id": int(parts[1]), "name": m["name"], "countries": " " + m["countries"]}
        if parts[0] == "movies" and parts[2] == "sessions":
            mid = int(parts[1])
            m = self.movies[mid]
            by_cin = {}
            for sid, s in self._day(date):
                if s["movie_id"] == mid:
                    by_cin.setdefault(s["cinema_id"], []).append(self._sess_json(sid, s))
            return {"movie": {"id": mid, "name": m["name"], "kind": m["kind"]}, "selectedDate": date,
                    "cinemas": [{"id": c, "sessions": ss} for c, ss in by_cin.items()],
                    "sessionFormats": fmt}
        if parts[0] == "cinemas" and parts[2] == "sessions":
            cid = int(parts[1])
            by_movie = {}
            for sid, s in self._day(date):
                m = self.movies[s["movie_id"]]
                if s["cinema_id"] == cid and m["listings"] != {"kinoteka"}:
                    by_movie.setdefault(s["movie_id"], []).append(self._sess_json(sid, s))
            return {"selectedDate": date, "sessionFormats": fmt,
                    "movies": [{"id": mid, "name": self.movies[mid]["name"], "kind": self.movies[mid]["kind"],
                                "sessions": ss} for mid, ss in by_movie.items()]}
        if parts[0] == "sessions" and parts[2] == "hallplan":
            return {"session": {"hallName": "Зал 1", "formatName": "2D"},
                    "hallPlan": {"seats": [{"s": 2}] * 3 + [{"s": 1}] * 7}}
        raise RuntimeError(f"HTTP 404 {path}")


class FakeApiMixin:
    def setUp(self):
        super().setUp()
        self.api = FakeKinomax()
        self._orig_api_get = kp.api_get
        kp.api_get = self.api.api_get
        kp._movie_details_cache.clear()
        kp._city_offsets.clear()

    def tearDown(self):
        kp.api_get = self._orig_api_get
        super().tearDown()

    def collect(self, storage, date=None):
        """То же, что делает парсер: собрать расписание и сохранить в хранилище."""
        schedule, coverage = kp.build_schedule(date, None, workers=2)
        flat = list(kp.flatten(schedule))
        removed = storage.save_schedule(flat, coverage, kp.REMOVAL_MARGIN_SEC)
        return flat, coverage, removed


# ---------------------------------------------------------------- тесты с БД

@unittest.skipUnless(TEST_URL, "нужен TEST_DATABASE_URL")
class DbTestCase(FakeApiMixin, unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        u = urlparse(TEST_URL)
        dbname = u.path.lstrip("/")
        assert dbname.endswith("_test"), "тесты работают только с БД *_test"
        admin = urlunparse(u._replace(path="/postgres"))
        with psycopg.connect(admin, autocommit=True) as c:
            if not c.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,)).fetchone():
                c.execute(f'CREATE DATABASE "{dbname}"')

    def setUp(self):
        super().setUp()
        with psycopg.connect(TEST_URL, autocommit=True) as c:
            c.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")

    def q(self, sql, *args):
        with psycopg.connect(TEST_URL) as c:
            return c.execute(sql, args).fetchall()

    def report(self):
        """session_id -> строка отчёта (как видит пользователь)."""
        rows = self.q("SELECT session_id, movie, country, cinema, session_date::text, starts_msk, "
                      "seats_sold FROM v_session_report")
        return {r[0]: r for r in rows}


class TestMigration(DbTestCase):
    OLD_SCHEMA = """
        CREATE TABLE cities (id integer PRIMARY KEY, name text NOT NULL);
        CREATE TABLE cinemas (id integer PRIMARY KEY, city_id integer NOT NULL REFERENCES cities(id),
                              name text NOT NULL);
        CREATE TABLE movies (id integer PRIMARY KEY, name text NOT NULL, kind text,
                             in_catalog boolean NOT NULL DEFAULT false,
                             in_kinoteka boolean NOT NULL DEFAULT false);
        CREATE TABLE sessions (id bigint PRIMARY KEY, movie_id integer NOT NULL REFERENCES movies(id),
                               cinema_id integer NOT NULL REFERENCES cinemas(id), session_date date NOT NULL,
                               starts_at timestamptz NOT NULL, time_local time NOT NULL, hall text,
                               format text, price text,
                               first_seen_at timestamptz NOT NULL DEFAULT now(),
                               last_seen_at timestamptz NOT NULL DEFAULT now());
        CREATE TABLE seat_snapshots (id bigserial PRIMARY KEY,
                                     session_id bigint NOT NULL REFERENCES sessions(id),
                                     checked_at timestamptz NOT NULL DEFAULT now(), minutes_before numeric(6,1),
                                     seats_total integer NOT NULL, seats_sold integer NOT NULL,
                                     seats_free integer NOT NULL, seats_other integer NOT NULL DEFAULT 0,
                                     UNIQUE (session_id, checked_at));
        CREATE VIEW v_session_report AS
            SELECT s.id AS session_id, m.name AS movie FROM sessions s JOIN movies m ON m.id = s.movie_id;
        INSERT INTO cities VALUES (1, 'Москва и МО');
        INSERT INTO cinemas VALUES (42, 1, 'Киномакс-Водный Москва');
        INSERT INTO movies (id, name, kind, in_catalog) VALUES (100, 'Старый фильм', 'film', true);
        INSERT INTO sessions (id, movie_id, cinema_id, session_date, starts_at, time_local)
            VALUES (5000, 100, 42, current_date, now() - interval '1 hour', '12:00');
        INSERT INTO seat_snapshots (session_id, minutes_before, seats_total, seats_sold, seats_free)
            VALUES (5000, 5, 100, 37, 63);
    """

    def test_old_db_is_migrated_without_data_loss(self):
        with psycopg.connect(TEST_URL, autocommit=True) as c:
            c.execute(self.OLD_SCHEMA)

        storage = DbStorage(TEST_URL)
        self.addCleanup(storage.close)

        cols = {r[0] for r in self.q("SELECT table_name || '.' || column_name FROM information_schema.columns "
                                     "WHERE table_schema = 'public'")}
        self.assertIn("movies.countries", cols)
        self.assertIn("sessions.removed_at", cols)
        self.assertIn("v_session_report.country", cols)
        # старые данные на месте и видны в новом отчёте
        self.assertEqual(self.q("SELECT name FROM movies WHERE id = 100"), [("Старый фильм",)])
        self.assertEqual(self.report()[5000][6], 37)
        self.assertIn(5000, storage.done)  # снятый сеанс не будет сниматься повторно

    def test_schema_apply_is_idempotent_and_pipeline_works_after_migration(self):
        with psycopg.connect(TEST_URL, autocommit=True) as c:
            c.execute(self.OLD_SCHEMA)
        DbStorage(TEST_URL).close()
        storage = DbStorage(TEST_URL)  # повторный старт — без ошибок
        self.addCleanup(storage.close)

        self.api.add_movie(1, "Новый фильм", countries="Великобритания, США")
        self.api.add_session(10, 1, 42, time.time() + 3 * 3600)
        self.collect(storage)
        self.assertEqual(self.report()[10][2], "Великобритания, США")
        self.assertEqual(self.q("SELECT countries FROM movies WHERE id = 1"), [(["Великобритания", "США"],)])


class TestScheduleChanges(DbTestCase):
    def setUp(self):
        super().setUp()
        self.storage = DbStorage(TEST_URL)
        self.addCleanup(self.storage.close)
        now = time.time()
        self.t = {"A1": now + 2 * 3600, "A2": now + 3 * 3600, "K": now + 2 * 3600, "soon": now + 5 * 60}
        self.api.add_movie(1, "Фильм А", countries="Россия")
        self.api.add_movie(2, "Гарри Поттер (Кинотека)", listings=("kinoteka",), kind="ownkinoteka",
                           countries="Великобритания, США")
        self.api.add_session(101, 1, 42, self.t["A1"])
        self.api.add_session(102, 1, 42, self.t["A2"])
        self.api.add_session(103, 2, 2, self.t["K"])      # Кинотека: видна только через /movies/{id}/sessions
        self.api.add_session(104, 1, 2, self.t["soon"])   # начнётся через 5 минут

    def test_initial_day_is_recorded_correctly(self):
        flat, coverage, removed = self.collect(self.storage)
        rep = self.report()
        self.assertEqual(set(rep), {101, 102, 103, 104})
        self.assertEqual(rep[103][1], "Гарри Поттер (Кинотека)")
        self.assertEqual(rep[103][3], "Киномакс-XL Москва")
        self.assertTrue(all(r[4] == msk_today() for r in rep.values()), "дата сеанса = сегодня")
        self.assertEqual(self.q("SELECT in_catalog, in_kinoteka FROM movies WHERE id = 2"), [(False, True)])
        self.assertEqual(coverage, {(42, msk_today()), (2, msk_today())})
        self.assertEqual(removed, [])

    def test_changes_during_the_day(self):
        self.collect(self.storage)

        # в течение дня расписание поменялось
        self.api.movies[1]["name"] = "Фильм А (новое название)"
        new_a2 = self.t["A2"] + 3600
        self.api.add_session(102, 1, 42, new_a2)                     # сеанс перенесли на час
        self.api.remove_session(103)                                # сеанс Кинотеки отменили
        self.api.add_movie(3, "Фильм Б", countries="Франция")        # новый фильм
        self.api.add_session(105, 3, 42, time.time() + 5 * 3600)
        self.api.remove_session(104)                                # «скоро начнётся» пропал (API прячет)

        _, _, removed = self.collect(self.storage)
        rep = self.report()

        self.assertEqual(removed, [103])
        self.assertEqual(set(rep), {101, 102, 104, 105}, "отменённый 103 не в отчёте, новый 105 — в отчёте")
        self.assertEqual(rep[101][1], "Фильм А (новое название)")
        self.assertEqual(rep[105][1:3], ("Фильм Б", "Франция"))
        starts = self.q("SELECT extract(epoch FROM starts_at)::bigint FROM sessions WHERE id = 102")[0][0]
        self.assertEqual(starts, int(new_a2), "время перенесённого сеанса обновилось")
        self.assertEqual(self.q("SELECT removed_at IS NULL FROM sessions WHERE id = 104"), [(True,)],
                         "сеанс, который вот-вот начнётся, не считается отменённым")
        self.assertEqual(self.q("SELECT removed_at IS NOT NULL FROM sessions WHERE id = 103"), [(True,)],
                         "отменённый сеанс остаётся в таблице с пометкой")

        # сеанс вернули в расписание
        self.api.add_session(103, 2, 2, self.t["K"])
        self.collect(self.storage)
        self.assertIn(103, self.report())

    def test_api_errors_do_not_cancel_sessions(self):
        self.collect(self.storage)
        self.api.fail.add("/movies/2/sessions")   # упал запрос сеансов фильма Кинотеки
        flat, coverage, removed = self.collect(self.storage)
        self.assertNotIn(103, {s["session_id"] for s in flat})
        self.assertEqual(coverage, set(), "город с ошибками не попадает в coverage")
        self.assertEqual(removed, [])
        self.assertIn(103, self.report(), "сеанс остался в отчёте")

    def test_measured_session_is_never_cancelled(self):
        self.collect(self.storage)
        self.storage.write({"session_id": 102, "minutes_before_start": 5, "seats_total": 10, "seats_sold": 3,
                            "seats_free": 7, "seats_other": 0, "hall": "Зал 1", "format": "2D"})
        self.api.remove_session(102)
        _, _, removed = self.collect(self.storage)
        self.assertEqual(removed, [])
        self.assertEqual(self.report()[102][6], 3)

    def test_days_do_not_mix(self):
        tomorrow = msk_today(1)
        self.api.add_session(201, 1, 42, time.time() + 26 * 3600, date=tomorrow)
        self.collect(self.storage)                 # сегодня
        self.collect(self.storage, date=tomorrow)  # завтра
        self.collect(self.storage)                 # снова сегодня: завтрашний сеанс не должен «отмениться»
        rep = self.report()
        self.assertEqual(rep[201][4], tomorrow)
        self.assertEqual({sid for sid, r in rep.items() if r[4] == msk_today()}, {101, 102, 103, 104})


# ---------------------------------------------------------------- очередь замеров (без БД)

class FakeWriter:
    target = "memory"

    def __init__(self):
        self.done, self.lock, self.rows = set(), threading.Lock(), []

    def write(self, row):
        self.rows.append(row)
        self.done.add(row["session_id"])

    def save_schedule(self, flat, coverage=(), margin_sec=0):
        return []


class TestMonitorQueue(unittest.TestCase):
    def setUp(self):
        self.calls = []
        self._orig = (kp.fetch_occupancy, kp.REMOVAL_MARGIN_SEC)
        kp.REMOVAL_MARGIN_SEC = 1

        def fake_fetch(s):
            self.calls.append((s["session_id"], time.time()))
            return {"session_id": s["session_id"], "seats_sold": 1, "seats_free": 9, "occupancy_pct": 10.0}
        kp.fetch_occupancy = fake_fetch

    def tearDown(self):
        kp.fetch_occupancy, kp.REMOVAL_MARGIN_SEC = self._orig

    @staticmethod
    def sess(sid, epoch):
        return {"session_id": sid, "epoch": epoch, "cinema_id": 42, "session_date": msk_today(),
                "city": "Москва", "cinema": "Водный", "movie": f"m{sid}", "time_local": "20:00"}

    def test_moved_session_is_measured_at_new_time_and_cancelled_is_skipped(self):
        t0 = time.time()
        a, b, c = self.sess(1, t0 + 4), self.sess(2, t0 + 4), self.sess(3, t0 + 4)
        coverage = {(42, msk_today())}
        a_moved = dict(a, epoch=t0 + 7)
        # через ~1 с приходит новое расписание: 1 перенесён на +7 с, 2 отменён, 3 без изменений
        refresh = lambda: ([a_moved, c], coverage)  # noqa: E731

        kp.monitor([a, b, c], FakeWriter(), lead_min=0, refresh_fn=refresh, refresh_min=1 / 60)

        got = {sid: ts - t0 for sid, ts in self.calls}
        self.assertEqual(sorted(got), [1, 3], "отменённый сеанс 2 не снимался, каждый — ровно один раз")
        self.assertEqual(len(self.calls), 2)
        self.assertGreaterEqual(got[1], 6.9, "перенесённый сеанс снят в новое время")
        self.assertLess(got[3], 5.5, "обычный сеанс снят вовремя")

    def test_same_start_time_does_not_crash_queue(self):
        t0 = time.time()
        sessions = [self.sess(i, t0 + 1) for i in range(5)]
        kp.monitor(sessions, FakeWriter(), lead_min=0, refresh_fn=None)
        self.assertEqual(sorted(sid for sid, _ in self.calls), list(range(5)))

    def test_already_measured_sessions_are_skipped_after_restart(self):
        t0 = time.time()
        w = FakeWriter()
        w.done.add(1)
        kp.monitor([self.sess(1, t0 + 1), self.sess(2, t0 + 1)], w, lead_min=0, refresh_fn=None)
        self.assertEqual([sid for sid, _ in self.calls], [2])


if __name__ == "__main__":
    unittest.main(verbosity=2)
