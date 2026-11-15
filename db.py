"""Хранилище в PostgreSQL: расписание (города, кинотеатры, фильмы, сеансы) и замеры мест."""

import logging
import os
import threading
import time
from datetime import datetime, timezone

import psycopg

log = logging.getLogger("kinomax.db")
SCHEMA_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "db", "schema.sql")


class DbStorage:
    def __init__(self, url, connect_timeout=120):
        self.url = url
        self.target = "PostgreSQL"
        self.lock = threading.Lock()
        self.conn = None
        self._connect(connect_timeout)
        with self.lock, self.conn.transaction():
            with open(SCHEMA_PATH, encoding="utf-8") as f:
                self.conn.execute(f.read())
        # сеансы, которые уже сняты (чтобы после перезапуска не снимать повторно)
        self.done = {r[0] for r in self.conn.execute(
            "SELECT DISTINCT session_id FROM seat_snapshots WHERE checked_at > now() - interval '3 days'")}
        log.info("БД подключена, схема применена (замеров за 3 дня: %d)", len(self.done))

    def close(self):
        if self.conn is not None:
            self.conn.close()

    def _connect(self, timeout):
        deadline = time.time() + timeout
        while True:
            try:
                self.conn = psycopg.connect(self.url, autocommit=True)
                return
            except psycopg.OperationalError as e:
                if time.time() > deadline:
                    raise
                log.info("Жду БД… (%s)", str(e).strip().splitlines()[0])
                time.sleep(3)

    def _run(self, fn):
        """Выполнить fn(conn) в транзакции; при обрыве соединения — переподключиться и повторить."""
        with self.lock:
            for attempt in (1, 2):
                try:
                    with self.conn.transaction():
                        return fn(self.conn)
                except psycopg.OperationalError:
                    if attempt == 2:
                        raise
                    log.warning("Соединение с БД потеряно, переподключаюсь")
                    self._connect(60)

    def save_schedule(self, flat, coverage=(), margin_sec=600):
        """Upsert расписания. coverage — (cinema_id, 'YYYY-MM-DD'), для которых расписание
        получено полностью: будущие сеансы этих кинотеатров/дат, которых нет в flat,
        помечаются removed_at (отменены). Возвращает id отменённых сеансов."""
        if not flat and not coverage:
            return []
        cov = sorted(coverage)
        cities = {(s["city_id"], s["city"]) for s in flat}
        cinemas = {(s["cinema_id"], s["city_id"], s["cinema"]) for s in flat}
        movies = {s["movie_id"]: (s["movie_id"], s["movie"], s["kind"], s.get("countries") or None,
                                  s["in_catalog"], s["in_kinoteka"])
                  for s in flat}
        sessions = {s["session_id"]: (
            s["session_id"], s["movie_id"], s["cinema_id"], s["session_date"],
            datetime.fromtimestamp(s["epoch"], tz=timezone.utc), s["time_local"], s["format"], s["price"],
        ) for s in flat}

        def tx(conn):
            cur = conn.cursor()
            cur.executemany("""
                INSERT INTO cities (id, name) VALUES (%s, %s)
                ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name""", sorted(cities))
            cur.executemany("""
                INSERT INTO cinemas (id, city_id, name) VALUES (%s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET city_id = EXCLUDED.city_id, name = EXCLUDED.name""",
                sorted(cinemas))
            cur.executemany("""
                INSERT INTO movies (id, name, kind, countries, in_catalog, in_kinoteka)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name, kind = EXCLUDED.kind,
                    countries = COALESCE(EXCLUDED.countries, movies.countries),
                    in_catalog  = movies.in_catalog  OR EXCLUDED.in_catalog,
                    in_kinoteka = movies.in_kinoteka OR EXCLUDED.in_kinoteka""", list(movies.values()))
            cur.executemany("""
                INSERT INTO sessions (id, movie_id, cinema_id, session_date, starts_at, time_local, format, price)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    movie_id = EXCLUDED.movie_id, cinema_id = EXCLUDED.cinema_id,
                    session_date = EXCLUDED.session_date, starts_at = EXCLUDED.starts_at,
                    time_local = EXCLUDED.time_local, price = EXCLUDED.price,
                    format = COALESCE(sessions.format, EXCLUDED.format),
                    last_seen_at = now(), removed_at = NULL""", list(sessions.values()))
            if not cov:
                return []
            cur.execute("""
                UPDATE sessions s SET removed_at = now()
                FROM unnest(%s::int[], %s::date[]) AS cov(cinema_id, d)
                WHERE s.cinema_id = cov.cinema_id AND s.session_date = cov.d
                  AND s.removed_at IS NULL
                  AND s.starts_at > now() + make_interval(secs => %s)
                  AND NOT (s.id = ANY(%s::bigint[]))
                  AND NOT EXISTS (SELECT 1 FROM seat_snapshots ss WHERE ss.session_id = s.id)
                RETURNING s.id""",
                ([c for c, _ in cov], [d for _, d in cov], margin_sec, list(sessions)))
            return [r[0] for r in cur.fetchall()]

        removed = self._run(tx)
        log.info("БД: сохранено городов %d, кинотеатров %d, фильмов %d, сеансов %d, отменено %d",
                 len(cities), len(cinemas), len(movies), len(sessions), len(removed))
        return removed

    def write(self, row):
        def tx(conn):
            conn.execute("""
                INSERT INTO seat_snapshots
                    (session_id, minutes_before, seats_total, seats_sold, seats_free, seats_other)
                VALUES (%s, %s, %s, %s, %s, %s)""",
                (row["session_id"], row["minutes_before_start"], row["seats_total"],
                 row["seats_sold"], row["seats_free"], row["seats_other"]))
            conn.execute("""
                UPDATE sessions SET hall = COALESCE(%s, hall), format = COALESCE(%s, format)
                WHERE id = %s""", (row["hall"], row["format"], row["session_id"]))

        self._run(tx)
        self.done.add(row["session_id"])
