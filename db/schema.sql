-- Схема БД парсера Киномакс. Применяется парсером при каждом старте (идемпотентно).

-- Города (id из API /locations)
CREATE TABLE IF NOT EXISTS cities (
    id      integer PRIMARY KEY,               -- location_id Киномакс
    name    text    NOT NULL                   -- «Москва и МО», «Казань»…
);

-- Кинотеатры (id из API /cinemas-simplified)
CREATE TABLE IF NOT EXISTS cinemas (
    id      integer PRIMARY KEY,               -- cinema_id Киномакс
    city_id integer NOT NULL REFERENCES cities(id),
    name    text    NOT NULL                   -- «Киномакс-Водный Москва»
);

-- Фильмы
CREATE TABLE IF NOT EXISTS movies (
    id          integer PRIMARY KEY,           -- movie_id Киномакс
    name        text    NOT NULL,
    kind        text,                          -- film / art / kinoteka / ownkinoteka
    countries   text[],                        -- страны производства: {Великобритания,США}
    in_catalog  boolean NOT NULL DEFAULT false,  -- есть в /movies
    in_kinoteka boolean NOT NULL DEFAULT false   -- есть в Кинотеке
);

-- Сеансы
CREATE TABLE IF NOT EXISTS sessions (
    id            bigint      PRIMARY KEY,     -- session_id Киномакс
    movie_id      integer     NOT NULL REFERENCES movies(id),
    cinema_id     integer     NOT NULL REFERENCES cinemas(id),
    session_date  date        NOT NULL,        -- день сеанса по местному времени города
    starts_at     timestamptz NOT NULL,        -- точное время начала
    time_local    time        NOT NULL,        -- время на сайте, по часам кинотеатра
    hall          text,                        -- «Зал 4» (заполняется при замере)
    format        text,                        -- 2D / 3D / IMAX / ВИП
    price         text,                        -- «от 780»
    first_seen_at timestamptz NOT NULL DEFAULT now(),
    last_seen_at  timestamptz NOT NULL DEFAULT now(),
    removed_at    timestamptz                  -- пропал из расписания (отменён); NULL = актуален
);
-- миграция для БД, созданных до появления колонки
ALTER TABLE movies   ADD COLUMN IF NOT EXISTS countries  text[];
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS removed_at timestamptz;

CREATE INDEX IF NOT EXISTS sessions_date_idx   ON sessions (session_date);
CREATE INDEX IF NOT EXISTS sessions_cinema_idx ON sessions (cinema_id, starts_at);
CREATE INDEX IF NOT EXISTS sessions_movie_idx  ON sessions (movie_id);

-- Замеры мест (за N минут до начала сеанса)
CREATE TABLE IF NOT EXISTS seat_snapshots (
    id             bigserial   PRIMARY KEY,
    session_id     bigint      NOT NULL REFERENCES sessions(id),
    checked_at     timestamptz NOT NULL DEFAULT now(),
    minutes_before numeric(6,1),               -- за сколько минут до начала сняли
    seats_total    integer     NOT NULL,
    seats_sold     integer     NOT NULL,       -- s = 2, куплено
    seats_free     integer     NOT NULL,       -- s = 1, свободно
    seats_other    integer     NOT NULL DEFAULT 0,  -- прочие статусы
    UNIQUE (session_id, checked_at)
);
CREATE INDEX IF NOT EXISTS seat_snapshots_session_idx ON seat_snapshots (session_id, checked_at DESC);

-- Отчёт: один сеанс = одна строка, с последним замером мест.
DROP VIEW IF EXISTS v_session_report;
CREATE VIEW v_session_report AS
SELECT
    ci.name                                          AS city,
    c.name                                           AS cinema,
    m.name                                           AS movie,
    array_to_string(m.countries, ', ')               AS country,
    m.in_kinoteka,
    s.session_date,
    s.time_local,
    (s.starts_at AT TIME ZONE 'Europe/Moscow')       AS starts_msk,
    s.hall,
    s.format,
    snap.seats_sold,
    snap.seats_total,
    snap.seats_free,
    round(100.0 * snap.seats_sold / NULLIF(snap.seats_sold + snap.seats_free, 0), 1) AS occupancy_pct,
    (snap.checked_at AT TIME ZONE 'Europe/Moscow')   AS checked_msk,
    snap.minutes_before,
    s.id   AS session_id,
    m.id   AS movie_id,
    c.id   AS cinema_id,
    ci.id  AS city_id
FROM sessions s
JOIN movies  m  ON m.id  = s.movie_id
JOIN cinemas c  ON c.id  = s.cinema_id
JOIN cities  ci ON ci.id = c.city_id
LEFT JOIN LATERAL (
    SELECT * FROM seat_snapshots ss
    WHERE ss.session_id = s.id
    ORDER BY ss.checked_at DESC
    LIMIT 1
) snap ON true
WHERE s.removed_at IS NULL;   -- отменённые сеансы в отчёт не попадают (остаются в таблице sessions)
