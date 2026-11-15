# Парсер Киномакс → PostgreSQL

Собирает сеансы всех кинотеатров Киномакс по всем городам (каталог `/movies` и Кинотека)
и за 5 минут до начала каждого сеанса снимает схему зала: сколько мест куплено и сколько свободно.

## Запуск

```bash
docker compose up -d --build     # поднять БД и парсер
docker compose logs -f parser    # смотреть лог
docker compose down              # остановить (данные БД сохраняются в volume pgdata)
```

Парсер работает постоянно:
- при старте сразу собирает расписание на сегодня и ставит замеры;
- **каждый день в 03:00 МСК** собирает расписание на новый день
  (в 03:00 МСК во Владивостоке 10:00, так что попадают все города);
- раз в час перечитывает расписание и добавляет новые сеансы;
- каждый сеанс снимается за 5 минут до начала;
- после перезапуска уже снятые сеансы повторно не снимаются.

### Если расписание меняется в течение дня

При каждом обновлении (раз в час и в 03:00) расписание сверяется с БД:

| Что случилось | Что делает парсер |
|---|---|
| новый фильм / сеанс | добавляется в БД и в очередь замеров |
| фильм переименован | название обновляется |
| сеанс перенесён | время обновляется, замер переставляется на новое время |
| сеанс пропал из расписания | `sessions.removed_at` = когда пропал; из `v_session_report` исчезает, замер не делается; если сеанс вернётся — снова станет активным |
| сеанс начинается меньше чем через 10 минут, или уже снят | никогда не считается отменённым (API прячет начавшиеся сеансы) |
| API ответил ошибкой по городу | отмены в этом городе в этот раз не проверяются, чтобы не удалить сеансы по ошибке |

### Тесты

```bash
docker compose --profile test run --rm --build tests
```

Работают на подставном API и отдельной БД `kinomax_test` — рабочие данные не трогают.
Проверяют миграцию старой БД, изменения расписания, ошибки API, разделение дней и очередь замеров.

### Добавление нового поля

1. Добавить колонку в `CREATE TABLE` в `db/schema.sql` **и** строку
   `ALTER TABLE ... ADD COLUMN IF NOT EXISTS ...` рядом с остальными миграциями —
   так она появится и в уже существующей БД.
2. Если поле нужно в отчёте — добавить его во view `v_session_report` (view пересоздаётся при каждом старте).
3. `docker compose up -d --build parser` — схема применится при старте, данные сохранятся.

Настройки в `.env` (образец — `.env.example`): время запуска, за сколько минут снимать, город, пароль БД.

## Подключение к БД

`localhost:5433`, база / пользователь / пароль: `kinomax` / `kinomax` / `kinomax`
(порт 5433, потому что 5432 занят другим проектом).

```bash
docker compose exec db psql -U kinomax -d kinomax
```

## Таблицы

| Таблица | Что хранит |
|---|---|
| `cities` | города |
| `cinemas` | кинотеатры (→ город) |
| `movies` | фильмы, флаги `in_catalog` / `in_kinoteka` |
| `sessions` | сеансы: фильм, кинотеатр, дата, время начала, зал, формат |
| `seat_snapshots` | замеры: куплено (`seats_sold`), свободно, всего, когда сняли |
| `v_session_report` | **готовый отчёт**: всё вместе, одна строка на сеанс |

## Примеры запросов

Сколько билетов куплено на каждый сеанс сегодня:

```sql
SELECT city, cinema, movie, session_date, time_local, starts_msk, hall,
       seats_sold AS куплено, seats_total AS всего, occupancy_pct AS "%"
FROM v_session_report
WHERE session_date = current_date AND seats_sold IS NOT NULL
ORDER BY city, cinema, starts_msk;
```

Продажи по кинотеатрам за день:

```sql
SELECT city, cinema, count(*) AS сеансов, sum(seats_sold) AS куплено, sum(seats_total) AS мест,
       round(100.0 * sum(seats_sold) / NULLIF(sum(seats_total), 0), 1) AS "%"
FROM v_session_report
WHERE session_date = '2026-11-15' AND seats_sold IS NOT NULL
GROUP BY city, cinema
ORDER BY куплено DESC;
```

Продажи по фильмам:

```sql
SELECT movie, in_kinoteka, count(*) AS сеансов, sum(seats_sold) AS куплено
FROM v_session_report
WHERE session_date >= current_date - 7 AND seats_sold IS NOT NULL
GROUP BY movie, in_kinoteka
ORDER BY куплено DESC;
```

Продажи по странам производства (фильм нескольких стран засчитывается каждой):

```sql
SELECT country, count(DISTINCT s.id) AS сеансов, sum(snap.seats_sold) AS куплено
FROM sessions s
JOIN movies m ON m.id = s.movie_id
CROSS JOIN LATERAL unnest(m.countries) AS country
JOIN LATERAL (SELECT seats_sold FROM seat_snapshots
              WHERE session_id = s.id ORDER BY checked_at DESC LIMIT 1) snap ON true
WHERE s.session_date = current_date
GROUP BY country
ORDER BY куплено DESC;
```

Фильмы конкретной страны: `WHERE 'США' = ANY(countries)` в таблице `movies`
или `WHERE country ILIKE '%США%'` в `v_session_report`.

## Без Docker

```bash
python3 kinomax_parser.py --city Москва          # пишет в ./output/ (CSV/JSON)
python3 kinomax_parser.py --city Пермь --now     # тест: снять залы прямо сейчас
DATABASE_URL=postgresql://kinomax:kinomax@localhost:5433/kinomax python3 kinomax_parser.py --daemon
```
