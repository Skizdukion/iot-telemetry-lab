# SQL drills

Load the data: `docker compose exec -T db psql -U iot -d iot < sql/seed.sql`
Then in psql: `SET search_path = drill; SET timezone = 'Asia/Ho_Chi_Minh';`

Tables: `sites(id, name, tz)`, `devices(id, site_id, model, installed_at)`,
`readings(device_id, ts, temperature, humidity)`. Readings arrive about once a minute
per device, over 2026-10-17 and 2026-10-18 (local time).

Write your answers in `sql/answers.sql`, one block per question with a comment header.
Interviewers often ask you to write SQL on a whiteboard, so try each one **without
running it first**, then run it and fix it. Note how many tries it took.

## Warm-up
1. Number of readings per device. Which device has the fewest, and why? (Look at the data.)
2. The latest reading per device: one row per device with `ts`, `temperature`.
   Write it two ways: `DISTINCT ON`, and a window function. Which is faster here? Check with `EXPLAIN ANALYZE`.
3. Average temperature per site per hour, for 2026-10-17 only (local time). Watch out for the time zone.

## Data quality
4. Find the duplicate rows: same `device_id` and `ts` appearing more than once. How many extra rows are there?
5. Delete the duplicates, keeping exactly one row of each. (Hint: `ctid`, or a CTE with `ROW_NUMBER()`.)
   Then add the constraint that stops it happening again. What is the cost of that constraint on a table that gets 10k inserts/sec?
6. Which devices had **no reading in the last 60 minutes of the dataset**? (Pretend "now" is `2026-10-18 23:59+07`.) This is "offline device detection".

## Windows & time-series
7. For `dev-004`, show each reading with the change from the previous reading (`LAG`). Find the biggest jump.
8. 15-minute rolling average temperature for `dev-001` (a window frame `RANGE BETWEEN interval '15 minutes' PRECEDING AND CURRENT ROW`).
9. **Gaps and islands:** for every device, list every period where no data arrived for more than 2 minutes:
   `device_id, gap_start, gap_end, duration`. You should find 4 gaps, plus think about how to report `dev-009`, which never came back.
10. **Islands:** for every device, list continuous "online" periods (`start`, `end`, number of readings). Islands are the inverse of the gaps.
11. Alert episodes: temperature > 80 counts as an alert. Consecutive alert readings from the same device form ONE episode.
    List the episodes: `device_id, started, ended, max_temp`. (Expected: 2 episodes.)
12. Top 3 hottest devices per day (`ROW_NUMBER` / `RANK` partitioned by day). What changes if two devices tie?
13. p50/p95/p99 temperature per device (`percentile_cont`).

## Design & performance (answer in comments, then prove it)
14. Run `EXPLAIN ANALYZE` on Q2 and Q6. Add the index you think helps, then run again. Which index, and why that column order?
15. Compare a B-tree vs a BRIN index on `readings(ts)`: size (`pg_relation_size`) and query time for a 1-hour range. When does BRIN stop being good?
16. The product team wants a "last 24h chart per device, 5-minute buckets". Write the query. Then explain how you'd keep it fast at 10,000 devices × 1 reading/sec.
    (Your answer should mention: partitioning/hypertables, continuous aggregates or rollup tables, retention.)
17. Redesign `readings` for a real system. What is the primary key? Do you store device time, server time, or both? Narrow (`metric, value`) or wide (one column per metric) table? Defend your choice.
