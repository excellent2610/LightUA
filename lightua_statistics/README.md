# LightUA project statistics

From the repository root, generate the aggregate regional table with:

```bash
python -m lightua_statistics.code.regions
```

The generator reads `DATABASE_URL` from the process environment or the root
`.env` file and connects to the existing PostgreSQL database in a read-only
transaction. It does not initialize or migrate the schema and does not require
the bot token. Connection, query, and file errors are reported without logging
the database URL and result in a non-zero exit status.

The command writes UTF-8 `lightua_statistics/tables/regions.csv` with canonical
Ukrainian region names and user counts. Unrecognized or missing names are grouped as
`Невідома область`; they are not guessed. CSV output contains no user
identifiers. The command also logs the total and the existing 24-hour active
user metric.

The CSV is not refreshed automatically by the bot; run the command above to
refresh it. For routine updates, schedule it as a separate daily job after user
data changes, rather than running a database-wide aggregation on every
registration or bot startup.

## Advertising statistics

Advertising aggregates are read from the bot's PostgreSQL tables by a separate
read-only transaction. Campaign exports include counts, unique users, clicks,
unique clickers, CTR, and first/last impression timestamps:

```bash
python -m lightua_statistics.code.advertising
python -m lightua_statistics.code.advertising --days 1
python -m lightua_statistics.code.advertising --days 7
python -m lightua_statistics.code.advertising --days 30
```

The `--days 1` period means today in `Europe/Kyiv`; 7 and 30 are rolling
periods. CSV output is written to
`lightua_statistics/tables/advertising.csv`. Click figures are measured only
when `PUBLIC_BASE_URL` points to the existing public aiohttp service. Without
it, the bot uses direct advertiser URLs and does not report unmeasured clicks.
The CSV's `click_tracking` column identifies whether redirect requests were
recorded or tracking was disabled. A recorded GET is not proof of human intent:
security scanners or prefetching may create extra events.
Signed click links bind the event to the intended Telegram ID, but cannot prove
which person opened a forwarded link or distinguish repeat opens from distinct
human intent.

The output path is based on the package location, not the current working
directory. To run the module from another directory, first change to the
repository root, install the project in the active environment if package
installation is configured, or add the project root to `PYTHONPATH`.
