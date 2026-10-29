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
user metric. Advertising statistics are intentionally not implemented until
the project has source data for them.

The output path is based on the package location, not the current working
directory. To run the module from another directory, first change to the
repository root, install the project in the active environment if package
installation is configured, or add the project root to `PYTHONPATH`.
