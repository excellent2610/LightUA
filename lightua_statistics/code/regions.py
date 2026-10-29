from __future__ import annotations

import asyncio
import csv
import logging
import os
import tempfile
from pathlib import Path

import asyncpg
from dotenv import load_dotenv

from lightua_statistics.code.users import UserStatistics, load_user_statistics

logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_PATH = PROJECT_ROOT / "lightua_statistics" / "tables" / "regions.csv"


async def fetch_user_statistics() -> UserStatistics:
    load_dotenv(PROJECT_ROOT / ".env")
    database_url = os.getenv("DATABASE_URL", "").strip()
    if not database_url:
        raise RuntimeError("DATABASE_URL is required to generate statistics")

    pool = await asyncpg.create_pool(database_url, min_size=1, max_size=2)
    try:
        async with pool.acquire() as connection:
            async with connection.transaction(readonly=True):
                return await load_user_statistics(connection)
    finally:
        await pool.close()


def write_regions_csv(
    statistics: UserStatistics,
    output_path: str | Path = DEFAULT_OUTPUT_PATH,
) -> None:
    destination = Path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None

    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="",
            prefix=f".{destination.name}.",
            suffix=".tmp",
            dir=destination.parent,
            delete=False,
        ) as output_file:
            temporary_path = Path(output_file.name)
            writer = csv.writer(output_file)
            writer.writerow(("Область", "Користувачів"))
            writer.writerows(statistics.users_by_region)

        os.replace(temporary_path, destination)
        temporary_path = None
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


async def generate_regions_csv(
    output_path: str | Path | None = None,
) -> UserStatistics:
    if output_path is None:
        output_path = DEFAULT_OUTPUT_PATH
    statistics = await fetch_user_statistics()
    write_regions_csv(statistics, output_path)
    return statistics


async def _main() -> None:
    statistics = await generate_regions_csv()
    logger.info(
        "Generated %s: total_users=%d active_users_24h=%d",
        DEFAULT_OUTPUT_PATH,
        statistics.total_users,
        statistics.active_users_24h,
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    try:
        asyncio.run(_main())
    except Exception as error:
        if str(error) == "DATABASE_URL is required to generate statistics":
            logger.error("%s", error)
        else:
            logger.error(
                "Could not generate region statistics (%s)",
                type(error).__name__,
            )
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
