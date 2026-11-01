from __future__ import annotations

import argparse
import asyncio
import csv
import logging
import os
import tempfile
from collections.abc import Mapping
from pathlib import Path

import asyncpg
from dotenv import load_dotenv

from db.database import (
    AD_CAMPAIGN_STATISTICS_QUERY,
    AD_OVERALL_STATISTICS_QUERY,
)
from utils.ads import click_tracking_configured

logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_PATH = PROJECT_ROOT / "lightua_statistics" / "tables" / "advertising.csv"
CSV_FIELDS = (
    "campaign_id",
    "name",
    "status",
    "click_tracking",
    "impressions",
    "unique_users",
    "clicks",
    "unique_clicks",
    "ctr_percent",
    "first_impression",
    "last_impression",
)


async def fetch_advertising_statistics(
    days: int | None = None,
) -> tuple[dict[str, object], list[dict[str, object]]]:
    """Fetch aggregate campaign data in a read-only transaction."""
    if days is not None and days <= 0:
        raise ValueError("days must be positive")
    load_dotenv(PROJECT_ROOT / ".env")
    database_url = os.getenv("DATABASE_URL", "").strip()
    if not database_url:
        raise RuntimeError("DATABASE_URL is required to generate advertising statistics")

    pool = await asyncpg.create_pool(database_url, min_size=1, max_size=2)
    try:
        async with pool.acquire() as connection:
            async with connection.transaction(readonly=True):
                overall_row = await connection.fetchrow(AD_OVERALL_STATISTICS_QUERY, days)
                campaign_rows = await connection.fetch(
                    AD_CAMPAIGN_STATISTICS_QUERY,
                    days,
                    None,
                )
                tracking_state = (
                    "redirect_requests" if click_tracking_configured() else "disabled"
                )
                overall = _serialize_row(
                    overall_row,
                    campaign_id="ALL",
                    name="Усі кампанії",
                    click_tracking=tracking_state,
                )
                campaigns = [
                    _serialize_row(row, click_tracking=tracking_state)
                    for row in campaign_rows
                ]
                return overall, campaigns
    finally:
        await pool.close()


def _serialize_row(
    row: Mapping[str, object] | None,
    *,
    campaign_id: str | None = None,
    name: str | None = None,
    click_tracking: str = "disabled",
) -> dict[str, object]:
    values = dict(row) if row is not None else {}
    impressions = int(values.get("impressions", 0) or 0)
    clicks = int(values.get("clicks", 0) or 0)
    enabled = values.get("enabled")
    return {
        "campaign_id": campaign_id if campaign_id is not None else values.get("campaign_id", ""),
        "name": name if name is not None else values.get("name", ""),
        "status": "enabled" if enabled is True else "disabled" if enabled is False else "",
        "click_tracking": click_tracking,
        "impressions": impressions,
        "unique_users": int(values.get("unique_users", 0) or 0),
        "clicks": clicks,
        "unique_clicks": int(values.get("unique_clicks", 0) or 0),
        "ctr_percent": f"{clicks / impressions * 100:.2f}" if impressions else "0.00",
        "first_impression": _iso_timestamp(values.get("first_impression")),
        "last_impression": _iso_timestamp(values.get("last_impression")),
    }


def _iso_timestamp(value: object) -> str:
    if value is None:
        return ""
    isoformat = getattr(value, "isoformat", None)
    return isoformat() if callable(isoformat) else str(value)


def write_advertising_csv(
    campaigns: list[dict[str, object]],
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
            writer = csv.DictWriter(output_file, fieldnames=CSV_FIELDS, lineterminator="\n")
            writer.writeheader()
            writer.writerows(campaigns)
        os.replace(temporary_path, destination)
        temporary_path = None
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


async def generate_advertising_csv(
    output_path: str | Path | None = None,
    *,
    days: int | None = None,
) -> tuple[dict[str, object], list[dict[str, object]]]:
    if output_path is None:
        output_path = DEFAULT_OUTPUT_PATH
    overall, campaigns = await fetch_advertising_statistics(days=days)
    write_advertising_csv([overall, *campaigns], output_path)
    return overall, campaigns


async def _main(days: int | None) -> None:
    overall, campaigns = await generate_advertising_csv(days=days)
    logger.info(
        "Generated %s: period=%s impressions=%s clicks=%s campaigns=%d",
        DEFAULT_OUTPUT_PATH,
        f"last_{days}_days" if days else "all_time",
        overall["impressions"],
        overall["clicks"],
        len(campaigns),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Export LightUA advertising statistics")
    parser.add_argument(
        "--days",
        type=int,
        choices=(1, 7, 30),
        help="Limit results to today (1) or the last 7/30 days; default is all-time",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    try:
        asyncio.run(_main(args.days))
    except Exception as error:
        if str(error) == "DATABASE_URL is required to generate advertising statistics":
            logger.error("%s", error)
        else:
            logger.error(
                "Could not generate advertising statistics (%s)",
                type(error).__name__,
            )
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
