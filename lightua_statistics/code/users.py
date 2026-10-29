from __future__ import annotations

import logging
import re
import unicodedata
from collections import Counter
from dataclasses import dataclass
from typing import Iterable, Mapping

import asyncpg

logger = logging.getLogger(__name__)

UNKNOWN_REGION = "Невідома область"

_OBLASTS = (
    "Вінницька область",
    "Волинська область",
    "Дніпропетровська область",
    "Донецька область",
    "Житомирська область",
    "Закарпатська область",
    "Запорізька область",
    "Івано-Франківська область",
    "Київська область",
    "Кіровоградська область",
    "Луганська область",
    "Львівська область",
    "Миколаївська область",
    "Одеська область",
    "Полтавська область",
    "Рівненська область",
    "Сумська область",
    "Тернопільська область",
    "Харківська область",
    "Херсонська область",
    "Хмельницька область",
    "Черкаська область",
    "Чернігівська область",
    "Чернівецька область",
)

_REGIONS = (*_OBLASTS, "Автономна Республіка Крим", "м. Київ", "м. Севастополь")
_REGION_BY_KEY = {
    re.sub(r"\s+область$", "", name.casefold()): name
    for name in _OBLASTS
}
_REGION_BY_KEY.update(
    {
        "автономна республіка крим": "Автономна Республіка Крим",
        "ар крим": "Автономна Республіка Крим",
        "м. київ": "м. Київ",
        "місто київ": "м. Київ",
        "м. севастополь": "м. Севастополь",
        "місто севастополь": "м. Севастополь",
    }
)

REGION_STATISTICS_QUERY = """
SELECT
    region_name,
    COUNT(*) AS user_count,
    COUNT(*) FILTER (
        WHERE last_active >= NOW() - INTERVAL '1 day'
    ) AS active_count
FROM users
GROUP BY region_name
"""


@dataclass(frozen=True, slots=True)
class UserStatistics:
    total_users: int
    active_users_24h: int
    users_by_region: tuple[tuple[str, int], ...]


def normalize_region_name(value: object) -> str:
    """Map known region labels to one canonical name without guessing typos."""
    if not isinstance(value, str):
        return UNKNOWN_REGION

    normalized = unicodedata.normalize("NFKC", value).replace("\u00a0", " ")
    normalized = re.sub(r"\s+", " ", normalized).strip(" \t\r\n.,;:")
    normalized = normalized.casefold()

    for suffix in (" область", " обл.", " обл"):
        if normalized.endswith(suffix):
            normalized = normalized[: -len(suffix)].rstrip()
            break

    return _REGION_BY_KEY.get(normalized, UNKNOWN_REGION)


def aggregate_user_rows(rows: Iterable[Mapping[str, object]]) -> UserStatistics:
    counts: Counter[str] = Counter()
    total_users = 0
    active_users_24h = 0

    for row_number, row in enumerate(rows, start=1):
        user_count = _read_count(row.get("user_count"), "user_count", row_number)
        if user_count is None:
            continue

        active_count = _read_count(row.get("active_count"), "active_count", row_number)
        if active_count is None:
            active_count = 0
        if active_count > user_count:
            logger.warning("Ignoring invalid active_count in statistics row %d", row_number)
            active_count = 0

        region_name = normalize_region_name(row.get("region_name"))
        counts[region_name] += user_count
        total_users += user_count
        active_users_24h += active_count

    ordered_regions = tuple(
        (name, counts[name])
        for name in _REGIONS
        if counts[name] > 0
    )
    unknown_count = counts[UNKNOWN_REGION]
    if unknown_count:
        ordered_regions += ((UNKNOWN_REGION, unknown_count),)

    return UserStatistics(
        total_users=total_users,
        active_users_24h=active_users_24h,
        users_by_region=ordered_regions,
    )


def _read_count(value: object, field: str, row_number: int) -> int | None:
    try:
        count = int(value)  # asyncpg returns integer counts for COUNT(*)
    except (TypeError, ValueError, OverflowError):
        logger.warning("Ignoring invalid %s in statistics row %d", field, row_number)
        return None

    if count < 0:
        logger.warning("Ignoring negative %s in statistics row %d", field, row_number)
        return None
    return count


async def load_user_statistics(connection: asyncpg.Connection) -> UserStatistics:
    """Read aggregate counts only; no user identifiers are selected or returned."""
    rows = await connection.fetch(REGION_STATISTICS_QUERY)
    return aggregate_user_rows(rows)
