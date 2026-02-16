from __future__ import annotations

from typing import Any

DEFAULT_REGION_KEY = "cherkasy"

REGIONS: dict[str, dict[str, Any]] = {
    "cherkasy": {
        "name": "Черкаська область",
        "url": "https://alerts.org.ua/cherkaska-oblast/",
        "queues": [1, 2, 3, 4, 5, 6],
        "subqueues": [1, 2],
        "swap_on_off": True,
    },
    "kirovohrad": {
        "name": "Кіровоградська область",
        "url": "https://alerts.org.ua/kirovogradska-oblast/",
        "queues": [1, 2, 3, 4, 5, 6],
        "subqueues": [1, 2],
        "swap_on_off": True,
    },
}


def list_regions() -> list[tuple[str, dict[str, Any]]]:
    return list(REGIONS.items())


def get_region_config(region: str | None) -> dict[str, Any]:
    key = (region or DEFAULT_REGION_KEY).strip().lower()
    return REGIONS.get(key, REGIONS[DEFAULT_REGION_KEY])


def get_region_name(region: str | None) -> str:
    return str(get_region_config(region).get("name", "Область"))


def region_supports_subqueues(region: str | None) -> bool:
    cfg = get_region_config(region)
    return bool(cfg.get("subqueues"))


def get_region_url(region: str | None) -> str:
    cfg = get_region_config(region)
    return str(cfg.get("url", "https://alerts.org.ua/cherkaska-oblast/"))


def region_swaps_on_off(region: str | None) -> bool:
    cfg = get_region_config(region)
    return bool(cfg.get("swap_on_off", False))
