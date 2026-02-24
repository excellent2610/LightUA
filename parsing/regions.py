from __future__ import annotations

import re
import threading
import time
from dataclasses import dataclass
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup


BASE_URL = "https://alerts.org.ua"
_CACHE_TTL_SECONDS = 60 * 60 * 6
_cache_lock = threading.Lock()
_cache_payload: tuple[float, list["Region"]] | None = None

_REGION_SLUG_RE = re.compile(r"^[a-z0-9-]+$")
_SLUG_BLACKLIST = {
    "api",
    "about",
    "pro-nas",
    "kontakt",
    "contacts",
    "privacy-policy",
    "robots.txt",
    "feed",
    "news",
    "novyny",
    "faq",
    "map",
    "maps",
    "donate",
    "support",
    "login",
    "signup",
    "wp-admin",
    "wp-content",
}

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}


@dataclass(frozen=True, slots=True)
class Region:
    slug: str
    name: str


def fetch_regions(timeout_seconds: int = 15, force_refresh: bool = False) -> list[Region]:
    global _cache_payload

    with _cache_lock:
        if not force_refresh and _cache_payload is not None:
            expires_at, cached_regions = _cache_payload
            if time.time() < expires_at:
                return list(cached_regions)

    regions = _regions_from_homepage(timeout_seconds=timeout_seconds)
    if not regions:
        regions = _regions_from_sitemaps(timeout_seconds=timeout_seconds)
    if not regions:
        raise RuntimeError("Не вдалося отримати список областей із alerts.org.ua")

    ordered = sorted(regions.values(), key=lambda r: r.name.lower())
    with _cache_lock:
        _cache_payload = (time.time() + _CACHE_TTL_SECONDS, ordered)

    return list(ordered)


def _regions_from_homepage(timeout_seconds: int) -> dict[str, Region]:
    html = _http_get_text(BASE_URL, timeout_seconds)
    if not html:
        return {}

    soup = BeautifulSoup(html, "html.parser")
    regions: dict[str, Region] = {}

    for anchor in soup.find_all("a", href=True):
        href = str(anchor.get("href", "")).strip()
        text = anchor.get_text("\n", strip=True)
        slug = _extract_first_segment(href)
        if slug is None:
            continue
        if not _is_region_candidate(slug, text):
            continue

        name = _normalize_region_name(text, slug)
        existing = regions.get(slug)
        if existing is None or _is_name_better(name, existing.name):
            regions[slug] = Region(slug=slug, name=name)

    return regions


def _regions_from_sitemaps(timeout_seconds: int) -> dict[str, Region]:
    sitemap_urls = (
        f"{BASE_URL}/sitemap.xml",
        f"{BASE_URL}/sitemap_index.xml",
        f"{BASE_URL}/post-sitemap.xml",
        f"{BASE_URL}/page-sitemap.xml",
    )

    regions: dict[str, Region] = {}
    for sitemap_url in sitemap_urls:
        xml = _http_get_text(sitemap_url, timeout_seconds)
        if not xml:
            continue
        soup = BeautifulSoup(xml, "xml")
        for loc_tag in soup.find_all("loc"):
            url_text = loc_tag.get_text(strip=True)
            slug = _extract_first_segment(url_text)
            if slug is None:
                continue
            if not _is_region_candidate(slug, ""):
                continue
            regions.setdefault(slug, Region(slug=slug, name=_slug_to_title(slug)))

    return regions


def _http_get_text(url: str, timeout_seconds: int) -> str:
    try:
        response = requests.get(url, headers=_HEADERS, timeout=timeout_seconds)
        response.raise_for_status()
        return response.text
    except requests.RequestException:
        return ""


def _extract_first_segment(href: str) -> str | None:
    normalized = urljoin(BASE_URL, href)
    parsed = urlparse(normalized)
    if parsed.netloc and "alerts.org.ua" not in parsed.netloc:
        return None

    segments = [segment.strip().lower() for segment in parsed.path.split("/") if segment.strip()]
    if not segments:
        return None

    first = segments[0]
    if not _REGION_SLUG_RE.match(first):
        return None
    return first


def _is_region_candidate(slug: str, text: str) -> bool:
    if slug in _SLUG_BLACKLIST:
        return False
    if slug.startswith("wp-"):
        return False

    text_lower = text.lower()
    if slug.endswith("-oblast"):
        return True
    if "область" in text_lower or "обл." in text_lower:
        return True
    if "місто київ" in text_lower or "м. київ" in text_lower:
        return True
    return False


def _normalize_region_name(text: str, slug: str) -> str:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    candidate = ""

    if lines:
        explicit_region = [
            line
            for line in lines
            if "област" in line.lower() or "київ" in line.lower() or "севастополь" in line.lower()
        ]
        if explicit_region:
            candidate = explicit_region[0]
        else:
            candidate = lines[0]

    candidate = _cleanup_region_label(candidate)
    if not candidate:
        return _slug_to_title(slug)
    return candidate


def _slug_to_title(slug: str) -> str:
    return " ".join(part.capitalize() for part in slug.split("-"))


def _is_name_better(candidate: str, current: str) -> bool:
    # Prefer names that look like explicit region labels over raw slug titles.
    candidate_score = int("область" in candidate.lower())
    current_score = int("область" in current.lower())
    return candidate_score > current_score


def _cleanup_region_label(value: str) -> str:
    if not value:
        return ""

    # Remove emoji-like symbols and metric tails from region labels.
    cleaned = value.replace("\u00A0", " ")
    cleaned = re.sub(r"[^\w\s\u0400-\u04FF\-']", " ", cleaned, flags=re.UNICODE)
    cleaned = re.sub(r"\b\d+\s*(?:г|год|год\.)\b", " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" -|")

    if not cleaned:
        return ""

    # Drop adjacent duplicates: "Черкаська Черкаська" -> "Черкаська".
    parts = cleaned.split(" ")
    deduped: list[str] = []
    for part in parts:
        if deduped and deduped[-1].lower() == part.lower():
            continue
        deduped.append(part)
    return " ".join(deduped).strip()
