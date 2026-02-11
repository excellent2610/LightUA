from __future__ import annotations

from collections import defaultdict
import re
from typing import Dict, List, Optional

from bs4 import BeautifulSoup

from core.normalizer import clean_text, normalize_queue
from errors.parsing import ParseError

TIME_HINT_RE = re.compile(r"\d{1,2}[:.]\d{2}\s*(?:-|–|—|до|to)\s*\d{1,2}[:.]\d{2}", re.IGNORECASE)


def _collect_from_rows(rows: List) -> Dict[str, List[str]]:
    collected: Dict[str, List[str]] = defaultdict(list)
    current_queue: Optional[str] = None
    for row in rows:
        cells = row.find_all(["th", "td"])
        if not cells:
            continue
        texts = [clean_text(cell.get_text(" ", strip=True)) for cell in cells]
        if not any(texts):
            continue
        row_queue: Optional[str] = None
        for idx, text in enumerate(texts):
            queue = normalize_queue(text)
            if not queue:
                continue
            row_queue = queue
            current_queue = queue
            chunk = " ".join(part for part in texts[idx:] if part)
            if chunk:
                collected[queue].append(chunk)
        if row_queue is None and current_queue is not None:
            row_text = " ".join(part for part in texts if part)
            if row_text and TIME_HINT_RE.search(row_text):
                collected[current_queue].append(row_text)
    return dict(collected)


def _text_fallback(html: str) -> Dict[str, List[str]]:
    collected: Dict[str, List[str]] = defaultdict(list)
    current_queue: Optional[str] = None
    for raw in html.splitlines():
        line = clean_text(raw)
        if not line:
            continue
        queue = normalize_queue(line)
        if queue:
            current_queue = queue
            collected[queue].append(line)
            continue
        if current_queue and TIME_HINT_RE.search(line):
            collected[current_queue].append(line)
    return dict(collected)


def extract_queue_chunks(html: str) -> Dict[str, List[str]]:
    soup = BeautifulSoup(html, "html.parser")
    aggregate: Dict[str, List[str]] = defaultdict(list)

    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        table_data = _collect_from_rows(rows)
        for queue, chunks in table_data.items():
            aggregate[queue].extend(chunks)
    if aggregate:
        return dict(aggregate)

    rows_data = _collect_from_rows(soup.find_all("tr"))
    if rows_data:
        return rows_data

    text_data = _text_fallback(html)
    if text_data:
        return text_data

    raise ParseError("Failed to find schedule table rows")
