from __future__ import annotations

import ast
import os
import re
import runpy
import sys
from pathlib import Path
from typing import Dict

try:
    from core.parser import build_schedule_url
except ModuleNotFoundError:
    # Support direct execution of this file (python bot/config.py).
    project_root = Path(__file__).resolve().parents[1]
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    from core.parser import build_schedule_url

_TOKEN_KEYS = (
    "BOT_TOKEN",
    "TELEGRAM_BOT_TOKEN",
    "TELEGRAM_TOKEN",
    "TOKEN",
    "TG_BOT_TOKEN",
    "API_TOKEN",
    "BOT_API_TOKEN",
    "TOKEN_BOT",
)
_TOKEN_FILES = ("token.txt", "bot_token.txt", "BOT_TOKEN.txt")


def _load_from_env() -> str:
    for key in _TOKEN_KEYS:
        value = os.getenv(key, "").strip()
        if value:
            return value
    return ""


def _parse_kv_lines(text: str) -> Dict[str, str]:
    data: Dict[str, str] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        data[key] = value
    return data


def _read_text_with_fallback(path: Path) -> str:
    for encoding in ("utf-8", "utf-8-sig", "cp1251"):
        try:
            return path.read_text(encoding=encoding)
        except Exception:
            continue
    return ""


def _load_from_dotenv() -> str:
    path = Path(".env")
    if not path.exists():
        return ""
    text = _read_text_with_fallback(path)
    if not text:
        return ""
    data = _parse_kv_lines(text)
    for key in _TOKEN_KEYS:
        value = data.get(key, "").strip()
        if value:
            return value
    for key, value in data.items():
        if "TOKEN" in key.upper() and value.strip():
            return value.strip()
    return ""


def _load_from_token_file() -> str:
    for file_name in _TOKEN_FILES:
        path = Path(file_name)
        if not path.exists():
            continue
        text = _read_text_with_fallback(path).strip()
        if not text:
            continue
        first_line = text.splitlines()[0].strip()
        if first_line:
            return first_line
    return ""


def _load_from_config_py_ast() -> str:
    path = Path("config.py")
    if not path.exists():
        return ""
    text = _read_text_with_fallback(path)
    if not text:
        return ""
    try:
        tree = ast.parse(text)
    except Exception:
        return ""

    exact: Dict[str, str] = {}
    generic: Dict[str, str] = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        if len(node.targets) != 1 or not isinstance(node.targets[0], ast.Name):
            continue
        key = node.targets[0].id
        if not isinstance(node.value, ast.Constant) or not isinstance(node.value.value, str):
            continue
        value = node.value.value.strip()
        if not value:
            continue
        if key in _TOKEN_KEYS:
            exact[key] = value
        elif "TOKEN" in key.upper():
            generic[key] = value

    for key in _TOKEN_KEYS:
        if key in exact:
            return exact[key]
    for key in sorted(generic):
        return generic[key]
    return ""


def _load_from_config_py_text() -> str:
    path = Path("config.py")
    if not path.exists():
        return ""
    text = _read_text_with_fallback(path)
    if not text:
        return ""
    for key in _TOKEN_KEYS:
        pattern = rf"(?mi)^\s*{re.escape(key)}\s*=\s*['\"]([^'\"]+)['\"]\s*$"
        match = re.search(pattern, text)
        if match and match.group(1).strip():
            return match.group(1).strip()
    generic = re.search(
        r"(?mi)^\s*([A-Z0-9_]*TOKEN[A-Z0-9_]*)\s*=\s*['\"]([^'\"]+)['\"]\s*$",
        text,
    )
    if generic and generic.group(2).strip():
        return generic.group(2).strip()
    return ""


def _load_from_config_py_runtime() -> str:
    path = Path("config.py")
    if not path.exists():
        return ""
    try:
        data = runpy.run_path(str(path))
    except Exception:
        return ""
    for key in _TOKEN_KEYS:
        value = data.get(key, "")
        if isinstance(value, str) and value.strip():
            return value.strip()
    for key, value in data.items():
        if "TOKEN" in key.upper() and isinstance(value, str) and value.strip():
            return value.strip()
    return ""


BOT_TOKEN = (
    _load_from_env()
    or _load_from_token_file()
    or _load_from_dotenv()
    or _load_from_config_py_ast()
    or _load_from_config_py_text()
    or _load_from_config_py_runtime()
)

# Region schedule pages (explicit mapping requested by product logic).
CHERKASY_SCHEDULE_URL = os.getenv("CHERKASY_SCHEDULE_URL", "https://alerts.org.ua/cherkaska-oblast/").strip()
KIROVOHRAD_SCHEDULE_URL = os.getenv("KIROVOHRAD_SCHEDULE_URL", "https://alerts.org.ua/kirovogradska-oblast/").strip()


def get_schedule_url_for_region(region: str | None) -> str:
    key = (region or "cherkasy").strip().lower()
    if key == "kirovohrad":
        return KIROVOHRAD_SCHEDULE_URL
    return CHERKASY_SCHEDULE_URL


# Priority: explicit URL > explicit date > today's date (Europe/Kyiv)
_explicit_url = os.getenv("SCHEDULE_URL", "").strip()
_explicit_date = os.getenv("SCHEDULE_DATE", "").strip()
SCHEDULE_URL = _explicit_url or build_schedule_url(_explicit_date or None)
