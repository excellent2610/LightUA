import os
from pathlib import Path

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = os.getenv("DB_PATH", str(BASE_DIR / "data" / "lightua.sqlite3"))

OBLAST_URLS = {
    "cherkaska": "https://alerts.org.ua/cherkaska-oblast/",
    "kirovogradska": "https://alerts.org.ua/kirovogradska-oblast/",
    "kyivska": "https://alerts.org.ua/kyivska-oblast/",
}

OBLAST_TITLES = {
    "cherkaska": "Черкаська область",
    "kirovogradska": "Кіровоградська область",
    "kyivska": "Київська область",
}

CHECK_INTERVAL_SECONDS = 60
NOTIFY_BEFORE_MINUTES = 5
REQUEST_TIMEOUT = 20
ALERTS_CF_CLEARANCE = os.getenv("ALERTS_CF_CLEARANCE", "")
