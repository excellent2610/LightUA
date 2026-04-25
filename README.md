# LightUA Telegram Bot

Telegram-бот на Python для відстеження стану світла за даними з `alerts.org.ua` та сповіщення користувачів у Telegram.

## Що вміє бот

- Реєстрація користувача: область -> черга -> підчерга
- Команди:
  - `/start`
  - `/graph`
  - `/notify_on`
  - `/notify_off`
  - `/notify_test`
  - `/quiet`
  - `/status`
- Фоновий scheduler із перевіркою стану та повідомленням тільки при зміні
- Тиха година для приглушення сповіщень
- Збереження даних у SQLite

## Актуальна структура проєкту

```text
LightUA/
├── bot/
│   ├── __init__.py
│   ├── main.py
│   ├── config.py
│   ├── dispatcher.py
│   └── scheduler.py
├── handlers/
│   ├── __init__.py
│   ├── start.py
│   ├── register.py
│   ├── graph.py
│   ├── notify.py
│   ├── quiet.py
│   ├── status.py
│   └── admin.py
├── parsing/
│   ├── __init__.py
│   ├── alerts_parser.py
│   └── regions.py
├── db/
│   ├── __init__.py
│   ├── database.py
│   └── models.py
├── utils/
│   ├── __init__.py
│   ├── formatting.py
│   ├── time_utils.py
│   └── ads.py
├── add/
│   ├── config.json
│   ├── ad1.json
│   └── ad2.json
├── .env.example
├── requirements.txt
├── Procfile
└── README.md
```

## Локальний запуск

1. Встановіть Python 3.10+.
2. Встановіть залежності:

```bash
pip install -r requirements.txt
```

3. Створіть `.env` на основі `.env.example` і заповніть `BOT_TOKEN`.
4. Запустіть бота:

```bash
python -m bot.main
```

## Налаштування (`.env`)

- `BOT_TOKEN` - токен Telegram-бота
- `DB_PATH` - шлях до SQLite бази (за замовчуванням `lightua.db`)
- `POLL_INTERVAL_SECONDS` - інтервал перевірки стану (мінімум 30)
- `REQUEST_TIMEOUT_SECONDS` - timeout HTTP-запитів до `alerts.org.ua`
- `LOG_LEVEL` - рівень логування (`DEBUG`, `INFO`, `WARNING`, `ERROR`)

Приклад:

```env
BOT_TOKEN=YOUR_TELEGRAM_BOT_TOKEN
DB_PATH=lightua.db
POLL_INTERVAL_SECONDS=60
REQUEST_TIMEOUT_SECONDS=15
LOG_LEVEL=INFO
```

## Технічні деталі

- Фреймворк бота: `aiogram` (v3)
- Тип роботи: `polling` (`dispatcher.start_polling`)
- Токен читається з env (`os.getenv`)
- Джерело даних: парсинг `alerts.org.ua` через `requests + BeautifulSoup`

## Залежності

Основні пакети з `requirements.txt`:

- `aiogram`
- `aiosqlite`
- `beautifulsoup4`
- `python-dotenv`
- `requests`
- `asyncpg`
