# LightUA Telegram Bot

Telegram-бот на Python для показу графіків відключень світла в Україні з парсингом `alerts.org.ua`.

## Можливості

- Покрокова реєстрація: область -> черга -> підчерга
- Команди:
  - `/start`
  - `/register`
  - `/graph`
  - `/notify_on`
  - `/notify_off`
  - `/quiet`
  - `/status`
- Фоновий scheduler (кожні 60 секунд) з повідомленнями тільки при зміні стану
- Тиха година (mute-інтервал)
- SQLite з таблицями `users` і `last_state`
- Асинхронний код (`aiogram 3`, `aiosqlite`)

## Архітектура

```text
LightUA/
├── bot/
│   ├── main.py
│   ├── config.py
│   ├── dispatcher.py
│   ├── scheduler.py
├── parsing/
│   ├── regions.py
│   ├── alerts_parser.py
├── db/
│   ├── database.py
│   ├── models.py
├── handlers/
│   ├── start.py
│   ├── register.py
│   ├── graph.py
│   ├── notify.py
│   ├── quiet.py
│   ├── status.py
├── utils/
│   ├── time_utils.py
│   ├── formatting.py
├── requirements.txt
├── .env.example
└── README.md
```

## Запуск локально

1. Встановіть Python 3.10+.
2. Встановіть залежності:

```bash
pip install -r requirements.txt
```

3. Створіть `.env` на основі `.env.example` і заповніть `BOT_TOKEN`.
4. Запустіть бот:

```bash
python -m bot.main
```

## Налаштування

Змінні оточення:

- `BOT_TOKEN` - токен Telegram-бота
- `DB_PATH` - шлях до SQLite бази
- `POLL_INTERVAL_SECONDS` - інтервал перевірки стану (мін. 30 сек)
- `REQUEST_TIMEOUT_SECONDS` - таймаут HTTP до `alerts.org.ua`
- `LOG_LEVEL` - рівень логування (`DEBUG`, `INFO`, `WARNING`, `ERROR`)

## Docker (опційно)

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["python", "-m", "bot.main"]
```

## Примітки щодо парсингу

- Використовується лише `alerts.org.ua` (`requests + BeautifulSoup`)
- Області не хардкодяться: збираються динамічно з сайту/мап сайтів
- Обробляються кейси:
  - відсутній графік
  - аварійні відключення
  - тимчасова недоступність сторінки
