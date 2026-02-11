# LightUA

Python parser for outage schedules in Cherkasy from `alerts.org.ua`.

## Run

```bash
pip install -r requirements.txt
python app.py
```

## Example usage

`app.py` calls `core.parser.get_schedule()` and prints JSON only.

## Telegram bot integration

Built-in Telegram bot is included.

1. Set env var with your token:

```bash
set BOT_TOKEN=your_telegram_bot_token
```

2. Start bot:

```bash
python bot.py
```

Commands:
- `/start`
- `/queue 1.1`
- `/schedule`
- `/schedule 1.1`
- `/all`
