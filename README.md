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
- Збереження стану й статистики в PostgreSQL
- Рекламні кампанії з індивідуальними лімітами показів і статистикою

## Актуальна структура проєкту

```text
LightUA/
├── bot/
│   ├── __init__.py
│   ├── main.py
│   ├── config.py
│   ├── dispatcher.py
│   ├── scheduler.py
│   └── shadow_scheduler.py
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
│   ├── alerts_energy.py
│   ├── kolysvitlo.py
│   ├── alerts_org.py
│   ├── base.py
│   ├── models.py
│   ├── normalization.py
│   ├── comparison.py
│   ├── database_comparison.py
│   ├── source_manager.py
│   ├── source_utils.py
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
- `DATABASE_URL` - URL наявної PostgreSQL бази даних
- `PUBLIC_BASE_URL` - публічний HTTPS-origin застосунку для відстеження рекламних кліків, наприклад `https://bot.example.com`; HTTP-адреси відхиляються, а без коректного HTTPS кнопки ведуть напряму на URL рекламодавця й кліки не вимірюються
- `POLL_INTERVAL_SECONDS` - інтервал перевірки стану в секундах (за замовчуванням 5)
- `REQUEST_TIMEOUT_SECONDS` - timeout HTTP-запитів до `alerts.org.ua`
- `SHADOW_REQUEST_TIMEOUT_SECONDS` - незалежний timeout HTTP-запитів shadow-джерел (за замовчуванням 15 секунд)
- `LOG_LEVEL` - рівень логування (`DEBUG`, `INFO`, `WARNING`, `ERROR`)
- `SHADOW_MODE_ENABLED` - вмикає окремий shadow scheduler (за замовчуванням вимкнений)
- `SHADOW_ALERTS_ENERGY_ENABLED`, `SHADOW_KOLYSVITLO_ENABLED`, `SHADOW_ALERTS_ORG_ENABLED` - незалежне вмикання кожного джерела; усі за замовчуванням вимкнені

Приклад:

```env
BOT_TOKEN=YOUR_TELEGRAM_BOT_TOKEN
DATABASE_URL=postgresql://USER:PASSWORD@HOST:5432/DATABASE
PUBLIC_BASE_URL=
POLL_INTERVAL_SECONDS=5
REQUEST_TIMEOUT_SECONDS=15
LOG_LEVEL=INFO
SHADOW_MODE_ENABLED=false
SHADOW_ALERTS_ENERGY_ENABLED=false
SHADOW_KOLYSVITLO_ENABLED=false
SHADOW_ALERTS_ORG_ENABLED=false
SHADOW_REQUEST_TIMEOUT_SECONDS=15
```

## Технічні деталі

- Фреймворк бота: `aiogram` (v3)
- Тип роботи: `polling` (`dispatcher.start_polling`)
- Токен читається з env (`os.getenv`)
- Таблиці рекламних кампаній створюються/оновлюються під час `Database.init()`; окрему міграційну команду запускати не потрібно
- Джерело даних: парсинг `alerts.org.ua` через `requests + BeautifulSoup`
- Shadow mode записує кожну фактичну спробу нових адаптерів лише в `source_schedule_observations`; cache hit не створює історичний запис. Shadow-таблиця та її additive-міграції створюються лише коли shadow mode і принаймні одне джерело ввімкнені; вимкнений shadow mode не запускає задачу й не додає shadow DDL до старту
- `content_hash` спостереження — SHA-256 від канонічно відсортованих нормалізованих інтервалів; cache hit не створює новий запис спостереження
- Джерела опитуються окремо (за замовчуванням кожні 15 хвилин; cache TTL 10 хвилин; мінімальний інтервал між HTTP-запитами 10 секунд). Retry-After для 429/503 зберігається в історії й відновлюється після рестарту; обмеження одного джерела не затримує інші
- Shadow отримує власний HTTP timeout (`SHADOW_REQUEST_TIMEOUT_SECONDS`), окремий від `REQUEST_TIMEOUT_SECONDS` основного парсера. Кожне ввімкнене джерело можна незалежно вимкнути
- Один target на джерело є свідомим поточним обмеженням: конфігурація з кількома target відхиляється, щоб уникнути голодування черг і порівняння різних профілів під одним source ID
- Порівняння можливе лише коли всі джерела повертають однаковий підтверджений operator ID. Автоматичне зіставлення різних slug/доменів оператора не виконується; без перевіреної alias-мапи результат лишається `not_comparable`
- Початкові shadow targets обмежені перевіреним профілем Черкаської області, черга 4.2. Наявність маршруту не вважається доказом підтримки всіх черг або операторів
- Усі записи часу спостереження та Retry-After зберігаються як timezone-aware UTC. Дата графіка та інтервали — локальний календар/час `Europe/Kyiv`; дані, отримані щойно, не вважаються свіжими, якщо `source_updated_at` або дата графіка застарілі
- Історія append-only не має автоматичного retention. Потрібно окремо визначити політику зберігання перед тривалим production-запуском
- Shadow тести використовують локальні fixtures і mock HTTP/DB. Реальна доступність сайтів та сумісність схеми з production PostgreSQL мають бути підтверджені окремим staging/canary запуском

## Реклама

Керування рекламою відбувається в `add/config.json`. Оголошення залишаються окремими JSON-файлами (`add/ad*.json`) і сумісні зі старим форматом `text`, `button_text`, `url`.

```json
{
  "id": "partner-example",
  "name": "Партнерська кампанія",
  "text": "Текст оголошення",
  "button_text": "Дізнатися більше",
  "url": "https://advertiser.example/",
  "enabled": true
}
```

Додайте шлях до файлу в масив `ads` у `add/config.json` і явно встановіть `"enabled": true`. `id` має бути унікальним; для старих JSON без `id` він визначається з назви файлу. Старі записи `ads` без `enabled` залишаються вимкненими для сумісності. Кампанія активна, лише якщо `ads_enabled` і `enabled` у файлі та записі `ads` увімкнені. Щоб вимкнути оголошення, встановіть `"enabled": false` у `config.json` (і, за потреби, у файлі оголошення). Зміни конфігурації кампаній підхоплюються без перезапуску; `PUBLIC_BASE_URL` зчитується під час старту й потребує перезапуску.

Налаштування частоти застосовуються окремо до Telegram ID отримувача:

- `show_every` — показувати рекламу після кожного N-го відповідного виклику; лічильник зберігається в PostgreSQL.
- `max_per_user_per_day` — денний ліміт за календарною добою `Europe/Kyiv`.
- `min_interval_hours` — мінімальний інтервал між показами.

Персональні покази резервуються транзакційно, щоб паралельні процеси не обійшли ліміти. Запис показу створюється тільки після успішної відповіді Telegram на надсилання повідомлення з рекламою; невдале надсилання звільняє резервування. CTR = кліки / покази × 100%, з нульовим захистом і двома знаками після коми. Статистика окремо рахує всі події та унікальних користувачів.

Адміністраторам зі списку `ADMINS` доступні:

- `/ads` — загальна статистика та кампанії за весь час.
- `/ads 1`, `/ads 7`, `/ads 30` — показники за сьогодні або останні 7/30 днів.
- `/adstats <id> [all|1|7|30]` — статистика однієї кампанії.

Кліки проходять через endpoint `/ads/click/{campaign_id}` уже наявного aiohttp-сервера. Посилання містить HMAC-підпис кампанії та ID користувача; endpoint бере адресу переходу тільки з БД, записує клік і повертає redirect. Для цього задайте `PUBLIC_BASE_URL` як публічний HTTPS-origin сервера. HTTP-адреса або некоректне значення відхиляються, відстеження вимикається, а кнопки безпечно ведуть прямо на URL кампанії. Посилання можна переслати або відкрити повторно: кліки атрибутуються ID отримувача з підписаного посилання, а не підтвердженому Telegram-акаунту браузера; кожен успішний запит redirect рахується окремою подією. HTTP-запит не доводить дію людини: сканери безпеки або попереднє завантаження посилань можуть створити додаткові кліки. Адмінська статистика прямо показує, чи ввімкнено redirect-відстеження.

Після успішної відповіді Telegram запис показу виконується у фоновому завданні з повторними спробами та з тим самим UUID резервування; унікальне обмеження PostgreSQL не дозволяє дублювати показ. Саме резервування вже збережене в PostgreSQL перед відправленням. Після доставки воно не видаляється, доки запис показу не буде підтверджено. Якщо процес аварійно завершиться під час тривалої недоступності БД, факт успішної доставки й факт запису в PostgreSQL неможливо зробити атомарними між Telegram і БД; такий випадок потребує моніторингу й окремої перевірки залишених резервувань.

Експорт рекламної статистики (read-only підключення, не змінює схему):

```bash
python -m lightua_statistics.code.advertising
python -m lightua_statistics.code.advertising --days 7
```

Результат записується в `lightua_statistics/tables/advertising.csv`.

## Залежності

Основні пакети з `requirements.txt`:

- `aiogram`
- `aiosqlite`
- `beautifulsoup4`
- `python-dotenv`
- `requests`
- `asyncpg`
