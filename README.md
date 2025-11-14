# Lead CRM

A Django-based CRM system for managing real estate leads with skip tracing, property data integration, and mailer generation capabilities.

## Features

- Lead management and tracking
- Skip tracing integration
- Property data lookup (ATTOM API, MassGIS)
- AI-powered mailer generation
- Stripe payment integration
- User profiles and team management
- AWS S3 storage for media and static files
- PostgreSQL database support

## Technology Stack

- **Backend**: Django 5.2
- **Database**: PostgreSQL (Railway)
- **Storage**: AWS S3
- **Payment Processing**: Stripe
- **AI Integration**: OpenAI API
- **Property Data**: ATTOM API, CourtListener API, MassGIS

## Setup

### Prerequisites

- Python 3.13+
- PostgreSQL database
- AWS S3 bucket
- Required API keys (see Environment Variables)

### Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd lead_CRM
```

2. Create and activate virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Create `.env` file in the `leadcrm/` directory with required environment variables (see below)

5. Run migrations:
```bash
cd leadcrm
python manage.py migrate
```

6. Create superuser:
```bash
python manage.py createsuperuser
```

7. Collect static files:
```bash
python manage.py collectstatic
```

8. Run development server:
```bash
python manage.py runserver
```

## Environment Variables

Create a `.env` file in the `leadcrm/` directory with the following variables:

```env
# Database
DATABASE_URL=postgresql://user:password@host:port/database

# AWS S3
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_STORAGE_BUCKET_NAME=your_bucket_name
AWS_S3_REGION_NAME=us-east-1
AWS_S3_CUSTOM_DOMAIN=your_bucket.s3.amazonaws.com

# API Keys
BATCHDATA_API_KEY=your_key
ATTOM_API_KEY=your_key
COURTLISTENER_API_KEY=your_key
OPENAI_API_KEY=your_key

# Stripe
STRIPE_PUBLISHABLE_KEY=your_key
STRIPE_SECRET_KEY=your_key
STRIPE_WEBHOOK_SECRET=your_secret
STRIPE_PRICE_INDIVIDUAL_STANDARD=price_id
STRIPE_PRICE_TEAM_STANDARD=price_id
STRIPE_PRICE_TEAM_PLUS=price_id

# Mailer Configuration
MAILER_CONTACT_PHONE=555-5555
MAILER_TEXT_KEYWORD=HOME
MAILER_AGENT_NAME=Your Name
MAILER_AI_ENABLED=1
MAILER_OPENAI_MODEL=gpt-4o-mini
```

## Project Structure

```
lead_CRM/
├── leadcrm/               # Django project root
│   ├── leadcrm/          # Main project settings
│   │   ├── settings.py   # Django settings
│   │   ├── urls.py       # URL configuration
│   │   └── storage_backends.py  # S3 storage backends
│   ├── accounts/         # User account management
│   ├── leads/            # Lead management app
│   └── manage.py         # Django management script
├── venv/                 # Virtual environment
├── .gitignore
└── README.md
```

## Apps

### accounts
User authentication, profiles, and subscription management.

### leads
Core lead management functionality including:
- Lead tracking and management
- Skip tracing
- Property data integration
- Mailer generation
- Saved parcel lists

## Celery & Background Jobs

The mortgage/assessor scraping pipeline runs via Celery workers backed by Redis.

### Local
1. Start Redis locally (`brew services start redis` or `docker run redis`).
2. Ensure `REDIS_URL` is set (defaults to `redis://localhost:6379/0` in `.env`).
3. Run the worker from the repo root:
   ```bash
   cd leadcrm
   celery -A leadcrm worker --loglevel=INFO --concurrency=4
   ```
4. In a separate shell, start the Django server as usual.

### Railway (Production)
1. **Provision Redis**: add Railway’s Redis plugin or create a Redis service.
2. **Set `REDIS_URL`**: add the connection string to BOTH the `web` service and the new `worker` service environment variables.
3. **Web service command** (Procfile already handles this):
   ```
   web: cd leadcrm && gunicorn leadcrm.wsgi --bind 0.0.0.0:$PORT --workers 4 --timeout 120
   ```
4. **Worker service command**:
   ```
   worker: cd leadcrm && celery -A leadcrm worker --loglevel=INFO --concurrency=4
   ```
5. Deploy/Restart both services; verify the worker logs show `ready` and that it picks up scraping tasks.

## Hybrid Market Value Precomputation

We precompute parcel-level market values every week during the 2 AM maintenance window to avoid UI latency. The job blends a hedonic ridge regression with recent comparable sales filtered by use code, style, and lot/building size. The results drive parcel equity calculations and the valuation panel on the detail page.

### Running the job manually

```bash
cd leadcrm
python manage.py compute_market_values --lookback-days 365 --target-comps 5
```

- Use `--town-id <id>` (repeatable) to scope to specific municipalities while testing.
- `--limit <n>` caps parcels per town for local dry-runs.
- Include `--dry-run` to exercise the engine without touching the `ParcelMarketValue` table.

Production should execute this command weekly via Celery Beat, cron, or Railway scheduled jobs (e.g., `0 2 * * 1 python manage.py compute_market_values`). The command bulk-upserts into `leads_parcelmarketvalue`, so rerunning it is idempotent.

## MassGIS Dataset Refresh

MassGIS parcel ZIPs drift over time as towns publish new fiscal-year snapshots. To keep the local cache fresh we schedule `python manage.py refresh_massgis --all --stale-days 30` every Friday at 1 AM ET (Railway cron `0 6 * * 5`). The command walks the catalog, re-downloads any dataset older than 30 days or reporting a newer Last-Modified header, and stores it under `gisdata/`. Because the valuation job runs one hour later, it always works with up-to-date parcel data.

You can also refresh on demand:

```bash
cd leadcrm
source ../.venv/bin/activate
python manage.py refresh_massgis --town 1 --stale-days 7
```

Use `--force` when you want to re-download regardless of staleness.

## Contributing

1. Create a new branch for your feature
2. Make your changes
3. Submit a pull request

## License

[Your License Here]
