# Railway Deployment - Market Values Compute

## What Changed

✅ **Critical Bug Fixed**: Prevented commercial properties from being compared to residential properties in comp selection
- File: `leadcrm/leads/valuation_engine.py` lines 406-412
- Change: Removed fallback to ALL properties when no same-category matches found

## Deployment Options for Railway

### Option 1: Run Once Tonight (Immediate)

Use Railway CLI to run the command once in production:

```bash
# Make sure you're in the project directory
cd "/Volumes/OWC Envoy Ultra/Websites/lead_CRM_clean"

# Run on the web service (or specify your Django service)
railway run python leadcrm/manage.py compute_market_values \
    --lookback-days 365 \
    --target-comps 5 \
    --batch-size 500
```

Or if you need to specify the service:
```bash
railway run -s web python leadcrm/manage.py compute_market_values --lookback-days 365 --target-comps 5 --batch-size 500
```

**Note**: This may take 1-6 hours depending on the number of towns/properties.

### Option 2: Deploy Cron Service (Recurring - Recommended)

I've added a `cron` service to your Procfile that will run scheduled tasks automatically.

#### Step 1: Deploy the Changes

```bash
cd "/Volumes/OWC Envoy Ultra/Websites/lead_CRM_clean"
git add .
git commit -m "Add cron service for market values computation

- Fix comp selection bug (no more commercial/residential mixing)
- Add run_scheduled_tasks management command
- Add cron service to Procfile for daily automated runs

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude <noreply@anthropic.com>"
git push
```

#### Step 2: Create the Cron Service in Railway

1. Go to your Railway project dashboard
2. Click **"+ New Service"**
3. Select **"Add Service from Existing Repo"**
4. Choose your repository
5. In the service settings:
   - **Name**: `cron` or `scheduled-tasks`
   - **Start Command**: `cd leadcrm && /opt/venv/bin/python manage.py run_scheduled_tasks --run-hour 2`
   - **Environment**: Make sure it has access to the same environment variables as your web service (DATABASE_URL, etc.)

6. Deploy the service

#### Step 3: Configure Run Time (Optional)

The cron service defaults to running at 2 AM UTC. To change this:

In Railway service settings, update the start command:
```bash
# For 3 AM UTC
cd leadcrm && /opt/venv/bin/python manage.py run_scheduled_tasks --run-hour 3

# For midnight UTC
cd leadcrm && /opt/venv/bin/python manage.py run_scheduled_tasks --run-hour 0
```

### Option 3: Manual Trigger via Railway Dashboard

1. Go to Railway dashboard
2. Select your `web` service (or any Django service)
3. Click on **"Run Command"** or open the service shell
4. Run:
```bash
cd leadcrm && python manage.py compute_market_values --lookback-days 365 --target-comps 5 --batch-size 500
```

## Monitoring in Production

### Check Logs

Via Railway Dashboard:
1. Select the service (web/cron)
2. View the **Logs** tab
3. Look for output from the compute command

Via Railway CLI:
```bash
# View cron service logs
railway logs -s cron

# View web service logs
railway logs -s web
```

### Verify Computation Success

Check the database for updated market values:
```bash
railway run python leadcrm/manage.py shell
```

Then in the shell:
```python
from leads.models import ParcelMarketValue
from django.utils import timezone
from datetime import timedelta

# Check how many were updated in the last 24 hours
recent = ParcelMarketValue.objects.filter(
    valued_at__gte=timezone.now() - timedelta(hours=24)
)
print(f"Market values updated in last 24h: {recent.count()}")

# Check a sample
sample = recent.first()
if sample:
    print(f"Sample: Town {sample.town_id}, Loc {sample.loc_id}")
    print(f"Market Value: ${sample.market_value:,.2f}")
    print(f"Comps Count: {sample.comparable_count}")
    print(f"Valued at: {sample.valued_at}")
```

## Environment Variables Needed

Make sure these are set in Railway for all services:

- `DATABASE_URL` - PostgreSQL connection
- `DJANGO_SETTINGS_MODULE` - Should be `leadcrm.settings`
- `SECRET_KEY` - Django secret key
- Any other env vars your app needs (AWS credentials, API keys, etc.)

## Troubleshooting

### Cron Service Keeps Restarting
This is normal - the service runs continuously and checks every 30 minutes if it's time to run.

### Command Times Out
For very large datasets:
1. Increase Railway service timeout
2. Or run for specific towns:
```bash
railway run python leadcrm/manage.py compute_market_values --town-id 39 --town-id 351
```

### Out of Memory
Reduce batch size:
```bash
--batch-size 100  # Instead of 500
```

### Check Running Processes
```bash
railway run ps aux | grep compute_market_values
```

## Cost Considerations

- **Cron Service**: Runs 24/7 but uses minimal resources while sleeping
- **One-off Command**: Only charges for execution time
- **Recommendation**: Use cron service for daily automation, or run weekly via one-off command to save costs

## Recommended Schedule

- **High-volume markets**: Daily at 2 AM
- **Medium-volume**: Every Sunday
- **Low-volume**: Monthly on the 1st

Adjust `--run-hour` or use cron expressions based on your needs.
