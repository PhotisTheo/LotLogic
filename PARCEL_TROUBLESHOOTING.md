# Parcel Workflow Troubleshooting Guide

## Quick Diagnosis

### 1. Parcel Search Returns 404 or No Results

**Diagnosis Steps**:
```bash
# Check if MassGIS data is available
ls -lh /gisdata/downloads/  # Should have town ZIP files

# Check MassGISParcelCache table
cd leadcrm && python manage.py shell
>>> from leads.models import MassGISParcelCache
>>> MassGISParcelCache.objects.count()  # Should be > 0

# Check if S3 is configured
>>> from django.conf import settings
>>> settings.USE_S3_FOR_GIS
```

**Root Causes**:
1. **GIS data not downloaded** → Run `python manage.py refresh_massgis --town 35 --force`
2. **S3 not configured** → Set `USE_S3_FOR_GIS=True` + AWS credentials
3. **Database cache expired** → Run `python manage.py cleanup_parcel_cache --dry-run`
4. **Missing town_id** → Verify town_id in query parameters

**Solution**:
```bash
# On Railway
# 1. Set environment variables in Railway dashboard:
#    - USE_S3_FOR_GIS=True
#    - AWS_ACCESS_KEY_ID=xxx
#    - AWS_SECRET_ACCESS_KEY=xxx
#    - AWS_STORAGE_BUCKET_NAME=your-bucket

# 2. Run migrations + refresh shapefiles
cd leadcrm && python manage.py migrate && python manage.py refresh_massgis --all

# 3. Test search
python manage.py shell
>>> from leads.services import search_massgis_parcels, get_massgis_catalog
>>> catalog = get_massgis_catalog()
>>> towns = list(catalog.keys())[:5]  # First 5 towns
>>> for town_id in towns:
...     try:
...         town, results, count, meta = search_massgis_parcels(
...             town_id=town_id, limit=10
...         )
...         print(f"Town {town_id}: {count} results")
...     except Exception as e:
...         print(f"Town {town_id}: {e}")
```

---

### 2. ATTOM Data Not Showing (Mortgages, Foreclosures Blank)

**Diagnosis Steps**:
```bash
cd leadcrm && python manage.py shell
>>> from leads.attom_service import fetch_attom_data_for_address
>>> data = fetch_attom_data_for_address("123 Main St", "Boston, MA 02101")
>>> print(data)  # Check if empty or has data
>>> from django.conf import settings
>>> settings.ATTOM_API_KEY  # Check if set
```

**Root Causes**:
1. **API key missing** → Set `ATTOM_API_KEY` in environment
2. **API quota exceeded** → Wait 24 hours or upgrade ATTOM account
3. **Property not in ATTOM database** → Normal for some properties
4. **Cache expired** → Run `python manage.py cleanup_parcel_cache` (removes 90+ day old entries)

**Solution**:
```bash
# Test ATTOM connectivity
cd leadcrm && python manage.py test_attom

# Check cache age
python manage.py shell
>>> from leads.models import AttomData
>>> from django.utils import timezone
>>> from datetime import timedelta
>>> recent = AttomData.objects.filter(
...     last_updated__gte=timezone.now() - timedelta(days=7)
... ).count()
>>> print(f"Recent ATTOM records (last 7 days): {recent}")

# Force refresh specific parcel
>>> from leads.attom_service import update_attom_data_for_parcel
>>> parcel_list = SavedParcelList.objects.first()
>>> update_attom_data_for_parcel(parcel_list, 35, "123456")
```

---

### 3. Liens/Legal Actions Not Showing

**Diagnosis Steps**:
```bash
cd leadcrm && python manage.py shell
>>> from leads.models import LegalAction, LienSearchAttempt
>>> LegalAction.objects.count()  # Should be > 0 after searches
>>> LienSearchAttempt.objects.count()  # Should be > 0
>>> from leads.background_lien_search import should_search_parcel
>>> from django.contrib.auth.models import User
>>> user = User.objects.first()
>>> should_search_parcel(user, 35, "123456")  # True = will search, False = cached
```

**Root Causes**:
1. **CourtListener API key missing** → Set `COURTLISTENER_API_KEY`
2. **Parcel cached recently** → 90-day cache prevents re-search
3. **Background thread pool not running** → Check Railway logs
4. **CourtListener rate limit hit** → Wait 1 hour

**Solution**:
```bash
# Verify API key
cd leadcrm && python manage.py shell
>>> from django.conf import settings
>>> settings.COURTLISTENER_API_KEY

# Clear search history to force re-search (careful!)
>>> from leads.models import LienSearchAttempt
>>> LienSearchAttempt.objects.all().delete()

# Manually search one parcel
>>> from leads.background_lien_search import search_parcel_background
>>> from leads.services import get_massgis_parcel_detail
>>> user = User.objects.first()
>>> parcel = get_massgis_parcel_detail(35, "123456")
>>> search_parcel_background(user, 35, "123456", {
...     "owner_name": parcel.owner_name,
...     "address": parcel.site_address,
...     "town_name": parcel.town.name,
...     "county": "Suffolk"
... })

# Monitor in logs
# Watch for: "Queued background search for 35/123456"
```

---

### 4. PDF Mailer Generation Fails

**Diagnosis Steps**:
```bash
cd leadcrm && python manage.py shell
>>> from leads.models import GeneratedMailer
>>> recent_mailers = GeneratedMailer.objects.order_by('-created_at')[:5]
>>> for m in recent_mailers:
...     print(f"{m.town_id}/{m.loc_id}: {len(m.html)} chars, AI: {m.ai_generated}")
```

**Root Causes**:
1. **WeasyPrint dependencies missing** → Already in requirements.txt, check installation
2. **OPENAI_API_KEY missing** → Set if AI-generated mailers needed
3. **S3 write permissions** → Check media storage configuration
4. **Template rendering error** → Check error logs in Railway

**Solution**:
```bash
# Verify WeasyPrint
python -c "from weasyprint import HTML, CSS; print('WeasyPrint OK')"

# Test mailer generation
cd leadcrm && python manage.py shell
>>> from leads.views import parcel_search_detail
>>> from django.test import RequestFactory
>>> from django.contrib.auth.models import User
>>> user = User.objects.first()
>>> factory = RequestFactory()
>>> request = factory.get('/')
>>> request.user = user
>>> # Call mailer generation endpoint
>>> from django.http import JsonResponse
>>> # This would need proper context - see views.py for parcel_generate_mailer
```

---

### 5. Static Files Not Loading (CSS/JS 404s)

**Diagnosis Steps**:
```bash
cd leadcrm && python manage.py collectstatic --noinput --verbosity 2

# Check S3 upload
python manage.py shell
>>> from django.conf import settings
>>> settings.STATIC_URL  # Should be S3 URL if configured
>>> print(f"Using S3: {settings.USE_S3}")

# Check file in S3
# AWS CLI: aws s3 ls s3://your-bucket/static/admin/
```

**Root Causes**:
1. **collectstatic not run** → Included in Railway deploy script
2. **S3 credentials missing** → Set AWS_* environment variables
3. **S3 permissions insufficient** → Check IAM policy (needs s3:PutObject)
4. **Django DEBUG=True** → Disables WhiteNoiseMiddleware

**Solution**:
```bash
# Collect static files
cd leadcrm && python manage.py collectstatic --noinput

# Verify STATIC_URL
python manage.py shell
>>> from django.conf import settings
>>> print(settings.STATIC_URL)
# Should be: https://your-bucket.s3.amazonaws.com/static/

# Force re-upload to S3
python manage.py collectstatic --noinput --clear
```

---

### 6. Database Errors on Railway

**Common Errors**:
- `psycopg2.OperationalError: could not translate host name`
  - Solution: Check `DATABASE_URL` is set correctly
  
- `psycopg2.OperationalError: connection timeout`
  - Solution: Railway PostgreSQL may be slow; increase timeout in settings.py
  
- `django.db.utils.ProgrammingError: relation does not exist`
  - Solution: Run `python manage.py migrate`

**Diagnosis**:
```bash
cd leadcrm && python manage.py dbshell  # Should open psql connection
```

---

### 7. Railway Deployment Fails

**Check logs**:
```bash
# Railway CLI: view deployment logs
railway logs

# Common issues:
# 1. "ModuleNotFoundError: No module named X"
#    → Missing dependency in requirements.txt
#
# 2. "DJANGO_SECRET_KEY environment variable is required"
#    → Missing DJANGO_SECRET_KEY in Railway environment
#
# 3. "collectstatic failed"
#    → S3 credentials missing or invalid
#
# 4. "migrate failed"
#    → DATABASE_URL missing or connection failed
```

**Fix**:
1. Check Railway environment variables are set
2. Verify `DATABASE_URL` format: `postgresql://user:pass@host:port/db`
3. Ensure `DJANGO_SECRET_KEY` is 50+ characters
4. Review railway.json deploy script is correct

---

## Environment Variable Checklist

Copy and configure this in Railway dashboard:

```env
# Core (Required)
DJANGO_SECRET_KEY=your-50-char-random-key
DATABASE_URL=postgresql://user:pass@host:port/db
DEBUG=False

# APIs (Required for features)
ATTOM_API_KEY=xxx
COURTLISTENER_API_KEY=xxx

# AWS S3 (Critical for production)
USE_S3_FOR_GIS=True
USE_S3=True
AWS_ACCESS_KEY_ID=xxx
AWS_SECRET_ACCESS_KEY=xxx
AWS_STORAGE_BUCKET_NAME=leadcrm-data
AWS_S3_REGION_NAME=us-east-1
AWS_S3_CUSTOM_DOMAIN=leadcrm-data.s3.amazonaws.com

# GIS Configuration
MASSGIS_LIVE_FETCH_ENABLED=True
MASSGIS_DIRECTORY_TIMEOUT=30
ATTOM_CACHE_MAX_AGE_DAYS=60

# Mailer Configuration (Optional)
MAILER_CONTACT_PHONE=555-5555
MAILER_AGENT_NAME=Your Name
MAILER_AI_ENABLED=1
MAILER_OPENAI_MODEL=gpt-4o-mini
OPENAI_API_KEY=xxx

# Stripe (Optional)
STRIPE_PUBLISHABLE_KEY=pk_xxx
STRIPE_SECRET_KEY=sk_xxx
STRIPE_WEBHOOK_SECRET=whsec_xxx
STRIPE_PRICE_INDIVIDUAL_STANDARD=price_xxx
STRIPE_PRICE_TEAM_STANDARD=price_xxx
STRIPE_PRICE_TEAM_PLUS=price_xxx
```

---

## Performance Optimization Checklist

If parcel searches are slow:

1. **Enable S3 for GIS data** (faster than local disk)
   ```env
   USE_S3_FOR_GIS=True
   ```

2. **Increase database connection pool**
   ```python
   # In settings.py DATABASE config
   'CONN_MAX_AGE': 600  # 10 minutes
   ```

3. **Pre-warm cache** by running before high-traffic times
   ```bash
   python manage.py refresh_massgis --all
   ```

4. **Monitor ATTOM API usage** (slow endpoint)
   ```bash
   python manage.py shell
   >>> from leads.models import AttomData
   >>> AttomData.objects.filter(created_at__gte=...).count()
   ```

5. **Increase Railway RAM** if needed (costs more)
   - Go to Railway > Environment > Environment Variables
   - Check current CPU/memory usage in logs

---

## Getting Help

If issues persist:

1. **Check Railway logs** (most recent first)
   ```bash
   railway logs --limit 100
   ```

2. **Test locally first**
   ```bash
   cd lead_CRM
   python -m venv venv
   source venv/bin/activate
   pip install -r leadcrm/requirements.txt
   cd leadcrm
   python manage.py runserver
   # Navigate to http://localhost:8000
   ```

3. **Check specific views in development**
   ```bash
   python manage.py runserver_plus  # IPython shell in errors
   ```

4. **Review error logs**
   - Check Django error logs
   - Check ATTOM API logs (debug prints)
   - Check MassGIS fetch logs (debug prints)

---

## Useful Command Reference

```bash
cd /Volumes/OWC\ Envoy\ Ultra/Websites/lead_CRM/leadcrm

# Database
python manage.py migrate
python manage.py makemigrations
python manage.py dbshell  # psql

# Cache/GIS
python manage.py cleanup_parcel_cache
python manage.py refresh_massgis --town 35 --force
python manage.py refresh_massgis --all

# Testing
python manage.py test_attom
python manage.py check --deploy
python manage.py shell

# Static files
python manage.py collectstatic --noinput
python manage.py collectstatic --noinput --clear

# Users
python manage.py createsuperuser
python manage.py changepassword <username>

# Background tasks
# (Runs when parcel detail page loaded)
# Check: /admin/leads/liensearchattempt/
```

