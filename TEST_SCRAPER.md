# Testing the ATTOM Replacement Scraper

## Quick Test (Newburyport)

You can test the scraper right now from the Railway dashboard:

### Option 1: Via Railway Shell

1. Go to Railway Dashboard → LotLogic service
2. Click "Shell" or use `railway shell`
3. Run:

```bash
cd leadcrm
/opt/venv/bin/python manage.py enrich_parcel_scraped --town-id 184 --loc-id 0141000000100000 --dry-run
```

**Town IDs for Testing:**
- Newburyport: 184
- West Newbury: 335
- Boston: 35
- Salem: 274

### Option 2: Trigger Background Job

From Railway shell or logs, run Python:

```python
from data_pipeline.jobs.task_queue import run_registry_task
from data_pipeline.town_registry_map import get_registry_for_town

# Test Newburyport scraping
registry_id = get_registry_for_town(184)  # Essex North
print(f"Registry: {registry_id}")

# Queue async task
task = run_registry_task.delay(
    config={'registry_id': registry_id},
    loc_id='184-0141000000100000',
    force_refresh=True
)

print(f"Task queued: {task.id}")
```

### Option 3: Test Celery Task Directly

```python
from leads.tasks import refresh_scraped_documents

# This will find stale parcels and queue scraping tasks
result = refresh_scraped_documents.delay()
print(f"Refresh task queued: {result.id}")
```

## What to Look For

### Success Indicators:

1. **In Logs:**
   ```
   📍 Loading parcels for town 184 (Newburyport)
   🔍 Searching registry for parcel...
   ✓ Found X records
   📄 Document downloaded: /path/to/doc.pdf
   Saved document to S3: scraped_documents/...
   ```

2. **In Database:**
   - Check `AttomData` table for town_id=184
   - Look for `raw_response` field with `scrape_sources` array
   - Each source should have `document_path` field

3. **In S3:**
   - Bucket: `lotlogic` (or your configured bucket)
   - Path: `scraped_documents/essex_north/doc_*.pdf`
   - Files should be accessible via presigned URL

### Failure Indicators:

1. **Registry Not Found:**
   ```
   ❌ No registry mapping found for town 184
   ```
   → Check `data_pipeline/town_registry_map.py`

2. **404 Error:**
   ```
   requests.exceptions.HTTPError: 404
   ```
   → Registry URL may have changed, need to update config

3. **Import Error:**
   ```
   ModuleNotFoundError: No module named 'data_pipeline'
   ```
   → Django app not finding the pipeline module

## Manual Test Without Railway

If you want to test locally first:

```bash
cd /Volumes/OWC\ Envoy\ Ultra/Websites/lead_CRM_clean/leadcrm

# Set database URL
export DATABASE_URL="postgresql://postgres:plssiHDQpNWeJolsZjUNxuztalopicsI@yamabiko.proxy.rlwy.net:48400/railway"

# Run test
../.venv/bin/python manage.py enrich_parcel_scraped \
  --town-id 184 \
  --loc-id 0141000000100000 \
  --dry-run
```

## Checking Results

### View Scraped Data in Django Admin

1. Go to: https://lotlogic-production.up.railway.app/admin/
2. Navigate to: Leads → Attom Data
3. Filter by town_id = 184
4. Click on a record
5. Look at `raw_response` field → `scrape_sources` array
6. Each entry should have:
   - `instrument_type`: "MORTGAGE" or "LIS PENDENS"
   - `document_date`: Recording date
   - `document_path`: S3 key or local path
   - `book`, `page`: Registry book/page numbers

### View on Parcel Detail Page

Once scraping works, documents will appear on parcel detail pages automatically with download links.

## Troubleshooting

### If Nothing Happens:

1. **Check Celery Workers are Running:**
   ```bash
   # In Railway shell
   ps aux | grep celery
   ```
   Should show: `celery -A leadcrm worker`

2. **Check Redis Connection:**
   ```python
   import redis
   from django.conf import settings
   r = redis.from_url(settings.CELERY_BROKER_URL)
   r.ping()  # Should return True
   ```

3. **Check Registry Mapping:**
   ```python
   from data_pipeline.town_registry_map import get_registry_for_town
   print(get_registry_for_town(184))  # Should return 'essex_north'
   ```

4. **Check S3 Configuration:**
   ```python
   from django.conf import settings
   print(f"USE_S3: {settings.USE_S3}")
   print(f"AWS_STORAGE_BUCKET_NAME: {settings.AWS_STORAGE_BUCKET_NAME}")
   ```

## Next Steps After Successful Test

1. ✅ Confirm documents appear in S3 bucket
2. ✅ Confirm AttomData records created in database
3. ✅ Test download URLs work (presigned S3 URLs)
4. ✅ View documents on parcel detail page
5. ✅ Let weekly automation run Sunday at 3 AM
6. ✅ Compare scraped data quality vs ATTOM API

## Expected Performance

- **Single parcel scrape**: 5-15 seconds
- **Document download**: 2-5 seconds
- **PDF parsing**: 1-3 seconds
- **Total per parcel**: ~10-25 seconds

- **Weekly batch** (1000 parcels): 3-7 hours
- **Rate limit**: 0.3-0.4 requests/sec per registry
- **Celery workers**: 4 concurrent tasks

Good luck! 🚀
