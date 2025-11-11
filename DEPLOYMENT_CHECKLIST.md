# Production Deployment Checklist

## Pre-Deployment Checks

### 1. Code Quality
- [x] Django `manage.py check --deploy` passes with no issues
- [x] All migrations created and tested locally
- [x] No syntax errors in Python files
- [x] No import errors or circular dependencies

### 2. Database Migrations
- [x] Migration `0028_corporateentity_and_more.py` created
- [ ] Backup production database before migration
- [ ] Test migration on staging/development database
- [ ] Verify migration can be rolled back if needed

### 3. Dependencies
- [x] `requirements.txt` updated with new packages:
  - `beautifulsoup4==4.12.3`
  - `pdfplumber==0.11.0`
  - `pytesseract==0.3.10`
  - `pdf2image==1.17.0`
- [ ] Install dependencies in production virtual environment

### 4. Environment Setup
- [ ] Verify production server has Tesseract OCR installed (optional, for TIFF parsing)
- [ ] Verify production has sufficient disk space for document storage
- [ ] Check that `GISDATA_ROOT` path exists and is writable
- [ ] Verify BeautifulSoup4 is installed in production venv

### 5. New Features Verification

#### LLC Owner Lookup
- [x] `CorporateEntity` model created
- [x] Database storage functions implemented
- [x] MA Secretary of Commonwealth scraper implemented
- [x] Integration into parcel detail view complete
- [x] 180-day caching system implemented

#### Attom Replacement Data Pipeline
- [x] All 20 MA Registry of Deeds configured
- [x] Document download implemented
- [x] Mortgage parser implemented
- [x] Database integration complete
- [x] 90-day caching for registry data
- [x] Attom data already integrated into parcel detail view

### 6. Configuration Files
- [x] `data_pipeline/config/sources.json` - All 20 registries configured
- [ ] Verify production settings for:
  - `ATTOM_CACHE_MAX_AGE_DAYS` (default: 60)
  - Storage paths for downloaded documents
  - Rate limiting settings

## Deployment Steps

### Step 1: Backup
```bash
# Backup production database
pg_dump leadcrm_production > backup_$(date +%Y%m%d_%H%M%S).sql

# Backup media files if any
tar -czf media_backup_$(date +%Y%m%d_%H%M%S).tar.gz /path/to/media
```

### Step 2: Pull Latest Code
```bash
cd /path/to/production/lead_CRM
git pull origin main
```

### Step 3: Activate Virtual Environment
```bash
source venv/bin/activate
```

### Step 4: Install Dependencies
```bash
cd leadcrm
pip install -r requirements.txt
```

### Step 5: Run Migrations
```bash
python manage.py migrate
```

Expected output:
```
Operations to perform:
  Apply all migrations: leads
Running migrations:
  Applying leads.0028_corporateentity_and_more... OK
```

### Step 6: Collect Static Files
```bash
python manage.py collectstatic --noinput
```

### Step 7: Restart Services
```bash
# If using Gunicorn
sudo systemctl restart gunicorn

# If using supervisor
sudo supervisorctl restart leadcrm

# If using systemd
sudo systemctl restart leadcrm.service
```

### Step 8: Verify Deployment
```bash
# Check that the service is running
sudo systemctl status gunicorn
# or
sudo supervisorctl status leadcrm

# Check for any errors in logs
tail -f /var/log/gunicorn/error.log
# or
tail -f /path/to/leadcrm/logs/django.log
```

## Post-Deployment Verification

### 1. Basic Health Checks
- [ ] Website loads without errors
- [ ] Login works
- [ ] Admin panel accessible
- [ ] No 500 errors in logs

### 2. Feature-Specific Tests

#### Test LLC Owner Lookup
1. [ ] Navigate to a parcel detail page with LLC owner
2. [ ] Verify "Actual Owner (LLC)" field appears
3. [ ] Verify business phone is displayed if available
4. [ ] Check that CorporateEntity record was created in database:
   ```bash
   python manage.py shell
   >>> from leads.models import CorporateEntity
   >>> CorporateEntity.objects.count()
   >>> CorporateEntity.objects.latest('last_updated')
   ```

#### Test Attom Replacement Data
1. [ ] Navigate to any parcel detail page
2. [ ] Verify AttomData fields show in "Sale History" section
3. [ ] Check for "ATTOM DATA AVAILABLE" marker
4. [ ] Verify mortgage data displays if available
5. [ ] Check that registry scraper can be run manually:
   ```bash
   python -m data_pipeline.cli registry-run --registry suffolk --owner "Smith" --dry-run
   ```

### 3. Performance Checks
- [ ] Page load times are acceptable
- [ ] No memory leaks (monitor for 24 hours)
- [ ] Database queries are efficient (check slow query log)
- [ ] LLC lookups complete within reasonable time (<5 seconds)

### 4. Error Monitoring
- [ ] Check Sentry/error tracking for new errors
- [ ] Monitor application logs for warnings
- [ ] Verify scraper errors are logged properly

## Rollback Plan

If issues occur, rollback with:

```bash
# Stop the service
sudo systemctl stop gunicorn

# Restore database backup
psql leadcrm_production < backup_YYYYMMDD_HHMMSS.sql

# Revert code
git reset --hard <previous-commit-hash>

# Restart service
sudo systemctl start gunicorn
```

## Optional: Test Data Pipeline

### Test Registry Scraping
```bash
# Test Suffolk registry by owner (dry-run, no DB writes)
python -m data_pipeline.cli registry-run \
  --registry suffolk \
  --owner "Smith" \
  --dry-run

# Test actual scraping with database save
python -m data_pipeline.cli registry-run \
  --registry suffolk \
  --loc-id "2507000_0501234000"
```

### Test LLC Owner Lookup
```bash
# Test corporate lookup (dry-run)
python -m data_pipeline.cli corporate-run \
  --entity-name "ABC REALTY LLC" \
  --dry-run

# Test actual lookup with database save
python -m data_pipeline.cli corporate-run \
  --entity-name "XYZ PROPERTIES LLC"
```

### Verify Caching
```bash
# Run same command twice - second should be instant (cache hit)
python -m data_pipeline.cli corporate-run --entity-name "ABC REALTY LLC"
python -m data_pipeline.cli corporate-run --entity-name "ABC REALTY LLC"
# Should see: "Corporate cache HIT: Data for entity_name=ABC REALTY LLC is X days old (fresh)"
```

## Future Enhancements (Not in This Deployment)

- [ ] Celery task queue for scheduled scraping
- [ ] Vision assessor adapter (175 municipalities)
- [ ] Bulk LLC owner lookup for saved lists
- [ ] Propensity score calculator
- [ ] Monitoring dashboard for scraper health

## Support Contacts

- **Code Issues**: Check GitHub issues
- **Server Issues**: Contact server admin
- **Database Issues**: Contact DBA
- **Pipeline Issues**: Check `data_pipeline/README.md`

## Notes

- **LLC Lookups**: Automated on parcel detail view for any owner containing LLC/Corp/Inc keywords
- **Caching**: LLC data cached for 180 days, registry data for 90 days
- **Rate Limiting**: Scrapers respect 0.3-0.4 RPS to avoid overwhelming public registries
- **Cost Savings**: This deployment eliminates ~$500-2000/month in Attom API costs
