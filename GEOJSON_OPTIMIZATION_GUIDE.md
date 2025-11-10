# GeoJSON Optimization Guide

## Problem
Map loads were slow because shapefiles were being read and converted to GeoJSON on EVERY request. This caused:
- 2-5 second delays even for "cached" towns
- Cache invalidation when workers restart (every 1000 requests)
- Repeated coordinate transformations (State Plane → WGS84)
- High CPU usage for GIS operations

## Solution
Pre-generate static GeoJSON files that can be served instantly from S3/CDN or static file storage.

---

## Step 1: Generate GeoJSON Files Locally (Testing)

### Test with a single town first (Boston):
```bash
cd leadcrm/leadcrm
python manage.py generate_town_geojson --towns 45 --limit 100
```

This will:
- Generate GeoJSON for Boston (town_id=45)
- Limit to 100 parcels for testing
- Save to `static/geojson/towns/town_45_Boston.geojson`

### Generate a few towns:
```bash
python manage.py generate_town_geojson --towns 45,157,285
```
- 45 = Boston
- 157 = Cambridge
- 285 = Somerville

### Check the output:
```bash
ls -lh static/geojson/towns/
```

You should see `.geojson` files with sizes ranging from a few KB to several MB.

---

## Step 2: Generate All Towns (Production)

### WARNING: This will take time and disk space!

Massachusetts has 351 towns. Generating all GeoJSON files:
- **Time:** ~30-60 minutes (depending on CPU)
- **Disk Space:** ~2-5 GB total

```bash
# Generate all towns
python manage.py generate_town_geojson

# Or with progress and forced regeneration:
python manage.py generate_town_geojson --force
```

---

## Step 3: Upload to S3 (Recommended for Production)

### One-time upload:
```bash
python manage.py generate_town_geojson --upload-s3
```

This will:
- Generate all GeoJSON files
- Upload them to your S3 bucket at `geojson/towns/`
- Set cache headers: `max-age=31536000` (1 year)

### Upload only (if files already generated):
```bash
python manage.py generate_town_geojson --upload-s3 --force
```

---

## Step 4: Serve Static Files

### Option A: Serve from S3 (Recommended)

GeoJSON files will be available at:
```
https://your-bucket.s3.amazonaws.com/geojson/towns/town_45_Boston.geojson
```

Or with CloudFront CDN:
```
https://your-cdn-domain.com/geojson/towns/town_45_Boston.geojson
```

### Option B: Serve from Django Static Files

Add to `.gitignore`:
```
static/geojson/towns/*.geojson
```

Files will be served at:
```
https://your-site.railway.app/static/geojson/towns/town_45_Boston.geojson
```

---

## Step 5: Update Frontend to Use Static Files

### Current (slow):
```javascript
// Frontend makes API call to /api/parcels-in-viewport/
// Backend reads shapefile, converts coordinates, returns GeoJSON
// Takes 2-5 seconds per town
fetch(`/api/parcels-in-viewport/?town_id=${townId}&...`)
```

### Optimized (fast):
```javascript
// Frontend loads pre-generated GeoJSON directly
// Takes <100ms from S3/CDN
fetch(`https://your-cdn.com/geojson/towns/town_${townId}_${townName}.geojson`)
  .then(r => r.json())
  .then(geojson => {
    // Filter parcels client-side by viewport bounds
    const parcelsInView = filterParcelsByBounds(geojson, mapBounds);
    // Render on map
    renderParcels(parcelsInView);
  });
```

**Benefits:**
- ✅ Instant load from CDN (cached in browser)
- ✅ No server processing required
- ✅ Can filter/search client-side using JavaScript
- ✅ Works even if backend is slow/down

---

## Step 6: Maintenance

### When to regenerate:

1. **MassGIS data updates** (rare - maybe once a year):
   ```bash
   python manage.py refresh_massgis  # Download latest shapefiles
   python manage.py generate_town_geojson --force --upload-s3
   ```

2. **Add new filtering fields** (if you need to expose new shapefile attributes):
   ```bash
   python manage.py generate_town_geojson --force --upload-s3
   ```

### Automation (optional):

Add to Railway cron or GitHub Actions:
```yaml
# .github/workflows/regenerate-geojson.yml
name: Regenerate GeoJSON
on:
  schedule:
    - cron: '0 0 1 * *'  # Monthly on 1st
  workflow_dispatch:  # Manual trigger
jobs:
  regenerate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Generate and upload
        run: |
          python manage.py generate_town_geojson --upload-s3
```

---

## Performance Comparison

### Before (Dynamic Generation):
- **First load:** 5-10 seconds (shapefile read + conversion)
- **Cached load:** 2-5 seconds (still converts coordinates)
- **After worker restart:** Back to 5-10 seconds

### After (Static GeoJSON):
- **First load:** 100-500ms (CDN fetch)
- **Cached load:** <50ms (browser cache)
- **After worker restart:** No impact (static files)

**🚀 Expected improvement: 10-100x faster!**

---

## Troubleshooting

### "No shapefile found"
Make sure you've downloaded MassGIS data first:
```bash
python manage.py refresh_massgis
```

### "Out of memory"
Generate in batches:
```bash
# Generate first 50 towns
python manage.py generate_town_geojson --towns 1,2,3,4,5,...,50

# Then next 50, etc.
```

### "S3 upload failed"
Check AWS credentials:
```bash
echo $AWS_ACCESS_KEY_ID
echo $AWS_SECRET_ACCESS_KEY
echo $AWS_STORAGE_BUCKET_NAME
```

### Files too large
Optimize GeoJSON (simplify geometries):
```bash
# Install mapshaper
npm install -g mapshaper

# Simplify geometry (reduces file size by ~50%)
mapshaper input.geojson -simplify 10% -o output.geojson
```

---

## Next Steps

1. **Test locally** with 1-3 towns
2. **Verify file sizes** are reasonable
3. **Update frontend** to fetch static files
4. **Generate all towns** and upload to S3
5. **Monitor performance** improvement
6. **Remove old API endpoint** once static files are working

---

## Questions?

- Check command help: `python manage.py generate_town_geojson --help`
- See generated files: `ls -lh static/geojson/towns/`
- Test a file: `python -m json.tool static/geojson/towns/town_45_Boston.geojson | head -50`
