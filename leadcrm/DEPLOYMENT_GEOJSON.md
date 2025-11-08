# GeoJSON System Deployment Guide

## What's Been Implemented

✅ Django management command to generate GeoJSON files
✅ New API endpoint `/api/town-geojson/<town_id>/` that serves pre-generated files
✅ Automatic fallback if GeoJSON isn't generated yet
✅ Optimal caching headers (1-year cache)
✅ .gitignore configured to exclude generated files

---

## How It Works

### Current Flow (Slow):
```
User pans map → Frontend calls /api/parcels-in-viewport/
→ Django reads shapefile from disk
→ Converts State Plane → WGS84 for each parcel
→ Filters to viewport
→ Returns JSON
→ 5-10 seconds per request ❌
```

### New Flow (Fast):
```
User pans map → Frontend calls /api/town-geojson/45/
→ Django serves pre-generated static file
→ Frontend filters to viewport in JavaScript
→ <500ms per request ✅
```

---

## Deployment Steps

### 1. Deploy the Code (Already Done!)

The code has been pushed to GitHub and will auto-deploy to Railway.

**New files:**
- `leads/management/commands/generate_town_geojson.py` - Generation command
- `leads/views.py` - New `town_geojson()` view added
- `leads/urls.py` - New route: `/api/town-geojson/<town_id>/`
- `.gitignore` - Excludes generated .geojson files

### 2. Generate GeoJSON Files in Production

Once deployed to Railway, you need to run the management command to generate the files.

**Option A: Railway CLI (Recommended)**

Install Railway CLI:
```bash
npm install -g @railway/cli
railway login
```

Then generate GeoJSON:
```bash
# Generate top 10 towns (fastest test)
railway run python leadcrm/leadcrm/manage.py generate_town_geojson --towns 45,157,285,19,38,105,251,107,235,275

# Or generate ALL towns (takes 1-2 hours)
railway run python leadcrm/leadcrm/manage.py generate_town_geojson
```

**Option B: Railway Dashboard**

1. Go to Railway dashboard → Your project
2. Click on your service
3. Go to **Settings** → **Deploy**
4. Add a **One-Off Command**:
   ```
   cd leadcrm/leadcrm && python manage.py generate_town_geojson --towns 45,157,285
   ```
5. Run it manually

**Option C: Add to Deployment (Run Once)**

Temporarily add to `railway.json` startCommand (remove after first run):
```json
"startCommand": "cd leadcrm/leadcrm && python manage.py generate_town_geojson --towns 45,157,285 && python manage.py migrate && ..."
```

⚠️ **Important:** Remove this after first successful deploy or it will regenerate on every restart!

### 3. Verify Files Were Generated

Check that files exist:
```bash
railway run ls -lh leadcrm/leadcrm/static/geojson/towns/
```

You should see files like:
```
town_45_Boston.geojson         (5.2 MB)
town_157_Cambridge.geojson     (2.1 MB)
town_285_Somerville.geojson    (1.8 MB)
```

### 4. Test the New API Endpoint

Visit in your browser:
```
https://your-app.railway.app/api/town-geojson/45/
```

You should see:
- **If GeoJSON exists:** Instant response with full GeoJSON data
- **If not generated yet:** 404 error with instructions

Headers should include:
```
Cache-Control: public, max-age=31536000, immutable
X-Served-From: static-geojson
```

---

## Frontend Integration (Next Step)

The API is ready, but the frontend still uses the old `/api/parcels-in-viewport/` endpoint.

### Current Code (in parcel_search.html):
```javascript
const apiUrl = `/api/parcels-in-viewport/?${params}`;
fetch(apiUrl).then(...)
```

### Updated Code (for static GeoJSON):
```javascript
// Check if town is selected
const townId = getTownIdFromFilters(); // You'll need to implement this

if (townId) {
  // Use new fast endpoint
  const geojsonUrl = `/api/town-geojson/${townId}/`;

  fetch(geojsonUrl)
    .then(response => response.json())
    .then(geojson => {
      // geojson.features contains ALL parcels for the town
      // Filter to viewport client-side
      const bounds = map.getBounds();
      const visibleParcels = geojson.features.filter(feature => {
        const coords = feature.geometry.coordinates[0][0];
        const [lng, lat] = coords;
        return lat >= bounds.getSouth() && lat <= bounds.getNorth() &&
               lng >= bounds.getWest() && lng <= bounds.getEast();
      });

      // Apply filters client-side
      const filtered = visibleParcels.filter(feature => {
        const props = feature.properties;
        // Apply price filter
        if (minPrice && props.TOTAL_VAL < minPrice) return false;
        if (maxPrice && props.TOTAL_VAL > maxPrice) return false;
        // Add other filters...
        return true;
      });

      // Render on map
      renderParcels(filtered);
    })
    .catch(error => {
      console.error('GeoJSON load failed, falling back:', error);
      // Fallback to old API
      fetch(`/api/parcels-in-viewport/?${params}`)...
    });
} else {
  // No specific town selected, use old API
  fetch(`/api/parcels-in-viewport/?${params}`)...
}
```

---

## Upload to S3 (Optional - Best Performance)

For maximum speed, upload generated files to S3:

### Generate and Upload:
```bash
railway run python leadcrm/leadcrm/manage.py generate_town_geojson --upload-s3
```

This requires AWS credentials set in Railway environment variables:
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_STORAGE_BUCKET_NAME`
- `AWS_S3_REGION_NAME`

Files will be uploaded to:
```
s3://your-bucket/geojson/towns/town_45_Boston.geojson
```

With 1-year cache headers set automatically.

### Update Frontend to Use S3:
```javascript
const geojsonUrl = `https://your-bucket.s3.amazonaws.com/geojson/towns/town_${townId}_${townName}.geojson`;
```

Or with CloudFront CDN:
```javascript
const geojsonUrl = `https://your-cdn.cloudfront.net/geojson/towns/town_${townId}_${townName}.geojson`;
```

---

## Performance Expectations

### Before (Current):
- **First load:** 5-10 seconds (shapefile processing)
- **Cached load:** 2-5 seconds (still converts coordinates)
- **Memory usage:** High (caching shapefiles)
- **CPU usage:** High (GIS operations)

### After (With Pre-Generated GeoJSON):
- **First load:** 100-500ms (static file fetch)
- **Cached load:** <50ms (browser cache)
- **Memory usage:** Near zero (no shapefile caching)
- **CPU usage:** Near zero (no GIS processing)

**Expected improvement: 10-100x faster!** 🚀

---

## Troubleshooting

### Files not found after generation
Check the path:
```bash
railway run python -c "from django.conf import settings; print(settings.BASE_DIR)"
```

Files should be at: `{BASE_DIR}/static/geojson/towns/`

### API returns 404
1. Verify files exist: `railway run ls leadcrm/leadcrm/static/geojson/towns/`
2. Check logs: `railway logs`
3. Test locally first before deploying

### Frontend still slow
1. Check browser dev tools → Network tab
2. Verify request goes to `/api/town-geojson/XX/` not `/api/parcels-in-viewport/`
3. Check response headers for `X-Served-From: static-geojson`

### Out of disk space
Generated files can be large (2-5 GB for all towns). Options:
1. Generate only top 50 towns: `--towns 45,157,285,...`
2. Upload to S3 and delete local files
3. Increase Railway disk allocation

---

## Maintenance

### When to Regenerate:

1. **MassGIS data updates** (rare - once a year):
   ```bash
   railway run python leadcrm/leadcrm/manage.py refresh_massgis
   railway run python leadcrm/leadcrm/manage.py generate_town_geojson --force --upload-s3
   ```

2. **Add new towns** (as needed):
   ```bash
   railway run python leadcrm/leadcrm/manage.py generate_town_geojson --towns 123,456
   ```

### Monitoring:

Check which towns have been generated:
```bash
railway run ls -lh leadcrm/leadcrm/static/geojson/towns/ | wc -l
```

Check total size:
```bash
railway run du -sh leadcrm/leadcrm/static/geojson/towns/
```

---

## Rollback Plan

If something goes wrong, you can instantly rollback:

1. **Keep old API:** The `/api/parcels-in-viewport/` endpoint still works
2. **Frontend fallback:** Code can detect 404 and fall back to old API
3. **Git revert:** `git revert HEAD` to undo deployment

---

## Next Steps

1. ✅ **Code is deployed** (automatic via Railway)
2. ⏳ **Generate GeoJSON files** (run management command in Railway)
3. ⏳ **Test new API** (visit `/api/town-geojson/45/`)
4. ⏳ **Update frontend** (modify JavaScript to use new endpoint)
5. ⏳ **Monitor performance** (check speed improvement)

---

## Questions?

- See full guide: `GEOJSON_OPTIMIZATION_GUIDE.md`
- Quick reference: `QUICK_START_GEOJSON.md`
- Command help: `python manage.py generate_town_geojson --help`
