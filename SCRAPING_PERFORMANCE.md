# Registry Scraping Performance & Coverage Plan

## Target: Quarterly Statewide Updates

**Requirement**: All 2.1M Massachusetts parcels refreshed every 90 days

## Current Configuration

### Daily Processing
- **Schedule**: Every day at 3 AM (Celery Beat)
- **Batch Size**: 25,000 parcels/day
- **Celery Workers**: 16 concurrent workers
- **Rate Limiting**: ~0.3-0.4 requests/sec per registry

### Coverage Timeline

| Metric | Value |
|--------|-------|
| Total Parcels | 2,174,504 |
| Daily Batch | 25,000 |
| Days to Full Coverage | 87 days |
| **Refresh Cycle** | **~3 months (quarterly)** |
| Annual Refreshes | 4 full cycles/year |

### Daily Performance

**Per parcel timing:**
- Registry search: 5-10 seconds
- Document download: 2-5 seconds
- PDF parsing: 1-3 seconds
- S3 upload: 1-2 seconds
- **Total per parcel**: 10-20 seconds avg

**Daily batch (25,000 parcels):**
- **Sequential time**: 25,000 × 15 sec = 104 hours
- **With 16 workers**: 104 / 16 = **6.5 hours**
- **Actual time** (with overhead): **~8-10 hours**

This fits comfortably within the 24-hour daily window.

## Prioritization Strategy

1. **First Pass** (Days 1-87):
   - Scrape all unscraped parcels (2.1M)
   - Build initial statewide coverage

2. **Ongoing** (Day 88+):
   - Refresh parcels older than 90 days
   - Maintains quarterly refresh cycle
   - Prioritizes stale data

## Registry Rate Limiting

**Per Registry Constraints:**
- Max 0.3-0.4 requests/second
- ~1,000-1,500 requests/hour per registry
- 20 registries across MA
- **Total capacity**: 20,000-30,000 parcels/hour

**Daily capacity** (8 hours/day):
- 160,000-240,000 parcels/day theoretical max
- **Current target**: 25,000/day (conservative)
- **Headroom**: 6-10x capacity available for scaling

## Cost Estimates

### S3 Storage
- Avg document size: 500 KB
- Documents per parcel: 2-3
- **Storage per parcel**: ~1.5 MB
- **Total for 2.1M parcels**: ~3.2 TB
- **S3 cost**: $73/month @ $0.023/GB

### Celery/Redis
- Task queue overhead: minimal
- Redis memory: ~1 GB
- **Cost**: Included in Railway tier

### Bandwidth
- Downloads: 2.1M × 1.5 MB = 3.2 TB initial
- Uploads to S3: Same
- **Monthly**: ~400 GB (quarterly refreshes)

## Monitoring

**Key metrics to track:**
- Daily parcels scraped (target: 25k)
- Success rate (target: >95%)
- Avg time per parcel (target: <20 sec)
- Queue backlog (target: <1000)
- Registry 404 rate (track failures)

**Progress logs:**
```
Starting daily scraping (batch_size=25000)
Total parcels: 2,174,504
Already scraped: 125,000
Unscraped: 2,049,504
Stale (>90 days): 0
Scraping 25,000 parcels today
Progress: 150,000/2,174,504 (6.9%)
Estimated days to full coverage: 81
```

## Scaling Options

If quarterly isn't fast enough, we can:

1. **Increase batch size to 50k/day**
   - Coverage: 44 days (monthly refresh)
   - Daily runtime: ~16-20 hours
   - Still fits in 24-hour window

2. **Add more worker processes**
   - Scale from 16 to 32 workers
   - Halves processing time
   - Requires more Railway resources

3. **Multi-shift scheduling**
   - Run at 3 AM and 3 PM (2x/day)
   - 50k parcels/day = monthly refresh
   - Better registry rate distribution

## Railway Resource Requirements

**Current setup:**
- **Web service**: 4 Gunicorn workers (Django)
- **Worker service**: 16 Celery workers (scraping)
- **Redis**: Task queue broker
- **Postgres**: Data storage

**Estimated costs:**
- Railway tier: Pro ($20/month base)
- Workers: ~$40-60/month
- Redis: Included
- S3: ~$73/month
- **Total**: ~$130-150/month

## Next Steps

1. ✅ Daily scraping configured (3 AM)
2. ✅ 25k batch size set
3. ✅ 16 workers deployed
4. ⏳ Monitor first daily run
5. ⏳ Adjust based on performance
6. ⏳ Scale up if needed

---

**Updated**: 2025-11-15
**Status**: Ready for deployment
**Target Go-Live**: Tomorrow (Sunday 3 AM first run)
