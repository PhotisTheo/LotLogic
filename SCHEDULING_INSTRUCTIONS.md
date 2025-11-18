# Market Values Computation Scheduling

## What Was Fixed

1. **Critical Bug Fix**: Fixed comp selection to prevent comparing commercial properties to residential properties
   - Previously, if no same-category comps were found, it would fall back to ALL properties
   - Now strictly filters to same property category (Residential, Commercial, etc.)

2. **Address Storage**: Comps already store full addresses (this was already working)

3. **Market Value Display**: Market value already displays on parcel detail page (this was already working)

## Running Tonight (One-Time)

### Option 1: Manual Run Tonight
Simply run the script at your desired time:
```bash
cd "/Volumes/OWC Envoy Ultra/Websites/lead_CRM_clean"
./run_market_values_compute.sh
```

### Option 2: Schedule for Specific Time Tonight (e.g., 2 AM)
```bash
# Schedule to run at 2:00 AM tonight
echo "./run_market_values_compute.sh" | at 2:00 AM
```

To verify it's scheduled:
```bash
atq
```

To cancel a scheduled job:
```bash
atrm <job_number>
```

## Setting Up Recurring Schedule

### Using Cron (Recommended for Regular Updates)

1. Open crontab editor:
```bash
crontab -e
```

2. Add one of these lines (choose based on your preference):

**Run every night at 2:00 AM:**
```cron
0 2 * * * /Volumes/OWC\ Envoy\ Ultra/Websites/lead_CRM_clean/run_market_values_compute.sh
```

**Run every Sunday at 3:00 AM:**
```cron
0 3 * * 0 /Volumes/OWC\ Envoy\ Ultra/Websites/lead_CRM_clean/run_market_values_compute.sh
```

**Run on the 1st of every month at 1:00 AM:**
```cron
0 1 1 * * /Volumes/OWC\ Envoy\ Ultra/Websites/lead_CRM_clean/run_market_values_compute.sh
```

3. Save and exit (`:wq` in vim, or `Ctrl+X` then `Y` in nano)

## Monitoring

### Check Logs
Logs are saved with timestamps in the project root:
```bash
ls -ltr /Volumes/OWC\ Envoy\ Ultra/Websites/lead_CRM_clean/market_values_*.log | tail -5
```

### View Latest Log
```bash
tail -f /Volumes/OWC\ Envoy\ Ultra/Websites/lead_CRM_clean/market_values_*.log
```

### Check Cron Jobs
```bash
crontab -l
```

## Parameters Explanation

The script runs with these settings:
- `--lookback-days 365`: Uses sales from the last year
- `--target-comps 5`: Tries to find 5 comparable sales per property
- `--batch-size 500`: Processes in batches of 500 for efficiency

To modify these, edit the `run_market_values_compute.sh` file.

## Expected Runtime

Depending on the number of towns and properties:
- Small dataset (1-10 towns): 5-30 minutes
- Medium dataset (10-50 towns): 30 minutes - 2 hours
- Large dataset (100+ towns): 2-6 hours

## Troubleshooting

### Permission Denied
```bash
chmod +x /Volumes/OWC\ Envoy\ Ultra/Websites/lead_CRM_clean/run_market_values_compute.sh
```

### Cron Not Running on macOS
Grant Full Disk Access to cron:
1. System Preferences → Security & Privacy → Privacy
2. Select "Full Disk Access"
3. Click the lock to make changes
4. Add `/usr/sbin/cron`

### Check if Process is Running
```bash
ps aux | grep compute_market_values
```
