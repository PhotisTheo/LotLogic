#!/bin/bash
# Script to compute market values for all towns
# This will process all towns and update the market values in the database

set -e  # Exit on error

# Change to the project directory
cd "/Volumes/OWC Envoy Ultra/Websites/lead_CRM_clean"

# Activate virtual environment
source .venv/bin/activate

# Change to Django project directory
cd leadcrm

# Log file with timestamp
LOG_FILE="/Volumes/OWC Envoy Ultra/Websites/lead_CRM_clean/market_values_$(date +%Y%m%d_%H%M%S).log"

echo "====================================" | tee -a "$LOG_FILE"
echo "Starting market values computation" | tee -a "$LOG_FILE"
echo "Started at: $(date)" | tee -a "$LOG_FILE"
echo "====================================" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"

# Run the compute_market_values command for all towns
# Options:
#   --lookback-days 365: Use sales from the last year
#   --target-comps 5: Try to find 5 comparable sales per property
#   --batch-size 500: Process in batches of 500 for efficiency
python manage.py compute_market_values \
    --lookback-days 365 \
    --target-comps 5 \
    --batch-size 500 \
    2>&1 | tee -a "$LOG_FILE"

echo "" | tee -a "$LOG_FILE"
echo "====================================" | tee -a "$LOG_FILE"
echo "Market values computation completed" | tee -a "$LOG_FILE"
echo "Finished at: $(date)" | tee -a "$LOG_FILE"
echo "Log saved to: $LOG_FILE" | tee -a "$LOG_FILE"
echo "====================================" | tee -a "$LOG_FILE"
