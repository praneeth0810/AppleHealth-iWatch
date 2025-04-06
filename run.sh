#!/bin/bash

# Enable error handling
set -e

# Logging setup
LOGFILE="run_all.log"
echo "Starting the health data pipeline at $(date)"

# Run Extract Data script
echo "[INFO] Running extract_data.py..."
python3 scripts/extract_data.py 2>&1 | tee -a $LOGFILE
if [ $? -ne 0 ]; then
  echo "[ERROR] extract_data.py failed. Check the log for details."
  exit 1
fi

# Run Transform Data script
echo "[INFO] Running transform_data.py..."
python3 scripts/transform_data.py 2>&1 | tee -a $LOGFILE
if [ $? -ne 0 ]; then
  echo "[ERROR] transform_data.py failed. Check the log for details."
  exit 1
fi

# Run Health Dashboard script
echo "[INFO] Launching health_dashboard.py..."
streamlit run scripts/health_dashboard.py 2>&1 | tee -a $LOGFILE
if [ $? -ne 0 ]; then
  echo "[ERROR] health_dashboard.py failed. Check the log for details."
  exit 1
fi

echo "Pipeline executed successfully at $(date)"