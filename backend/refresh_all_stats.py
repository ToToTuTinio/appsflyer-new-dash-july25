import os
import sys
import time
import datetime
from pathlib import Path
import sqlite3
import json

# Import app logic (fixed import for backend directory)
from app import get_active_apps, process_report_async, make_api_request, get_period_dates, DB_PATH

# Periods to refresh
PERIODS = [
    ('last10', 'Last 10 Days'),
    ('mtd', 'Month to Date'),
    ('lastmonth', 'Last Month'),
    ('last30', 'Last 30 Days')
]

def refresh_stats_and_fraud():
    print(f"[CRON] Refresh started at {datetime.datetime.now()}")
    # Get active apps
    active_data = get_active_apps(force_fetch=True)
    apps = active_data.get('apps', [])
    print(f"[CRON] Found {len(apps)} active apps.")
    # Load event selections if needed
    event_selections = {}
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('SELECT app_id, event1, event2 FROM app_event_selections')
        for app_id, event1, event2 in c.fetchall():
            event_selections[app_id] = [event1, event2]
        conn.close()
    except Exception as e:
        print(f"[CRON] Error loading event selections: {e}")
    # Refresh stats for all periods
    for period, label in PERIODS:
        print(f"[CRON] Refreshing stats for: {label} ({period})")
        try:
            process_report_async(apps, period, event_selections)
        except Exception as e:
            print(f"[CRON] Error refreshing stats for {period}: {e}")
    # Refresh fraud for all periods
    import requests
    for period, label in PERIODS:
        print(f"[CRON] Refreshing fraud for: {label} ({period})")
        try:
            # Simulate a POST to /get_fraud endpoint
            payload = {"apps": apps, "period": period, "force": True}
            url = "http://localhost:5000/get_fraud"
            resp = requests.post(url, json=payload, timeout=600)
            print(f"[CRON] Fraud refresh for {period}: {resp.status_code}")
        except Exception as e:
            print(f"[CRON] Error refreshing fraud for {period}: {e}")
    print(f"[CRON] Refresh finished at {datetime.datetime.now()}")

if __name__ == "__main__":
    refresh_stats_and_fraud()