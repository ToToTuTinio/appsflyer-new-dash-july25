import os
import sys
from redis import Redis
from rq import Worker, Queue, Connection
import json
import sqlite3
from datetime import datetime, timedelta
import pytz
from app import get_period_dates, make_api_request, find_media_source_idx

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Redis connection
redis_conn = Redis(host='localhost', port=6379, db=0)

def process_stats_report(apps, period):
    """Process stats report for the last 30 days"""
    try:
        start_date, end_date = get_period_dates('last30')
        result = []
        for app in apps:
            app_id = app['app_id']
            app_name = app['app_name']
            print(f"Processing stats for app: {app_name} (App ID: {app_id})...")
            
            # Get the selected events for this app
            conn = sqlite3.connect('event_selections.db')
            c = conn.cursor()
            c.execute('SELECT event1, event2 FROM app_event_selections WHERE app_id = ?', (app_id,))
            row = c.fetchone()
            conn.close()
            
            selected_events = []
            if row:
                event1, event2 = row
                if event1: selected_events.append(event1)
                if event2: selected_events.append(event2)
            
            # Process the report
            table = []
            for event in selected_events:
                url = f"https://hq1.appsflyer.com/api/raw-data/export/app/{app_id}/in_app_events_report/v5"
                params = {
                    "from": start_date,
                    "to": end_date,
                    "event_name": event
                }
                response = make_api_request(url, params)
                if response and response.status_code == 200:
                    rows = response.text.strip().split("\n")
                    if len(rows) > 1:
                        header = rows[0].split(",")
                        date_idx = header.index("Event Time") if "Event Time" in header else None
                        event_idx = header.index("Event Value") if "Event Value" in header else None
                        for row in rows[1:]:
                            cols = row.split(",")
                            if date_idx is not None and event_idx is not None and len(cols) > max(date_idx, event_idx):
                                event_date = cols[date_idx].split(" ")[0]
                                event_value = cols[event_idx]
                                table.append({
                                    "date": event_date,
                                    event: event_value
                                })
            
            result.append({
                'app_id': app_id,
                'app_name': app_name,
                'selected_events': selected_events,
                'table': table
            })
        
        # Save to cache
        conn = sqlite3.connect('event_selections.db')
        c = conn.cursor()
        c.execute('REPLACE INTO stats_cache (range, data, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)',
                  ('last30', json.dumps({'apps': result})))
        conn.commit()
        conn.close()
        
        return {'status': 'completed', 'result': result}
    except Exception as e:
        print(f"Error in process_stats_report: {str(e)}")
        return {'status': 'failed', 'error': str(e)}

def process_fraud_report(apps, period):
    """Process fraud report for the last 30 days"""
    try:
        start_date, end_date = get_period_dates('last30')
        fraud_list = []
        
        for app in apps:
            app_id = app['app_id']
            app_name = app['app_name']
            print(f"Processing fraud for app: {app_name} (App ID: {app_id})...")
            
            table = []
            app_errors = []
            agg = {}
            
            def add_metric(date, media_source, key):
                k = (date, media_source)
                if k not in agg:
                    agg[k] = {
                        "date": date,
                        "media_source": media_source,
                        "blocked_installs_rt": 0,
                        "blocked_installs_pa": 0,
                        "blocked_in_app_events": 0,
                        "fraud_post_inapps": 0,
                        "blocked_clicks": 0,
                        "blocked_install_postbacks": 0
                    }
                agg[k][key] += 1
            
            # Process all fraud metrics
            metrics = [
                ('blocked_installs_rt', 'blocked_installs_report/v5', 'Install Time'),
                ('blocked_installs_pa', 'detection/v5', 'Install Time'),
                ('blocked_in_app_events', 'blocked_in_app_events_report/v5', 'Event Time'),
                ('fraud_post_inapps', 'fraud-post-inapps/v5', 'Event Time'),
                ('blocked_clicks', 'blocked_clicks_report/v5', 'Click Time'),
                ('blocked_install_postbacks', 'blocked_install_postbacks/v5', 'Install Time')
            ]
            
            for metric, endpoint, time_field in metrics:
                url = f"https://hq1.appsflyer.com/api/raw-data/export/app/{app_id}/{endpoint}"
                params = {"from": start_date, "to": end_date}
                response = make_api_request(url, params)
                
                if response == 'timeout':
                    print(f"Timeout detected for {endpoint} {app_id}, skipping to next metric.")
                    continue
                    
                if response and response.status_code == 200:
                    rows = response.text.strip().split("\n")
                    if len(rows) > 1:
                        header = rows[0].split(",")
                        date_idx = header.index(time_field) if time_field in header else None
                        ms_idx = find_media_source_idx(header)
                        
                        if ms_idx is None:
                            print(f"ERROR: Could not find 'Media Source' column in {endpoint} for app {app_id}. Header: {header}")
                            continue
                            
                        for row in rows[1:]:
                            cols = row.split(",")
                            if date_idx is not None and len(cols) > date_idx:
                                event_date = cols[date_idx].split(" ")[0]
                                media_source = cols[ms_idx].strip() if ms_idx is not None and len(cols) > ms_idx else "Unknown"
                                add_metric(event_date, media_source, metric)
                elif response is not None:
                    app_errors.append(f"{endpoint} API error: {response.status_code} {response.text[:200]}")
            
            # Aggregate all (date, media_source) rows
            for (date, media_source), row in sorted(agg.items()):
                table.append(row)
            
            fraud_list.append({
                'app_id': app_id,
                'app_name': app_name,
                'table': table,
                'errors': app_errors
            })
        
        # Save to cache
        if fraud_list:
            conn = sqlite3.connect('event_selections.db')
            c = conn.cursor()
            c.execute('REPLACE INTO fraud_cache (range, data, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)',
                      ('last30', json.dumps({'apps': fraud_list})))
            conn.commit()
            conn.close()
        
        return {'status': 'completed', 'result': fraud_list}
    except Exception as e:
        print(f"Error in process_fraud_report: {str(e)}")
        return {'status': 'failed', 'error': str(e)}

if __name__ == '__main__':
    # Start worker
    with Connection(redis_conn):
        worker = Worker([Queue('default')])
        worker.work() 