from flask import Flask, jsonify, render_template, request, session, redirect, url_for
from flask_cors import CORS
import os
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import time
from functools import wraps
import requests
import csv
from io import StringIO
import datetime
import sqlite3
import sys
import json
import pytz
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from redis import Redis
from rq import Queue
import uuid
from threading import Thread

# Initialize Redis connection
redis_conn = Redis(host='localhost', port=6379, db=0)
# Initialize RQ queue
task_queue = Queue(connection=redis_conn)

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from appsflyer_login import get_apps_with_installs

# Get the project root directory and load environment variables
project_root = Path(__file__).parent.parent
env_path = project_root / '.env.local'

print(f"Looking for .env file at: {env_path}")
print(f"File exists: {env_path.exists()}")

# Read the .env.local file directly
if env_path.exists():
    with open(env_path) as f:
        for line in f:
            if line.strip() and not line.startswith('#'):
                key, value = line.strip().split('=', 1)
                os.environ[key] = value.strip('"').strip("'")

app = Flask(__name__)
CORS(app)
app.secret_key = os.getenv('FLASK_SECRET_KEY', 's3cr3t_k3y_4g3ncy_d4sh_2025_!@#%$^&*()_+')

# Initialize rate limiter
limiter = Limiter(
    app=app,
    key_func=get_remote_address,
    default_limits=["10000 per day", "5000 per hour"]
)

# --- GLOBAL JSON ERROR HANDLER ---
from werkzeug.exceptions import HTTPException

@app.errorhandler(Exception)
def handle_exception(e):
    # If the error is an HTTPException, use its code and description
    if isinstance(e, HTTPException):
        response = {
            "error": e.description,
            "code": e.code
        }
        return jsonify(response), e.code
    # Otherwise, it's a non-HTTP error
    return jsonify({
        "error": str(e),
        "code": 500
    }), 500

# Dashboard Configuration
DASHBOARD_USERNAME = os.getenv('DASHBOARD_USERNAME')
DASHBOARD_PASSWORD = os.getenv('DASHBOARD_PASSWORD')

if not all([DASHBOARD_USERNAME, DASHBOARD_PASSWORD]):
    raise ValueError("DASHBOARD_USERNAME and DASHBOARD_PASSWORD not found in environment variables")

# AppsFlyer Configuration
EMAIL = os.getenv('EMAIL')
PASSWORD = os.getenv('PASSWORD')
APPSFLYER_API_KEY = os.getenv('APPSFLYER_API_KEY')

if not all([EMAIL, PASSWORD]):
    raise ValueError("EMAIL and PASSWORD not found in environment variables")

DB_PATH = 'event_selections.db'

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS app_event_selections (
        app_id TEXT PRIMARY KEY,
        event1 TEXT,
        event2 TEXT,
        is_active INTEGER DEFAULT 0
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS stats_cache (
        range TEXT PRIMARY KEY,
        data TEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS fraud_cache (
        range TEXT PRIMARY KEY,
        data TEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS event_cache (
        app_id TEXT PRIMARY KEY,
        data TEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS apps_cache (
        id INTEGER PRIMARY KEY,
        data TEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    conn.commit()
    conn.close()

# Add the new column if it doesn't exist
def add_is_active_column():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    try:
        c.execute('ALTER TABLE app_event_selections ADD COLUMN is_active INTEGER DEFAULT 0')
    except sqlite3.OperationalError:
        # Column already exists
        pass
    conn.commit()
    conn.close()

init_db()
add_is_active_column()

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'logged_in' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

@app.route('/')
def login():
    if 'logged_in' in session:
        return redirect(url_for('dashboard'))
    return render_template('login.html')

@app.route('/login', methods=['POST'])
@limiter.limit("5 per minute")
def handle_login():
    data = request.get_json()
    if data['email'] == DASHBOARD_USERNAME and data['password'] == DASHBOARD_PASSWORD:
        session['logged_in'] = True
        return jsonify({'success': True})
    return jsonify({'success': False, 'message': 'Invalid credentials'}), 401

@app.route('/logout')
def logout():
    session.pop('logged_in', None)
    return redirect(url_for('login'))

@app.route('/check-auth')
def check_auth():
    if 'logged_in' in session:
        return jsonify({'authenticated': True})
    return jsonify({'authenticated': False}), 401

@app.route('/dashboard')
@login_required
def dashboard():
    return render_template('dashboard.html')

def get_active_apps(max_retries=7, force_fetch=False):
    """
    Fetch the list of active apps from cache if less than 24 hours old and has non-active apps,
    otherwise fetch from AppsFlyer and update cache.
    """
    import pytz
    gmt2 = pytz.timezone('Europe/Berlin')
    now = datetime.datetime.now(gmt2)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Get active status from database first
    c.execute('SELECT app_id, is_active FROM app_event_selections')
    active_status = dict(c.fetchall())
    
    c.execute('SELECT data, updated_at FROM apps_cache ORDER BY updated_at DESC LIMIT 1')
    row = c.fetchone()
    
    if row and not force_fetch:
        data, updated_at = row
        updated_at_dt = datetime.datetime.strptime(updated_at, '%Y-%m-%d %H:%M:%S')
        updated_at_dt = gmt2.localize(updated_at_dt)
        cache_age = (now - updated_at_dt).total_seconds()
        
        # First check if cache is fresh enough
        if cache_age < 86400:
            cached_data = json.loads(data)
            
            # Update active status from database for all apps
            for app in cached_data.get('apps', []):
                # If app exists in database, use its status, otherwise default to active (True)
                app['is_active'] = bool(active_status.get(app['app_id'], 1))
            
            # Check if we have any non-active apps
            has_non_active = any(not app.get('is_active', True) for app in cached_data.get('apps', []))
            
            # Use cache only if we have some non-active apps and cache is fresh
            if has_non_active:
                cached_data['fetch_time'] = updated_at_dt.strftime('%Y-%m-%d %H:%M:%S')
                cached_data['used_cache'] = True
                conn.close()
                return cached_data

    # If no cache, cache is old, all apps are active, or force_fetch is True, fetch new data
    apps = get_apps_with_installs(EMAIL, PASSWORD, max_retries=max_retries)
    
    # Add active status to each app, defaulting to active (True) if not in database
    for app in apps:
        app['is_active'] = bool(active_status.get(app['app_id'], 1))
    
    fetch_time = now.strftime('%Y-%m-%d %H:%M:%S')
    result = {
        "count": len(apps),
        "apps": apps,
        "fetch_time": fetch_time,
        "used_cache": False
    }
    
    # Update cache
    c.execute('DELETE FROM apps_cache')  # Only keep one row
    c.execute('INSERT INTO apps_cache (data, updated_at) VALUES (?, ?)', 
             (json.dumps(result), fetch_time))
    conn.commit()
    conn.close()
    return result

@app.route('/active-apps')
@login_required
def active_apps():
    try:
        result = get_active_apps()
        return jsonify(result)
    
    except Exception as e:
        print(f"Error in active_apps endpoint: {str(e)}")
        return jsonify({
            'error': str(e),
            'count': 0,
            'apps': [],
            'fetch_time': '0.00 seconds'
        }), 500

@app.route('/app-stats/<app_id>')
@login_required
def app_stats(app_id):
    headers = {
        "Authorization": f"Bearer {APPSFLYER_API_KEY}"
    }
    # Example: Installs report (adjust endpoint as needed)
    installs_url = f"https://hq1.appsflyer.com/api/raw-data/export/app/{app_id}/installs_report/v5"
    try:
        resp = requests.get(installs_url, headers=headers)
        if resp.status_code == 200:
            # For now, just return the raw response (CSV or JSON)
            return resp.text, 200, {'Content-Type': resp.headers.get('Content-Type', 'text/plain')}
        else:
            return jsonify({"error": f"API error: {resp.status_code}", "details": resp.text}), resp.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500

def get_period_dates(period):
    today = datetime.date.today()
    if period in ('today',):
        start_date = end_date = today
    elif period in ('yesterday',):
        start_date = end_date = today - datetime.timedelta(days=1)
    elif period in ('last30', '30d'):
        start_date = today - datetime.timedelta(days=29)
        end_date = today
    elif period in ('last10', '10d'):
        start_date = today - datetime.timedelta(days=9)
        end_date = today
    elif period == 'mtd':
        start_date = today.replace(day=1)
        end_date = today
    elif period == 'lastmonth':
        first_of_this_month = today.replace(day=1)
        last_month_end = first_of_this_month - datetime.timedelta(days=1)
        start_date = last_month_end.replace(day=1)
        end_date = last_month_end
    else:
        # Default to last 10 days
        start_date = today - datetime.timedelta(days=9)
        end_date = today
    return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')

@app.route('/app-events/<app_id>')
@login_required
def app_events(app_id):
    import time
    import requests
    today = datetime.date.today()
    start_date = (today - datetime.timedelta(days=10)).strftime("%Y-%m-%d")
    end_date = today.strftime("%Y-%m-%d")
    url = f"https://hq1.appsflyer.com/api/raw-data/export/app/{app_id}/in_app_events_report/v5"
    params = {"from": start_date, "to": end_date}
    headers = {"accept": "text/csv", "authorization": f"Bearer {APPSFLYER_API_KEY}"}
    
    # Check event_cache first
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS event_cache (
        app_id TEXT PRIMARY KEY,
        data TEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    conn.commit()
    c.execute('SELECT data, updated_at FROM event_cache WHERE app_id = ?', (app_id,))
    row = c.fetchone()
    if row:
        data, updated_at = row
        result = json.loads(data)
        result['updated_at'] = updated_at
        conn.close()
        return jsonify(result)
    
    # Get app name from static apps
    app_name = app_id
    static_apps = [
        {"app_id": "id905953485", "app_name": "NordVPN: VPN Fast & Secure"},
        {"app_id": "id1211206916", "app_name": "888poker IT"}
    ]
    for app in static_apps:
        if app["app_id"] == app_id:
            app_name = app["app_name"]
            break
            
    print(f"[GET EVENTS] Fetching In-App Events for: {app_name} (App ID: {app_id}) from {start_date} to {end_date}...")
    max_retries = 2
    retry_delay = 5
    retries = 0
    start_time = time.time()
    
    while retries < max_retries:
        try:
            response = requests.get(url, headers=headers, params=params, timeout=90)
            response.raise_for_status()
            rows = response.text.strip().split("\n")
            if not rows:
                return jsonify({
                    "events": [], 
                    "fetch_time": f"{time.time()-start_time:.2f} seconds",
                    "error": "No data returned from API"
                })
                
            header = rows[0].split(",")
            data = [row.split(",") for row in rows[1:]]
            
            if "Event Name" not in header:
                print(f"[GET EVENTS] No 'Event Name' column found for app: {app_id}")
                return jsonify({
                    "events": [], 
                    "fetch_time": f"{time.time()-start_time:.2f} seconds",
                    "error": "No 'Event Name' column in API response"
                })
                
            event_name_index = header.index("Event Name")
            event_names = set()
            for row in data:
                if len(row) > event_name_index:
                    event_names.add(row[event_name_index])
                    
            elapsed = time.time() - start_time
            print(f"[GET EVENTS] Done fetching events for app: {app_name} (App ID: {app_id}) in {elapsed:.2f} seconds. Found {len(event_names)} events.")
            result = {
                "events": sorted(list(event_names)), 
                "fetch_time": f"{elapsed:.2f} seconds"
            }
            # Save to cache
            c.execute('REPLACE INTO event_cache (app_id, data, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)', (app_id, json.dumps(result)))
            conn.commit()
            conn.close()
            return jsonify(result)
            
        except requests.exceptions.HTTPError as http_err:
            response_content = response.text.strip() if hasattr(response, 'text') else ''
            print(f"[GET EVENTS] HTTP error occurred: {http_err}")
            print("Response Content:", response_content)
            
            if "Limit reached for daily-report" in response_content:
                retries += 1
                if retries < max_retries:
                    print(f"Limit reached for daily-report. Retrying in {retry_delay} seconds... (Attempt {retries}/{max_retries})")
                    time.sleep(retry_delay)
                    continue
                else:
                    print("Max retries reached for daily-report. Skipping In-App Events for this app.")
                    return jsonify({
                        "events": [], 
                        "fetch_time": f"{time.time()-start_time:.2f} seconds", 
                        "error": "API rate limit reached after multiple retries"
                    })
            else:
                print("Unhandled HTTP error. Skipping In-App Events for this app.")
                return jsonify({
                    "events": [], 
                    "fetch_time": f"{time.time()-start_time:.2f} seconds", 
                    "error": f"HTTP Error: {str(http_err)}",
                    "response_content": response_content
                })
                
        except Exception as e:
            retries += 1
            print(f"An error occurred while fetching In-App Events for {app_name} (App ID: {app_id}): {e}")
            if retries < max_retries:
                print(f"Retrying in {retry_delay} seconds... (Attempt {retries}/{max_retries})")
                time.sleep(retry_delay)
                continue
            else:
                print("Max retries reached or unrecoverable error occurred. Skipping In-App Events for this app.")
                return jsonify({
                    "events": [], 
                    "fetch_time": f"{time.time()-start_time:.2f} seconds", 
                    "error": str(e)
                })
                
    print("Max retries reached or unrecoverable error occurred. Skipping In-App Events for this app.")
    return jsonify({
        "events": [], 
        "fetch_time": f"{time.time()-start_time:.2f} seconds", 
        "error": "Max retries reached"
    })

def make_api_request(url, params, max_retries=7, retry_delay=30):
    headers = {
        "Authorization": f"Bearer {APPSFLYER_API_KEY}",
        "accept": "text/csv"
    }
    for attempt in range(max_retries):
        try:
            print(f"[API] Making request to {url} (attempt {attempt + 1}/{max_retries})")
            resp = requests.get(url, headers=headers, params=params, timeout=90)
            if resp.status_code == 200:
                return resp
            print(f"[API] Request failed with status {resp.status_code}")
            print(f"[API] Response headers: {dict(resp.headers)}")
            print(f"[API] Response body: {resp.text}")
            
            # Check for specific error messages that should skip retries
            error_text = resp.text.lower()
            skip_retry_messages = [
                "limit reached for daily-report",
                "you've reached your maximum number of in-app event reports that can be downloaded today for this app",
                "you've reached your maximum number of in-app event reports that can be downloaded today for this account",
                "you've reached your maximum number of install reports that can be downloaded today for this app",
                "you've reached your maximum number of install reports that can be downloaded today for this account",
                "your current subscription package doesn't include raw data reports",
                "subscription package doesn't include raw data"
            ]
            
            if any(msg in error_text for msg in skip_retry_messages):
                print("[API] Detected API limitation. Skipping retries for this request.")
                return None
                
            if resp.status_code == 429:  # Rate limit
                retry_after = int(resp.headers.get('Retry-After', retry_delay))
                print(f"[API] Rate limited. Waiting {retry_after} seconds...")
                time.sleep(retry_after)
                continue
            if attempt < max_retries - 1:
                print(f"[API] Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
        except requests.exceptions.Timeout as e:
            print(f"[API] Timeout error: {str(e)}")
            return 'timeout'
        except requests.exceptions.RequestException as e:
            print(f"[API] Request error: {str(e)}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"[API] Exception response headers: {dict(e.response.headers)}")
                print(f"[API] Exception response body: {e.response.text}")
            if attempt < max_retries - 1:
                print(f"[API] Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
    return None

@app.route('/all-apps-stats', methods=['POST'])
@login_required
def all_apps_stats():
    data = request.get_json()
    active_apps = data.get('apps', [])
    period = data.get('period', 'last10')
    selected_events = data.get('selected_events', {})
    start_date, end_date = get_period_dates(period)
    print(f"[STATS] /all-apps-stats called for period: {period} ({start_date} to {end_date})")
    print(f"[STATS] Apps: {[app['app_id'] for app in active_apps]}")
    stats_list = []
    
    # Build a unique cache key for stats: period:event1:event2:app_ids
    app_ids = '-'.join(sorted([app['app_id'] for app in active_apps]))
    event1 = ''
    event2 = ''
    if active_apps and selected_events:
        first_app_id = active_apps[0]['app_id']
        events = selected_events.get(first_app_id, [])
        if len(events) > 0:
            event1 = events[0] or ''
        if len(events) > 1:
            event2 = events[1] or ''
    cache_key = f"{period}:{event1}:{event2}:{app_ids}"
    # Check cache
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT data, updated_at FROM stats_cache WHERE range = ?', (cache_key,))
    row = c.fetchone()
    if row:
        data, updated_at = row
        result = json.loads(data)
        # Only use cache if it contains at least one app
        if result.get('apps') and len(result['apps']) > 0:
            result['updated_at'] = updated_at
            conn.close()
            return jsonify(result)
    
    for app in active_apps:
        app_id = app['app_id']
        app_name = app['app_name']
        print(f"[STATS] Fetching stats for app: {app_name} (App ID: {app_id})...")
        
        # Use the aggregate daily report endpoint for main stats
        url = f"https://hq1.appsflyer.com/api/agg-data/export/app/{app_id}/daily_report/v5"
        params = {"from": start_date, "to": end_date}
        
        try:
            print(f"[STATS] Calling daily_report API for {app_id}...")
            resp = make_api_request(url, params)
            if resp == 'timeout':
                print(f"[STATS] Timeout detected for {app_id}, returning processing status.")
                return jsonify({'status': 'processing'})
            daily_stats = {}
            
            if resp and resp.status_code == 200:
                print(f"[STATS] Got daily_report for {app_id}.")
                rows = resp.text.strip().split("\n")
                if len(rows) < 2:  # Only header or empty
                    print(f"[STATS] No data returned for {app_id}")
                    stats_list.append({
                        'app_id': app_id,
                        'app_name': app_name,
                        'table': [],
                        'selected_events': [],
                        'traffic': 0,
                        'error': 'No data returned from API'
                    })
                    continue
                header = rows[0].split(",")
                print(f"[STATS] daily_report header for {app_id}: {header}")
                data_rows = [row.split(",") for row in rows[1:]]
                # Case-insensitive column mapping
                col_map = {col.lower().strip(): i for i, col in enumerate(header)}
                def find_col(*names):
                    for name in names:
                        for col in header:
                            if col.lower().replace('_','').replace(' ','') == name.lower().replace('_','').replace(' ',''):
                                return header.index(col)
                    return None
                impressions_idx = find_col('impressions', 'Impressions')
                clicks_idx = find_col('clicks', 'Clicks')
                installs_idx = find_col('installs', 'Installs')
                date_idx = find_col('date', 'Date')
                media_source_idx = find_col('media_source', 'media source', 'Media Source', 'Media Source (pid)', 'media_source (pid)', 'pid', 'Media Source (PID)', 'media_source (PID)')
                if None in [impressions_idx, clicks_idx, installs_idx, date_idx]:
                    print(f"[STATS] WARNING: Could not find all required columns for {app_id}")
                    continue
                if media_source_idx is None:
                    print(f"[STATS] WARNING: Could not find media source column for {app_id}. Skipping all installs for safety.")
                    continue
                for row in data_rows:
                    if len(row) <= max(impressions_idx, clicks_idx, installs_idx, date_idx, media_source_idx):
                        continue
                    media_source = row[media_source_idx].strip().lower()
                    date = row[date_idx] if date_idx is not None and len(row) > date_idx else ''
                    if not date:
                        continue
                    impressions = int(row[impressions_idx]) if impressions_idx is not None and len(row) > impressions_idx and row[impressions_idx].isdigit() else 0
                    clicks = int(row[clicks_idx]) if clicks_idx is not None and len(row) > clicks_idx and row[clicks_idx].isdigit() else 0
                    installs = int(row[installs_idx]) if installs_idx is not None and len(row) > installs_idx and row[installs_idx].isdigit() else 0
                    
                    # Initialize daily stats if not exists
                    daily_stats.setdefault(date, {"impressions": 0, "clicks": 0, "total_installs": 0, "organic_installs": 0})
                    
                    # Add to totals
                    daily_stats[date]["impressions"] += impressions
                    daily_stats[date]["clicks"] += clicks
                    daily_stats[date]["total_installs"] += installs
                    
                    # Track organic installs separately
                    if media_source == 'organic':
                        daily_stats[date]["organic_installs"] += installs
                
                # After processing all rows, calculate non-organic installs
                for date in daily_stats:
                    daily_stats[date]["installs"] = daily_stats[date]["total_installs"] - daily_stats[date]["organic_installs"]
                    if daily_stats[date]["installs"] < 0:
                        daily_stats[date]["installs"] = 0  # Prevent negative installs
            else:
                print(f"[STATS] daily_report API error for {app_id}: {resp.status_code if resp else 'No response'}")
                continue
            # Blocked Installs (RT)
            print(f"[STATS] Calling blocked_installs_report API for {app_id}...")
            blocked_rt_url = f"https://hq1.appsflyer.com/api/raw-data/export/app/{app_id}/blocked_installs_report/v5"
            blocked_rt_params = {"from": start_date, "to": end_date}
            blocked_rt_resp = make_api_request(blocked_rt_url, blocked_rt_params)
            if blocked_rt_resp == 'timeout':
                print(f"[STATS] Timeout detected for blocked_installs_report {app_id}, returning processing status.")
                return jsonify({'status': 'processing'})
            
            # Process Blocked Installs (RT) data
            if blocked_rt_resp and blocked_rt_resp.status_code == 200:
                rows = blocked_rt_resp.text.strip().split("\n")
                if len(rows) > 1:
                    header = rows[0].split(",")
                    date_idx = header.index("Install Time") if "Install Time" in header else None
                    for row in rows[1:]:
                        cols = row.split(",")
                        if date_idx is not None and len(cols) > date_idx:
                            install_date = cols[date_idx].split(" ")[0]
                            if install_date in daily_stats:
                                daily_stats[install_date]["blocked_installs_rt"] = daily_stats[install_date].get("blocked_installs_rt", 0) + 1

            # Blocked Installs (PA)
            print(f"[STATS] Calling detection API for {app_id}...")
            blocked_pa_url = f"https://hq1.appsflyer.com/api/raw-data/export/app/{app_id}/detection/v5"
            blocked_pa_params = {"from": start_date, "to": end_date}
            blocked_pa_resp = make_api_request(blocked_pa_url, blocked_pa_params)
            if blocked_pa_resp == 'timeout':
                print(f"[STATS] Timeout detected for detection API {app_id}, returning processing status.")
                return jsonify({'status': 'processing'})
            
            # Process Blocked Installs (PA) data
            if blocked_pa_resp and blocked_pa_resp.status_code == 200:
                rows = blocked_pa_resp.text.strip().split("\n")
                if len(rows) > 1:
                    header = rows[0].split(",")
                    date_idx = header.index("Install Time") if "Install Time" in header else None
                    for row in rows[1:]:
                        cols = row.split(",")
                        if date_idx is not None and len(cols) > date_idx:
                            install_date = cols[date_idx].split(" ")[0]
                            if install_date in daily_stats:
                                daily_stats[install_date]["blocked_installs_pa"] = daily_stats[install_date].get("blocked_installs_pa", 0) + 1

            # In-App Events (for selected events)
            event_data = {}
            selected = selected_events.get(app_id, [])
            # Helper to detect error events
            def is_error_event(ev):
                if not ev: return True
                evl = ev.lower()
                return (
                    'maximum nu' in evl or
                    'subscription' in evl or
                    'error' in evl or
                    'failed' in evl or
                    "doesn't include" in evl or
                    'not include' in evl or
                    'your current subscription pack' in evl
                )
            # Only fetch in-app events if there are real events
            real_events = [ev for ev in selected if ev and not is_error_event(ev)]
            if real_events:
                print(f"[STATS] Calling in_app_events_report API for {app_id} (events: {real_events})...")
                events_url = f"https://hq1.appsflyer.com/api/raw-data/export/app/{app_id}/in_app_events_report/v5"
                events_params = {"from": start_date, "to": end_date}
                events_resp = make_api_request(events_url, events_params)
                if events_resp and events_resp.status_code == 200:
                    event_rows = events_resp.text.strip().split("\n")
                    event_header = event_rows[0].split(",")
                    event_name_index = event_header.index("Event Name") if "Event Name" in event_header else None
                    event_time_index = event_header.index("Event Time") if "Event Time" in event_header else None
                    for row in event_rows[1:]:
                        cols = row.split(",")
                        if event_name_index is not None and event_time_index is not None and len(cols) > max(event_name_index, event_time_index):
                            event_name = cols[event_name_index]
                            event_date = cols[event_time_index].split(" ")[0]
                            if event_name in real_events:
                                event_data.setdefault(event_name, {})
                                event_data[event_name].setdefault(event_date, 0)
                                event_data[event_name][event_date] += 1
                else:
                    print(f"[STATS] in_app_events_report API error for {app_id}: {events_resp.status_code if events_resp else 'No response'}")
            else:
                print(f"[STATS] Skipping in_app_events_report API for {app_id} (no real events)")
            # Prepare daily stats for frontend
            all_dates = sorted(daily_stats.keys())
            table = []
            for date in all_dates:
                # Skip dates that have no stats data
                if not any(daily_stats[date].get(key, 0) > 0 for key in ["impressions", "clicks", "installs", "blocked_installs_rt", "blocked_installs_pa"]):
                    continue
                    
                row = {
                    "date": date,
                    "impressions": daily_stats[date].get("impressions", 0),
                    "clicks": daily_stats[date].get("clicks", 0),
                    "installs": daily_stats[date].get("installs", 0),
                    "blocked_installs_rt": daily_stats[date].get("blocked_installs_rt", 0),
                    "blocked_installs_pa": daily_stats[date].get("blocked_installs_pa", 0),
                }
                # Calculated rates
                row["imp_to_click"] = round(row["clicks"] / row["impressions"], 2) if row["impressions"] > 0 else 0
                row["click_to_install"] = (row["installs"] / row["clicks"]) if row["clicks"] > 0 else 0
                row["blocked_rt_rate"] = round(row["blocked_installs_rt"] / row["installs"], 2) if row["installs"] > 0 else 0
                row["blocked_pa_rate"] = round(row["blocked_installs_pa"] / row["installs"], 2) if row["installs"] > 0 else 0
                # Add event counts
                if selected:
                    for event in selected:
                        row[event] = event_data.get(event, {}).get(date, 0)
                table.append(row)
            stats_list.append({
                'app_id': app_id,
                'app_name': app_name,
                'table': table,
                'selected_events': selected,
                'traffic': sum(r['impressions'] + r['clicks'] for r in table)
            })
        except Exception as e:
            print(f"[STATS] Error for app {app_id}: {e}")
            stats_list.append({
                'app_id': app_id,
                'app_name': app_name,
                'table': [],
                'selected_events': [],
                'traffic': 0,
                'error': str(e)
            })
    print(f"[STATS] Done. Returning stats for {len(stats_list)} apps.")
    stats_list.sort(key=lambda x: x['traffic'], reverse=True)
    # Save to cache ONLY if there is at least one app
    if len(stats_list) > 0:
        c.execute('REPLACE INTO stats_cache (range, data, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)', (cache_key, json.dumps({'apps': stats_list})))
        conn.commit()
    conn.close()
    return jsonify({'apps': stats_list})

@app.route('/event-selections', methods=['GET'])
@login_required
def get_event_selections():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    try:
        c.execute('SELECT app_id, event1, event2, is_active FROM app_event_selections')
        rows = c.fetchall()
        selections = {}
        for row in rows:
            app_id, event1, event2, is_active = row
            selections[app_id] = {
                'event1': event1,
                'event2': event2,
                'is_active': bool(is_active)
            }
        return jsonify({"success": True, "selections": selections})
    except Exception as e:
        print(f"Error getting event selections: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        conn.close()

@app.route('/event-selections', methods=['POST'])
@login_required
def save_event_selections():
    data = request.get_json()
    if not data:
        return jsonify({"success": False, "error": "No data provided"}), 400
    
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    try:
        # Check if it's a single update or bulk update
        if isinstance(data, dict) and 'app_id' in data:
            # Single update
            app_id = data.get('app_id')
            event1 = data.get('event1')
            event2 = data.get('event2')
            is_active = data.get('is_active', False)
            
            c.execute('''INSERT OR REPLACE INTO app_event_selections 
                        (app_id, event1, event2, is_active) 
                        VALUES (?, ?, ?, ?)''', 
                     (app_id, event1, event2, 1 if is_active else 0))
        else:
            # Bulk update
            for app_id, app_data in data.items():
                event1 = app_data.get('event1')
                event2 = app_data.get('event2')
                is_active = app_data.get('is_active', False)
                
                c.execute('''INSERT OR REPLACE INTO app_event_selections 
                            (app_id, event1, event2, is_active) 
                            VALUES (?, ?, ?, ?)''', 
                         (app_id, event1, event2, 1 if is_active else 0))
        
        conn.commit()
        return jsonify({"success": True})
    except Exception as e:
        print(f"Error saving event selections: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        conn.close()

@app.route('/get_apps')
@login_required
def get_apps():
    try:
        result = get_active_apps()
        # Add used_cache flag to indicate if we used cached data
        result['used_cache'] = result.get('fetch_time') is not None
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/get_events')
@login_required
def get_events():
    try:
        app_id = request.args.get('app_id')
        if not app_id:
            return jsonify({"error": "app_id is required"}), 400

        # Simulate a check for raw data report availability
        # In production, this would be a real API call to AppsFlyer
        if app_id == "id6633423879":  # SportsMillions Pick'em
            return jsonify({
                "events": ["Not Include Events"],
                "error": "not_included"
            })

        # For now, return static test events for other apps
        return jsonify({
            "events": [
                "af_purchase",
                "af_complete_registration",
                "af_level_achieved",
                "af_tutorial_completion"
            ]
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/get_stats', methods=['GET'])
def get_stats():
    period = request.args.get('period', 'last30')
    try:
        # Get cached data for the period
        cache_key = f'stats_{period}'
        cached_data = cache.get(cache_key)
        if cached_data:
            return jsonify(cached_data)
        
        # If no cached data, return empty result
        return jsonify({'apps': []})
    except Exception as e:
        app.logger.error(f"Error getting stats: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/update-credential', methods=['POST'])
@login_required
def update_credential():
    allowed_keys = {'APPSFLYER_API_KEY', 'EMAIL', 'PASSWORD'}
    data = request.get_json()
    key = data.get('key')
    value = data.get('value')
    if key not in allowed_keys:
        return jsonify({'success': False, 'error': 'Invalid key'}), 400
    env_path = Path(__file__).parent.parent / '.env.local'
    try:
        # Read all lines
        lines = []
        found = False
        if env_path.exists():
            with open(env_path, 'r') as f:
                for line in f:
                    if line.strip().startswith(f'{key}='):
                        lines.append(f'{key}="{value}"\n')
                        found = True
                    else:
                        lines.append(line)
        if not found:
            lines.append(f'{key}="{value}"\n')
        with open(env_path, 'w') as f:
            f.writelines(lines)
        # Update in-memory env var
        os.environ[key] = value
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/profile-info')
@login_required
def profile_info():
    return jsonify({
        'agency': 'N/A',
        'api_key': os.getenv('APPSFLYER_API_KEY', ''),
        'email': os.getenv('EMAIL', ''),
        'password': os.getenv('PASSWORD', '')
    })

def find_media_source_idx(header):
    # More flexible matching for Media Source column
    def norm(col):
        return col.lower().replace(' ', '').replace('_', '').replace('(', '').replace(')', '')
    # Try exact match first
    for i, col in enumerate(header):
        if norm(col) == 'mediasource':
            return i
    # Try partial match if exact match fails
    for i, col in enumerate(header):
        if 'media' in norm(col) and 'source' in norm(col):
            return i
    print(f"[FRAUD] WARNING: Could not find Media Source column in header: {header}")
    return None

@app.route('/get_fraud', methods=['GET'])
def get_fraud():
    period = request.args.get('period', 'last30')
    try:
        # Get cached data for the period
        cache_key = f'fraud_{period}'
        cached_data = cache.get(cache_key)
        if cached_data:
            return jsonify(cached_data)
        
        # If no cached data, return empty result
        return jsonify({'apps': []})
    except Exception as e:
        app.logger.error(f"Error getting fraud data: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/get_fraud_source', methods=['GET'])
def get_fraud_source():
    period = request.args.get('period', 'last30')
    try:
        # Get cached data for the period
        cache_key = f'fraud_source_{period}'
        cached_data = cache.get(cache_key)
        if cached_data:
            return jsonify(cached_data)
        
        # If no cached data, return empty result
        return jsonify({'apps': []})
    except Exception as e:
        app.logger.error(f"Error getting fraud source data: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/overview')
@login_required
def overview():
    try:
        # Read the most recent 'last30' stats_cache entry (regardless of event selections or app IDs)
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT data, updated_at FROM stats_cache WHERE range LIKE 'last30%' ORDER BY updated_at DESC LIMIT 1")
        row = c.fetchone()
        total_impressions = 0
        total_clicks = 0
        total_installs = 0
        last_updated = None
        date_map = {}
        if row:
            data, updated_at = row
            stats = json.loads(data)
            # Convert last_updated to GMT+2
            if updated_at:
                import datetime
                utc_dt = datetime.datetime.strptime(updated_at, '%Y-%m-%d %H:%M:%S')
                utc_dt = utc_dt.replace(tzinfo=pytz.utc)
                gmt2 = pytz.timezone('Europe/Berlin')
                last_updated = utc_dt.astimezone(gmt2).strftime('%Y-%m-%d %H:%M:%S')
            for app in stats.get('apps', []):
                for entry in app.get('table', []):
                    date = entry.get('date')
                    if date:
                        if date not in date_map:
                            date_map[date] = {'impressions': 0, 'clicks': 0, 'installs': 0}
                        date_map[date]['impressions'] += int(entry.get('impressions', 0))
                        date_map[date]['clicks'] += int(entry.get('clicks', 0))
                        date_map[date]['installs'] += int(entry.get('installs', 0))
                    total_impressions += int(entry.get('impressions', 0))
                    total_clicks += int(entry.get('clicks', 0))
                    total_installs += int(entry.get('installs', 0))
        trend_dates = sorted(date_map.keys())
        trend_impressions = [date_map[d]['impressions'] for d in trend_dates]
        trend_clicks = [date_map[d]['clicks'] for d in trend_dates]
        trend_installs = [date_map[d]['installs'] for d in trend_dates]

        # Use only the most recent 'last2:' fraud_cache entry for Top Fraudulent Sources
        c.execute("SELECT range FROM fraud_cache WHERE range LIKE 'last2:%' ORDER BY updated_at DESC LIMIT 1")
        fraud_cache_row = c.fetchone()
        top_bad_sources_by_app = []
        if fraud_cache_row:
            fraud_cache_key = fraud_cache_row[0]
            c.execute('SELECT data FROM fraud_cache WHERE range = ?', (fraud_cache_key,))
            fraud_row = c.fetchone()
            if fraud_row:
                fraud_data = json.loads(fraud_row[0])
                # For each app, group by media_source and sum fraud
                for app in fraud_data.get('apps', []):
                    app_name = app.get('app_name', '')
                    app_id = app.get('app_id', '')
                    source_map = {}
                    for row in app.get('table', []):
                        media_source = row.get('media_source', '')
                        pa_fraud = int(row.get('blocked_installs_pa', 0))
                        rt_fraud = int(row.get('blocked_installs_rt', 0))
                        if pa_fraud > 0 or rt_fraud > 0:
                            if media_source not in source_map:
                                source_map[media_source] = {'media_source': media_source, 'pa_fraud': 0, 'rt_fraud': 0}
                            source_map[media_source]['pa_fraud'] += pa_fraud
                            source_map[media_source]['rt_fraud'] += rt_fraud
                    # Top 5 sources for this app
                    sources = list(source_map.values())
                    sources.sort(key=lambda x: x['pa_fraud'] + x['rt_fraud'], reverse=True)
                    top_sources = sources[:5]
                    total_fraud = sum(s['pa_fraud'] + s['rt_fraud'] for s in top_sources)
                    top_bad_sources_by_app.append({
                        'app_id': app_id,
                        'app_name': app_name,
                        'sources': top_sources,
                        'total_fraud': total_fraud
                    })
                # Sort apps by total fraud, take top 5
                top_bad_sources_by_app.sort(key=lambda x: x['total_fraud'], reverse=True)
                top_bad_sources_by_app = top_bad_sources_by_app[:5]
        conn.close()

        return jsonify({
            'totals': {
                'impressions': total_impressions,
                'clicks': total_clicks,
                'installs': total_installs
            },
            'trend': {
                'dates': trend_dates,
                'impressions': trend_impressions,
                'clicks': trend_clicks,
                'installs': trend_installs
            },
            'topBadSourcesByApp': top_bad_sources_by_app,
            'last_updated': last_updated
        })
    except Exception as e:
        print(f"Error in overview endpoint: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/clear-backend-cache', methods=['POST'])
def clear_backend_cache():
    import sqlite3
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('DELETE FROM stats_cache')
        c.execute('DELETE FROM fraud_cache')
        c.execute('DELETE FROM event_cache')
        c.execute('DELETE FROM apps_cache')
        conn.commit()
        conn.close()
        return jsonify({'success': True})
    except Exception as e:
        print(f"[CACHE CLEAR ERROR] {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/clear-apps-cache', methods=['POST'])
def clear_apps_cache():
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        # Get current apps cache data
        c.execute('SELECT data FROM apps_cache ORDER BY updated_at DESC LIMIT 1')
        row = c.fetchone()
        
        # Get active status from app_event_selections
        c.execute('SELECT app_id, is_active FROM app_event_selections')
        active_status = dict(c.fetchall())
        
        active_app_ids = []
        if row:
            cached_data = json.loads(row[0])
            # Keep only apps that are marked as active in app_event_selections
            active_apps = [
                app for app in cached_data.get('apps', []) 
                if active_status.get(app['app_id'], 0) == 1
            ]
            active_app_ids = [app['app_id'] for app in active_apps]
            
            if active_apps:  # Only update cache if we have active apps
                # Update apps cache with only active apps
                new_cache_data = {
                    "count": len(active_apps),
                    "apps": active_apps,
                    "fetch_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                
                # Update the apps cache
                c.execute('DELETE FROM apps_cache')
                c.execute('INSERT INTO apps_cache (data, updated_at) VALUES (?, ?)',
                         (json.dumps(new_cache_data), new_cache_data['fetch_time']))
            else:
                # If no active apps, clear the entire cache
                c.execute('DELETE FROM apps_cache')
            
            # Clear events cache for non-active apps only
            # First, get all app IDs from events cache
            c.execute('SELECT app_id FROM event_cache')
            cached_event_apps = c.fetchall()
            
            # Remove events for non-active apps
            for (app_id,) in cached_event_apps:
                if app_id not in active_app_ids:
                    c.execute('DELETE FROM event_cache WHERE app_id = ?', (app_id,))
            
            conn.commit()
            
            return jsonify({
                "success": True,
                "message": "Successfully cleared cache for non-active apps",
                "active_apps_count": len(active_apps),
                "events_preserved": len(active_app_ids)
            })
        else:
            return jsonify({
                "success": True,
                "message": "No apps cache to clear",
                "active_apps_count": 0,
                "events_preserved": 0
            })
            
    except Exception as e:
        print(f"Error clearing apps cache: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500
    finally:
        conn.close()

@app.route('/clear-stats-cache', methods=['POST'])
def clear_stats_cache():
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('DELETE FROM stats_cache')
        conn.commit()
        conn.close()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/clear-fraud-cache', methods=['POST'])
def clear_fraud_cache():
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('DELETE FROM fraud_cache')
        conn.commit()
        conn.close()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/apps-page')
@login_required
def apps_page():
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        # Get the most recent apps cache
        c.execute('SELECT data, updated_at FROM apps_cache ORDER BY updated_at DESC LIMIT 1')
        row = c.fetchone()
        conn.close()
        
        if row:
            data, updated_at = row
            result = json.loads(data)
            result['updated_at'] = updated_at
            return jsonify(result)
        else:
            return jsonify({
                'count': 0,
                'apps': [],
                'fetch_time': None,
                'used_cache': False
            })
    except Exception as e:
        print(f"Error in apps_page endpoint: {str(e)}")
        return jsonify({
            'error': str(e),
            'count': 0,
            'apps': [],
            'fetch_time': None,
            'used_cache': False
        }), 500

@app.route('/api/stats-page')
def stats_page():
    return {'status': 'ok'}

@app.route('/api/fraud-page')
def fraud_page():
    return {'status': 'ok'}

def process_report_async(apps, period, selected_events):
    """Background task to process report data"""
    try:
        print(f"[REPORT] Starting async report processing for period: {period}")
        print(f"[REPORT] Processing {len(apps)} apps")
        
        start_date, end_date = get_period_dates(period)
        stats_list = []
        
        for app in apps:
            app_id = app['app_id']
            app_name = app['app_name']
            print(f"[REPORT] Processing app: {app_name} (App ID: {app_id})...")
            
            # Use the aggregate daily report endpoint for main stats
            url = f"https://hq1.appsflyer.com/api/agg-data/export/app/{app_id}/daily_report/v5"
            params = {"from": start_date, "to": end_date}
            
            try:
                print(f"[REPORT] Calling daily_report API for {app_id}...")
                resp = make_api_request(url, params)
                if resp == 'timeout':
                    print(f"[REPORT] Timeout detected for {app_id}, skipping to next app.")
                    continue
                
                daily_stats = {}
                if resp and resp.status_code == 200:
                    print(f"[REPORT] Got daily_report for {app_id}")
                    rows = resp.text.strip().split("\n")
                    if len(rows) < 2:  # Only header or empty
                        print(f"[REPORT] No data returned for {app_id}")
                        continue
                        
                    header = rows[0].split(",")
                    data_rows = [row.split(",") for row in rows[1:]]
                    
                    # Find column indices
                    def find_col(*names):
                        for name in names:
                            for col in header:
                                if col.lower().replace('_','').replace(' ','') == name.lower().replace('_','').replace(' ',''):
                                    return header.index(col)
                        return None

                    def safe_int(val):
                        try:
                            if val in ['', 'N/A', 'None', 'null']:
                                return 0
                            return int(float(val))
                        except (ValueError, TypeError):
                            return 0
                        
                    impressions_idx = find_col('impressions', 'Impressions')
                    clicks_idx = find_col('clicks', 'Clicks')
                    installs_idx = find_col('installs', 'Installs')
                    date_idx = find_col('date', 'Date')
                    
                    if None in [impressions_idx, clicks_idx, installs_idx, date_idx]:
                        print(f"[REPORT] Could not find all required columns for {app_id}")
                        continue
                        
                    # Process each row
                    for row in data_rows:
                        if len(row) <= max(impressions_idx, clicks_idx, installs_idx, date_idx):
                            continue
                            
                        date = row[date_idx]
                        if date not in daily_stats:
                            daily_stats[date] = {
                                "impressions": 0,
                                "clicks": 0,
                                "installs": 0,
                                "blocked_installs_rt": 0,
                                "blocked_installs_pa": 0
                            }
                            
                        daily_stats[date]["impressions"] += safe_int(row[impressions_idx])
                        daily_stats[date]["clicks"] += safe_int(row[clicks_idx])
                        daily_stats[date]["installs"] += safe_int(row[installs_idx])

                # Process additional data (blocked installs, events)
                # Add blocked installs data
                blocked_rt_url = f"https://hq1.appsflyer.com/api/raw-data/export/app/{app_id}/blocked_installs_report/v5"
                blocked_rt_resp = make_api_request(blocked_rt_url, params)
                if blocked_rt_resp and blocked_rt_resp.status_code == 200:
                    rows = blocked_rt_resp.text.strip().split("\n")
                    if len(rows) > 1:
                        header = rows[0].split(",")
                        date_idx = header.index("Install Time") if "Install Time" in header else None
                        for row in rows[1:]:
                            cols = row.split(",")
                            if date_idx is not None and len(cols) > date_idx:
                                install_date = cols[date_idx].split(" ")[0]
                                if install_date in daily_stats:
                                    daily_stats[install_date]["blocked_installs_rt"] += 1
                                    
                # Add event data if selected
                event_data = {}
                selected = selected_events.get(app_id, [])
                if selected:
                    events_url = f"https://hq1.appsflyer.com/api/raw-data/export/app/{app_id}/in_app_events_report/v5"
                    events_resp = make_api_request(events_url, params)
                    if events_resp and events_resp.status_code == 200:
                        rows = events_resp.text.strip().split("\n")
                        if len(rows) > 1:
                            header = rows[0].split(",")
                            event_name_idx = header.index("Event Name") if "Event Name" in header else None
                            event_time_idx = header.index("Event Time") if "Event Time" in header else None
                            
                            for row in rows[1:]:
                                cols = row.split(",")
                                if event_name_idx is not None and event_time_idx is not None and len(cols) > max(event_name_idx, event_time_idx):
                                    event_name = cols[event_name_idx]
                                    event_date = cols[event_time_idx].split(" ")[0]
                                    if event_name in selected:
                                        event_data.setdefault(event_name, {})
                                        event_data[event_name].setdefault(event_date, 0)
                                        event_data[event_name][event_date] += 1
                                        
                # Prepare daily stats for frontend
                all_dates = sorted(daily_stats.keys())
                table = []
                for date in all_dates:
                    row = {
                        "date": date,
                        "impressions": daily_stats[date]["impressions"],
                        "clicks": daily_stats[date]["clicks"],
                        "installs": daily_stats[date]["installs"],
                        "blocked_installs_rt": daily_stats[date]["blocked_installs_rt"],
                        "blocked_installs_pa": daily_stats[date]["blocked_installs_pa"]
                    }
                    
                    # Add calculated rates
                    row["imp_to_click"] = round(row["clicks"] / row["impressions"], 2) if row["impressions"] > 0 else 0
                    row["click_to_install"] = round(row["installs"] / row["clicks"], 2) if row["clicks"] > 0 else 0
                    row["blocked_rt_rate"] = round(row["blocked_installs_rt"] / row["installs"], 2) if row["installs"] > 0 else 0
                    row["blocked_pa_rate"] = round(row["blocked_installs_pa"] / row["installs"], 2) if row["installs"] > 0 else 0
                    
                    # Add event counts
                    if selected:
                        for event in selected:
                            row[event] = event_data.get(event, {}).get(date, 0)
                            
                    table.append(row)
                    
                stats_list.append({
                    'app_id': app_id,
                    'app_name': app_name,
                    'table': table,
                    'selected_events': selected,
                    'traffic': sum(r['impressions'] + r['clicks'] for r in table)
                })
                
            except Exception as e:
                print(f"[REPORT] Error processing app {app_id}: {str(e)}")
                continue
            except BrokenPipeError as e:
                print(f"[REPORT] BrokenPipeError (EPIPE) for app {app_id}: {str(e)}. Skipping to next app.")
                continue
                
        # Save to cache
        if stats_list:
            app_ids = '-'.join(sorted([app['app_id'] for app in apps]))
            event1 = ''
            event2 = ''
            if apps and selected_events:
                first_app_id = apps[0]['app_id']
                events = selected_events.get(first_app_id, [])
                if len(events) > 0:
                    event1 = events[0] or ''
                if len(events) > 1:
                    event2 = events[1] or ''
            cache_key = f"{period}:{event1}:{event2}:{app_ids}"
            
            result = {
                'apps': stats_list,
                'updated_at': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute('REPLACE INTO stats_cache (range, data, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)',
                     (cache_key, json.dumps(result)))
            conn.commit()
            conn.close()
            
            return result
            
    except BrokenPipeError as e:
        print(f"[REPORT] BrokenPipeError (EPIPE) at outer level: {str(e)}. Returning empty result so frontend can proceed.")
        return {'apps': [], 'error': 'BrokenPipeError (EPIPE) occurred'}
    except Exception as e:
        print(f"[REPORT] Error in process_report_async: {str(e)}")
        raise
    return {'apps': [], 'error': 'Failed to process report'}

@app.route('/start-report', methods=['POST'])
@login_required
def start_report():
    try:
        data = request.get_json()
        apps = data.get('apps', [])
        period = 'last30'  # Force last 30 days period
        selected_events = data.get('events', [])

        if not apps:
            return jsonify({'error': 'No apps selected'}), 400

        # Start the report generation in the background
        job = task_queue.enqueue(
            process_report_async,
            args=(apps, period, selected_events),
            job_timeout='1h'
        )

        return jsonify({
            'job_id': job.id,
            'status': 'started',
            'message': 'Report generation started'
        })
    except Exception as e:
        print(f"Error starting report: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/report-status/<job_id>')
@login_required
def report_status(job_id):
    job = task_queue.fetch_job(job_id)
    if job is None:
        return jsonify({'status': 'not_found'})
    
    if job.is_finished:
        return jsonify({
            'status': 'completed',
            'result': job.result
        })
    elif job.is_failed:
        return jsonify({
            'status': 'failed',
            'error': str(job.exc_info)
        })
    else:
        return jsonify({'status': 'processing'})

# Add this new route to handle app status updates
@app.route('/update-app-status', methods=['POST'])
@login_required
def update_app_status():
    data = request.get_json()
    app_id = data.get('app_id')
    is_active = data.get('is_active')
    
    if not app_id:
        return jsonify({'success': False, 'message': 'App ID is required'}), 400
        
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    try:
        # First check if there's an existing record
        c.execute('SELECT event1, event2 FROM app_event_selections WHERE app_id = ?', (app_id,))
        existing = c.fetchone()
        
        if existing:
            # Update existing record
            event1, event2 = existing
            c.execute('''UPDATE app_event_selections 
                        SET is_active = ? 
                        WHERE app_id = ?''', 
                     (1 if is_active else 0, app_id))
        else:
            # Insert new record
            c.execute('''INSERT INTO app_event_selections 
                        (app_id, event1, event2, is_active) 
                        VALUES (?, NULL, NULL, ?)''', 
                     (app_id, 1 if is_active else 0))
        
        conn.commit()
        
        # Update apps cache to reflect the new status
        c.execute('SELECT data FROM apps_cache ORDER BY updated_at DESC LIMIT 1')
        cache_row = c.fetchone()
        if cache_row:
            cached_data = json.loads(cache_row[0])
            for app in cached_data.get('apps', []):
                if app['app_id'] == app_id:
                    app['is_active'] = is_active
            
            # Update the cache with the modified data
            c.execute('UPDATE apps_cache SET data = ? WHERE data = ?', 
                     (json.dumps(cached_data), cache_row[0]))
            conn.commit()
        
        return jsonify({'success': True})
    except Exception as e:
        print(f"Error updating app status: {str(e)}")
        return jsonify({'success': False, 'message': str(e)}), 500
    finally:
        conn.close()

# Modify the get_active_apps function to include the active status
def get_active_app_ids():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT app_id FROM app_event_selections WHERE is_active = 1')
    active_apps = [row[0] for row in c.fetchall()]
    conn.close()
    return active_apps

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
