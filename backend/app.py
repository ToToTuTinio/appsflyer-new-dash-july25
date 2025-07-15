from flask import Flask, jsonify, render_template, request, session, redirect, url_for, Response
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
import logging

# Configure logging for Railway
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Initialize Redis connection for Railway (or localhost)
import os
from urllib.parse import urlparse

# Get Redis URL from environment variable (Railway provides REDIS_URL)
redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
logger.info(f"🔍 Redis URL: {redis_url}")

try:
    # Parse Redis URL to extract connection parameters
    parsed_url = urlparse(redis_url)
    redis_host = parsed_url.hostname or 'localhost'
    redis_port = parsed_url.port or 6379
    redis_db = int(parsed_url.path.lstrip('/')) if parsed_url.path and len(parsed_url.path) > 1 else 0
    
    logger.info(f"🔍 Connecting to Redis at {redis_host}:{redis_port} (db: {redis_db})")

# Initialize Redis connection
    redis_conn = Redis(host=redis_host, port=redis_port, db=redis_db, decode_responses=True)
    
    # Test Redis connection
    redis_conn.ping()
    logger.info(f"✅ Redis connected successfully to {redis_host}:{redis_port}")
    
    # Initialize RQ queue
    task_queue = Queue(connection=redis_conn)
    
except Exception as e:
    logger.warning(f"⚠️  Redis connection failed: {e}")
    logger.info("📝 Background tasks will be disabled")
    redis_conn = None
    task_queue = None

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from appsflyer_login import get_apps_with_installs

# Get the project root directory and load environment variables
project_root = Path(__file__).parent.parent
env_path = project_root / '.env.local'

logger.info(f"Looking for .env file at: {env_path}")
logger.info(f"File exists: {env_path.exists()}")

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

# Initialize rate limiter with Redis if available
if redis_conn:
    limiter = Limiter(
        app=app,
        key_func=get_remote_address,
        default_limits=["10000 per day", "5000 per hour"],
        storage_uri=redis_url
    )
    logger.info("✅ Rate limiter using Redis backend")
else:
    limiter = Limiter(
        app=app,
        key_func=get_remote_address,
        default_limits=["10000 per day", "5000 per hour"]
    )
    logger.info("⚠️  Rate limiter using in-memory storage (not recommended for production)")

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

# Database path - use persistent volume in Railway, fallback to local for development
# More reliable Railway detection - Railway sets multiple environment variables
def is_railway_environment():
    railway_vars = [
        'RAILWAY_ENVIRONMENT',
        'RAILWAY_SERVICE_NAME', 
        'RAILWAY_PROJECT_ID',
        'RAILWAY_DEPLOYMENT_ID',
        'RAILWAY_REPLICA_ID'
    ]
    return any(os.getenv(var) for var in railway_vars)

DB_PATH = os.getenv('DB_PATH', '/data/event_selections.db' if is_railway_environment() else 'event_selections.db')

# Debug logging for Railway environment detection
print(f"🔍 Railway Environment Detection:")
print(f"  RAILWAY_ENVIRONMENT: {os.getenv('RAILWAY_ENVIRONMENT')}")
print(f"  RAILWAY_SERVICE_NAME: {os.getenv('RAILWAY_SERVICE_NAME')}")
print(f"  RAILWAY_PROJECT_ID: {os.getenv('RAILWAY_PROJECT_ID')}")
print(f"  RAILWAY_DEPLOYMENT_ID: {os.getenv('RAILWAY_DEPLOYMENT_ID')}")
print(f"  RAILWAY_REPLICA_ID: {os.getenv('RAILWAY_REPLICA_ID')}")
print(f"  Is Railway Environment: {is_railway_environment()}")
print(f"  Using DB_PATH: {DB_PATH}")
print(f"  Database file exists: {os.path.exists(DB_PATH)}")
if is_railway_environment():
    print(f"  /data directory exists: {os.path.exists('/data')}")
    print(f"  /data directory contents: {os.listdir('/data') if os.path.exists('/data') else 'N/A'}")
print(f"=========================================")

def init_db():
    # Ensure the database directory exists (for persistent storage)
    db_dir = os.path.dirname(DB_PATH)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)
        logger.info(f"Created database directory: {db_dir}")
    
    logger.info(f"Initializing database at: {DB_PATH}")
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
    
    # Create table for storing original raw AppsFlyer CSV data
    c.execute('''CREATE TABLE IF NOT EXISTS raw_appsflyer_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        app_id TEXT NOT NULL,
        app_name TEXT NOT NULL,
        endpoint_type TEXT NOT NULL,
        period TEXT NOT NULL,
        raw_csv_data TEXT NOT NULL,
        start_date TEXT NOT NULL,
        end_date TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(app_id, endpoint_type, period, start_date, end_date)
    )''')
    
    # Create table for auto-run timing management
    c.execute('''CREATE TABLE IF NOT EXISTS auto_run_settings (
        id INTEGER PRIMARY KEY DEFAULT 1,
        last_run_time TEXT,
        next_run_time TEXT,
        auto_run_enabled INTEGER DEFAULT 1,
        auto_run_interval_hours INTEGER DEFAULT 6,
        is_running INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    
    # Create table for manual apps
    c.execute('''CREATE TABLE IF NOT EXISTS manual_apps (
        app_id TEXT PRIMARY KEY,
        app_name TEXT NOT NULL,
        status TEXT DEFAULT 'active',
        event1 TEXT,
        event2 TEXT,
        is_active INTEGER DEFAULT 1,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    
    # Initialize auto-run settings with default values if not exists
    c.execute('''INSERT OR IGNORE INTO auto_run_settings (id) VALUES (1)''')
    
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

def get_active_apps(max_retries=7, force_fetch=False, allow_appsflyer_api=True):
    """
    Fetch the list of active apps from cache, optionally fetch from AppsFlyer if cache is old.
    
    Args:
        max_retries: Maximum retries for AppsFlyer API calls
        force_fetch: Force fetch from AppsFlyer even if cache is fresh
        allow_appsflyer_api: Whether to allow AppsFlyer API calls at all (False = cache only)
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
        
        # Always use cached data if available (no time expiration)
        cached_data = json.loads(data)
        
        # Update active status from database for all apps
        for app in cached_data.get('apps', []):
            # If app exists in database, use its status, otherwise default to active (True)
            app['is_active'] = bool(active_status.get(app['app_id'], 1))
            # Mark existing apps as non-manual if not already marked
            if 'is_manual' not in app:
                app['is_manual'] = False
        
        # Add manual apps to cached data
        c.execute('''SELECT app_id, app_name, status, event1, event2, is_active 
                     FROM manual_apps ORDER BY app_name''')
        manual_apps = c.fetchall()
        
        for manual_app in manual_apps:
            app_id, app_name, status, event1, event2, is_active = manual_app
            cached_data['apps'].append({
                'app_id': app_id,
                'app_name': app_name,
                'status': status,
                'event1': event1,
                'event2': event2,
                'is_active': bool(is_active),
                'is_manual': True  # Mark as manual apps
            })
        
        # Update count to include manual apps
        cached_data['count'] = len(cached_data['apps'])
        
        # Return cached data regardless of age
        cached_data['fetch_time'] = updated_at_dt.strftime('%Y-%m-%d %H:%M:%S')
        cached_data['used_cache'] = True
        conn.close()
        return cached_data

    # If no cache or cache is old, check if we're allowed to call AppsFlyer API
    if not allow_appsflyer_api:
        # Return cached data even if it's old, or empty list if no cache
        if row:
            cached_data = json.loads(data)
            # Update active status from database for all apps
            for app in cached_data.get('apps', []):
                app['is_active'] = bool(active_status.get(app['app_id'], 1))
                if 'is_manual' not in app:
                    app['is_manual'] = False
            
            # Add manual apps to cached data
            c.execute('''SELECT app_id, app_name, status, event1, event2, is_active 
                         FROM manual_apps ORDER BY app_name''')
            manual_apps = c.fetchall()
            
            for manual_app in manual_apps:
                app_id, app_name, status, event1, event2, is_active = manual_app
                cached_data['apps'].append({
                    'app_id': app_id,
                    'app_name': app_name,
                    'status': status,
                    'event1': event1,
                    'event2': event2,
                    'is_active': bool(is_active),
                    'is_manual': True
                })
            
            cached_data['count'] = len(cached_data['apps'])
            cached_data['used_cache'] = True
            conn.close()
            return cached_data
        else:
            # No cache and not allowed to call API, return just manual apps
            manual_apps = []
            c.execute('''SELECT app_id, app_name, status, event1, event2, is_active 
                         FROM manual_apps ORDER BY app_name''')
            manual_apps_data = c.fetchall()
            
            for manual_app in manual_apps_data:
                app_id, app_name, status, event1, event2, is_active = manual_app
                manual_apps.append({
                    'app_id': app_id,
                    'app_name': app_name,
                    'status': status,
                    'event1': event1,
                    'event2': event2,
                    'is_active': bool(is_active),
                    'is_manual': True
                })
            
            conn.close()
            return {
                "count": len(manual_apps),
                "apps": manual_apps,
                "fetch_time": now.strftime('%Y-%m-%d %H:%M:%S'),
                "used_cache": True
            }

    # If allowed to call API, fetch new data from AppsFlyer
    apps = get_apps_with_installs(EMAIL, PASSWORD, max_retries=max_retries)
    
    # Add active status to each app, defaulting to active (True) if not in database
    for app in apps:
        app['is_active'] = bool(active_status.get(app['app_id'], 1))
        app['is_manual'] = False  # Mark as synced apps
    
    # Get manual apps and add them to the list
    c.execute('''SELECT app_id, app_name, status, event1, event2, is_active 
                 FROM manual_apps ORDER BY app_name''')
    manual_apps = c.fetchall()
    
    for manual_app in manual_apps:
        app_id, app_name, status, event1, event2, is_active = manual_app
        apps.append({
            'app_id': app_id,
            'app_name': app_name,
            'status': status,
            'event1': event1,
            'event2': event2,
            'is_active': bool(is_active),
            'is_manual': True  # Mark as manual apps
        })
    
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
    """Legacy endpoint - uses cache only to avoid unexpected AppsFlyer API calls"""
    try:
        result = get_active_apps(allow_appsflyer_api=False)
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

def save_raw_appsflyer_data(app_id, app_name, endpoint_type, period, raw_csv_data, start_date, end_date):
    """Save original raw AppsFlyer CSV data to database"""
    try:
        if not raw_csv_data or len(raw_csv_data.strip()) == 0:
            print(f"[RAW_DATA] Skipping save - no data for {app_id} {endpoint_type}")
            return
            
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        # Replace existing data for the same app/endpoint/period/date range
        c.execute('''INSERT OR REPLACE INTO raw_appsflyer_data 
                     (app_id, app_name, endpoint_type, period, raw_csv_data, start_date, end_date, created_at) 
                     VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)''',
                  (app_id, app_name, endpoint_type, period, raw_csv_data, start_date, end_date))
        
        conn.commit()
        conn.close()
        
        # Get data size in a readable format
        data_size = len(raw_csv_data)
        if data_size < 1024:
            size_str = f"{data_size} bytes"
        elif data_size < 1024 * 1024:
            size_str = f"{data_size / 1024:.1f} KB"
        else:
            size_str = f"{data_size / (1024 * 1024):.1f} MB"
            
        print(f"[RAW_DATA] Saved {endpoint_type} data for {app_name} ({app_id}) - {size_str}")
        
    except Exception as e:
        print(f"[RAW_DATA] Error saving raw data for {app_id} {endpoint_type}: {str(e)}")

def make_api_request(url, params, max_retries=7, retry_delay=30, app_id=None, app_name=None, period=None):
    """Make API request to AppsFlyer and optionally save raw data"""
    headers = {
        "Authorization": f"Bearer {APPSFLYER_API_KEY}",
        "accept": "text/csv"
    }
    
    # Extract endpoint type from URL for saving raw data
    endpoint_type = None
    if app_id and app_name and period:
        if "daily_report" in url:
            endpoint_type = "daily_report"
        elif "blocked_installs_report" in url:
            endpoint_type = "blocked_installs_report"
        elif "installs_report" in url:
            endpoint_type = "installs_report"
        elif "detection" in url:
            endpoint_type = "detection"
        elif "blocked_in_app_events_report" in url:
            endpoint_type = "blocked_in_app_events_report"
        elif "fraud-post-inapps" in url:
            endpoint_type = "fraud_post_inapps"
        elif "blocked_clicks_report" in url:
            endpoint_type = "blocked_clicks_report"
        elif "blocked_install_postbacks" in url:
            endpoint_type = "blocked_install_postbacks"
        elif "in_app_events_report" in url:
            endpoint_type = "in_app_events_report"
    
    for attempt in range(max_retries):
        try:
            print(f"[API] Making request to {url} (attempt {attempt + 1}/{max_retries})")
            resp = requests.get(url, headers=headers, params=params, timeout=90)
            if resp.status_code == 200:
                # Save raw data if we have all required info
                if endpoint_type and app_id and app_name and period:
                    start_date = params.get('from', '')
                    end_date = params.get('to', '')
                    save_raw_appsflyer_data(app_id, app_name, endpoint_type, period, resp.text, start_date, end_date)
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
    
    # Add comprehensive processing tracking
    total_apps = len(active_apps)
    processed_apps = 0
    skipped_apps = 0
    
    print(f"[STATS] Starting stats data processing for {total_apps} apps...")
    
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
        
        timeout_count = 0
        app_errors = []
        
        # Use the aggregate daily report endpoint for main stats
        url = f"https://hq1.appsflyer.com/api/agg-data/export/app/{app_id}/daily_report/v5"
        params = {"from": start_date, "to": end_date}
        
        try:
            print(f"[STATS] Calling daily_report API for {app_id}...")
            resp = make_api_request(url, params, app_id=app_id, app_name=app_name, period=period)
            if resp == 'timeout':
                print(f"[STATS] Timeout detected for daily_report {app_id}, continuing with other APIs...")
                timeout_count += 1
                app_errors.append("Daily Report API timeout")
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
            # Installs Report (for raw data export)
            print(f"[STATS] Calling installs_report API for {app_id}...")
            installs_url = f"https://hq1.appsflyer.com/api/raw-data/export/app/{app_id}/installs_report/v5"
            installs_params = {"from": start_date, "to": end_date}
            installs_resp = make_api_request(installs_url, installs_params, app_id=app_id, app_name=app_name, period=period)
            if installs_resp == 'timeout':
                print(f"[STATS] Timeout detected for installs_report {app_id}, continuing with other APIs...")
                timeout_count += 1
                app_errors.append("Installs Report API timeout")
            
            # Blocked Installs (RT)
            print(f"[STATS] Calling blocked_installs_report API for {app_id}...")
            blocked_rt_url = f"https://hq1.appsflyer.com/api/raw-data/export/app/{app_id}/blocked_installs_report/v5"
            blocked_rt_params = {"from": start_date, "to": end_date}
            blocked_rt_resp = make_api_request(blocked_rt_url, blocked_rt_params, app_id=app_id, app_name=app_name, period=period)
            if blocked_rt_resp == 'timeout':
                print(f"[STATS] Timeout detected for blocked_installs_report {app_id}, continuing with other APIs...")
                timeout_count += 1
                app_errors.append("Blocked Installs (RT) API timeout")
            
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
            blocked_pa_resp = make_api_request(blocked_pa_url, blocked_pa_params, app_id=app_id, app_name=app_name, period=period)
            if blocked_pa_resp == 'timeout':
                print(f"[STATS] Timeout detected for detection API {app_id}, continuing with other APIs...")
                timeout_count += 1
                app_errors.append("Blocked Installs (PA) API timeout")
            
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
                events_resp = make_api_request(events_url, events_params, app_id=app_id, app_name=app_name, period=period)
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
            # Determine if we should skip this app entirely
            if timeout_count >= 3:  # All 3 main API calls timed out
                print(f"[STATS] Skipping app {app_name} ({app_id}) - all API calls timed out")
                skipped_apps += 1
                continue
                
            print(f"[STATS] Successfully processed app {app_name} ({app_id}) with {timeout_count} timeouts")
            processed_apps += 1
            
            stats_list.append({
                'app_id': app_id,
                'app_name': app_name,
                'table': table,
                'selected_events': selected,
                'traffic': sum(r['impressions'] + r['clicks'] for r in table),
                'errors': app_errors
            })
        except Exception as e:
            print(f"[STATS] Error for app {app_id}: {e}")
            skipped_apps += 1
            stats_list.append({
                'app_id': app_id,
                'app_name': app_name,
                'table': [],
                'selected_events': [],
                'traffic': 0,
                'error': str(e)
            })
    stats_list.sort(key=lambda x: x['traffic'], reverse=True)
    
    # Save to cache ONLY if there is at least one app
    if len(stats_list) > 0:
        c.execute('REPLACE INTO stats_cache (range, data, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)', (cache_key, json.dumps({'apps': stats_list})))
        conn.commit()
        print(f"[STATS] Saved {len(stats_list)} apps to cache with key: {cache_key}")
    else:
        print(f"[STATS] No apps to cache - stats_list is empty")
        
    conn.close()
    
    # Final completion logging
    print(f"[STATS] ===== STATS PROCESSING COMPLETED =====")
    print(f"[STATS] Total apps requested: {total_apps}")
    print(f"[STATS] Apps successfully processed: {processed_apps}")
    print(f"[STATS] Apps skipped due to timeouts: {skipped_apps}")
    print(f"[STATS] Apps included in response: {len(stats_list)}")
    print(f"[STATS] Returning response with {len(stats_list)} apps")
    print(f"[STATS] ==========================================")
    
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
        saved_count = 0
        
        # Check if it's a single update or bulk update
        if isinstance(data, dict) and 'app_id' in data:
            # Single update
            app_id = data.get('app_id')
            event1 = data.get('event1')
            event2 = data.get('event2')
            is_active = data.get('is_active', False)
            
            print(f"[SAVE] Single update for app {app_id}: event1={event1}, event2={event2}, active={is_active}")
            
            c.execute('''INSERT OR REPLACE INTO app_event_selections 
                        (app_id, event1, event2, is_active) 
                        VALUES (?, ?, ?, ?)''', 
                     (app_id, event1, event2, 1 if is_active else 0))
            saved_count = 1
        else:
            # Bulk update
            print(f"[SAVE] Bulk update for {len(data)} apps")
            for app_id, app_data in data.items():
                event1 = app_data.get('event1')
                event2 = app_data.get('event2')
                is_active = app_data.get('is_active', False)
                app_name = app_data.get('app_name', app_id)
                
                print(f"[SAVE] - App {app_name} ({app_id}): event1={event1}, event2={event2}, active={is_active}")
                
                c.execute('''INSERT OR REPLACE INTO app_event_selections 
                            (app_id, event1, event2, is_active) 
                            VALUES (?, ?, ?, ?)''', 
                         (app_id, event1, event2, 1 if is_active else 0))
                saved_count += 1
        
        conn.commit()
        print(f"[SAVE] Successfully saved {saved_count} app configurations to database (permanent storage)")
        return jsonify({"success": True, "saved_count": saved_count})
    except Exception as e:
        print(f"[SAVE] Error saving event selections: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        conn.close()

@app.route('/get_apps')
@login_required
def get_apps():
    """Only this endpoint should trigger AppsFlyer API calls (via 'Sync Apps' button)"""
    try:
        result = get_active_apps(allow_appsflyer_api=True)
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

@app.route('/get_stats')
@login_required
def get_stats():
    try:
        range_key = request.args.get('range', '10d')
        force = request.args.get('force', '0') == '1'
        # If force, trigger a new fetch (client should call /all-apps-stats)
        if force:
            return jsonify({'error': 'Force fetch not implemented in /get_stats. Please use /all-apps-stats.'}), 400
        # Return the most recent stats if available
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        # Use LIKE query for all ranges to ensure consistent behavior
        c.execute("SELECT data, updated_at FROM stats_cache WHERE range LIKE ? ORDER BY updated_at DESC LIMIT 1", (f"{range_key}%",))
        row = c.fetchone()
        conn.close()
        if row:
            data, updated_at = row
            result = json.loads(data)
            result['updated_at'] = updated_at
            return jsonify(result)
        else:
            return jsonify({'apps': [], 'updated_at': None})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/update-credential', methods=['POST'])
@login_required
def update_credential():
    allowed_keys = {'APPSFLYER_API_KEY', 'EMAIL', 'PASSWORD'}
    data = request.get_json()
    key = data.get('key')
    value = data.get('value')
    
    # Validate input
    if not key or not value:
        return jsonify({'success': False, 'error': 'Key and value are required'}), 400
    
    if key not in allowed_keys:
        return jsonify({'success': False, 'error': 'Invalid key'}), 400
    
    # Basic validation for specific keys
    if key == 'EMAIL' and '@' not in value:
        return jsonify({'success': False, 'error': 'Invalid email format'}), 400
    
    if key == 'APPSFLYER_API_KEY' and len(value.strip()) < 10:
        return jsonify({'success': False, 'error': 'API key appears to be too short'}), 400
    
    if key == 'PASSWORD' and len(value.strip()) < 4:
        return jsonify({'success': False, 'error': 'Password must be at least 4 characters'}), 400
    
    env_path = Path(__file__).parent.parent / '.env.local'
    backup_path = Path(__file__).parent.parent / '.env.backup'
    
    try:
        # Ensure parent directory exists
        env_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Create backup of current .env.local file
        if env_path.exists():
            with open(env_path, 'r') as original:
                with open(backup_path, 'w') as backup:
                    backup.write(original.read())
            print(f"[CREDENTIAL] Created backup of .env.local at {backup_path}")
        
        # Read all lines
        lines = []
        found = False
        if env_path.exists():
            with open(env_path, 'r') as f:
                for line in f:
                    if line.strip().startswith(f'{key}='):
                        lines.append(f'{key}="{value}"\n')
                        found = True
                        print(f"[CREDENTIAL] Updated existing {key} in .env.local")
                    else:
                        lines.append(line)
        
        # If key not found, add it
        if not found:
            lines.append(f'{key}="{value}"\n')
            print(f"[CREDENTIAL] Added new {key} to .env.local")
        
        # Write updated content
        with open(env_path, 'w') as f:
            f.writelines(lines)
        
        # Verify file was written correctly
        if not env_path.exists():
            raise Exception("Failed to write .env.local file")
        
        # Update in-memory env var
        os.environ[key] = value
        
        print(f"[CREDENTIAL] Successfully updated {key} in .env.local and memory")
        print(f"[CREDENTIAL] File location: {env_path.absolute()}")
        print(f"[CREDENTIAL] File permissions: {oct(env_path.stat().st_mode)[-3:]}")
        print(f"[CREDENTIAL] File size: {env_path.stat().st_size} bytes")
        
        return jsonify({
            'success': True, 
            'message': f'{key} updated successfully',
            'file_path': str(env_path.absolute()),
            'backup_created': backup_path.exists()
        })
        
    except PermissionError:
        error_msg = f"Permission denied writing to {env_path}. Check file permissions."
        print(f"[CREDENTIAL] ERROR: {error_msg}")
        return jsonify({'success': False, 'error': error_msg}), 500
    except Exception as e:
        error_msg = f"Failed to update credential: {str(e)}"
        print(f"[CREDENTIAL] ERROR: {error_msg}")
        
        # Try to restore from backup if it exists
        if backup_path.exists() and env_path.exists():
            try:
                with open(backup_path, 'r') as backup:
                    with open(env_path, 'w') as original:
                        original.write(backup.read())
                print(f"[CREDENTIAL] Restored .env.local from backup due to error")
            except:
                print(f"[CREDENTIAL] Failed to restore backup")
        
        return jsonify({'success': False, 'error': error_msg}), 500

@app.route('/profile-info')
@login_required
def profile_info():
    return jsonify({
        'agency': 'N/A',
        'api_key': os.getenv('APPSFLYER_API_KEY', ''),
        'email': os.getenv('EMAIL', ''),
        'password': os.getenv('PASSWORD', '')
    })

@app.route('/env-status')
@login_required
def env_status():
    """Debug endpoint to check .env.local file status for deployment"""
    env_path = Path(__file__).parent.parent / '.env.local'
    backup_path = Path(__file__).parent.parent / '.env.backup'
    
    status = {
        'env_file_exists': env_path.exists(),
        'env_file_path': str(env_path.absolute()),
        'env_file_readable': False,
        'env_file_writable': False,
        'env_file_size': 0,
        'env_file_permissions': None,
        'backup_exists': backup_path.exists(),
        'credentials_loaded': {
            'EMAIL': bool(os.getenv('EMAIL')),
            'PASSWORD': bool(os.getenv('PASSWORD')),
            'APPSFLYER_API_KEY': bool(os.getenv('APPSFLYER_API_KEY')),
        },
        'deployment_ready': False
    }
    
    try:
        if env_path.exists():
            # Check file permissions and readability
            stat_info = env_path.stat()
            status['env_file_size'] = stat_info.st_size
            status['env_file_permissions'] = oct(stat_info.st_mode)[-3:]
            
            # Test readability
            try:
                with open(env_path, 'r') as f:
                    content = f.read()
                    status['env_file_readable'] = True
                    status['env_lines_count'] = len([line for line in content.split('\n') if line.strip()])
            except:
                status['env_file_readable'] = False
            
            # Test writability
            try:
                test_path = env_path.parent / '.env.test'
                with open(test_path, 'w') as f:
                    f.write('test')
                test_path.unlink()  # Delete test file
                status['env_file_writable'] = True
            except:
                status['env_file_writable'] = False
        
        # Check if deployment ready
        status['deployment_ready'] = (
            status['env_file_exists'] and 
            status['env_file_readable'] and 
            status['env_file_writable'] and
            all(status['credentials_loaded'].values())
        )
        
        print(f"[ENV-STATUS] Environment status check: {status}")
        
    except Exception as e:
        print(f"[ENV-STATUS] Error checking environment status: {e}")
        status['error'] = str(e)
    
    return jsonify(status)

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

@app.route('/get_fraud', methods=['POST'])
@login_required
def get_fraud():
    try:
        data = request.get_json()
        active_apps = data.get('apps', [])
        period = data.get('period', 'last10')
        force = data.get('force', False)
        start_date, end_date = get_period_dates(period)
        # Create a unique cache key based on period and sorted app IDs
        app_ids = '-'.join(sorted([app['app_id'] for app in active_apps]))
        cache_key = f"{period}:{app_ids}"
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS fraud_cache (
            range TEXT PRIMARY KEY,
            data TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        conn.commit()
        if not force:
            # Use LIKE query for all ranges to ensure consistent behavior
            c.execute("SELECT data, updated_at FROM fraud_cache WHERE range LIKE ? ORDER BY updated_at DESC LIMIT 1", (f"{period}%",))
            row = c.fetchone()
            if row:
                data, updated_at = row
                result = json.loads(data)
                # Only use cache if it contains at least one app
                if result.get('apps') and len(result['apps']) > 0:
                    result['updated_at'] = updated_at
                    conn.close()
                    return jsonify(result)
        fraud_list = []
        total_apps = len(active_apps)
        processed_apps = 0
        skipped_apps = 0
        
        print(f"[FRAUD] Starting fraud data processing for {total_apps} apps...")
        
        for app in active_apps:
            app_id = app['app_id']
            app_name = app['app_name']
            print(f"[FRAUD] Fetching fraud data for app: {app_name} (App ID: {app_id})...")
            table = []
            app_errors = []
            timeout_count = 0
            
            # Helper: aggregate by (date, media_source)
            agg = {}
            def add_metric(date, media_source, key, count=1):
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
                        "blocked_install_postbacks": 0,
                        "event1": 0,
                        "event2": 0
                    }
                agg[k][key] += count
            
            # Installs Report (for raw data export)
            print(f"[FRAUD] Calling installs_report API for {app_id}...")
            installs_url = f"https://hq1.appsflyer.com/api/raw-data/export/app/{app_id}/installs_report/v5"
            installs_params = {"from": start_date, "to": end_date}
            installs_resp = make_api_request(installs_url, installs_params, app_id=app_id, app_name=app_name, period=period)
            if installs_resp == 'timeout':
                print(f"[FRAUD] Timeout detected for installs_report {app_id}, continuing with other APIs...")
                timeout_count += 1
                app_errors.append("Installs Report API timeout")
            
            # Blocked Installs (RT)
            blocked_rt_url = f"https://hq1.appsflyer.com/api/raw-data/export/app/{app_id}/blocked_installs_report/v5"
            blocked_rt_params = {"from": start_date, "to": end_date}
            blocked_rt_resp = make_api_request(blocked_rt_url, blocked_rt_params, app_id=app_id, app_name=app_name, period=period)
            if blocked_rt_resp == 'timeout':
                print(f"[FRAUD] Timeout detected for blocked_installs_report {app_id}, continuing with other APIs...")
                timeout_count += 1
                app_errors.append("Blocked Installs (RT) API timeout")
            if blocked_rt_resp and blocked_rt_resp.status_code == 200:
                rt_text = blocked_rt_resp.text.strip()
                if rt_text:
                    import io
                    csv_reader = csv.reader(io.StringIO(rt_text))
                    rows = list(csv_reader)
                    print(f"[FRAUD] Blocked Installs (RT) for app {app_id}: {len(rows)} rows received")
                    if len(rows) > 1:
                        header = rows[0]
                        print(f"[FRAUD] Blocked Installs (RT) header: {header}")
                        date_idx = header.index("Install Time") if "Install Time" in header else None
                        ms_idx = find_media_source_idx(header)
                        print(f"[FRAUD] Blocked Installs (RT) indices - date_idx: {date_idx}, ms_idx: {ms_idx}")
                        if ms_idx is None:
                            print(f"[FRAUD] ERROR: Could not find exact 'Media Source' column in Blocked Installs (RT) for app {app_id}. Header: {header}")
                        rt_count = 0
                        for row in rows[1:]:
                            if date_idx is not None and len(row) > date_idx:
                                install_date = row[date_idx].split(" ")[0]
                                media_source = row[ms_idx].strip() if ms_idx is not None and len(row) > ms_idx else "Unknown"
                                add_metric(install_date, media_source, "blocked_installs_rt")
                                rt_count += 1
                        print(f"[FRAUD] Blocked Installs (RT) for app {app_id}: {rt_count} records processed")
                    else:
                        print(f"[FRAUD] Blocked Installs (RT) for app {app_id}: No data rows (header only)")
            elif blocked_rt_resp is not None:
                print(f"[FRAUD] Blocked Installs (RT) API error for app {app_id}: {blocked_rt_resp.status_code} {blocked_rt_resp.text[:200]}")
                app_errors.append(f"Blocked Installs (RT) API error: {blocked_rt_resp.status_code} {blocked_rt_resp.text[:200]}")
            else:
                print(f"[FRAUD] Blocked Installs (RT) for app {app_id}: No response received")
            # Blocked Installs (PA)
            blocked_pa_url = f"https://hq1.appsflyer.com/api/raw-data/export/app/{app_id}/detection/v5"
            blocked_pa_params = {"from": start_date, "to": end_date}
            blocked_pa_resp = make_api_request(blocked_pa_url, blocked_pa_params, app_id=app_id, app_name=app_name, period=period)
            if blocked_pa_resp == 'timeout':
                print(f"[FRAUD] Timeout detected for detection API {app_id}, continuing with other APIs...")
                timeout_count += 1
                app_errors.append("Blocked Installs (PA) API timeout")
            if blocked_pa_resp and blocked_pa_resp.status_code == 200:
                pa_text = blocked_pa_resp.text.strip()
                if pa_text:
                    import io
                    csv_reader = csv.reader(io.StringIO(pa_text))
                    rows = list(csv_reader)
                    if len(rows) > 1:
                        header = rows[0]
                        date_idx = header.index("Install Time") if "Install Time" in header else None
                        ms_idx = find_media_source_idx(header)
                        if ms_idx is None:
                            print(f"[FRAUD] ERROR: Could not find exact 'Media Source' column in Blocked Installs (PA) for app {app_id}. Header: {header}")
                        for row in rows[1:]:
                            if date_idx is not None and len(row) > date_idx:
                                install_date = row[date_idx].split(" ")[0]
                                media_source = row[ms_idx].strip() if ms_idx is not None and len(row) > ms_idx else "Unknown"
                                add_metric(install_date, media_source, "blocked_installs_pa")
            elif blocked_pa_resp is not None:
                app_errors.append(f"Blocked Installs (PA) API error: {blocked_pa_resp.status_code} {blocked_pa_resp.text[:200]}")
            # Blocked In-App Events
            blocked_events_url = f"https://hq1.appsflyer.com/api/raw-data/export/app/{app_id}/blocked_in_app_events_report/v5"
            blocked_events_params = {"from": start_date, "to": end_date}
            blocked_events_resp = make_api_request(blocked_events_url, blocked_events_params, app_id=app_id, app_name=app_name, period=period)
            if blocked_events_resp == 'timeout':
                print(f"[FRAUD] Timeout detected for blocked_in_app_events_report {app_id}, continuing with other APIs...")
                timeout_count += 1
                app_errors.append("Blocked In-App Events API timeout")
            if blocked_events_resp and blocked_events_resp.status_code == 200:
                events_text = blocked_events_resp.text.strip()
                if events_text:
                    import io
                    csv_reader = csv.reader(io.StringIO(events_text))
                    rows = list(csv_reader)
                    if len(rows) > 1:
                        header = rows[0]
                        date_idx = header.index("Event Time") if "Event Time" in header else None
                        ms_idx = find_media_source_idx(header)
                        if ms_idx is None:
                            print(f"[FRAUD] ERROR: Could not find exact 'Media Source' column in Blocked In-App Events for app {app_id}. Header: {header}")
                        for row in rows[1:]:
                            if date_idx is not None and len(row) > date_idx:
                                event_date = row[date_idx].split(" ")[0]
                                media_source = row[ms_idx].strip() if ms_idx is not None and len(row) > ms_idx else "Unknown"
                                add_metric(event_date, media_source, "blocked_in_app_events")
            elif blocked_events_resp is not None:
                app_errors.append(f"Blocked In-App Events API error: {blocked_events_resp.status_code} {blocked_events_resp.text[:200]}")
            # Fraud Post Inapps
            fraud_post_inapps_url = f"https://hq1.appsflyer.com/api/raw-data/export/app/{app_id}/fraud-post-inapps/v5"
            fraud_post_inapps_params = {"from": start_date, "to": end_date}
            fraud_post_inapps_resp = make_api_request(fraud_post_inapps_url, fraud_post_inapps_params, app_id=app_id, app_name=app_name, period=period)
            if fraud_post_inapps_resp == 'timeout':
                print(f"[FRAUD] Timeout detected for fraud-post-inapps {app_id}, continuing with other APIs...")
                timeout_count += 1
                app_errors.append("Fraud Post-InApps API timeout")
            if fraud_post_inapps_resp and fraud_post_inapps_resp.status_code == 200:
                fraud_text = fraud_post_inapps_resp.text.strip()
                if fraud_text:
                    import io
                    csv_reader = csv.reader(io.StringIO(fraud_text))
                    rows = list(csv_reader)
                    if len(rows) > 1:
                        header = rows[0]
                        date_idx = header.index("Event Time") if "Event Time" in header else None
                        ms_idx = find_media_source_idx(header)
                        if ms_idx is None:
                            print(f"[FRAUD] ERROR: Could not find exact 'Media Source' column in Fraud Post InApps for app {app_id}. Header: {header}")
                        for row in rows[1:]:
                            if date_idx is not None and len(row) > date_idx:
                                event_date = row[date_idx].split(" ")[0]
                                media_source = row[ms_idx].strip() if ms_idx is not None and len(row) > ms_idx else "Unknown"
                                add_metric(event_date, media_source, "fraud_post_inapps")
            elif fraud_post_inapps_resp is not None:
                app_errors.append(f"Fraud Post-InApps API error: {fraud_post_inapps_resp.status_code} {fraud_post_inapps_resp.text[:200]}")
            # Blocked Clicks
            blocked_clicks_url = f"https://hq1.appsflyer.com/api/raw-data/export/app/{app_id}/blocked_clicks_report/v5"
            blocked_clicks_params = {"from": start_date, "to": end_date}
            blocked_clicks_resp = make_api_request(blocked_clicks_url, blocked_clicks_params, app_id=app_id, app_name=app_name, period=period)
            if blocked_clicks_resp == 'timeout':
                print(f"[FRAUD] Timeout detected for blocked_clicks_report {app_id}, continuing with other APIs...")
                timeout_count += 1
                app_errors.append("Blocked Clicks API timeout")
            if blocked_clicks_resp and blocked_clicks_resp.status_code == 200:
                clicks_text = blocked_clicks_resp.text.strip()
                if clicks_text:
                    import io
                    csv_reader = csv.reader(io.StringIO(clicks_text))
                    rows = list(csv_reader)
                    if len(rows) > 1:
                        header = rows[0]
                        date_idx = header.index("Click Time") if "Click Time" in header else None
                        ms_idx = find_media_source_idx(header)
                        if ms_idx is None:
                            print(f"[FRAUD] ERROR: Could not find exact 'Media Source' column in Blocked Clicks for app {app_id}. Header: {header}")
                        for row in rows[1:]:
                            if date_idx is not None and len(row) > date_idx:
                                click_date = row[date_idx].split(" ")[0]
                                media_source = row[ms_idx].strip() if ms_idx is not None and len(row) > ms_idx else "Unknown"
                                add_metric(click_date, media_source, "blocked_clicks")
            elif blocked_clicks_resp is not None:
                app_errors.append(f"Blocked Clicks API error: {blocked_clicks_resp.status_code} {blocked_clicks_resp.text[:200]}")
            # Blocked Install Postbacks
            blocked_postbacks_url = f"https://hq1.appsflyer.com/api/raw-data/export/app/{app_id}/blocked_install_postbacks/v5"
            blocked_postbacks_params = {"from": start_date, "to": end_date}
            blocked_postbacks_resp = make_api_request(blocked_postbacks_url, blocked_postbacks_params, app_id=app_id, app_name=app_name, period=period)
            if blocked_postbacks_resp == 'timeout':
                print(f"[FRAUD] Timeout detected for blocked_install_postbacks {app_id}, continuing with other APIs...")
                timeout_count += 1
                app_errors.append("Blocked Install Postbacks API timeout")
            if blocked_postbacks_resp and blocked_postbacks_resp.status_code == 200:
                postbacks_text = blocked_postbacks_resp.text.strip()
                if postbacks_text:
                    import io
                    csv_reader = csv.reader(io.StringIO(postbacks_text))
                    rows = list(csv_reader)
                    if len(rows) > 1:
                        header = rows[0]
                        date_idx = header.index("Install Time") if "Install Time" in header else None
                        ms_idx = find_media_source_idx(header)
                        if ms_idx is None:
                            print(f"[FRAUD] ERROR: Could not find exact 'Media Source' column in Blocked Install Postbacks for app {app_id}. Header: {header}")
                        for row in rows[1:]:
                            if date_idx is not None and len(row) > date_idx:
                                install_date = row[date_idx].split(" ")[0]
                                media_source = row[ms_idx].strip() if ms_idx is not None and len(row) > ms_idx else "Unknown"
                                add_metric(install_date, media_source, "blocked_install_postbacks")
            elif blocked_postbacks_resp is not None:
                app_errors.append(f"Blocked Install Postbacks API error: {blocked_postbacks_resp.status_code} {blocked_postbacks_resp.text[:200]}")
            
            # Helper function to detect error events
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
            
            # Get event selections for this app to fetch event1 and event2 data
            conn_temp = sqlite3.connect(DB_PATH)
            c_temp = conn_temp.cursor()
            c_temp.execute('SELECT event1, event2 FROM app_event_selections WHERE app_id = ?', (app_id,))
            event_row = c_temp.fetchone()
            conn_temp.close()
            
            selected_events = []
            if event_row:
                event1, event2 = event_row
                if event1 and event1.strip() and not is_error_event(event1):
                    selected_events.append(('event1', event1))
                if event2 and event2.strip() and not is_error_event(event2):
                    selected_events.append(('event2', event2))
            
            # Fetch event1 and event2 data per media source
            if selected_events:
                print(f"[FRAUD] Fetching event data for {app_id} (events: {[e[1] for e in selected_events]})...")
                events_url = f"https://hq1.appsflyer.com/api/raw-data/export/app/{app_id}/in_app_events_report/v5"
                events_params = {"from": start_date, "to": end_date}
                events_resp = make_api_request(events_url, events_params, app_id=app_id, app_name=app_name, period=period)
                
                if events_resp == 'timeout':
                    print(f"[FRAUD] Timeout detected for in_app_events_report {app_id}, continuing...")
                    timeout_count += 1
                    app_errors.append("In-App Events API timeout")
                elif events_resp and events_resp.status_code == 200:
                    event_text = events_resp.text.strip()
                    if event_text:
                        import io
                        # Use proper CSV parsing to handle quoted fields with commas
                        csv_reader = csv.reader(io.StringIO(event_text))
                        event_rows = list(csv_reader)
                        
                        if len(event_rows) > 1:
                            event_header = event_rows[0]
                            event_name_idx = event_header.index("Event Name") if "Event Name" in event_header else None
                            event_time_idx = event_header.index("Event Time") if "Event Time" in event_header else None
                            event_ms_idx = find_media_source_idx(event_header)
                            
                            print(f"[FRAUD] In-app events CSV header: {event_header}")
                            print(f"[FRAUD] Event parsing indices - name: {event_name_idx}, time: {event_time_idx}, media_source: {event_ms_idx}")
                            
                            if event_name_idx is not None and event_time_idx is not None and event_ms_idx is not None:
                                for row in event_rows[1:]:
                                    if len(row) > max(event_name_idx, event_time_idx, event_ms_idx):
                                        event_name = row[event_name_idx]
                                        event_date = row[event_time_idx].split(" ")[0]
                                        media_source = row[event_ms_idx].strip()
                                        
                                        # Debug logging for media source extraction
                                        print(f"[FRAUD] Event: {event_name}, Date: {event_date}, Media Source: '{media_source}'")
                                        
                                        # Check if this event matches event1 or event2
                                        for event_key, event_value in selected_events:
                                            if event_name == event_value:
                                                add_metric(event_date, media_source, event_key)
                                                print(f"[FRAUD] Added {event_key} event for media source: '{media_source}'")
                                                break
                        else:
                            print(f"[FRAUD] Could not find required columns in in_app_events_report for {app_id}")
                            if event_name_idx is None:
                                print(f"[FRAUD] Event Name column not found in header: {event_header}")
                            if event_time_idx is None:
                                print(f"[FRAUD] Event Time column not found in header: {event_header}")
                            if event_ms_idx is None:
                                print(f"[FRAUD] Media Source column not found in header: {event_header}")
                elif events_resp is not None:
                    print(f"[FRAUD] In-App Events API error for {app_id}: {events_resp.status_code}")
                    app_errors.append(f"In-App Events API error: {events_resp.status_code}")
                else:
                    print(f"[FRAUD] No response from in_app_events_report API for {app_id}")
            else:
                print(f"[FRAUD] No valid events selected for {app_id}, skipping event data collection")
            
            # Aggregate all (date, media_source) rows
            for (date, media_source), row in sorted(agg.items()):
                # Include all rows, even if all metrics are zero
                print(f"[FRAUD] Adding row for media source: {media_source} on date: {date}")
                print(f"[FRAUD] Row metrics: {row}")
                table.append(row)
                
            # Debug: Print app totals
            app_totals = {
                'blocked_installs_rt': sum(row.get('blocked_installs_rt', 0) for row in table),
                'blocked_installs_pa': sum(row.get('blocked_installs_pa', 0) for row in table),
                'blocked_in_app_events': sum(row.get('blocked_in_app_events', 0) for row in table),
                'fraud_post_inapps': sum(row.get('fraud_post_inapps', 0) for row in table),
                'blocked_clicks': sum(row.get('blocked_clicks', 0) for row in table),
                'blocked_install_postbacks': sum(row.get('blocked_install_postbacks', 0) for row in table),
                'event1': sum(row.get('event1', 0) for row in table),
                'event2': sum(row.get('event2', 0) for row in table)
            }
            print(f"[FRAUD] App {app_name} totals: {app_totals}")
            print(f"[FRAUD] Final table for {app_name} has {len(table)} rows")
            print(f"[FRAUD] Unique media sources: {sorted(set(row['media_source'] for row in table))}")
            
            # Determine if we should skip this app entirely
            if timeout_count >= 7:  # All 7 API calls timed out (including events)
                print(f"[FRAUD] Skipping app {app_name} ({app_id}) - all API calls timed out")
                skipped_apps += 1
                continue
                
            print(f"[FRAUD] Successfully processed app {app_name} ({app_id}) with {timeout_count} timeouts")
            processed_apps += 1
            
            # Include event names for frontend display
            event1_name = None
            event2_name = None
            if event_row:
                event1_name, event2_name = event_row
            
            fraud_list.append({
                'app_id': app_id,
                'app_name': app_name,
                'table': table,
                'errors': app_errors,
                'event1_name': event1_name,
                'event2_name': event2_name
            })
        # Save to cache ONLY if there is at least one app
        if len(fraud_list) > 0:
            c.execute('REPLACE INTO fraud_cache (range, data, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)',
                      (cache_key, json.dumps({'apps': fraud_list})))
            conn.commit()
            print(f"[FRAUD] Saved {len(fraud_list)} apps to cache with key: {cache_key}")
        else:
            print(f"[FRAUD] No apps to cache - fraud_list is empty")
            
        conn.close()
        
        # Final completion logging
        print(f"[FRAUD] ===== FRAUD PROCESSING COMPLETED =====")
        print(f"[FRAUD] Total apps requested: {total_apps}")
        print(f"[FRAUD] Apps successfully processed: {processed_apps}")
        print(f"[FRAUD] Apps skipped due to timeouts: {skipped_apps}")
        print(f"[FRAUD] Apps included in response: {len(fraud_list)}")
        print(f"[FRAUD] Returning response with {len(fraud_list)} apps")
        print(f"[FRAUD] ==========================================")
        
        return jsonify({'apps': fraud_list})
    except Exception as e:
        print(f"[FRAUD] ===== FRAUD PROCESSING FAILED =====")
        print(f"[FRAUD] Exception occurred: {e}")
        print(f"[FRAUD] Exception type: {type(e).__name__}")
        import traceback
        print(f"[FRAUD] Full traceback: {traceback.format_exc()}")
        print(f"[FRAUD] ===============================")
        return jsonify({'error': str(e)}), 500

@app.route('/api/overview')
@login_required
def overview():
    try:
        # Read the most recent 'last10' stats_cache entry (regardless of event selections or app IDs)
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT data, updated_at FROM stats_cache WHERE range LIKE 'last10%' ORDER BY updated_at DESC LIMIT 1")
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

        # Use only the most recent 'last10:' fraud_cache entry for Top Fraudulent Sources
        c.execute("SELECT range FROM fraud_cache WHERE range LIKE 'last10:%' ORDER BY updated_at DESC LIMIT 1")
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
        
        # Clear all cache tables
        c.execute('DELETE FROM stats_cache')
        c.execute('DELETE FROM fraud_cache')
        c.execute('DELETE FROM event_cache')
        c.execute('DELETE FROM apps_cache')
        
        # Clear all manual apps data
        c.execute('DELETE FROM manual_apps')
        
        # Clear all app event selections and status data
        c.execute('DELETE FROM app_event_selections')
        
        # Clear all saved CSV export data
        c.execute('DELETE FROM raw_appsflyer_data')
        
        # Note: We don't clear auto_run_settings as those are user configuration preferences
        
        conn.commit()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': 'All backend data cleared successfully',
            'cleared_tables': [
                'stats_cache',
                'fraud_cache', 
                'event_cache',
                'apps_cache',
                'manual_apps',
                'app_event_selections',
                'raw_appsflyer_data'
            ],
            'preserved_tables': ['auto_run_settings']
        })
    except Exception as e:
        print(f"[BACKEND CLEAR ERROR] {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/clear-apps-cache', methods=['POST'])
def clear_apps_cache():
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        # Clear all apps cache (synced apps)
        c.execute('DELETE FROM apps_cache')
        
        # Clear all events cache
        c.execute('DELETE FROM event_cache')
        
        # Clear manual apps as well (user wants to clear ALL apps)
        c.execute('DELETE FROM manual_apps')
        
        # Also clear any app event selections for manual apps
        c.execute('DELETE FROM app_event_selections WHERE app_id NOT IN (SELECT app_id FROM apps_cache)')
        
        conn.commit()
        
        return jsonify({
            "success": True,
            "message": "Successfully cleared all apps cache, events cache, and manual apps",
            "cleared": ["apps_cache", "event_cache", "manual_apps", "related_app_event_selections"],
            "note": "All apps cleared - both synced and manual"
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
        
        # Count rows before deletion
        c.execute('SELECT COUNT(*) FROM fraud_cache')
        count_before = c.fetchone()[0]
        
        c.execute('DELETE FROM fraud_cache')
        conn.commit()
        conn.close()
        
        print(f"[CACHE] Cleared {count_before} fraud cache entries")
        return jsonify({'success': True, 'message': f'Fraud cache cleared ({count_before} entries)'})
    except Exception as e:
        print(f"[CACHE] Error clearing fraud cache: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/apps-page')
@login_required
def apps_page():
    """Tab switching endpoint - should NEVER trigger AppsFlyer API calls"""
    try:
        result = get_active_apps(allow_appsflyer_api=False)
        return jsonify({
            'count': result['count'],
            'apps': result['apps'],
            'fetch_time': result['fetch_time'],
            'used_cache': result['used_cache'],
            'updated_at': result['fetch_time']
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

@app.route('/get_subpage_10d')
def get_subpage_10d():
    import logging
    app.logger.debug('GET /get_subpage_10d')
    return get_stats_for_range('10d')



# --- Fraud Analytics endpoints ---
@app.route('/get_fraud_subpage_10d')
def get_fraud_subpage_10d():
    import logging
    app.logger.debug('GET /get_fraud_subpage_10d')
    return get_fraud_for_range('10d')



# Helper to fetch fraud for a given range (10d only)
def get_fraud_for_range(range_key):
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        # Only support 10d range now
        if range_key == '10d':
            keys = ['10d', 'last10']
        else:
            keys = [range_key]
        
        row = None
        for key in keys:
            c.execute("SELECT data, updated_at FROM fraud_cache WHERE range LIKE ? ORDER BY updated_at DESC LIMIT 1", (f"{key}%",))
            row = c.fetchone()
            if row:
                break
        conn.close()
        if row:
            data, updated_at = row
            result = json.loads(data)
            result['updated_at'] = updated_at
            return jsonify(result)
        else:
            return jsonify({'apps': [], 'updated_at': None})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Helper to fetch stats for a given range (for Stats endpoints)
def get_stats_for_range(range_key):
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        period_map = {
            '10d': ['10d', 'last10'],
            'mtd': ['mtd'],
            'lastmonth': ['lastmonth'],
            '30d': ['30d', 'last30']
        }
        keys = period_map.get(range_key, [range_key])
        row = None
        for key in keys:
            c.execute("SELECT data, updated_at FROM stats_cache WHERE range LIKE ? ORDER BY updated_at DESC LIMIT 1", (f"{key}%",))
            row = c.fetchone()
            if row:
                break
        conn.close()
        if row:
            data, updated_at = row
            result = json.loads(data)
            result['updated_at'] = updated_at
            return jsonify(result)
        else:
            return jsonify({'apps': [], 'updated_at': None})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def process_report_async(apps, period, selected_events):
    """Background task to process report data"""
    try:
        print(f"[REPORT] Starting async report processing for period: {period}")
        print(f"[REPORT] Processing {len(apps)} apps")
        
        # Add comprehensive processing tracking
        total_apps = len(apps)
        processed_apps = 0
        skipped_apps = 0
        
        print(f"[REPORT] Starting report data processing for {total_apps} apps...")
        
        start_date, end_date = get_period_dates(period)
        stats_list = []
        
        for app in apps:
            app_id = app['app_id']
            app_name = app['app_name']
            print(f"[REPORT] Processing app: {app_name} (App ID: {app_id})...")
            
            timeout_count = 0
            app_errors = []
            
            # Use the aggregate daily report endpoint for main stats
            url = f"https://hq1.appsflyer.com/api/agg-data/export/app/{app_id}/daily_report/v5"
            params = {"from": start_date, "to": end_date}
            
            try:
                print(f"[REPORT] Calling daily_report API for {app_id}...")
                resp = make_api_request(url, params, app_id=app_id, app_name=app_name, period=period)
                if resp == 'timeout':
                    print(f"[REPORT] Timeout detected for daily_report {app_id}, continuing with other APIs...")
                    timeout_count += 1
                    app_errors.append("Daily Report API timeout")
                
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
                blocked_rt_resp = make_api_request(blocked_rt_url, params, app_id=app_id, app_name=app_name, period=period)
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
                    events_resp = make_api_request(events_url, params, app_id=app_id, app_name=app_name, period=period)
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
                    
                # Determine if we should skip this app entirely
                if timeout_count >= 2:  # Multiple API calls timed out
                    print(f"[REPORT] Skipping app {app_name} ({app_id}) - multiple API calls timed out")
                    skipped_apps += 1
                    continue
                    
                print(f"[REPORT] Successfully processed app {app_name} ({app_id}) with {timeout_count} timeouts")
                processed_apps += 1
                
                stats_list.append({
                    'app_id': app_id,
                    'app_name': app_name,
                    'table': table,
                    'selected_events': selected,
                    'traffic': sum(r['impressions'] + r['clicks'] for r in table),
                    'errors': app_errors
                })
                
            except Exception as e:
                print(f"[REPORT] Error processing app {app_id}: {str(e)}")
                skipped_apps += 1
                continue
            except BrokenPipeError as e:
                print(f"[REPORT] BrokenPipeError (EPIPE) for app {app_id}: {str(e)}. Skipping to next app.")
                skipped_apps += 1
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
            
            print(f"[REPORT] Saved {len(stats_list)} apps to cache with key: {cache_key}")
        else:
            print(f"[REPORT] No apps to cache - stats_list is empty")
            result = {
                'apps': [],
                'updated_at': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
        
        # Final completion logging
        print(f"[REPORT] ===== REPORT PROCESSING COMPLETED =====")
        print(f"[REPORT] Total apps requested: {total_apps}")
        print(f"[REPORT] Apps successfully processed: {processed_apps}")
        print(f"[REPORT] Apps skipped due to timeouts: {skipped_apps}")
        print(f"[REPORT] Apps included in response: {len(stats_list)}")
        print(f"[REPORT] Returning response with {len(stats_list)} apps")
        print(f"[REPORT] ==========================================")
        
        return result
            
    except BrokenPipeError as e:
        print(f"[REPORT] ===== REPORT PROCESSING FAILED (BROKEN PIPE) =====")
        print(f"[REPORT] BrokenPipeError at outer level: {str(e)}")
        print(f"[REPORT] Returning empty result so frontend can proceed")
        print(f"[REPORT] ==========================================")
        return {'apps': [], 'error': 'BrokenPipeError (EPIPE) occurred'}
    except Exception as e:
        print(f"[REPORT] ===== REPORT PROCESSING FAILED =====")
        print(f"[REPORT] Exception occurred: {e}")
        print(f"[REPORT] Exception type: {type(e).__name__}")
        import traceback
        print(f"[REPORT] Full traceback: {traceback.format_exc()}")
        print(f"[REPORT] ==========================================")
        raise
    return {'apps': [], 'error': 'Failed to process report'}

@app.route('/start-report', methods=['POST'])
@login_required
def start_report():
    data = request.get_json()
    apps = data.get('apps', [])
    period = data.get('period')
    selected_events = data.get('selected_events', [])

    # Run synchronously (not as a background job)
    result = process_report_async(apps, period, selected_events)
    return jsonify({
        'status': 'completed',
        'result': result
    })

@app.route('/report-status/<job_id>')
@login_required
def report_status(job_id):
    # Handle case where Redis/task_queue is not available
    if task_queue is None:
        return jsonify({'status': 'not_found', 'message': 'Background tasks disabled'})
    
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

@app.route('/api/manual-apps', methods=['POST'])
@login_required
def add_manual_app():
    """Add a manual app to the database"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'error': 'No data provided'}), 400
        
        app_id = data.get('app_id', '').strip()
        app_name = data.get('app_name', '').strip()
        status = data.get('status', 'active').strip()
        event1 = data.get('event1', '').strip()
        event2 = data.get('event2', '').strip()
        
        # Validate required fields
        if not app_id:
            return jsonify({'success': False, 'error': 'App ID is required and cannot be empty'}), 400
        if not app_name:
            return jsonify({'success': False, 'error': 'App Name is required and cannot be empty'}), 400
        if not event1:
            return jsonify({'success': False, 'error': 'Event 1 is required and cannot be empty'}), 400
        if not event2:
            return jsonify({'success': False, 'error': 'Event 2 is required and cannot be empty'}), 400
        
        # Validate status
        if status not in ['active', 'inactive']:
            return jsonify({'success': False, 'error': 'Status must be either "active" or "inactive"'}), 400
            
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        # Check if app already exists in manual_apps or in AppsFlyer apps
        c.execute('SELECT app_id FROM manual_apps WHERE app_id = ?', (app_id,))
        if c.fetchone():
            return jsonify({'success': False, 'error': f'App ID "{app_id}" already exists as a manual app. Each app ID must be unique.'}), 400
        
        # Check if app exists in AppsFlyer apps (from apps_cache)
        c.execute('SELECT data FROM apps_cache ORDER BY updated_at DESC LIMIT 1')
        cache_row = c.fetchone()
        if cache_row:
            cached_data = json.loads(cache_row[0])
            existing_app_ids = [app['app_id'] for app in cached_data.get('apps', [])]
            if app_id in existing_app_ids:
                return jsonify({'success': False, 'error': f'App ID "{app_id}" already exists in synced apps from AppsFlyer. Cannot add duplicate app IDs.'}), 400
        
        # Insert manual app
        is_active = 1 if status == 'active' else 0
        c.execute('''INSERT INTO manual_apps 
                    (app_id, app_name, status, event1, event2, is_active) 
                    VALUES (?, ?, ?, ?, ?, ?)''',
                 (app_id, app_name, status, event1, event2, is_active))
        
        # Also add to app_event_selections table for consistency
        c.execute('''INSERT OR REPLACE INTO app_event_selections 
                    (app_id, event1, event2, is_active) 
                    VALUES (?, ?, ?, ?)''',
                 (app_id, event1, event2, is_active))
        
        conn.commit()
        conn.close()
        
        return jsonify({
            'success': True, 
            'message': 'Manual app added successfully',
            'app': {
                'app_id': app_id,
                'app_name': app_name,
                'status': status,
                'event1': event1,
                'event2': event2,
                'is_active': is_active == 1,
                'is_manual': True
            }
        })
        
    except Exception as e:
        print(f"Error adding manual app: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/apps-database-only', methods=['GET'])
@login_required
def apps_database_only():
    """Get apps from database/cache only - NO AppsFlyer API calls"""
    try:
        import pytz
        gmt2 = pytz.timezone('Europe/Berlin')
        now = datetime.datetime.now(gmt2)
        
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        # Get active status from database
        c.execute('SELECT app_id, is_active FROM app_event_selections')
        active_status = dict(c.fetchall())
        
        # Get cached AppsFlyer apps (if available)
        c.execute('SELECT data, updated_at FROM apps_cache ORDER BY updated_at DESC LIMIT 1')
        cache_row = c.fetchone()
        
        apps = []
        fetch_time = now.strftime('%Y-%m-%d %H:%M:%S')
        
        if cache_row:
            data, updated_at = cache_row
            cached_data = json.loads(data)
            
            # Get synced apps from cache
            for app in cached_data.get('apps', []):
                app['is_active'] = bool(active_status.get(app['app_id'], 1))
                app['is_manual'] = False
                apps.append(app)
            
            fetch_time = cached_data.get('fetch_time', updated_at)
        
        # Get manual apps from database
        c.execute('''SELECT app_id, app_name, status, event1, event2, is_active 
                     FROM manual_apps ORDER BY app_name''')
        manual_apps = c.fetchall()
        
        for manual_app in manual_apps:
            app_id, app_name, status, event1, event2, is_active = manual_app
            apps.append({
                'app_id': app_id,
                'app_name': app_name,
                'status': status,
                'event1': event1,
                'event2': event2,
                'is_active': bool(is_active),
                'is_manual': True
            })
        
        conn.close()
        
        return jsonify({
            'count': len(apps),
            'apps': apps,
            'fetch_time': fetch_time,
            'used_cache': True,
            'source': 'database_only'
        })
        
    except Exception as e:
        print(f"Error in apps_database_only endpoint: {str(e)}")
        return jsonify({
            'error': str(e),
            'count': 0,
            'apps': [],
            'fetch_time': '0.00 seconds'
        }), 500

# Modify the get_active_apps function to include the active status
def get_active_app_ids():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT app_id FROM app_event_selections WHERE is_active = 1')
    active_apps = [row[0] for row in c.fetchall()]
    conn.close()
    return active_apps

def parse_raw_csv_data(raw_csv_data):
    """Parse raw CSV data using proper CSV parser to handle malformed data"""
    try:
        if not raw_csv_data or not raw_csv_data.strip():
            return []
        
        import io
        import csv
        
        # Use proper CSV parsing to handle malformed data
        csv_reader = csv.reader(io.StringIO(raw_csv_data.strip()))
        rows = list(csv_reader)
        
        # Filter out empty rows
        rows = [row for row in rows if row and any(cell.strip() for cell in row)]
        
        return rows
    except Exception as e:
        print(f"[CSV_PARSE] Error parsing CSV data: {str(e)}")
        # Fallback to line splitting if CSV parsing fails
        lines = raw_csv_data.strip().split('\n')
        return [line.split(',') for line in lines if line.strip()]

# Raw Data Export Endpoints
@app.route('/export/stats/raw', methods=['GET'])
@login_required
def export_stats_raw():
    """Export raw stats data for CSV export"""
    try:
        range_key = request.args.get('range', 'last10')
        
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        # Get the most recent stats data for the range
        c.execute("SELECT data, updated_at FROM stats_cache WHERE range LIKE ? ORDER BY updated_at DESC LIMIT 1", (f"{range_key}%",))
        row = c.fetchone()
        conn.close()
        
        if not row:
            return jsonify({'error': 'No stats data available. Please generate a report first.'}), 404
            
        data, updated_at = row
        stats = json.loads(data)
        
        # Convert the cached data to raw format for CSV export
        raw_data = []
        for app in stats.get('apps', []):
            app_name = app.get('app_name', 'Unknown App')
            app_id = app.get('app_id', 'Unknown ID')
            
            for entry in app.get('table', []):
                row_data = {
                    'App Name': app_name,
                    'App ID': app_id,
                    'Date': entry.get('date', ''),
                    'Impressions': entry.get('impressions', 0),
                    'Clicks': entry.get('clicks', 0),
                    'Installs': entry.get('installs', 0),
                    'Blocked Installs RT': entry.get('blocked_installs_rt', 0),
                    'Blocked Installs PA': entry.get('blocked_installs_pa', 0),
                    'Imp to Click Rate': entry.get('imp_to_click', 0),
                    'Click to Install Rate': entry.get('click_to_install', 0),
                    'Blocked RT Rate': entry.get('blocked_rt_rate', 0),
                    'Blocked PA Rate': entry.get('blocked_pa_rate', 0),
                    'Period': f'Last 10 Days',
                    'Data Type': 'Stats Report',
                    'Updated At': updated_at
                }
                
                # Add event data if available
                selected_events = app.get('selected_events', [])
                for event in selected_events:
                    if event in entry:
                        row_data[f'Event: {event}'] = entry.get(event, 0)
                
                raw_data.append(row_data)
        
        return jsonify({
            'data': raw_data,
            'total_records': len(raw_data),
            'apps_count': len(stats.get('apps', [])),
            'updated_at': updated_at
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/export/fraud/raw', methods=['GET'])
@login_required
def export_fraud_raw():
    """Export raw fraud data for CSV export"""
    try:
        range_key = request.args.get('range', 'last10')
        
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        # Get the most recent fraud data for the range
        c.execute("SELECT data, updated_at FROM fraud_cache WHERE range LIKE ? ORDER BY updated_at DESC LIMIT 1", (f"{range_key}%",))
        row = c.fetchone()
        conn.close()
        
        if not row:
            return jsonify({'error': 'No fraud data available. Please generate a report first.'}), 404
            
        data, updated_at = row
        fraud_data = json.loads(data)
        
        # Convert the cached data to raw format for CSV export
        raw_data = []
        for app in fraud_data.get('apps', []):
            app_name = app.get('app_name', 'Unknown App')
            app_id = app.get('app_id', 'Unknown ID')
            event1_name = app.get('event1_name')
            event2_name = app.get('event2_name')
            # Use fallback names if event names are empty or None
            event1_name = event1_name.strip() if event1_name and event1_name.strip() else 'Event 1'
            event2_name = event2_name.strip() if event2_name and event2_name.strip() else 'Event 2'
            
            for entry in app.get('table', []):
                row_data = {
                    'App Name': app_name,
                    'App ID': app_id,
                    'Date': entry.get('date', ''),
                    'Media Source': entry.get('media_source', ''),
                    'Blocked Installs RT': entry.get('blocked_installs_rt', 0),
                    'Blocked Installs PA': entry.get('blocked_installs_pa', 0),
                    'Blocked In-App Events': entry.get('blocked_in_app_events', 0),
                    'Fraud Post In-Apps': entry.get('fraud_post_inapps', 0),
                    'Blocked Clicks': entry.get('blocked_clicks', 0),
                    'Blocked Install Postbacks': entry.get('blocked_install_postbacks', 0),
                    event1_name: entry.get('event1', 0),
                    event2_name: entry.get('event2', 0),
                    'Period': f'Last 10 Days',
                    'Data Type': 'Fraud Report',
                    'Updated At': updated_at
                }
                
                raw_data.append(row_data)
        
        return jsonify({
            'data': raw_data,
            'total_records': len(raw_data),
            'apps_count': len(fraud_data.get('apps', [])),
            'updated_at': updated_at
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Raw AppsFlyer Data Export Endpoints
@app.route('/export/raw/daily_report', methods=['GET'])
@login_required
def export_raw_daily_report():
    """Export raw daily report data"""
    try:
        period = request.args.get('period', 'last10')
        app_id = request.args.get('app_id')
        
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        if app_id:
            # Export for specific app
            c.execute('''SELECT app_name, endpoint_type, period, raw_csv_data, start_date, end_date, created_at 
                        FROM raw_appsflyer_data 
                        WHERE app_id = ? AND endpoint_type = 'daily_report' AND period = ?
                        ORDER BY created_at DESC LIMIT 1''', (app_id, period))
        else:
            # Export for all apps
            c.execute('''SELECT app_name, endpoint_type, period, raw_csv_data, start_date, end_date, created_at 
                        FROM raw_appsflyer_data 
                        WHERE endpoint_type = 'daily_report' AND period = ?
                        ORDER BY app_name, created_at DESC''', (period,))
        
        rows = c.fetchall()
        conn.close()
        
        if not rows:
            return jsonify({'error': 'No daily report data found'}), 404
        
        # Combine all CSV data
        combined_csv = ""
        header_added = False
        
        for app_name, endpoint_type, period, raw_csv_data, start_date, end_date, created_at in rows:
            parsed_rows = parse_raw_csv_data(raw_csv_data)
            if not header_added and parsed_rows:
                # Add header with generic info for multi-app export
                combined_csv += f"# Daily Report Data - Period: {period} ({start_date} to {end_date})\n"
                combined_csv += f"# Generated: {created_at}\n"
                # Convert header row to CSV format and add App_Name column
                header_csv = ','.join(parsed_rows[0]) + ",App_Name\n"
                combined_csv += header_csv
                header_added = True
            
            # Add data rows with proper CSV formatting
            for row in parsed_rows[1:]:
                if row:  # Skip empty rows
                    row_csv = ','.join(f'"{cell}"' if ',' in cell else cell for cell in row)
                    combined_csv += row_csv + f",{app_name}\n"
        
        # Return as downloadable CSV
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"AppsFlyer_Raw_Daily_Report_{period}_{timestamp}.csv"
        
        return Response(
            combined_csv,
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename={filename}'}
        )
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/export/raw/blocked_installs_report', methods=['GET'])
@login_required
def export_raw_blocked_installs_report():
    """Export raw blocked installs report data"""
    try:
        period = request.args.get('period', 'last10')
        app_id = request.args.get('app_id')
        
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        if app_id:
            c.execute('''SELECT app_name, endpoint_type, period, raw_csv_data, start_date, end_date, created_at 
                        FROM raw_appsflyer_data 
                        WHERE app_id = ? AND endpoint_type = 'blocked_installs_report' AND period = ?
                        ORDER BY created_at DESC LIMIT 1''', (app_id, period))
        else:
            c.execute('''SELECT app_name, endpoint_type, period, raw_csv_data, start_date, end_date, created_at 
                        FROM raw_appsflyer_data 
                        WHERE endpoint_type = 'blocked_installs_report' AND period = ?
                        ORDER BY app_name, created_at DESC''', (period,))
        
        rows = c.fetchall()
        conn.close()
        
        if not rows:
            return jsonify({'error': 'No blocked installs report data found'}), 404
        
        combined_csv = ""
        header_added = False
        
        for app_name, endpoint_type, period, raw_csv_data, start_date, end_date, created_at in rows:
            parsed_rows = parse_raw_csv_data(raw_csv_data)
            if not header_added and parsed_rows:
                combined_csv += f"# Blocked Installs Report Data - Period: {period} ({start_date} to {end_date})\n"
                combined_csv += f"# Generated: {created_at}\n"
                # Convert header row to CSV format and add App_Name column
                header_csv = ','.join(parsed_rows[0]) + ",App_Name\n"
                combined_csv += header_csv
                header_added = True
            
            # Add data rows with proper CSV formatting
            for row in parsed_rows[1:]:
                if row:  # Skip empty rows
                    row_csv = ','.join(f'"{cell}"' if ',' in cell else cell for cell in row)
                    combined_csv += row_csv + f",{app_name}\n"
        
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"AppsFlyer_Raw_Blocked_Installs_{period}_{timestamp}.csv"
        
        return Response(
            combined_csv,
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename={filename}'}
        )
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/export/raw/detection', methods=['GET'])
@login_required
def export_raw_detection():
    """Export raw detection (PA) data"""
    try:
        period = request.args.get('period', 'last10')
        app_id = request.args.get('app_id')
        
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        if app_id:
            c.execute('''SELECT app_name, endpoint_type, period, raw_csv_data, start_date, end_date, created_at 
                        FROM raw_appsflyer_data 
                        WHERE app_id = ? AND endpoint_type = 'detection' AND period = ?
                        ORDER BY created_at DESC LIMIT 1''', (app_id, period))
        else:
            c.execute('''SELECT app_name, endpoint_type, period, raw_csv_data, start_date, end_date, created_at 
                        FROM raw_appsflyer_data 
                        WHERE endpoint_type = 'detection' AND period = ?
                        ORDER BY app_name, created_at DESC''', (period,))
        
        rows = c.fetchall()
        conn.close()
        
        if not rows:
            return jsonify({'error': 'No detection data found'}), 404
        
        combined_csv = ""
        header_added = False
        
        for app_name, endpoint_type, period, raw_csv_data, start_date, end_date, created_at in rows:
            parsed_rows = parse_raw_csv_data(raw_csv_data)
            if not header_added and parsed_rows:
                combined_csv += f"# Detection (PA) Data - Period: {period} ({start_date} to {end_date})\n"
                combined_csv += f"# Generated: {created_at}\n"
                # Convert header row to CSV format and add App_Name column
                header_csv = ','.join(parsed_rows[0]) + ",App_Name\n"
                combined_csv += header_csv
                header_added = True
            
            # Add data rows with proper CSV formatting
            for row in parsed_rows[1:]:
                if row:  # Skip empty rows
                    row_csv = ','.join(f'"{cell}"' if ',' in cell else cell for cell in row)
                    combined_csv += row_csv + f",{app_name}\n"
        
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"AppsFlyer_Raw_Detection_PA_{period}_{timestamp}.csv"
        
        return Response(
            combined_csv,
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename={filename}'}
        )
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/export/raw/blocked_in_app_events_report', methods=['GET'])
@login_required
def export_raw_blocked_in_app_events():
    """Export raw blocked in-app events data"""
    try:
        period = request.args.get('period', 'last10')
        app_id = request.args.get('app_id')
        
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        if app_id:
            c.execute('''SELECT app_name, endpoint_type, period, raw_csv_data, start_date, end_date, created_at 
                        FROM raw_appsflyer_data 
                        WHERE app_id = ? AND endpoint_type = 'blocked_in_app_events_report' AND period = ?
                        ORDER BY created_at DESC LIMIT 1''', (app_id, period))
        else:
            c.execute('''SELECT app_name, endpoint_type, period, raw_csv_data, start_date, end_date, created_at 
                        FROM raw_appsflyer_data 
                        WHERE endpoint_type = 'blocked_in_app_events_report' AND period = ?
                        ORDER BY app_name, created_at DESC''', (period,))
        
        rows = c.fetchall()
        conn.close()
        
        if not rows:
            return jsonify({'error': 'No blocked in-app events data found'}), 404
        
        combined_csv = ""
        header_added = False
        
        for app_name, endpoint_type, period, raw_csv_data, start_date, end_date, created_at in rows:
            parsed_rows = parse_raw_csv_data(raw_csv_data)
            if not header_added and parsed_rows:
                combined_csv += f"# Blocked In-App Events Data - Period: {period} ({start_date} to {end_date})\n"
                combined_csv += f"# Generated: {created_at}\n"
                # Convert header row to CSV format and add App_Name column
                header_csv = ','.join(parsed_rows[0]) + ",App_Name\n"
                combined_csv += header_csv
                header_added = True
            
            # Add data rows with proper CSV formatting
            for row in parsed_rows[1:]:
                if row:  # Skip empty rows
                    row_csv = ','.join(f'"{cell}"' if ',' in cell else cell for cell in row)
                    combined_csv += row_csv + f",{app_name}\n"
        
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"AppsFlyer_Raw_Blocked_InApp_Events_{period}_{timestamp}.csv"
        
        return Response(
            combined_csv,
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename={filename}'}
        )
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/export/raw/fraud_post_inapps', methods=['GET'])
@login_required
def export_raw_fraud_post_inapps():
    """Export raw fraud post-inapps data"""
    try:
        period = request.args.get('period', 'last10')
        app_id = request.args.get('app_id')
        
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        if app_id:
            c.execute('''SELECT app_name, endpoint_type, period, raw_csv_data, start_date, end_date, created_at 
                        FROM raw_appsflyer_data 
                        WHERE app_id = ? AND endpoint_type = 'fraud_post_inapps' AND period = ?
                        ORDER BY created_at DESC LIMIT 1''', (app_id, period))
        else:
            c.execute('''SELECT app_name, endpoint_type, period, raw_csv_data, start_date, end_date, created_at 
                        FROM raw_appsflyer_data 
                        WHERE endpoint_type = 'fraud_post_inapps' AND period = ?
                        ORDER BY app_name, created_at DESC''', (period,))
        
        rows = c.fetchall()
        conn.close()
        
        if not rows:
            return jsonify({'error': 'No fraud post-inapps data found'}), 404
        
        combined_csv = ""
        header_added = False
        
        for app_name, endpoint_type, period, raw_csv_data, start_date, end_date, created_at in rows:
            parsed_rows = parse_raw_csv_data(raw_csv_data)
            if not header_added and parsed_rows:
                combined_csv += f"# Fraud Post-InApps Data - Period: {period} ({start_date} to {end_date})\n"
                combined_csv += f"# Generated: {created_at}\n"
                # Convert header row to CSV format and add App_Name column
                header_csv = ','.join(parsed_rows[0]) + ",App_Name\n"
                combined_csv += header_csv
                header_added = True
            
            # Add data rows with proper CSV formatting
            for row in parsed_rows[1:]:
                if row:  # Skip empty rows
                    row_csv = ','.join(f'"{cell}"' if ',' in cell else cell for cell in row)
                    combined_csv += row_csv + f",{app_name}\n"
        
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"AppsFlyer_Raw_Fraud_Post_InApps_{period}_{timestamp}.csv"
        
        return Response(
            combined_csv,
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename={filename}'}
        )
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/export/raw/blocked_clicks_report', methods=['GET'])
@login_required
def export_raw_blocked_clicks():
    """Export raw blocked clicks data"""
    try:
        period = request.args.get('period', 'last10')
        app_id = request.args.get('app_id')
        
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        if app_id:
            c.execute('''SELECT app_name, endpoint_type, period, raw_csv_data, start_date, end_date, created_at 
                        FROM raw_appsflyer_data 
                        WHERE app_id = ? AND endpoint_type = 'blocked_clicks_report' AND period = ?
                        ORDER BY created_at DESC LIMIT 1''', (app_id, period))
        else:
            c.execute('''SELECT app_name, endpoint_type, period, raw_csv_data, start_date, end_date, created_at 
                        FROM raw_appsflyer_data 
                        WHERE endpoint_type = 'blocked_clicks_report' AND period = ?
                        ORDER BY app_name, created_at DESC''', (period,))
        
        rows = c.fetchall()
        conn.close()
        
        if not rows:
            return jsonify({'error': 'No blocked clicks data found'}), 404
        
        combined_csv = ""
        header_added = False
        
        for app_name, endpoint_type, period, raw_csv_data, start_date, end_date, created_at in rows:
            parsed_rows = parse_raw_csv_data(raw_csv_data)
            if not header_added and parsed_rows:
                combined_csv += f"# Blocked Clicks Data - Period: {period} ({start_date} to {end_date})\n"
                combined_csv += f"# Generated: {created_at}\n"
                # Convert header row to CSV format and add App_Name column
                header_csv = ','.join(parsed_rows[0]) + ",App_Name\n"
                combined_csv += header_csv
                header_added = True
            
            # Add data rows with proper CSV formatting
            for row in parsed_rows[1:]:
                if row:  # Skip empty rows
                    row_csv = ','.join(f'"{cell}"' if ',' in cell else cell for cell in row)
                    combined_csv += row_csv + f",{app_name}\n"
        
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"AppsFlyer_Raw_Blocked_Clicks_{period}_{timestamp}.csv"
        
        return Response(
            combined_csv,
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename={filename}'}
        )
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/export/raw/blocked_install_postbacks', methods=['GET'])
@login_required
def export_raw_blocked_install_postbacks():
    """Export raw blocked install postbacks data"""
    try:
        period = request.args.get('period', 'last10')
        app_id = request.args.get('app_id')
        
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        if app_id:
            c.execute('''SELECT app_name, endpoint_type, period, raw_csv_data, start_date, end_date, created_at 
                        FROM raw_appsflyer_data 
                        WHERE app_id = ? AND endpoint_type = 'blocked_install_postbacks' AND period = ?
                        ORDER BY created_at DESC LIMIT 1''', (app_id, period))
        else:
            c.execute('''SELECT app_name, endpoint_type, period, raw_csv_data, start_date, end_date, created_at 
                        FROM raw_appsflyer_data 
                        WHERE endpoint_type = 'blocked_install_postbacks' AND period = ?
                        ORDER BY app_name, created_at DESC''', (period,))
        
        rows = c.fetchall()
        conn.close()
        
        if not rows:
            return jsonify({'error': 'No blocked install postbacks data found'}), 404
        
        combined_csv = ""
        header_added = False
        
        for app_name, endpoint_type, period, raw_csv_data, start_date, end_date, created_at in rows:
            parsed_rows = parse_raw_csv_data(raw_csv_data)
            if not header_added and parsed_rows:
                combined_csv += f"# Blocked Install Postbacks Data - Period: {period} ({start_date} to {end_date})\n"
                combined_csv += f"# Generated: {created_at}\n"
                # Convert header row to CSV format and add App_Name column
                header_csv = ','.join(parsed_rows[0]) + ",App_Name\n"
                combined_csv += header_csv
                header_added = True
            
            # Add data rows with proper CSV formatting
            for row in parsed_rows[1:]:
                if row:  # Skip empty rows
                    row_csv = ','.join(f'"{cell}"' if ',' in cell else cell for cell in row)
                    combined_csv += row_csv + f",{app_name}\n"
        
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"AppsFlyer_Raw_Blocked_Install_Postbacks_{period}_{timestamp}.csv"
        
        return Response(
            combined_csv,
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename={filename}'}
        )
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/export/raw/in_app_events_report', methods=['GET'])
@login_required
def export_raw_in_app_events():
    """Export raw in-app events data"""
    try:
        period = request.args.get('period', 'last10')
        app_id = request.args.get('app_id')
        
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        if app_id:
            c.execute('''SELECT app_name, endpoint_type, period, raw_csv_data, start_date, end_date, created_at 
                        FROM raw_appsflyer_data 
                        WHERE app_id = ? AND endpoint_type = 'in_app_events_report' AND period = ?
                        ORDER BY created_at DESC LIMIT 1''', (app_id, period))
        else:
            c.execute('''SELECT app_name, endpoint_type, period, raw_csv_data, start_date, end_date, created_at 
                        FROM raw_appsflyer_data 
                        WHERE endpoint_type = 'in_app_events_report' AND period = ?
                        ORDER BY app_name, created_at DESC''', (period,))
        
        rows = c.fetchall()
        conn.close()
        
        if not rows:
            return jsonify({'error': 'No in-app events data found'}), 404
        
        combined_csv = ""
        header_added = False
        
        for app_name, endpoint_type, period, raw_csv_data, start_date, end_date, created_at in rows:
            parsed_rows = parse_raw_csv_data(raw_csv_data)
            if not header_added and parsed_rows:
                combined_csv += f"# In-App Events Data - Period: {period} ({start_date} to {end_date})\n"
                combined_csv += f"# Generated: {created_at}\n"
                # Convert header row to CSV format and add App_Name column
                header_csv = ','.join(parsed_rows[0]) + ",App_Name\n"
                combined_csv += header_csv
                header_added = True
            
            # Add data rows with proper CSV formatting
            for row in parsed_rows[1:]:
                if row:  # Skip empty rows
                    row_csv = ','.join(f'"{cell}"' if ',' in cell else cell for cell in row)
                    combined_csv += row_csv + f",{app_name}\n"
        
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"AppsFlyer_Raw_InApp_Events_{period}_{timestamp}.csv"
        
        return Response(
            combined_csv,
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename={filename}'}
        )
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/export/raw/installs_report', methods=['GET'])
@login_required
def export_raw_installs_report():
    """Export raw installs report data"""
    try:
        period = request.args.get('period', 'last10')
        app_id = request.args.get('app_id')
        
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        if app_id:
            c.execute('''SELECT app_name, endpoint_type, period, raw_csv_data, start_date, end_date, created_at 
                        FROM raw_appsflyer_data 
                        WHERE app_id = ? AND endpoint_type = 'installs_report' AND period = ?
                        ORDER BY created_at DESC LIMIT 1''', (app_id, period))
        else:
            c.execute('''SELECT app_name, endpoint_type, period, raw_csv_data, start_date, end_date, created_at 
                        FROM raw_appsflyer_data 
                        WHERE endpoint_type = 'installs_report' AND period = ?
                        ORDER BY app_name, created_at DESC''', (period,))
        
        rows = c.fetchall()
        conn.close()
        
        if not rows:
            return jsonify({'error': 'No installs report data found'}), 404
        
        combined_csv = ""
        header_added = False
        
        for app_name, endpoint_type, period, raw_csv_data, start_date, end_date, created_at in rows:
            parsed_rows = parse_raw_csv_data(raw_csv_data)
            if not header_added and parsed_rows:
                combined_csv += f"# Installs Report Data - Period: {period} ({start_date} to {end_date})\n"
                combined_csv += f"# Generated: {created_at}\n"
                # Convert header row to CSV format and add App_Name column
                header_csv = ','.join(parsed_rows[0]) + ",App_Name\n"
                combined_csv += header_csv
                header_added = True
            
            # Add data rows with proper CSV formatting
            for row in parsed_rows[1:]:
                if row:  # Skip empty rows
                    row_csv = ','.join(f'"{cell}"' if ',' in cell else cell for cell in row)
                    combined_csv += row_csv + f",{app_name}\n"
        
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"AppsFlyer_Raw_Installs_Report_{period}_{timestamp}.csv"
        
        return Response(
            combined_csv,
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename={filename}'}
        )
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# --- AUTO-RUN MANAGEMENT API ENDPOINTS ---

@app.route('/api/auto-run-status', methods=['GET'])
@login_required
def get_auto_run_status():
    """Get current auto-run status and timing information"""
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        c.execute('''SELECT last_run_time, next_run_time, auto_run_enabled, 
                           auto_run_interval_hours, is_running, updated_at 
                    FROM auto_run_settings WHERE id = 1''')
        row = c.fetchone()
        conn.close()
        
        if not row:
            # Initialize default settings if none exist
            return jsonify({
                'last_run_time': None,
                'next_run_time': None,
                'auto_run_enabled': True,
                'auto_run_interval_hours': 6,
                'is_running': False,
                'updated_at': None
            })
        
        last_run_time, next_run_time, auto_run_enabled, auto_run_interval_hours, is_running, updated_at = row
        
        # Calculate next run time if last run time exists
        if last_run_time and auto_run_enabled:
            from datetime import datetime, timedelta
            try:
                last_run_dt = datetime.fromisoformat(last_run_time.replace('Z', '+00:00'))
                next_run_dt = last_run_dt + timedelta(hours=auto_run_interval_hours)
                next_run_time = next_run_dt.isoformat()
            except:
                next_run_time = None
        
        return jsonify({
            'last_run_time': last_run_time,
            'next_run_time': next_run_time,
            'auto_run_enabled': bool(auto_run_enabled),
            'auto_run_interval_hours': auto_run_interval_hours,
            'is_running': bool(is_running),
            'updated_at': updated_at
        })
    
    except Exception as e:
        logger.error(f"Error getting auto-run status: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/auto-run-status', methods=['POST'])
@login_required
def update_auto_run_status():
    """Update auto-run status and timing"""
    try:
        data = request.get_json()
        
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        # Prepare update fields
        update_fields = []
        values = []
        
        if 'last_run_time' in data:
            update_fields.append('last_run_time = ?')
            values.append(data['last_run_time'])
        
        if 'auto_run_enabled' in data:
            update_fields.append('auto_run_enabled = ?')
            values.append(1 if data['auto_run_enabled'] else 0)
        
        if 'auto_run_interval_hours' in data:
            update_fields.append('auto_run_interval_hours = ?')
            values.append(data['auto_run_interval_hours'])
        
        if 'is_running' in data:
            update_fields.append('is_running = ?')
            values.append(1 if data['is_running'] else 0)
        
        if update_fields:
            update_fields.append('updated_at = CURRENT_TIMESTAMP')
            values.append(1)  # id = 1
            
            query = f"UPDATE auto_run_settings SET {', '.join(update_fields)} WHERE id = ?"
            c.execute(query, values)
            conn.commit()
        
        conn.close()
        
        logger.info(f"Auto-run status updated: {data}")
        return jsonify({'success': True})
    
    except Exception as e:
        logger.error(f"Error updating auto-run status: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/auto-run-execute', methods=['POST'])
@login_required
def execute_auto_run():
    """Execute auto-run manually or via scheduler"""
    try:
        # Mark as running
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('UPDATE auto_run_settings SET is_running = 1, updated_at = CURRENT_TIMESTAMP WHERE id = 1')
        conn.commit()
        conn.close()
        
        logger.info("Auto-run execution started")
        
        # Get active apps (cache only - no AppsFlyer API calls for auto-run)
        active_apps_result = get_active_apps(allow_appsflyer_api=False)
        if not active_apps_result or not active_apps_result.get('apps'):
            logger.error("No active apps found for auto-run")
            # Mark as not running
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute('UPDATE auto_run_settings SET is_running = 0, updated_at = CURRENT_TIMESTAMP WHERE id = 1')
            conn.commit()
            conn.close()
            return jsonify({'error': 'No active apps found'}), 400
        
        # Filter to only active apps
        active_apps = [app for app in active_apps_result['apps'] if app.get('is_active', True)]
        if not active_apps:
            logger.error("No active apps found after filtering")
            # Mark as not running
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute('UPDATE auto_run_settings SET is_running = 0, updated_at = CURRENT_TIMESTAMP WHERE id = 1')
            conn.commit()
            conn.close()
            return jsonify({'error': 'No active apps found after filtering'}), 400
        
        logger.info(f"Found {len(active_apps)} active apps for auto-run")
        
        # Get event selections
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('SELECT app_id, event1, event2 FROM app_event_selections')
        selections = c.fetchall()
        conn.close()
        
        selected_events = {}
        for app_id, event1, event2 in selections:
            selected_events[app_id] = [event1 or '', event2 or '']
        
        # Generate stats reports for different periods
        periods = ['last10']
        stats_results = []
        
        for period in periods:
            logger.info(f"Generating stats for period: {period}")
            try:
                # Create request data
                request_data = {
                    'apps': active_apps,
                    'period': period,
                    'selected_events': selected_events
                }
                
                # Call the existing all_apps_stats endpoint logic
                stats_result = all_apps_stats_logic(request_data)
                if stats_result:
                    stats_results.append(stats_result)
                    logger.info(f"Successfully generated stats for period: {period}")
                else:
                    logger.error(f"Failed to generate stats for period: {period}")
                    
            except Exception as e:
                logger.error(f"Error generating stats for period {period}: {str(e)}")
        
        # Generate fraud reports
        fraud_periods = ['10d']
        fraud_results = []
        
        for period in fraud_periods:
            logger.info(f"Generating fraud report for period: {period}")
            try:
                # Create request data
                request_data = {
                    'apps': active_apps,
                    'period': period,
                    'force': True
                }
                
                # Call the existing get_fraud endpoint logic
                fraud_result = get_fraud_logic(request_data)
                if fraud_result:
                    fraud_results.append(fraud_result)
                    logger.info(f"Successfully generated fraud report for period: {period}")
                else:
                    logger.error(f"Failed to generate fraud report for period: {period}")
                    
            except Exception as e:
                logger.error(f"Error generating fraud report for period {period}: {str(e)}")
        
        # Update last run time and mark as not running
        from datetime import datetime
        current_time = datetime.now().isoformat()
        
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''UPDATE auto_run_settings 
                    SET last_run_time = ?, is_running = 0, updated_at = CURRENT_TIMESTAMP 
                    WHERE id = 1''', (current_time,))
        conn.commit()
        conn.close()
        
        logger.info(f"Auto-run executed successfully. Stats: {len(stats_results)} periods, Fraud: {len(fraud_results)} periods")
        return jsonify({
            'success': True, 
            'executed_at': current_time,
            'stats_periods': len(stats_results),
            'fraud_periods': len(fraud_results),
            'processed_apps': len(active_apps)
        })
    
    except Exception as e:
        # Make sure to mark as not running on error
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute('UPDATE auto_run_settings SET is_running = 0, updated_at = CURRENT_TIMESTAMP WHERE id = 1')
            conn.commit()
            conn.close()
        except:
            pass
        
        logger.error(f"Error executing auto-run: {str(e)}")
        return jsonify({'error': str(e)}), 500


def all_apps_stats_logic(request_data):
    """Extract the logic from all_apps_stats endpoint for reuse"""
    try:
        active_apps = request_data.get('apps', [])
        period = request_data.get('period', 'last10')
        selected_events = request_data.get('selected_events', {})
        
        start_date, end_date = get_period_dates(period)
        logger.info(f"[AUTO-STATS] Processing {len(active_apps)} apps for period: {period} ({start_date} to {end_date})")
        
        # Build cache key
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
        
        # Check cache first
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('SELECT data, updated_at FROM stats_cache WHERE range = ?', (cache_key,))
        row = c.fetchone()
        if row:
            data, updated_at = row
            result = json.loads(data)
            if result.get('apps') and len(result['apps']) > 0:
                logger.info(f"[AUTO-STATS] Using cached data for {period}")
                conn.close()
                return result
        conn.close()
        
        # Generate fresh data (simplified version for auto-run)
        # For auto-run, we'll use a simplified approach to avoid timeouts
        stats_list = []
        
        for app in active_apps:
            try:
                app_id = app['app_id']
                app_name = app['app_name']
                
                # Use daily report endpoint
                url = f"https://hq1.appsflyer.com/api/agg-data/export/app/{app_id}/daily_report/v5"
                params = {"from": start_date, "to": end_date}
                
                resp = make_api_request(url, params, max_retries=3, retry_delay=5, app_id=app_id, app_name=app_name, period=period)
                
                if resp and resp.status_code == 200:
                    # Process the response (simplified)
                    daily_stats = {}
                    rows = resp.text.strip().split("\n")
                    
                    if len(rows) > 1:
                        header = rows[0].split(",")
                        data_rows = [row.split(",") for row in rows[1:]]
                        
                        # Find column indices
                        def find_col(*names):
                            for name in names:
                                for i, col in enumerate(header):
                                    if col.lower().replace('_','').replace(' ','') == name.lower().replace('_','').replace(' ',''):
                                        return i
                            return None
                        
                        impressions_idx = find_col('impressions', 'Impressions')
                        clicks_idx = find_col('clicks', 'Clicks')
                        installs_idx = find_col('installs', 'Installs')
                        date_idx = find_col('date', 'Date')
                        media_source_idx = find_col('media_source', 'media source', 'Media Source')
                        
                        if all(idx is not None for idx in [impressions_idx, clicks_idx, installs_idx, date_idx, media_source_idx]):
                            for row in data_rows:
                                if len(row) > max(impressions_idx, clicks_idx, installs_idx, date_idx, media_source_idx):
                                    date = row[date_idx]
                                    media_source = row[media_source_idx].strip().lower()
                                    
                                    if date not in daily_stats:
                                        daily_stats[date] = {"impressions": 0, "clicks": 0, "total_installs": 0, "organic_installs": 0}
                                    
                                    impressions = int(row[impressions_idx]) if row[impressions_idx].isdigit() else 0
                                    clicks = int(row[clicks_idx]) if row[clicks_idx].isdigit() else 0
                                    installs = int(row[installs_idx]) if row[installs_idx].isdigit() else 0
                                    
                                    daily_stats[date]["impressions"] += impressions
                                    daily_stats[date]["clicks"] += clicks
                                    daily_stats[date]["total_installs"] += installs
                                    
                                    if media_source == 'organic':
                                        daily_stats[date]["organic_installs"] += installs
                        
                        # Calculate non-organic installs
                        for date in daily_stats:
                            daily_stats[date]["installs"] = daily_stats[date]["total_installs"] - daily_stats[date]["organic_installs"]
                            if daily_stats[date]["installs"] < 0:
                                daily_stats[date]["installs"] = 0
                        
                        # Convert to table format
                        table = []
                        for date in sorted(daily_stats.keys()):
                            row = {
                                "date": date,
                                "impressions": daily_stats[date]["impressions"],
                                "clicks": daily_stats[date]["clicks"],
                                "installs": daily_stats[date]["installs"],
                                "blocked_installs_rt": 0,
                                "blocked_installs_pa": 0,
                            }
                            
                            # Calculate rates
                            row["imp_to_click"] = round(row["clicks"] / row["impressions"], 2) if row["impressions"] > 0 else 0
                            row["click_to_install"] = round(row["installs"] / row["clicks"], 2) if row["clicks"] > 0 else 0
                            row["blocked_rt_rate"] = 0
                            row["blocked_pa_rate"] = 0
                            
                            table.append(row)
                        
                        stats_list.append({
                            'app_id': app_id,
                            'app_name': app_name,
                            'table': table,
                            'selected_events': selected_events.get(app_id, []),
                            'traffic': sum(r['impressions'] + r['clicks'] for r in table),
                            'errors': []
                        })
                        
                        logger.info(f"[AUTO-STATS] Processed {app_name} successfully")
                else:
                    logger.error(f"[AUTO-STATS] Failed to get data for {app_name}")
                    
            except Exception as e:
                logger.error(f"[AUTO-STATS] Error processing {app.get('app_name', app.get('app_id'))}: {str(e)}")
        
        # Sort by traffic
        stats_list.sort(key=lambda x: x['traffic'], reverse=True)
        
        # Save to cache
        if stats_list:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            result = {'apps': stats_list}
            c.execute('REPLACE INTO stats_cache (range, data, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)', 
                     (cache_key, json.dumps(result)))
            conn.commit()
            conn.close()
            logger.info(f"[AUTO-STATS] Saved {len(stats_list)} apps to cache")
            return result
        
        return None
        
    except Exception as e:
        logger.error(f"[AUTO-STATS] Error in stats logic: {str(e)}")
        return None


def get_fraud_logic(request_data):
    """Extract the logic from get_fraud endpoint for reuse"""
    try:
        active_apps = request_data.get('apps', [])
        period = request_data.get('period', 'last10')
        force = request_data.get('force', True)
        
        start_date, end_date = get_period_dates(period)
        logger.info(f"[AUTO-FRAUD] Processing {len(active_apps)} apps for period: {period} ({start_date} to {end_date})")
        
        # Build cache key
        app_ids = '-'.join(sorted([app['app_id'] for app in active_apps]))
        cache_key = f"{period}:{app_ids}"
        
        # Check cache first (unless forced)
        if not force:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute('SELECT data, updated_at FROM fraud_cache WHERE range LIKE ? ORDER BY updated_at DESC LIMIT 1', 
                     (f"{period}%",))
            row = c.fetchone()
            if row:
                data, updated_at = row
                result = json.loads(data)
                if result.get('apps') and len(result['apps']) > 0:
                    logger.info(f"[AUTO-FRAUD] Using cached data for {period}")
                    conn.close()
                    return result
            conn.close()
        
        # Generate fresh fraud data (simplified for auto-run)
        fraud_list = []
        
        for app in active_apps:
            try:
                app_id = app['app_id']
                app_name = app['app_name']
                
                # Use daily report endpoint for fraud data
                url = f"https://hq1.appsflyer.com/api/raw-data/export/app/{app_id}/daily_report/v5"
                params = {"from": start_date, "to": end_date}
                
                resp = make_api_request(url, params, max_retries=3, retry_delay=5, app_id=app_id, app_name=app_name, period=period)
                
                if resp and resp.status_code == 200:
                    # Process fraud data (simplified)
                    fraud_data = {}
                    rows = resp.text.strip().split("\n")
                    
                    if len(rows) > 1:
                        header = rows[0].split(",")
                        
                        # Find media source column
                        media_source_idx = find_media_source_idx(header)
                        date_idx = next((i for i, col in enumerate(header) if 'date' in col.lower()), None)
                        
                        if media_source_idx is not None and date_idx is not None:
                            for row in rows[1:]:
                                cols = row.split(",")
                                if len(cols) > max(media_source_idx, date_idx):
                                    date = cols[date_idx]
                                    media_source = cols[media_source_idx].strip()
                                    
                                    if date not in fraud_data:
                                        fraud_data[date] = {}
                                    
                                    fraud_data[date][media_source] = fraud_data[date].get(media_source, 0) + 1
                        
                        # Convert to table format
                        table = []
                        for date in sorted(fraud_data.keys()):
                            for media_source, count in fraud_data[date].items():
                                table.append({
                                    "date": date,
                                    "media_source": media_source,
                                    "blocked_installs": count,
                                    "blocked_clicks": 0,
                                    "blocked_in_app_events": 0
                                })
                        
                        fraud_list.append({
                            'app_id': app_id,
                            'app_name': app_name,
                            'table': table
                        })
                        
                        logger.info(f"[AUTO-FRAUD] Processed {app_name} successfully")
                else:
                    logger.error(f"[AUTO-FRAUD] Failed to get data for {app_name}")
                    
            except Exception as e:
                logger.error(f"[AUTO-FRAUD] Error processing {app.get('app_name', app.get('app_id'))}: {str(e)}")
        
        # Save to cache
        if fraud_list:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            result = {'apps': fraud_list}
            c.execute('REPLACE INTO fraud_cache (range, data, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)', 
                     (cache_key, json.dumps(result)))
            conn.commit()
            conn.close()
            logger.info(f"[AUTO-FRAUD] Saved {len(fraud_list)} apps to cache")
            return result
        
        return None
        
    except Exception as e:
        logger.error(f"[AUTO-FRAUD] Error in fraud logic: {str(e)}")
        return None

# --- BACKGROUND WORKER FOR AUTO-RUN ---
import threading
import time as time_module

def auto_run_background_worker():
    """Background worker to check for and trigger auto-run execution"""
    logger.info("🚀 Auto-run background worker started")
    
    while True:
        try:
            # Check if auto-run should be triggered
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            
            c.execute('''SELECT last_run_time, auto_run_enabled, auto_run_interval_hours, is_running 
                        FROM auto_run_settings WHERE id = 1''')
            row = c.fetchone()
            conn.close()
            
            if row:
                last_run_time, auto_run_enabled, auto_run_interval_hours, is_running = row
                
                if auto_run_enabled and not is_running:
                    if last_run_time:
                        try:
                            last_run_dt = datetime.datetime.fromisoformat(last_run_time.replace('Z', '+00:00'))
                            if last_run_dt.tzinfo is None:
                                last_run_dt = last_run_dt.replace(tzinfo=datetime.timezone.utc)
                            
                            time_since_last_run = datetime.datetime.now(datetime.timezone.utc) - last_run_dt
                            interval_hours = auto_run_interval_hours or 6
                            
                            if time_since_last_run.total_seconds() >= (interval_hours * 3600):
                                logger.info(f"⏰ Auto-run timer expired. Last run: {last_run_time}, Interval: {interval_hours}h")
                                
                                # Trigger auto-run execution
                                try:
                                    # Mark as running
                                    conn = sqlite3.connect(DB_PATH)
                                    c = conn.cursor()
                                    c.execute('UPDATE auto_run_settings SET is_running = 1 WHERE id = 1')
                                    conn.commit()
                                    conn.close()
                                    
                                    # Execute auto-run
                                    with app.app_context():
                                        result = execute_auto_run_logic()
                                        if result:
                                            logger.info("✅ Background auto-run completed successfully")
                                        else:
                                            logger.error("❌ Background auto-run failed")
                                    
                                except Exception as e:
                                    logger.error(f"❌ Error in background auto-run: {str(e)}")
                                    # Make sure to mark as not running
                                    try:
                                        conn = sqlite3.connect(DB_PATH)
                                        c = conn.cursor()
                                        c.execute('UPDATE auto_run_settings SET is_running = 0 WHERE id = 1')
                                        conn.commit()
                                        conn.close()
                                    except:
                                        pass
                        except Exception as e:
                            logger.error(f"❌ Error parsing last run time: {str(e)}")
                    else:
                        logger.info("📝 No last run time found, skipping auto-run trigger")
                else:
                    if not auto_run_enabled:
                        logger.debug("⏸️ Auto-run disabled")
                    if is_running:
                        logger.debug("🔄 Auto-run already running")
            
            # Sleep for 60 seconds before checking again
            time_module.sleep(60)
            
        except Exception as e:
            logger.error(f"❌ Error in auto-run background worker: {str(e)}")
            time_module.sleep(60)  # Sleep and continue


def execute_auto_run_logic():
    """Execute the auto-run logic without Flask request context"""
    try:
        # Get active apps (cache only - no AppsFlyer API calls for background auto-run)
        active_apps_result = get_active_apps(allow_appsflyer_api=False)
        if not active_apps_result or not active_apps_result.get('apps'):
            logger.error("No active apps found for background auto-run")
            return False
        
        # Filter to only active apps
        active_apps = [app for app in active_apps_result['apps'] if app.get('is_active', True)]
        if not active_apps:
            logger.error("No active apps found after filtering for background auto-run")
            return False
        
        logger.info(f"Found {len(active_apps)} active apps for background auto-run")
        
        # Get event selections
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('SELECT app_id, event1, event2 FROM app_event_selections')
        selections = c.fetchall()
        conn.close()
        
        selected_events = {}
        for app_id, event1, event2 in selections:
            selected_events[app_id] = [event1 or '', event2 or '']
        
        # Generate stats reports
        periods = ['last10']
        stats_results = []
        
        for period in periods:
            logger.info(f"Background: Generating stats for period: {period}")
            try:
                request_data = {
                    'apps': active_apps,
                    'period': period,
                    'selected_events': selected_events
                }
                
                stats_result = all_apps_stats_logic(request_data)
                if stats_result:
                    stats_results.append(stats_result)
                    logger.info(f"Background: Successfully generated stats for period: {period}")
                else:
                    logger.error(f"Background: Failed to generate stats for period: {period}")
                    
            except Exception as e:
                logger.error(f"Background: Error generating stats for period {period}: {str(e)}")
        
        # Generate fraud reports
        fraud_periods = ['10d']
        fraud_results = []
        
        for period in fraud_periods:
            logger.info(f"Background: Generating fraud report for period: {period}")
            try:
                request_data = {
                    'apps': active_apps,
                    'period': period,
                    'force': True
                }
                
                fraud_result = get_fraud_logic(request_data)
                if fraud_result:
                    fraud_results.append(fraud_result)
                    logger.info(f"Background: Successfully generated fraud report for period: {period}")
                else:
                    logger.error(f"Background: Failed to generate fraud report for period: {period}")
                    
            except Exception as e:
                logger.error(f"Background: Error generating fraud report for period {period}: {str(e)}")
        
        # Update last run time and mark as not running
        current_time = datetime.datetime.now().isoformat()
        
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''UPDATE auto_run_settings 
                    SET last_run_time = ?, is_running = 0, updated_at = CURRENT_TIMESTAMP 
                    WHERE id = 1''', (current_time,))
        conn.commit()
        conn.close()
        
        logger.info(f"Background auto-run completed successfully. Stats: {len(stats_results)} periods, Fraud: {len(fraud_results)} periods")
        return True
        
    except Exception as e:
        logger.error(f"Error in background auto-run logic: {str(e)}")
        return False


# Start background worker thread
def start_background_worker():
    """Start the background worker thread"""
    worker_thread = threading.Thread(target=auto_run_background_worker, daemon=True)
    worker_thread.start()
    logger.info("🚀 Background worker thread started")

# Add before the profile configuration section
@app.route('/get_events_source', methods=['POST'])
@login_required
def get_events_source():
    """Get events per source data - reuses fraud data but filters to events only"""
    try:
        data = request.get_json()
        active_apps = data.get('apps', [])
        period = data.get('period', 'last10')
        
        print(f"[EVENTS_SOURCE] Starting events per source processing for {len(active_apps)} apps, period: {period}")
        
        # Use fraud cache since it already contains event data
        app_ids = '-'.join(sorted([app['app_id'] for app in active_apps]))
        cache_key = f"{period}:{app_ids}"
        
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        # Try to find fraud cache data that contains the events
        c.execute("SELECT data, updated_at FROM fraud_cache WHERE range LIKE ? ORDER BY updated_at DESC LIMIT 1", (f"{period}%",))
        row = c.fetchone()
        
        if not row:
            print(f"[EVENTS_SOURCE] No fraud cache found for period {period}, returning empty result")
            conn.close()
            return jsonify({
                'apps': [],
                'updated_at': None,
                'message': 'No data available. Please run a fraud report first to collect event data.'
            })
        
        data_json, updated_at = row
        fraud_data = json.loads(data_json)
        
        print(f"[EVENTS_SOURCE] Found fraud cache data with {len(fraud_data.get('apps', []))} apps")
        
        # Transform fraud data to focus on events only
        events_list = []
        
        for app in fraud_data.get('apps', []):
            app_id = app.get('app_id')
            app_name = app.get('app_name')
            event1_name = app.get('event1_name', 'Event 1')
            event2_name = app.get('event2_name', 'Event 2')
            
            print(f"[EVENTS_SOURCE] Processing app: {app_name} (events: {event1_name}, {event2_name})")
            
            # Filter table to only include rows with events
            events_table = []
            for row in app.get('table', []):
                event1_count = row.get('event1', 0)
                event2_count = row.get('event2', 0)
                
                # Only include rows where there are actual events
                if event1_count > 0 or event2_count > 0:
                    events_row = {
                        'date': row.get('date'),
                        'media_source': row.get('media_source'),
                        'event1': event1_count,
                        'event2': event2_count
                    }
                    events_table.append(events_row)
            
            # Calculate totals for this app
            total_event1 = sum(row.get('event1', 0) for row in events_table)
            total_event2 = sum(row.get('event2', 0) for row in events_table)
            
            print(f"[EVENTS_SOURCE] App {app_name}: {len(events_table)} rows with events, totals: {event1_name}={total_event1}, {event2_name}={total_event2}")
            
            events_list.append({
                'app_id': app_id,
                'app_name': app_name,
                'table': events_table,
                'event1_name': event1_name.strip() if event1_name and event1_name.strip() else 'Event 1',
                'event2_name': event2_name.strip() if event2_name and event2_name.strip() else 'Event 2',
                'total_event1': total_event1,
                'total_event2': total_event2
            })
        
        conn.close()
        
        result = {
            'apps': events_list,
            'updated_at': updated_at,
            'period': period
        }
        
        print(f"[EVENTS_SOURCE] Returning {len(events_list)} apps with events data")
        return jsonify(result)
        
    except Exception as e:
        print(f"[EVENTS_SOURCE] Error: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/get_events_source_subpage_10d')
@login_required
def get_events_source_subpage_10d():
    """Get cached events source data for 10d period"""
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        # Get the most recent fraud cache (which contains events data)
        c.execute("SELECT data, updated_at FROM fraud_cache WHERE range LIKE 'last10%' ORDER BY updated_at DESC LIMIT 1")
        row = c.fetchone()
        
        if not row:
            print("[EVENTS_SOURCE_SUBPAGE] No fraud cache found for 10d period")
            conn.close()
            return jsonify({'apps': [], 'updated_at': None})
        
        data_json, updated_at = row
        fraud_data = json.loads(data_json)
        
        # Transform to events-only format
        events_list = []
        for app in fraud_data.get('apps', []):
            # Filter to events only
            events_table = []
            for row in app.get('table', []):
                event1_count = row.get('event1', 0)
                event2_count = row.get('event2', 0)
                
                if event1_count > 0 or event2_count > 0:
                    events_table.append({
                        'date': row.get('date'),
                        'media_source': row.get('media_source'),
                        'event1': event1_count,
                        'event2': event2_count
                    })
            
            if events_table:  # Only include apps with events
                events_list.append({
                    'app_id': app.get('app_id'),
                    'app_name': app.get('app_name'),
                    'table': events_table,
                    'event1_name': app.get('event1_name', 'Event 1'),
                    'event2_name': app.get('event2_name', 'Event 2'),
                    'total_event1': sum(row.get('event1', 0) for row in events_table),
                    'total_event2': sum(row.get('event2', 0) for row in events_table)
                })
        
        conn.close()
        
        result = {
            'apps': events_list,
            'updated_at': updated_at,
            'period': '10d'
        }
        
        print(f"[EVENTS_SOURCE_SUBPAGE] Returning {len(events_list)} apps with events data")
        return jsonify(result)
        
    except Exception as e:
        print(f"[EVENTS_SOURCE_SUBPAGE] Error: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/remove-app', methods=['POST'])
@login_required
def remove_single_app():
    """Remove a single app from cache and/or manual apps"""
    try:
        data = request.get_json()
        app_id = data.get('app_id')
        
        if not app_id:
            return jsonify({'success': False, 'error': 'App ID is required'}), 400
        
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        # Check if it's a manual app
        c.execute('SELECT COUNT(*) FROM manual_apps WHERE app_id = ?', (app_id,))
        is_manual = c.fetchone()[0] > 0
        
        # Remove from manual apps if it exists there
        if is_manual:
            c.execute('DELETE FROM manual_apps WHERE app_id = ?', (app_id,))
            print(f"Removed manual app: {app_id}")
        
        # Remove from app_event_selections
        c.execute('DELETE FROM app_event_selections WHERE app_id = ?', (app_id,))
        
        # If it was a synced app, we need to update the apps_cache to remove it
        if not is_manual:
            c.execute('SELECT data FROM apps_cache ORDER BY updated_at DESC LIMIT 1')
            cache_row = c.fetchone()
            
            if cache_row:
                cached_data = json.loads(cache_row[0])
                # Remove the app from the cached apps list
                cached_data['apps'] = [app for app in cached_data['apps'] if app['app_id'] != app_id]
                cached_data['count'] = len(cached_data['apps'])
                
                # Update cache
                c.execute('DELETE FROM apps_cache')
                c.execute('INSERT INTO apps_cache (data, updated_at) VALUES (?, ?)', 
                         (json.dumps(cached_data), datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
                print(f"Removed synced app from cache: {app_id}")
        
        conn.commit()
        conn.close()
        
        return jsonify({
            'success': True, 
            'message': f'App {app_id} removed successfully',
            'app_type': 'manual' if is_manual else 'synced'
        })
        
    except Exception as e:
        print(f"Error removing app: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/remove-apps-bulk', methods=['POST'])
@login_required
def remove_multiple_apps():
    """Remove multiple apps from cache and/or manual apps"""
    try:
        data = request.get_json()
        app_ids = data.get('app_ids', [])
        
        if not app_ids:
            return jsonify({'success': False, 'error': 'App IDs are required'}), 400
        
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        removed_manual = []
        removed_synced = []
        
        # Process each app
        for app_id in app_ids:
            # Check if it's a manual app
            c.execute('SELECT COUNT(*) FROM manual_apps WHERE app_id = ?', (app_id,))
            is_manual = c.fetchone()[0] > 0
            
            if is_manual:
                # Remove from manual apps
                c.execute('DELETE FROM manual_apps WHERE app_id = ?', (app_id,))
                removed_manual.append(app_id)
            else:
                removed_synced.append(app_id)
            
            # Remove from app_event_selections
            c.execute('DELETE FROM app_event_selections WHERE app_id = ?', (app_id,))
        
        # Update apps_cache to remove synced apps
        if removed_synced:
            c.execute('SELECT data FROM apps_cache ORDER BY updated_at DESC LIMIT 1')
            cache_row = c.fetchone()
            
            if cache_row:
                cached_data = json.loads(cache_row[0])
                # Remove the synced apps from the cached apps list
                cached_data['apps'] = [app for app in cached_data['apps'] if app['app_id'] not in removed_synced]
                cached_data['count'] = len(cached_data['apps'])
                
                # Update cache
                c.execute('DELETE FROM apps_cache')
                c.execute('INSERT INTO apps_cache (data, updated_at) VALUES (?, ?)', 
                         (json.dumps(cached_data), datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        
        conn.commit()
        conn.close()
        
        return jsonify({
            'success': True, 
            'message': f'Removed {len(app_ids)} apps successfully',
            'removed_manual': removed_manual,
            'removed_synced': removed_synced,
            'total_removed': len(app_ids)
        })
        
    except Exception as e:
        print(f"Error removing apps: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/debug/db-status')
def debug_db_status():
    """Debug endpoint to check database status and persistent storage"""
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        # Check if apps table exists and count rows
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='apps'")
        apps_table_exists = c.fetchone() is not None
        
        apps_count = 0
        if apps_table_exists:
            c.execute("SELECT COUNT(*) FROM apps")
            apps_count = c.fetchone()[0]
        
        # Check if event_selections table exists and count rows
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='event_selections'")
        event_selections_table_exists = c.fetchone() is not None
        
        event_selections_count = 0
        if event_selections_table_exists:
            c.execute("SELECT COUNT(*) FROM event_selections")
            event_selections_count = c.fetchone()[0]
        
        conn.close()
        
        return jsonify({
            'database_path': DB_PATH,
            'is_railway_environment': is_railway_environment(),
            'database_exists': os.path.exists(DB_PATH),
            'data_directory_exists': os.path.exists('/data') if is_railway_environment() else 'N/A',
            'data_directory_contents': os.listdir('/data') if is_railway_environment() and os.path.exists('/data') else 'N/A',
            'apps_table_exists': apps_table_exists,
            'apps_count': apps_count,
            'event_selections_table_exists': event_selections_table_exists,
            'event_selections_count': event_selections_count,
            'railway_env_vars': {
                'RAILWAY_ENVIRONMENT': os.getenv('RAILWAY_ENVIRONMENT'),
                'RAILWAY_SERVICE_NAME': os.getenv('RAILWAY_SERVICE_NAME'),
                'RAILWAY_PROJECT_ID': os.getenv('RAILWAY_PROJECT_ID'),
                'RAILWAY_DEPLOYMENT_ID': os.getenv('RAILWAY_DEPLOYMENT_ID'),
                'RAILWAY_REPLICA_ID': os.getenv('RAILWAY_REPLICA_ID')
            }
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    # Start the background worker
    start_background_worker()
    
    # Use PORT environment variable provided by Railway, default to 5000
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
