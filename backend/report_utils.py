import json
import sqlite3
from datetime import datetime
import pytz
import requests
import time
from rq import Queue
from redis import Redis

# Initialize Redis connection
redis_conn = Redis(host='localhost', port=6379, db=0)
# Initialize RQ queue
task_queue = Queue(connection=redis_conn)

DB_PATH = 'event_selections.db'

def process_report_async(apps, period, selected_events):
    """Process report asynchronously using RQ"""
    job = task_queue.enqueue(
        'app.process_report_task',
        apps,
        period,
        selected_events,
        job_timeout='1h'
    )
    return job.id

def get_fraud_data(apps, period):
    """Get fraud data for the specified period"""
    # Implementation of fraud data retrieval
    # This is a placeholder - you'll need to implement the actual fraud data retrieval logic
    pass

def get_active_app_ids():
    """Get list of active app IDs from database"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT app_id FROM app_event_selections WHERE is_active = 1')
    active_apps = [row[0] for row in c.fetchall()]
    conn.close()
    return active_apps 