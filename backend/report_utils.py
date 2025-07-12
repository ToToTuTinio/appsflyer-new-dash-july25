import json
import sqlite3
from datetime import datetime
import pytz
import requests
import time
from rq import Queue
from redis import Redis
import logging
from urllib.parse import urlparse
import os

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get Redis URL from environment variable (Railway provides REDIS_URL)
redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')

try:
    # Parse Redis URL to extract connection parameters
    parsed_url = urlparse(redis_url)
    redis_host = parsed_url.hostname or 'localhost'
    redis_port = parsed_url.port or 6379
    redis_db = int(parsed_url.path.lstrip('/')) if parsed_url.path and len(parsed_url.path) > 1 else 0
    
    # Initialize Redis connection
    redis_conn = Redis(host=redis_host, port=redis_port, db=redis_db, decode_responses=True)
    
    # Test Redis connection
    redis_conn.ping()
    print(f"✅ Report Utils Redis connected successfully to {redis_host}:{redis_port}")
    
    # Initialize RQ queue
    task_queue = Queue(connection=redis_conn)
    
except Exception as e:
    print(f"⚠️  Report Utils Redis connection failed: {e}")
    redis_conn = None
    task_queue = None

DB_PATH = 'event_selections.db'

def process_report_async(apps, period, selected_events):
    """Process report asynchronously using RQ"""
    if task_queue is None:
        logger.error("Cannot process report - Redis/task_queue not available")
        return None
        
    logger.info(f"Starting report generation for period: {period}")
    logger.info(f"Number of apps to process: {len(apps) if apps else 'all active apps'}")
    
    job = task_queue.enqueue(
        'app.process_report_task',
        args=(apps, period, selected_events),
        job_timeout='1h'
    )
    logger.info(f"Report generation job enqueued with ID: {job.id}")
    return job.id

def get_fraud_data(apps, period):
    """Get fraud data for the specified period"""
    logger.info(f"Starting fraud data retrieval for period: {period}")
    logger.info(f"Number of apps to process: {len(apps) if apps else 'all active apps'}")
    # Implementation of fraud data retrieval
    # This is a placeholder - you'll need to implement the actual fraud data retrieval logic
    pass

def get_active_app_ids():
    """Get list of active app IDs from database"""
    logger.info("Retrieving active app IDs from database")
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT app_id FROM app_event_selections WHERE is_active = 1')
    active_apps = [row[0] for row in c.fetchall()]
    conn.close()
    logger.info(f"Found {len(active_apps)} active apps")
    return active_apps 