import json
import sqlite3
from datetime import datetime
import pytz
import requests
import time
from rq import Queue
from redis import Redis
import logging
import os

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Redis connection
redis_conn = Redis(host='localhost', port=6379, db=0)
# Initialize RQ queue
task_queue = Queue(connection=redis_conn)

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
    # Check for Railway environment variables
    if any(os.getenv(var) for var in railway_vars):
        return True
    
    # Fallback: Check for Railway-specific conditions
    # Railway typically sets PORT environment variable
    if os.getenv('PORT') and not os.getenv('RAILWAY_ENVIRONMENT'):
        # Additional Railway detection - Railway apps usually run on port 8080 or similar
        port = os.getenv('PORT', '5000')
        if port != '5000':  # Default Flask port is 5000, Railway uses different ports
            return True
    
    return False

DB_PATH = os.getenv('DB_PATH', '/data/event_selections.db' if is_railway_environment() else 'event_selections.db')

def process_report_async(apps, period, selected_events):
    """Process report asynchronously using RQ"""
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