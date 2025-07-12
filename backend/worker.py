import os
import sys
from redis import Redis
from rq import Worker, Queue, Connection
from urllib.parse import urlparse

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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
    print(f"✅ Worker Redis connected successfully to {redis_host}:{redis_port}")
    
except Exception as e:
    print(f"⚠️  Worker Redis connection failed: {e}")
    redis_conn = None

if __name__ == '__main__':
    if redis_conn:
        # Start worker
        with Connection(redis_conn):
            worker = Worker([Queue('default')])
            worker.work()
    else:
        print("⚠️  Cannot start worker - Redis connection failed") 