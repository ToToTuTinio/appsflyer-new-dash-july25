import os
import sys
from rq import Connection, Worker
from redis import Redis

# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the app to ensure all dependencies are loaded
from backend.app import app

if __name__ == '__main__':
    # Connect to Redis
    redis_conn = Redis(host='localhost', port=6379, db=0)
    
    # Start the worker
    with Connection(redis_conn):
        worker = Worker(['default'])
        worker.work() 