import os
import sys
from redis import Redis
from rq import Worker, Queue, Connection

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Redis connection
redis_conn = Redis(host='localhost', port=6379, db=0)

if __name__ == '__main__':
    # Start worker
    with Connection(redis_conn):
        worker = Worker([Queue('default')])
        worker.work() 