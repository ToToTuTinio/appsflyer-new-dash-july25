#!/bin/bash

# Start Redis if not running
redis-cli ping > /dev/null 2>&1 || redis-server &

# Start the RQ worker in the background
python backend/worker.py &

# Start the Flask app
python backend/app.py 