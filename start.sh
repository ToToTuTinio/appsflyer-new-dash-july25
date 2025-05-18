#!/bin/bash

# Check if Redis is already running
if ! pgrep redis-server > /dev/null; then
    echo "Starting Redis server..."
    redis-server --daemonize yes
else
    echo "Redis server is already running"
fi

# Start the server in the background and redirect output to gunicorn.out
cd backend
source ../venv/bin/activate

# Set environment variables for logging
export FLASK_ENV=development
export FLASK_DEBUG=1
export PYTHONUNBUFFERED=1

# Start gunicorn with detailed logging
gunicorn -w 4 -b 0.0.0.0:5000 app:app \
    --timeout 3600 \
    --log-level debug \
    --access-logfile - \
    --error-logfile - \
    --capture-output \
    --enable-stdio-inheritance \
    --log-format '%(asctime)s [%(levelname)s] %(message)s' \
    --date-format '%Y-%m-%d %H:%M:%S' \
    >> ../gunicorn.out 2>&1 &

# Store the background process ID
SERVER_PID=$!

# Start RQ worker in the background
python worker.py >> ../worker.out 2>&1 &
WORKER_PID=$!

# Function to handle script termination
cleanup() {
    echo "Shutting down server and worker..."
    kill $SERVER_PID
    kill $WORKER_PID
    redis-cli shutdown
    exit 0
}

# Set up trap to catch termination signal
trap cleanup SIGINT SIGTERM

# Show logs in real-time with timestamps
echo "Server and worker started. Showing logs (Press Ctrl+C to stop)..."
echo "----------------------------------------"
tail -f ../gunicorn.out ../worker.out | while read line; do
    echo "$(date '+%Y-%m-%d %H:%M:%S') $line"
done 