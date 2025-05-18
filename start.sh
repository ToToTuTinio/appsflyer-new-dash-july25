#!/bin/bash

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
    --logger-class gunicorn.glogging.Logger \
    --access-logformat "%(asctime)s [%(levelname)s] %(message)s" \
    >> ../gunicorn.out 2>&1 &

# Store the background process ID
SERVER_PID=$!

# Function to handle script termination
cleanup() {
    echo "Shutting down server..."
    kill $SERVER_PID
    exit 0
}

# Set up trap to catch termination signal
trap cleanup SIGINT SIGTERM

# Show logs in real-time with timestamps
echo "Server started. Showing logs (Press Ctrl+C to stop)..."
echo "----------------------------------------"
tail -f ../gunicorn.out | while read line; do
    echo "$(date '+%Y-%m-%d %H:%M:%S') $line"
done 