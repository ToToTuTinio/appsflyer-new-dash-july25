#!/bin/bash

# Create logs directory if it doesn't exist
mkdir -p logs

# Start the server in the background and redirect output to gunicorn.out
cd backend
source ../venv/bin/activate

# Set environment variables for logging
export FLASK_ENV=development
export FLASK_DEBUG=1
export PYTHONUNBUFFERED=1
export LOG_LEVEL=DEBUG

# Start gunicorn with detailed logging
echo "Starting server..."
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

# Function to clean up on exit
cleanup() {
    echo "Stopping server..."
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