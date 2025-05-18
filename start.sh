#!/bin/bash

# Start the server in the background and redirect output to gunicorn.out
cd backend
source ../venv/bin/activate

# Set environment variables for logging
export FLASK_ENV=development
export FLASK_DEBUG=1
export PYTHONUNBUFFERED=1

# Clear previous log file
echo "" > ../gunicorn.out

# Start gunicorn with basic configuration
gunicorn app:app \
    -w 4 \
    -b 0.0.0.0:5000 \
    --timeout 3600 \
    --log-level debug \
    --error-logfile ../gunicorn.out \
    --access-logfile ../gunicorn.out \
    --capture-output \
    >> ../gunicorn.out 2>&1 &

# Store the background process ID
SERVER_PID=$!

# Wait a moment to check if the server started successfully
sleep 2
if ! ps -p $SERVER_PID > /dev/null; then
    echo "Failed to start server. Check gunicorn.out for details."
    exit 1
fi

# Function to handle script termination
cleanup() {
    echo "Shutting down server..."
    kill $SERVER_PID 2>/dev/null
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