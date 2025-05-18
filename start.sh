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
    --access-logfile /dev/null \
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

# Show logs in real-time with timestamps, filtering out HTTP access logs
echo "Server started. Showing important logs (Press Ctrl+C to stop)..."
echo "----------------------------------------"
tail -f ../gunicorn.out | grep -v "GET\|POST\|PUT\|DELETE\|HEAD\|OPTIONS" | while read line; do
    # Skip lines containing common HTTP status codes
    if ! echo "$line" | grep -q "200\|301\|302\|304\|400\|401\|403\|404\|500\|502\|503\|504"; then
        # Add timestamp and print the line
        echo "$(date '+%Y-%m-%d %H:%M:%S') $line"
    fi
done 