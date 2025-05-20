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

# Start gunicorn with nohup to keep it running after terminal closure
nohup gunicorn app:app \
    -w 2 \
    -b 0.0.0.0:5000 \
    --timeout 3600 \
    --log-level debug \
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

# Save the PID to a file for later use
echo $SERVER_PID > ../server.pid

# Function to handle script termination
cleanup() {
    echo "Shutting down server..."
    kill $SERVER_PID 2>/dev/null
    rm -f ../server.pid
    exit 0
}

# Set up trap to catch termination signal
trap cleanup SIGINT SIGTERM

# Show logs in real-time with timestamps, filtering but keeping important requests
echo "Server started. Showing important logs (Press Ctrl+C to stop)..."
echo "----------------------------------------"
tail -f ../gunicorn.out | while read line; do
    # Keep important page navigation and API calls
    if echo "$line" | grep -q "GET /api/\|GET /dashboard\|GET /stats\|GET /fraud\|POST /get_stats\|POST /get_fraud\|POST /start-report\|GET /report-status\|POST /event-selections\|GET /active-apps\|POST /clear-apps-cache\|DEBUG in app:\|ERROR in app:\|WARNING in app:"; then
        # Add timestamp and print the line
        echo "$(date '+%Y-%m-%d %H:%M:%S') $line"
    # Keep non-HTTP request logs (usually application logs)
    elif ! echo "$line" | grep -q "GET\|POST\|PUT\|DELETE\|HEAD\|OPTIONS\|200\|301\|302\|304\|400\|401\|403\|404\|500\|502\|503\|504"; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') $line"
    fi
done 