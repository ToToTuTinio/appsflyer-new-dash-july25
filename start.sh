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
    -w 4 \
    -b 0.0.0.0:5000 \
    --timeout 36000 \
    --keep-alive 5 \
    --graceful-timeout 36000\
    --worker-class sync \
    --worker-connections 1000 \
    --log-level debug \
    --capture-output \
    --preload \
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

echo "Server started successfully with PID: $SERVER_PID"
echo "To view logs, run: ./tail_logs.sh" 