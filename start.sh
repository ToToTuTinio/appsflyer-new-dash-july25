#!/bin/bash

# Start the server in the background and redirect output to gunicorn.out
cd backend
source ../venv/bin/activate
gunicorn -w 4 -b 0.0.0.0:5000 app:app --timeout 3600 >> ../gunicorn.out 2>&1 &

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

# Show logs in real-time
echo "Server started. Showing logs (Press Ctrl+C to stop)..."
tail -f ../gunicorn.out 