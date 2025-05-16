#!/bin/bash

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Function to print status messages
print_status() {
    echo -e "${GREEN}[✓] $1${NC}"
}

print_error() {
    echo -e "${RED}[✗] $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}[!] $1${NC}"
}

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    print_error "Python 3 is not installed. Please install Python 3 and try again."
    exit 1
fi

# Check if virtual environment exists, create if it doesn't
if [ ! -d "venv" ]; then
    print_warning "Virtual environment not found. Creating one..."
    python3 -m venv venv
    if [ $? -ne 0 ]; then
        print_error "Failed to create virtual environment"
        exit 1
    fi
fi

# Activate virtual environment
source venv/bin/activate
if [ $? -ne 0 ]; then
    print_error "Failed to activate virtual environment"
    exit 1
fi

# Install/upgrade pip
python3 -m pip install --upgrade pip

# Install requirements
print_status "Installing requirements..."
pip install -r requirements.txt
pip install -r backend/requirements.txt

# Check if .env file exists
if [ ! -f ".env" ]; then
    print_warning "No .env file found. Creating template..."
    echo "FLASK_APP=app.py
FLASK_ENV=development
FLASK_DEBUG=1
PYTHONUNBUFFERED=1" > .env
fi

# Set environment variables
export $(cat .env | xargs)

# Create logs directory if it doesn't exist
mkdir -p logs

# Start the server in the background
cd backend
print_status "Starting server..."
gunicorn -w 4 -b 0.0.0.0:5000 app:app \
    --timeout 3600 \
    --log-level debug \
    --access-logfile ../logs/access.log \
    --error-logfile ../logs/error.log \
    --capture-output \
    --enable-stdio-inheritance \
    --log-format '%(asctime)s [%(levelname)s] %(message)s' \
    --date-format '%Y-%m-%d %H:%M:%S' \
    >> ../logs/gunicorn.log 2>&1 &

# Store the background process ID
SERVER_PID=$!

# Function to handle script termination
cleanup() {
    print_status "Shutting down server..."
    kill $SERVER_PID 2>/dev/null
    print_status "Server stopped"
    exit 0
}

# Set up trap to catch termination signal
trap cleanup SIGINT SIGTERM

# Show logs in real-time
print_status "Server started successfully!"
print_status "Showing logs (Press Ctrl+C to stop)..."
echo "----------------------------------------"
tail -f ../logs/gunicorn.log | while read line; do
    echo "$(date '+%Y-%m-%d %H:%M:%S') $line"
done 