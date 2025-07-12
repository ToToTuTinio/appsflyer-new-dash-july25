#!/bin/bash
set -e

# Verify we're in the correct directory (should be /app/backend from Dockerfile WORKDIR)
echo "Current directory: $(pwd)"
echo "Directory contents: $(ls -la)"

# Verify Python is available
echo "Python version: $(python3 --version)"

# Verify required environment variables
echo "Checking environment variables..."
if [ -z "$DASHBOARD_USERNAME" ]; then
    echo "WARNING: DASHBOARD_USERNAME not set"
fi
if [ -z "$DASHBOARD_PASSWORD" ]; then
    echo "WARNING: DASHBOARD_PASSWORD not set"
fi

# Start the application (no cd needed, we're already in the right directory)
echo "Starting Flask application..."
exec python3 app.py 