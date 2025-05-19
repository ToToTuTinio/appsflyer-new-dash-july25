#!/bin/bash

# Check if running as root
if [ "$EUID" -ne 0 ]; then 
    echo "Please run as root (sudo)"
    exit 1
fi

# Create log directory if it doesn't exist
mkdir -p /home/or/appsflyer-dash-2025-live/logs

# Set proper permissions
chown -R or:or /home/or/appsflyer-dash-2025-live/logs

# Copy service file to systemd directory
cp appsflyer-dashboard.service /etc/systemd/system/

# Reload systemd to recognize new service
systemctl daemon-reload

# Enable service to start on boot
systemctl enable appsflyer-dashboard

# Start the service
systemctl start appsflyer-dashboard

# Check service status
echo "Checking service status..."
systemctl status appsflyer-dashboard

echo "Service has been installed and started. You can check logs with:"
echo "journalctl -u appsflyer-dashboard -f" 