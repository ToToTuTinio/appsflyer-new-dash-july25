#!/bin/bash

# Get the absolute path of the project directory
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Create systemd service file
cat > appsflyer-dashboard.service << EOL
[Unit]
Description=Appsflyer Dashboard Service
After=network.target

[Service]
Type=simple
User=$USER
WorkingDirectory=$PROJECT_DIR
Environment="PATH=$PROJECT_DIR/venv/bin:/usr/local/bin:/usr/bin:/bin"
Environment="FLASK_ENV=development"
Environment="FLASK_DEBUG=1"
Environment="PYTHONUNBUFFERED=1"
Environment="HOME=/home/$USER"
Environment="DISPLAY=:0"
Environment="CHROME_DRIVER_PATH=/usr/local/bin/chromedriver"
ExecStartPre=/bin/bash -c 'if [ ! -f /usr/local/bin/chromedriver ]; then echo "ChromeDriver not found. Installing..."; sudo apt-get update && sudo apt-get install -y chromium-chromedriver; fi'
ExecStart=$PROJECT_DIR/venv/bin/gunicorn app:app -w 4 -b 0.0.0.0:5000 --timeout 3600 --log-level debug --capture-output
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOL

# Move service file to systemd directory
sudo mv appsflyer-dashboard.service /etc/systemd/system/

# Reload systemd to recognize new service
sudo systemctl daemon-reload

# Enable service to start on boot
sudo systemctl enable appsflyer-dashboard

# Stop existing service if running
sudo systemctl stop appsflyer-dashboard

# Start the service
sudo systemctl start appsflyer-dashboard

# Show service status
echo "Service status:"
sudo systemctl status appsflyer-dashboard

# Show logs
echo "Showing logs (Press Ctrl+C to stop)..."
echo "----------------------------------------"
sudo journalctl -u appsflyer-dashboard -f 