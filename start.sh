#!/bin/bash

# Get the absolute path of the project directory
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Function to install ChromeDriver
install_chromedriver() {
    echo "Installing ChromeDriver..."
    
    # Create a directory for ChromeDriver if it doesn't exist
    sudo mkdir -p /opt/chromedriver
    
    # Download and install ChromeDriver
    CHROME_DRIVER_VERSION=$(curl -s https://chromedriver.storage.googleapis.com/LATEST_RELEASE)
    wget -q "https://chromedriver.storage.googleapis.com/${CHROME_DRIVER_VERSION}/chromedriver_linux64.zip"
    unzip -q chromedriver_linux64.zip
    sudo mv chromedriver /opt/chromedriver/
    sudo chmod +x /opt/chromedriver/chromedriver
    rm chromedriver_linux64.zip
    
    # Create symlink to make it available in PATH
    sudo ln -sf /opt/chromedriver/chromedriver /usr/local/bin/chromedriver
    sudo ln -sf /opt/chromedriver/chromedriver /usr/bin/chromedriver
    
    echo "ChromeDriver installed successfully"
}

# Check and install ChromeDriver if needed
if ! command -v chromedriver &> /dev/null; then
    install_chromedriver
fi

# Create systemd service file
cat > appsflyer-dashboard.service << EOL
[Unit]
Description=Appsflyer Dashboard Service
After=network.target

[Service]
Type=simple
User=$USER
WorkingDirectory=$PROJECT_DIR
Environment="PATH=$PROJECT_DIR/venv/bin:/usr/local/bin:/usr/bin:/bin:/opt/chromedriver"
Environment="FLASK_ENV=development"
Environment="FLASK_DEBUG=1"
Environment="PYTHONUNBUFFERED=1"
Environment="HOME=/home/$USER"
Environment="DISPLAY=:0"
Environment="CHROME_DRIVER_PATH=/opt/chromedriver/chromedriver"
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