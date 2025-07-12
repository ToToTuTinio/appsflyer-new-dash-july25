# Use Python 3.11 slim image as base
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies including Chrome and required libraries
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    unzip \
    curl \
    xvfb \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libdrm2 \
    libxkbcommon0 \
    libxss1 \
    libgtk-3-0 \
    libxrandr2 \
    libasound2 \
    libpangocairo-1.0-0 \
    libatk1.0-0 \
    libcairo-gobject2 \
    libgtk-3-0 \
    libgdk-pixbuf2.0-0 \
    libxcomposite1 \
    libxcursor1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxi6 \
    libxrender1 \
    libxtst6 \
    libnss3 \
    libgconf-2-4 \
    && rm -rf /var/lib/apt/lists/*

# Install Chrome and ChromeDriver with exact version matching - avoid cache issues
RUN echo "Installing Chrome 131 and matching ChromeDriver..." && \
    wget -q "https://storage.googleapis.com/chrome-for-testing-public/131.0.6778.108/linux64/chrome-linux64.zip" -O chrome.zip && \
    wget -q "https://storage.googleapis.com/chrome-for-testing-public/131.0.6778.108/linux64/chromedriver-linux64.zip" -O chromedriver.zip && \
    unzip -o chrome.zip && \
    unzip -o chromedriver.zip && \
    mv chrome-linux64 /opt/chrome && \
    mv chromedriver-linux64/chromedriver /usr/local/bin/chromedriver && \
    chmod +x /opt/chrome/chrome && \
    chmod +x /usr/local/bin/chromedriver && \
    rm -rf chrome.zip chromedriver.zip chromedriver-linux64 && \
    echo "Chrome and ChromeDriver installation complete"

# Create Chrome wrapper script for containerized environment
RUN echo '#!/bin/bash\n\
export DISPLAY=${DISPLAY:-:99}\n\
export CHROME_DEVEL_SANDBOX=/opt/chrome/chrome_sandbox\n\
exec /opt/chrome/chrome \\\n\
  --no-sandbox \\\n\
  --disable-gpu \\\n\
  --disable-dev-shm-usage \\\n\
  --disable-setuid-sandbox \\\n\
  --disable-extensions \\\n\
  --disable-default-apps \\\n\
  --disable-background-networking \\\n\
  --disable-background-timer-throttling \\\n\
  --disable-renderer-backgrounding \\\n\
  --disable-backgrounding-occluded-windows \\\n\
  --disable-ipc-flooding-protection \\\n\
  --disable-hang-monitor \\\n\
  --disable-prompt-on-repost \\\n\
  --disable-translate \\\n\
  --disable-crash-reporter \\\n\
  --disable-domain-reliability \\\n\
  --disable-component-update \\\n\
  --disable-client-side-phishing-detection \\\n\
  --disable-back-forward-cache \\\n\
  --disable-field-trial-config \\\n\
  --metrics-recording-only \\\n\
  --disable-background-mode \\\n\
  --password-store=basic \\\n\
  --use-mock-keychain \\\n\
  --force-color-profile=srgb \\\n\
  --single-process \\\n\
  "$@"' > /usr/bin/google-chrome && \
    chmod +x /usr/bin/google-chrome

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire application
COPY . .

# Set environment variables for Chrome
ENV CHROME_BIN=/usr/bin/google-chrome
ENV CHROMEDRIVER_PATH=/usr/local/bin/chromedriver
ENV DISPLAY=:99
ENV CHROME_OPTS="--no-sandbox --disable-gpu --disable-dev-shm-usage --disable-setuid-sandbox --headless"
ENV CHROME_DEVEL_SANDBOX=/opt/chrome/chrome_sandbox
ENV GOOGLE_CHROME_SHIM=/usr/bin/google-chrome

# Create directories for Chrome
RUN mkdir -p /tmp/.X11-unix && chmod 1777 /tmp/.X11-unix

# Change to backend directory
WORKDIR /app/backend

# Note: Running as root for Chrome compatibility in containers
# Chrome flags (--no-sandbox, --disable-setuid-sandbox) provide security

# Expose port
EXPOSE $PORT

# Start the application
CMD ["python", "app.py"] 