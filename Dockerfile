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

# Add Google Chrome repository
RUN wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list

# Install Google Chrome
RUN apt-get update && apt-get install -y \
    google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

# Install ChromeDriver compatible with Chrome 138 - Force clean installation
RUN rm -f /usr/local/bin/chromedriver* && \
    CHROME_VERSION=$(google-chrome --version | awk '{print $3}') && \
    echo "Full Chrome version: $CHROME_VERSION" && \
    echo "Installing ChromeDriver for Chrome 138..." && \
    wget -q "https://storage.googleapis.com/chrome-for-testing-public/138.0.7204.87/linux64/chromedriver-linux64.zip" -O chromedriver.zip && \
    unzip -o chromedriver.zip && \
    mv chromedriver-linux64/chromedriver /usr/local/bin/chromedriver && \
    chmod +x /usr/local/bin/chromedriver && \
    rm -rf chromedriver.zip chromedriver-linux64 && \
    echo "ChromeDriver installation complete" && \
    /usr/local/bin/chromedriver --version

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire application
COPY . .

# Set environment variables for Chrome
ENV CHROME_BIN=/usr/bin/google-chrome-stable
ENV CHROMEDRIVER_PATH=/usr/local/bin/chromedriver
ENV DISPLAY=:99
ENV CHROME_OPTS="--no-sandbox --disable-gpu --disable-dev-shm-usage --disable-setuid-sandbox --headless"

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