FROM python:3.12-slim

# Install only essential system packages and Chrome (lightweight)
RUN apt-get update && apt-get install -y \
    wget \
    unzip \
    curl \
    && wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update \
    && apt-get install -y google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements and install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the ENTIRE application including your working bin/ directory
COPY . .

# Make sure your local ChromeDriver is executable
RUN chmod +x bin/chromedriver* 2>/dev/null || true

# Set environment variables exactly like local
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Run exactly like your local development
WORKDIR /app/backend
CMD ["python", "app.py"] 