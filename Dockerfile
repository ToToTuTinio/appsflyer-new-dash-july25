FROM python:3.12-slim

# Install basic shell commands (including cd) + Chrome requirements
RUN apt-get update && apt-get install -y \
    bash \
    coreutils \
    wget \
    unzip \
    curl \
    gnupg \
    && wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update \
    && apt-get install -y google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first (for better Docker layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy everything (including your working bin/ directory)
COPY . .

# Make ChromeDriver executable and ensure proper permissions
RUN chmod +x bin/chromedriver* 2>/dev/null || true

# Set environment variables for Railway
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Set final working directory to backend (where app.py is located)
WORKDIR /app/backend

# Run Python directly - no shell commands needed
CMD ["python3", "app.py"] 