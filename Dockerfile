FROM python:3.12-slim

# Install system dependencies, Chrome, and ALL shell utilities
RUN apt-get update && apt-get install -y \
    bash \
    coreutils \
    util-linux \
    wget \
    gnupg \
    unzip \
    curl \
    && wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list \
    && apt-get update \
    && apt-get install -y google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

# Install ChromeDriver
RUN CHROMEDRIVER_VERSION=`curl -sS chromedriver.storage.googleapis.com/LATEST_RELEASE` && \
    wget -N http://chromedriver.storage.googleapis.com/$CHROMEDRIVER_VERSION/chromedriver_linux64.zip -P ~/ && \
    unzip ~/chromedriver_linux64.zip -d ~/ && \
    rm ~/chromedriver_linux64.zip && \
    mv ~/chromedriver /usr/local/bin/chromedriver && \
    chmod +x /usr/local/bin/chromedriver

# Create a fake 'cd' command as backup
RUN echo '#!/bin/bash' > /usr/local/bin/cd && echo 'builtin cd "$@"' >> /usr/local/bin/cd && chmod +x /usr/local/bin/cd

# Set working directory
WORKDIR /app

# Copy requirements and install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV SHELL=/bin/bash

# Create startup script with explicit bash
RUN echo '#!/bin/bash\ncd /app/backend\nexec python app.py' > /start.sh && chmod +x /start.sh

# Use bash to run the startup script
CMD ["/bin/bash", "/start.sh"] 