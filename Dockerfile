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

# Copy everything
WORKDIR /app
COPY . .

# Install Python packages
RUN pip install -r requirements.txt

# Make ChromeDriver executable
RUN chmod +x bin/chromedriver* 2>/dev/null || true

# Run EXACTLY like your local setup
WORKDIR /app/backend
CMD ["python3", "app.py"] 