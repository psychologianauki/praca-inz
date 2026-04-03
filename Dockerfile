FROM python:3.11-slim

WORKDIR /app

# Instalacja zależności systemowych i Chromium dla Selenium
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    libeccodes0 \
    libeccodes-data \
    libeccodes-tools \
    wget \
    curl \
    unzip \
    gnupg \
    chromium \
    chromium-driver \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "main.py"]