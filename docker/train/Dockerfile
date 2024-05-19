FROM python:3.11-slim AS base
WORKDIR /app

COPY requirements/price_prediction/requirements.txt .
COPY .env .

RUN apt-get update && apt-get install -y \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir -r ./requirements.txt