# Build ml-data-scarper image
FROM python:3.11-slim AS base
WORKDIR /app

COPY requirements/ml_data/requirements.txt .
COPY .env .

RUN pip install --no-cache-dir -r ./requirements.txt && playwright install --with-deps chromium
