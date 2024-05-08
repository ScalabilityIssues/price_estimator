FROM python:3.11-slim AS base
WORKDIR /app

COPY ./requirements.txt .

RUN apt-get update && apt-get install -y \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir -r ./requirements.txt