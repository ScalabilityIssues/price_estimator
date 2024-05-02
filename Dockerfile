# Build ml-data-scarper image
FROM python:3.11-slim AS base
WORKDIR /app

COPY ./requirements.txt .
RUN pip install --no-cache-dir -r ./requirements.txt && playwright install --with-deps chromium

COPY ./crontab /app
RUN apt-get update && apt-get install -y cron
RUN crontab /app/crontab

CMD ["crond", "-f"]