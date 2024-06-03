# Build ml-data-scarper image
FROM python:3.11-slim AS base
WORKDIR /app

COPY requirements/ml_data/requirements.txt .
COPY .env .

RUN pip install --no-cache-dir -r ./requirements.txt && playwright install --with-deps chromium

COPY src/scrape.py src/utils_scrape.py src/progress.py /app/src/

COPY configs/scrape /app/configs/scrape

VOLUME [ "/app/data/scraped/" ]

LABEL org.opencontainers.image.source="https://github.com/ScalabilityIssues/price_estimator"

ENTRYPOINT [ "python3", "src/scrape.py" ]