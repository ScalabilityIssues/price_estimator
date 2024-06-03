FROM python:3.11-slim AS base
WORKDIR /app

COPY requirements/price_prediction/requirements.txt .

RUN apt-get update && apt-get install -y \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir -r ./requirements.txt

COPY src/train.py src/utils_predict.py src/progress.py /app/src/

COPY configs/train /app/configs/train

VOLUME [ "/app/out", "/app/data/scraped/" ]


LABEL org.opencontainers.image.source="https://github.com/ScalabilityIssues/price_estimator"

ENTRYPOINT [ "python3", "src/train.py" ]