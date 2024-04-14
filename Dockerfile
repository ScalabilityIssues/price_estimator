# Build ml-data-scarper image

FROM python:3.11.7-alpine3.19
WORKDIR /app

COPY ./requirements.txt .

RUN pip install --no-cache-dir -r ./requirements.txt
RUN rm ./requirements.txt
