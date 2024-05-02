FROM python:3.11-slim AS base
WORKDIR /app


FROM base AS build-proto

RUN pip install --no-cache-dir grpcio-tools

COPY ./proto/ ./proto/
RUN python3 -m grpc_tools.protoc \
    -Iproto/ \
    --python_out=. --pyi_out=. --grpc_python_out=. \
    proto/priceest/prices.proto proto/commons/*.proto



FROM base AS app

RUN apt-get update && apt-get install -y \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY ./requirements.txt .
RUN pip install --no-cache-dir -r ./requirements.txt

COPY ./src .
COPY --from=build-proto /app/priceest priceest
COPY --from=build-proto /app/commons commons

RUN mkdir -p /app/out

EXPOSE 50051

CMD python3 predict.py