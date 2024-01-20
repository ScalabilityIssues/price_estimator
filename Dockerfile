FROM condaforge/miniforge3:latest
WORKDIR /app


COPY ./requirements.yaml /tmp/requirements.yaml
RUN mamba env create -f /tmp/requirements.yaml