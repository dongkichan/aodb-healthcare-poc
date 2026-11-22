# One image for the API, the seed job, the worker, the tests, and the benchmark.
FROM python:3.12.16-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    HF_HOME=/models

WORKDIR /srv

# CPU-only PyTorch for both arm64 (Apple Silicon) and amd64, then the pinned dependencies.
COPY requirements.txt constraints.txt ./
RUN pip install --index-url https://download.pytorch.org/whl/cpu torch==2.6.0 \
 && pip install -r requirements.txt -c constraints.txt

# Bake the embedding model into the image, so a fresh clone needs no download at demo time.
RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')"
ENV HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1

COPY . .
RUN useradd --system --uid 10001 app && chown -R app /srv /models
USER app
