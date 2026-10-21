# One image for both the API and the ingest worker (different command).
# Build from the repo root:   docker build -f app/Dockerfile -t iot-app .

# ---- stage 1: install dependencies into a virtualenv (needs pip, build tools, caches)
FROM python:3.12-slim AS build
WORKDIR /src
# Copy ONLY requirements first: this layer is cached until requirements.txt changes,
# so editing your code doesn't re-download every package.
COPY app/requirements.txt .
RUN python -m venv /venv && /venv/bin/pip install --no-cache-dir -r requirements.txt

# ---- stage 2: the runtime image: just Python + the venv + our code (smaller, fewer tools to attack)
FROM python:3.12-slim
RUN useradd --create-home --uid 1000 app
COPY --from=build /venv /venv
WORKDIR /srv
COPY app ./app
USER app
ENV PATH=/venv/bin:$PATH \
    PYTHONUNBUFFERED=1
EXPOSE 8000
# Default: the API. The worker overrides this with: python -m app.ingest.worker
CMD ["uvicorn", "app.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
