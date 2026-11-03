FROM python:3.13-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

WORKDIR /build
COPY . .
# The api extra too, so one image serves the CLI and `tradingagents serve`.
RUN pip install --no-cache-dir ".[api]"

FROM python:3.13-slim AS runtime

# TRADINGAGENTS_IN_DOCKER lets an update notice name the rebuild command.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TRADINGAGENTS_IN_DOCKER=1

COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

RUN useradd --create-home appuser \
 && install -d -m 0755 -o appuser -g appuser /home/appuser/.tradingagents
USER appuser
WORKDIR /home/appuser/app

COPY --from=builder --chown=appuser:appuser /build .

# The web API (docker-compose.api.yml builds this stage). The CLI is in the same
# image: run `tradingagents ...` in the container's terminal.
FROM runtime AS api
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/healthz', timeout=4)"]
ENTRYPOINT ["tradingagents", "serve", "--host", "0.0.0.0", "--port", "8000"]

# The interactive CLI: the last stage, so a plain `docker build` or
# `docker compose build` still builds it.
FROM runtime AS cli
ENTRYPOINT ["tradingagents"]
