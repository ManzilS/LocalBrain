# ── Stage 1: Build ──────────────────────────────────────
FROM python:3.12-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app
COPY pyproject.toml ./
RUN uv sync --frozen --no-dev

COPY src/ src/
COPY plugins.yaml access.config.json ./

# ── Stage 2: Runtime ────────────────────────────────────
FROM python:3.12-slim AS runtime

RUN groupadd -r app && useradd -r -g app app

WORKDIR /app

COPY --from=builder /app/.venv .venv
COPY --from=builder /app/src src
COPY --from=builder /app/plugins.yaml /app/access.config.json ./
COPY --from=builder /usr/local/bin/uv /usr/local/bin/uv

ENV PATH="/app/.venv/bin:$PATH"
ENV LOCALBRAIN_HOST=0.0.0.0
ENV LOCALBRAIN_PORT=8090
ENV LOCALBRAIN_DATA_DIR=/data

RUN mkdir -p /data && chown app:app /data

USER app

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8090/health')"

EXPOSE 8090

CMD ["uv", "run", "python", "-m", "src.gateway.main"]
