# syntax=docker/dockerfile:1.7

FROM python:3.14-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:0.11.18 /uv /usr/local/bin/uv

WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never

# Dependencies first, so the layer is reused while the source changes.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY src ./src
RUN uv sync --frozen --no-dev --no-editable

# Runtime stage. confluent-kafka wheels bundle librdkafka with SSL and SCRAM,
# so nothing beyond the interpreter is needed.
FROM python:3.14-slim

RUN useradd --create-home --uid 10001 app
WORKDIR /app

COPY --from=builder /app/.venv /app/.venv

ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1
USER app

ENTRYPOINT ["warehouse-delivery"]
