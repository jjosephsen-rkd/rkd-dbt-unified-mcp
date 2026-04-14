FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN pip install uv && uv sync --no-dev

COPY src/ ./src/
COPY config/ ./config/

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s \
    CMD curl -f http://localhost:8000/health || exit 1

CMD ["uv", "run", "python", "-m", "src.dbt_mcp.server"]
