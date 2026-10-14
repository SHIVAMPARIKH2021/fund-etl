FROM python:3.14-slim

WORKDIR /app

# Copy the official, standalone uv binary directly (instant, no pip overhead)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Copy dependency manifests
COPY pyproject.toml uv.lock ./

# Install project dependencies
RUN uv sync --frozen --no-dev --no-install-project

# Copy application files
COPY README.md* ./
COPY src/ ./src/
COPY main.py ./

ENV PYTHONPATH=/app/src

# Run using the virtual environment directly
ENTRYPOINT ["uv", "run", "--no-sync", "python", "main.py"]