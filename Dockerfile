FROM python:3.14-slim

WORKDIR /app

# Pin uv to match your build-backend configuration
RUN pip install "uv>=0.12.13,<0.13.0"

# Copy dependency manifests
COPY pyproject.toml uv.lock ./

# Install external pre-compiled wheels into .venv
RUN uv sync --frozen --no-dev --no-install-project

# Copy application code
COPY README.md* ./
COPY src/ ./src/
COPY main.py ./

ENV PYTHONPATH=/app/src

# Run using the baked virtual environment without run-time sync
ENTRYPOINT ["uv", "run", "--no-sync", "python", "main.py"]