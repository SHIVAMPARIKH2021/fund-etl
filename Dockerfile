FROM python:3.14-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install uv matching your build system requirement
RUN pip install "uv>=0.12.13,<0.13.0"

# Copy dependency manifests
COPY pyproject.toml uv.lock ./

# Install ONLY third-party dependencies into the virtual environment
RUN uv sync --frozen --no-dev --no-install-project

# Copy application code and files
COPY README.md* ./
COPY src/ ./src/
COPY main.py ./

# Set python path so Python finds packages in /app/src
ENV PYTHONPATH=/app/src

# Default entrypoint to run the pipeline
ENTRYPOINT ["uv", "run", "--no-sync", "python", "main.py"]