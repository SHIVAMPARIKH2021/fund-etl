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

# Step 1: Install ONLY third-party dependencies (cached unless locks change)
RUN uv sync --frozen --no-dev --no-install-project

# Step 2: Copy application source code
COPY src/ ./src/
COPY main.py ./

# Step 3: Fast install of the local project package
RUN uv sync --frozen --no-dev

# Set python path
ENV PYTHONPATH=/app/src

ENTRYPOINT ["uv", "run", "python", "main.py"]