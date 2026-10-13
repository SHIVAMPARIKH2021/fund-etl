FROM python:3.14-slim
WORKDIR /app
# Install system dependencies if required
RUN apt-get update && apt-get install -y --no-install-recommends     build-essential     && rm -rf /var/lib/apt/lists/*
# Install uv for fast dependency management
RUN pip install uv
# Copy dependency specification files
COPY pyproject.toml uv.lock ./
# Install project dependencies
RUN uv sync --frozen --no-dev
# Copy application source code
COPY src/ ./src/
COPY main.py ./
# Set python path
ENV PYTHONPATH=/app/src
# Default entrypoint to run the pipeline with command line arguments
ENTRYPOINT ["uv", "run", "python", "main.py"]
