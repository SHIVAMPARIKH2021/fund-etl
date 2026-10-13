FROM python:3.14-slim

WORKDIR /app

# Install uv for fast dependency management
RUN pip install uv

# Copy project files
COPY . .

# Install dependencies
RUN uv sync

# Set entrypoint
ENTRYPOINT ["python", "main.py"]

