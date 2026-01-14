# Prefect Worker with Snowflake and MinIO support
FROM prefecthq/prefect:2-python3.11

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY flows/ ./flows/
COPY data/ ./data/
COPY scripts/ ./scripts/

# Set Python path
ENV PYTHONPATH=/app

# Default command (overridden by docker-compose)
CMD ["prefect", "worker", "start", "--pool", "default-pool"]
