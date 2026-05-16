FROM python:3.11-slim

WORKDIR /app

# Copy engine source
COPY engine/ ./engine/
COPY adapters/ ./adapters/
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Default: run self-check
CMD ["python", "-m", "pytest", "engine/", "-v"]
