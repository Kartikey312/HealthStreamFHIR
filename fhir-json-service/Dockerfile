FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY fhir-json-service/requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY fhir-json-service/src ./src
COPY shared ../shared

# Set PYTHONPATH to include shared module
ENV PYTHONPATH=/app:/:$PYTHONPATH

CMD ["python", "src/main.py"]
