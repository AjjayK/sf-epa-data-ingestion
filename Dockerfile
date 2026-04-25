FROM python:3.10-slim

WORKDIR /app

ARG ENVIRONMENT=test
ENV ENVIRONMENT=$ENVIRONMENT

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

RUN echo "Container built for environment: $ENVIRONMENT"

# Run script
CMD ["python", "epa_ingestion.py"]