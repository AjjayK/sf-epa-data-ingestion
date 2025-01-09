FROM python:3.9-slim

WORKDIR /app

ARG SNOWFLAKE_USER
ARG SNOWFLAKE_PASSWORD
ARG SNOWFLAKE_ACCOUNT
ARG ENVIRONMENT=test


ENV SNOWFLAKE_USER=$SNOWFLAKE_USER
ENV SNOWFLAKE_PASSWORD=$SNOWFLAKE_PASSWORD
ENV SNOWFLAKE_ACCOUNT=$SNOWFLAKE_ACCOUNT
ENV ENVIRONMENT=$ENVIRONMENT
# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

RUN echo "Container built for environment: $ENVIRONMENT"

# Run script
CMD ["python", "epa_processor.py"]