FROM python:3.12-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies (excluding Airflow — installed separately)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Airflow with constraints
RUN pip install --no-cache-dir "apache-airflow==2.10.5" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt"

# Copy project code
COPY . .

ENV AIRFLOW_HOME=/app/airflow

CMD ["airflow", "standalone"]
