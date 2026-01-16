FROM python:3.10-slim

WORKDIR /app

# Install dependencies for HDFS, Pandas, and Parquet
RUN pip install --no-cache-dir hdfs pandas pyarrow requests

COPY process_silver.py .

ENV PYTHONUNBUFFERED=1

CMD ["python", "process_silver.py"]