FROM python:3.10-slim

WORKDIR /app
COPY producer.py /app/producer.py
RUN pip install --no-cache-dir kafka-python requests requests_tor

CMD ["python", "producer.py"]
# CMD ["python", "-c", "import time; time.sleep(10**9)"]
