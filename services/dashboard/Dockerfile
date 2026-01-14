# Dockerfile for Streamlit previous Dashboard Service
FROM python:3.10-slim

WORKDIR /app
COPY dashboard.py /app/dashboard.py
# Copy plotly wheel dependencies
COPY plotly_offline /app/plotly_offline
RUN pip install --no-cache-dir streamlit hdfs pandas\
    && pip install --no-cache-dir /app/plotly_offline/tenacity-*.whl \
    && pip install --no-cache-dir /app/plotly_offline/plotly-*.whl
EXPOSE 8501
ENV PYTHONUNBUFFERED=1

CMD ["streamlit", "run", "dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]

# CMD ["python", "-c", "import time; time.sleep(10**9)"]