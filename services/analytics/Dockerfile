FROM python:3.10-slim

WORKDIR /app

# Install Streamlit
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app
COPY app.py .

# Expose Streamlit port
EXPOSE 8501

# Start Streamlit in headless mode
CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0", "--server.port=8501"]
