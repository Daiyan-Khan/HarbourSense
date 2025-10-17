# edge-analyzer/Dockerfile
FROM python:3.9-slim

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code,
# INCLUDING edge-analyzer.py and anomaly_model.pkl
COPY . .

# Run the analyzer script
CMD ["python", "edge-analyzer.py"]
