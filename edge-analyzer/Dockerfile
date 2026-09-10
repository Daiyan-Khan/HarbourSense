# edge-analyzer/Dockerfile
FROM python:3.12-slim

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Generate a model artifact during image build when one is not committed
RUN rm -f anomaly_model.pkl && python create_model.py

CMD ["python", "edge-analyzer.py"]
