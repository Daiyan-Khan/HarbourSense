# Dockerfile - For Python Backend Microservice

FROM python:3.10-slim

WORKDIR /app

# Install system dependencies (for any C extensions in ML libs)
RUN apt-get update && apt-get install -y build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy app source
COPY . ./

# Expose FastAPI port
EXPOSE 8000

# Copy and set up entrypoint script
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Run the entrypoint
ENTRYPOINT ["/entrypoint.sh"]
