# Dockerfile for Node.js Port Simulator
FROM node:18-slim

WORKDIR /app

# Install Node dependencies (assuming package.json in repo)
COPY package*.json ./
RUN npm install

# Copy simulation code
COPY . ./

# Copy certs for AWS IoT
COPY certs/ ./certs/

CMD ["nodemon", "port.js"]  # Adjust to your main sim file, e.g., port_simulator.js
