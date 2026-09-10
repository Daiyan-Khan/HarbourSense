# Dockerfile for Node.js Port Simulator
FROM node:24-slim

WORKDIR /app

# Install Node dependencies (assuming package.json in repo)
COPY package*.json ./
RUN npm ci

# Copy simulation code
COPY . ./

COPY entrypoint.sh /entrypoint.sh
RUN sed -i 's/\r$//' /entrypoint.sh && chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
CMD ["node", "port.js"]
