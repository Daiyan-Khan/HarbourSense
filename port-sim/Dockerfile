FROM node:18-slim
WORKDIR /app
COPY . .
RUN npm install
EXPOSE 8080  # Adjust if needed
CMD ["sh", "-c", "node sensor.js & node port.js"]  # Run both in one container; split if preferred
