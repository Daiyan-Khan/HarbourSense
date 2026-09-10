FROM node:24-alpine AS build

WORKDIR /app

COPY package.json package-lock.json ./
RUN apk add --no-cache git && npm ci --no-audit --no-fund

COPY . .
ARG REACT_APP_DATA_SOURCE=live
ARG REACT_APP_API_BASE_URL=http://localhost:8000
ARG PUBLIC_URL=/
ENV REACT_APP_DATA_SOURCE=$REACT_APP_DATA_SOURCE
ENV REACT_APP_API_BASE_URL=$REACT_APP_API_BASE_URL
ENV PUBLIC_URL=$PUBLIC_URL
RUN npm run build

FROM node:24-alpine
WORKDIR /app
RUN npm install -g serve@14.2.4 --no-audit --no-fund
COPY --from=build /app/build ./build
USER node
EXPOSE 3000
CMD ["serve", "-s", "build", "-l", "3000"]
