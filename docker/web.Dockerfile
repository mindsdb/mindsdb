FROM node:22-slim AS builder

WORKDIR /build
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci --ignore-scripts
COPY frontend/ ./
RUN npm run build:web

FROM nginx:alpine-slim AS runtime

LABEL org.opencontainers.image.title="cowork-web"
LABEL org.opencontainers.image.source="https://github.com/mindsdb/minds-platform"

COPY --from=builder /build/dist/renderer-web/ /usr/share/nginx/html/
COPY docker/nginx.conf /etc/nginx/conf.d/default.conf

COPY frontend/scripts/nginx-logging.conf.template /etc/nginx/diagnostic-logging.conf.template
COPY frontend/scripts/nginx-diagnostics.sh /usr/local/bin/nginx-diagnostics
ENV LOG_LEVEL=WARNING ACCESS_LOG_MODE=errors
STOPSIGNAL SIGTERM
CMD ["/bin/sh", "/usr/local/bin/nginx-diagnostics"]

EXPOSE 80
