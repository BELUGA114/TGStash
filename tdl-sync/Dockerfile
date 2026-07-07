FROM iyear/tdl:latest AS tdl

FROM python:3.12-slim

# ca-certificates: tdl 是纯静态二进制，但走 TLS 连接 Telegram 服务器需要根证书
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=tdl /usr/bin/tdl /usr/local/bin/tdl

WORKDIR /app
COPY db.py archiver.py ./

ENTRYPOINT ["python", "archiver.py"]
