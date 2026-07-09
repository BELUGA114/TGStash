# 编译阶段：用完整镜像编译 TgCrypto（无预编译 wheel）
FROM python:3.12 AS builder
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# 运行阶段：仅保留编译产物
FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates ffmpeg \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /install /usr/local
COPY db.py listener.py login.py search.py ./
CMD ["python", "-u", "listener.py"]
