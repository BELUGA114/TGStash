# 编译阶段：用完整镜像编译 TgCrypto（无预编译 wheel）
FROM python:3.12 AS builder
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# 使用静态编译的 ffmpeg
FROM alpine:3.21 AS ffmpeg-fetcher
RUN wget -q "https://github.com/BtbN/FFmpeg-Builds/releases/download/latest/ffmpeg-master-latest-linux64-lgpl.tar.xz" \
    -O /tmp/ffmpeg.tar.xz && \
    tar -xf /tmp/ffmpeg.tar.xz -C /tmp && \
    mv /tmp/ffmpeg-master-latest-linux64-lgpl/bin/ffmpeg \
       /tmp/ffmpeg-master-latest-linux64-lgpl/bin/ffprobe \
       /usr/local/bin/

# 运行阶段：仅保留编译产物 + 静态 ffmpeg
FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /install /usr/local
COPY --from=ffmpeg-fetcher /usr/local/bin/ffmpeg /usr/local/bin/ffprobe /usr/local/bin/
COPY db.py listener.py login.py search.py ./
CMD ["python", "-u", "listener.py"]
