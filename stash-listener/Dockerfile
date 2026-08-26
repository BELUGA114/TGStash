FROM iyear/tdl:latest AS tdl

# 编译阶段：用完整镜像编译 TgCrypto（无预编译 wheel）
FROM python:3.12 AS builder
WORKDIR /app
COPY stash-listener/requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# 使用静态编译的 ffmpeg
FROM alpine:3.21 AS ffmpeg-fetcher
RUN wget -q "https://github.com/BtbN/FFmpeg-Builds/releases/download/latest/ffmpeg-master-latest-linux64-gpl.tar.xz" \
    -O /tmp/ffmpeg.tar.xz && \
    tar -xf /tmp/ffmpeg.tar.xz -C /tmp && \
    mv /tmp/ffmpeg-master-latest-linux64-gpl/bin/ffmpeg \
       /tmp/ffmpeg-master-latest-linux64-gpl/bin/ffprobe \
       /usr/local/bin/

# 运行阶段：仅保留编译产物 + 静态 ffmpeg
FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /install /usr/local
COPY --from=tdl /usr/bin/tdl /usr/local/bin/tdl
COPY --from=ffmpeg-fetcher /usr/local/bin/ffmpeg /usr/local/bin/ffprobe /usr/local/bin/
# 模块被拍平到 /app，scripts/ 下的脚本靠 PYTHONPATH 才能 import 到 db 等模块
ENV PYTHONPATH=/app
# 通配而非逐个列举：历史上两次漏文件导致容器启动 ModuleNotFoundError。
# .dockerignore 已排除 __pycache__/ 和 *.pyc
COPY stash-listener/*.py ./
COPY scripts/ ./scripts/
CMD ["python", "-u", "listener.py"]
