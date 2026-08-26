FROM iyear/tdl:0.20.4 AS tdl

# 编译阶段：用完整镜像编译 TgCrypto（无预编译 wheel）
FROM python:3.12 AS builder
WORKDIR /app
COPY stash-listener/requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# 使用静态编译的 ffmpeg。必须是 GPL 版：LGPL 构建不含 libx264 编码器
FROM alpine:3.21 AS ffmpeg-fetcher
# 钉死具体构建。原先用 releases/download/latest/ 浮动链接，同一个 git tag
# 隔几个月重新构建可能拿到完全不同的 ffmpeg
ARG FFMPEG_RELEASE=autobuild-2026-08-25-13-06
ARG FFMPEG_BUILD=ffmpeg-N-126264-g007cd1fd43-linux64-gpl
RUN wget -q "https://github.com/BtbN/FFmpeg-Builds/releases/download/${FFMPEG_RELEASE}/${FFMPEG_BUILD}.tar.xz" \
    -O /tmp/ffmpeg.tar.xz && \
    tar -xf /tmp/ffmpeg.tar.xz -C /tmp && \
    mv "/tmp/${FFMPEG_BUILD}/bin/ffmpeg" \
       "/tmp/${FFMPEG_BUILD}/bin/ffprobe" \
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
