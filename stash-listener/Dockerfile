FROM iyear/tdl:0.20.4 AS tdl

# 编译阶段：用完整镜像编译 TgCrypto（无预编译 wheel）
FROM python:3.12 AS builder
WORKDIR /app
COPY stash-listener/requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# 静态 ffmpeg。必须是 GPL 版：LGPL 构建不含 libx264 编码器。
# 从不可变的 OCI 镜像层取，而不是 BtbN release 资产 —— BtbN 的 autobuild 会被
# GC，钉死的 tag 迟早 404（见提交 e34d9c）。mwader/static-ffmpeg 是 GPL 静态
# 构建、含 libx264，二进制在 /ffmpeg /ffprobe。按 multi-arch index digest 钉死：
# 同一个 git tag 永远拿到同一份 9.0.2，且镜像层不像 release 资产会被删。
# 升级只改 tag + digest，libx264 回归由 CI 冒烟（真跑一次 -c:v libx264）兜住。
FROM mwader/static-ffmpeg:9.0.2@sha256:7d9bdaaf887f7e6ce6151f67325c344074b5ff1fb75316011c3376503e449a7b AS ffmpeg

# 运行阶段：仅保留编译产物 + 静态 ffmpeg
FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /install /usr/local
COPY --from=tdl /usr/bin/tdl /usr/local/bin/tdl
COPY --from=ffmpeg /ffmpeg /ffprobe /usr/local/bin/
# 独立运维脚本也需从 /app 导入 tgstash 包。
ENV PYTHONPATH=/app
COPY stash-listener/*.py ./
COPY stash-listener/tgstash/ ./tgstash/
COPY scripts/ ./scripts/
CMD ["python", "-u", "listener.py"]
