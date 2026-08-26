# TGStash

轻量 Telegram 个人媒体归档工具——自动备份到私有频道并去除来源，基于 [Kurigram](https://github.com/KurimuzonAkuma/kurigram) 和 [tdl](https://github.com/iyear/tdl) 实现

## 特性

- **双路径归档**：转发媒体自动处理，禁止转发的消息发 t.me 链接即可
- **高速下载**：使用 [tdl](https://github.com/iyear/tdl) 并行分块下载
- **媒体组保留**：整组合并为一条消息上传，维持原始排版
- **视频友好**：使用 ffprobe 重测元数据 + ffmpeg 抽帧缩略图，解决大文件无缩略图/时长显示 00:00
- **视频压缩（可选）**：启用后超过体积阈值的视频用 ffmpeg H.264 CRF 转码归档，压缩产物比原始小才用压缩版，小视频/压缩不划算的保持原样
- **账号安全**：保守限流（BATCH_SIZE/上传冷却/扫描间隔），账号 > 响应速度

## 快速开始

### 1. 准备

1. [my.telegram.org](https://my.telegram.org) 申请 `api_id` / `api_hash`
2. 创建两个频道：接收频道（入口）、备份频道（终点，**私有**）
3. 准备一个 Telegram 账号，加入两个频道并设为管理员

### 2. 本地部署

复制 `.env.example` 为 `.env`，填写凭据

```env
TG_API_ID=your_api_id
TG_API_HASH=your_api_hash
RECEIVE_CHAT_ID=your_receive_chat_id    # 接收频道的 chat id（获取频道 ID 后填写）
ARCHIVE_CHAT_ID=your_archive_chat_id    # 备份频道的 chat id（获取频道 ID 后填写）
HTTP_PROXY=http://host:port             # 代理地址
```

启动容器

```bash
docker compose build
# Kurigram 登录
docker compose run --rm stash-listener python login.py
# tdl 也需要单独登录一次（与 Kurigram 是两套 session）
docker compose run --rm stash-listener tdl -n archiver login -T qr    # 二维码登录
docker compose run --rm stash-listener tdl -n archiver login -T code    # 验证码登录
docker compose up -d
```

获取频道 ID（登录后容器内运行）：

```bash
docker compose run --rm stash-listener python scripts/get_chat_ids.py
```

添加 `-100` 前缀后填入 `.env` 的 `RECEIVE_CHAT_ID` / `ARCHIVE_CHAT_ID` 后执行

```bash
docker compose restart stash-listener
```

### 3. 服务器部署

使用 `docker-compose.deploy.yml`，镜像从 GitHub Container Registry 拉取或本地构建，首次需在本地运行 

```bash
# Kurigram 登录
docker compose run --rm stash-listener python login.py
# tdl 登录
docker compose run --rm stash-listener tdl -n archiver login -T qr    # 二维码
docker compose run --rm stash-listener tdl -n archiver login -T code    # 验证码
```

生成 `data/session/`，将其上传到服务器同路径

## 使用

- **路径一（转发）**：向接收频道转发媒体消息，扫描间隔后自动归档，原消息下方回复 `✅ 已归档`
- **路径二（链接）**：复制消息链接发到接收频道。支持 `t.me/username/123`（公开）和 `t.me/c/数字/123`（私有，账号需已加入），原链接原地编辑添加 `✅ 已归档`
- **失败处理**：归档失败会在接收频道回复原消息告警（首次失败提示将重试，重试满 `RETRY_MAX_ATTEMPTS` 次后标记跳过并保留原消息），单条失败不阻塞后续归档。

## 配置

| 变量 | 默认值 | 说明 |
|---|---|---|
| `TG_API_ID` | — | 必填 |
| `TG_API_HASH` | — | 必填 |
| `RECEIVE_CHAT_ID` | — | 必填，`-100` 前缀 |
| `ARCHIVE_CHAT_ID` | — | 必填，`-100` 前缀 |
| `SCAN_INTERVAL_SECONDS` | `300` | 扫描间隔（秒） |
| `BATCH_SIZE` | `10` | 每轮最多处理消息数 |
| `UPLOAD_COOLDOWN_SECONDS` | `5` | 每次上传后等待（秒） |
| `RETRY_MAX_ATTEMPTS` | `3` | 同一条消息归档失败 N 次后跳过/剔除（记录 + 接收频道回复告警，原消息保留） |
| `TDL_NAMESPACE` | `archiver` | tdl session 命名空间 |
| `TDL_THREADS` | `4` | tdl 单文件分块下载线程数上限 |
| `TDL_LIMIT` | `2` | tdl 同时下载的任务数 |
| `TDL_DELAY_SECONDS` | `1` | tdl 每个下载任务之间的间隔（秒） |
| `TDL_TIMEOUT_SECONDS` | `0` | tdl 进程超时，0 表示不超时 |
| `VIDEO_COMPRESS_ENABLED` | `false` | 是否启用视频压缩（H.264 CRF 恒定质量转码） |
| `VIDEO_COMPRESS_MIN_SIZE_MB` | `100` | 超过此体积(MB)的视频才触发压缩 |
| `VIDEO_COMPRESS_CRF` | `28` | H.264 CRF 恒定质量值，越小质量越高体积越大 |
| `VIDEO_COMPRESS_THREADS` | `4` | x264 编码线程数上限，设为 `0` 则不限制 |
| `HTTP_PROXY` | — | 代理地址，如 `http://host:port` |
| `LOG_LEVEL` | `INFO` | 日志级别：DEBUG/INFO/WARNING/ERROR |
> 容器内 `127.0.0.1` 指向容器自身，代理在本机用 `host.docker.internal` 或宿主机 IP

## 搜索

```bash
docker compose exec stash-listener python search.py 关键词
```

FTS5 + trigram 分词器。关键词需要至少 3 个字符

## 调试工具

`scripts/` 目录下的辅助脚本（容器内运行）：

```bash
docker compose run --rm stash-listener python scripts/get_chat_ids.py           # 列出频道 ID
docker compose run --rm stash-listener python scripts/delete_message.py 12345   # 删除消息记录并回退 checkpoint
```

- **get_chat_ids.py** — 列出当前账号加入的所有频道 ID 和标题，用于填写 `.env`
- **delete_message.py** — 按 `source_message_id` 删除数据库记录并回退 checkpoint，支持 `--dry-run` 预览、`--db` 指定路径、多个 ID

## 本地开发

测试使用 pytest，覆盖 `db.py`（schema/checkpoint/去重/FTS5/并发场景）与 `tdl_downloader.py`（注入假 runner，不连接 Telegram）

```bash
python -m pip install -r requirements-dev.txt
python -m pytest
```

## 目录结构

```
├── stash-listener/    # 主服务（listener/tdl_downloader/login/search/db）
├── scripts/           # 辅助脚本（get_chat_ids/delete_message/test_db/test_tdl_downloader）
├── data/              # 运行时（session/tdl-session/db/tmp，需持久化 db/）
├── .env.example       # 配置模板
└── docker-compose.yml
```

## 许可证

本项目整体以 [AGPL-3.0](LICENSE) 发布。第三方组件保留各自许可证：

| 组件 | 许可证 |
|---|---|
| Kurigram | LGPL-3.0-or-later |
| tdl | AGPL-3.0 |
| Pillow | MIT-CMU |
| ffmpeg / ffprobe | GPL（BtbN 静态构建） |
