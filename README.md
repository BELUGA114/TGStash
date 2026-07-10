# TGStash

轻量 Telegram 个人媒体归档——自动备份到私有频道，去除来源、支持全文搜索。

## 特性

- **双路径归档**：转发媒体自动处理，禁止转发的频道发 t.me 链接即可
- **双层去重**：file_unique_id + SHA-256，不会重复上传
- **媒体组保留**：整组打包上传，维持原始排版
- **视频友好**：本地 ffprobe 重测元数据 + ffmpeg 抽帧缩略图，解决大文件无缩略图/时长显示 00:00
- **账号安全**：保守限流（BATCH_SIZE/上传冷却/扫描间隔），账号 > 速度

## 快速开始

### 准备

1. [my.telegram.org](https://my.telegram.org) 申请 `api_id` / `api_hash`
2. 创建两个频道：接收频道（入口）、备份频道（终点，**私有**）
3. 准备一个 Telegram 账号，加入两个频道并设为管理员

### 部署

```bash
git clone https://github.com/BELUGA114/TGStash.git
cd TGStash
cp .env.example .env    # 编辑填入 TG_API_ID / TG_API_HASH / 频道 ID
docker compose build
docker compose run --rm stash-listener python login.py
docker compose up -d
```

获取频道 ID（登录后容器内运行）：

```bash
docker compose run --rm stash-listener python -c "
import os, asyncio
from pyrogram import Client
async def main():
    app = Client('listener', api_id=int(os.environ['TG_API_ID']), api_hash=os.environ['TG_API_HASH'], workdir='/data/session')
    async with app:
        async for d in app.get_dialogs():
            if d.chat.id < 0: print(f'{d.chat.id}  {d.chat.title}')
asyncio.run(main())"
```

填入 `.env` 的 `RECEIVE_CHAT_ID` / `ARCHIVE_CHAT_ID` 后执行

```bash
docker compose restart stash-listener
```

### 服务器部署

使用 `docker-compose.deploy.yml`，镜像从 GitHub Container Registry 拉取，无需本地构建。首次需在本地跑一次 

```bash
docker compose run --rm stash-listener python login.py 
```

生成 `data/session/`，上传到服务器同路径

## 使用

- **路径一（转发）**：向接收频道转发媒体消息，扫描间隔后自动归档，原消息下方回复 `✅ 已归档`
- **路径二（链接）**：复制消息链接发到接收频道。支持 `t.me/username/123`（公开）和 `t.me/c/数字/123`（私有，需已加入）。媒体组整组保留，原链接原地编辑

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
| `HTTP_PROXY` | — | 代理地址，如 `http://host:port` |
| `LOG_LEVEL` | `INFO` | 日志级别：DEBUG/INFO/WARNING/ERROR |

> 容器内 `127.0.0.1` 指向容器自身，代理在本机用 `host.docker.internal` 或宿主机 IP

## 搜索

```bash
docker compose exec stash-listener python search.py 关键词
```

FTS5 + trigram 分词器。关键词需要至少 3 个字符

## 运维

```bash
docker compose logs -f stash-listener    # 实时日志
docker compose restart stash-listener    # 重启
docker compose down                      # 停止
```

### 备用工具

`tdl-sync`（不随 `up` 启动）：

```bash
docker compose run --rm tdl-sync tdl -n archiver login -T qr
docker compose run --rm tdl-sync tdl -n archiver chat ls
```

## 目录结构

```
├── stash-listener/    # 主服务（listener/login/search/db）
├── tdl-sync/          # 备用批量导出工具
├── data/              # 运行时（session/db/tmp，需持久化 db/）
├── .env.example       # 配置模板
└── docker-compose.yml
```

## 许可证

[MIT](LICENSE)
