# TGStash

一个轻量的 Telegram 个人媒体归档系统，将 Telegram 上的媒体文件自动备份到私有频道

## 特性

- **双路径归档**：转发媒体自动处理，禁止转发的消息发链接即可归档
- **双层去重**：file_unique_id 快速判重 + SHA-256 精确判重，不会重复上传
- **媒体组完整保留**：相册/多图消息整组打包上传，维持原始排版
- **格式自动修复**：WebP/PNG/GIF 自动转 JPEG，保证客户端内联浏览
- **账号安全优先**：较为极端的限流机制，账号最重要


## 快速开始

### 准备工作

1. 在 <https://my.telegram.org> 申请 `api_id` / `api_hash`
2. 创建两个频道——接收频道（转发入口）和备份频道（归档终点）
3. 将闲置账号加入两个频道，设为**管理员**
4. 路径二的目标频道也加入（普通成员即可）


### 本地部署

```bash
git clone <repo>
cd TGStash
cp .env.example .env
# 编辑 .env，填入 TG_API_ID 和 TG_API_HASH
```

```bash
docker compose build
docker compose run --rm stash-listener python login.py   # 交互式登录
docker compose up -d
```

### 服务器部署（预构建镜像）

使用 `docker-compose.deploy.yml`，镜像从 GitHub Container Registry 拉取，无需本地构建。使用编排文件，逐个填写环境变量，或直接：

```bash
docker compose -f docker-compose.deploy.yml up -d
```

环境变量（`-e` 传入或手动填写）：

```
TG_API_ID=          # 必填
TG_API_HASH=        # 必填
RECEIVE_CHAT_ID=    # 必填
ARCHIVE_CHAT_ID=    # 必填
HTTP_PROXY=         # 可选，形如 http://host:port
SCAN_INTERVAL_SECONDS=300
BATCH_SIZE=10
UPLOAD_COOLDOWN_SECONDS=5
```

首次使用需要先登录生成 session 文件。在本地机器上跑一次 login 生成 `data/session/` 目录，上传到服务器的 `./data/session/` 即可。容器启动后会自动复用。


### 获取频道 ID

登录后在容器内运行：

```bash
docker compose run --rm stash-listener python -c "
import os, asyncio
from pyrogram import Client
API_ID = int(os.environ['TG_API_ID'])
API_HASH = os.environ['TG_API_HASH']
async def main():
    app = Client('listener', api_id=API_ID, api_hash=API_HASH, workdir='/data/session')
    async with app:
        async for d in app.get_dialogs():
            if d.chat.id < 0:
                print(f'{d.chat.id}  {d.chat.title}')
asyncio.run(main())
"
```

将输出的频道 ID 填入 `.env` 的 `RECEIVE_CHAT_ID` 和 `ARCHIVE_CHAT_ID`，然后重启：

```bash
docker compose restart stash-listener
```


## 使用

### 路径一：转发媒体

向接收频道转发带媒体的消息。扫描间隔后文件出现在备份频道，原消息下方自动回复 `✅ 已归档`

### 路径二：归档禁止转发的消息

复制目标消息的链接，作为文本发送到接收频道。支持两种格式：

```
https://t.me/username/123    公开频道/用户
https://t.me/c/123456/123    私有频道（需已加入）
```

链接指向媒体组时，整组照片一起归档，各自 caption 保留。原链接消息原地编辑为 `✅ 已归档`

## 配置参考

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `TG_API_ID` | — | 必填，来自 my.telegram.org |
| `TG_API_HASH` | — | 必填 |
| `RECEIVE_CHAT_ID` | — | 必填，接收频道 ID（`-100` 前缀） |
| `ARCHIVE_CHAT_ID` | — | 必填，备份频道 ID |
| `SCAN_INTERVAL_SECONDS` | `300` | 处理后的冷却间隔，同时也决定无消息时的轮询周期 |
| `BATCH_SIZE` | `10` | 每轮最多处理的消息数，超出留给下轮 |
| `UPLOAD_COOLDOWN_SECONDS` | `5` | 每次上传后的等待时间 |
| `HTTP_PROXY` | — | HTTP 代理地址，形如 `http://127.0.0.1:10086`。不需要则留空 |

Docker 容器内 `127.0.0.1` 指向容器自身而非宿主机——如果代理在本机，可使用 `host.docker.internal`（Windows/Mac）或宿主机 IP：


## 搜索

```bash
docker compose exec stash-listener python search.py 关键词
```

基于 SQLite FTS5 + trigram 分词器。trigram 按每 3 个字符切分处理中文，关键词至少 3 个字符

但是实际上意义不大


## 目录结构

```
TGStash/
├── .env.example              # 配置模板
├── docker-compose.yml        # stash-listener 常驻 + tdl-sync 备用
├── stash-listener/           # 主服务
│   ├── listener.py           #   核心：扫描、去重、转换、上传
│   ├── login.py              #   首次登录
│   ├── search.py             #   命令行搜索
│   ├── db.py                 #   SQLite 数据层
│   ├── Dockerfile
│   └── requirements.txt
├── tdl-sync/                 # 备用工具，手动触发
├── tests/
│   └── test_db.py            # DB 层 32 个单元测试
└── data/                     # 运行时数据（挂载到容器）
    ├── session/              #   Kurigram 登录凭据
    ├── db/archive.db         #   共享数据库，需要持久化
    └── tmp/                  #   下载临时目录，处理完自动清空
```

`data/db/archive.db` 需要备份的文件，丢失后去重能力和全文索引需要重建，但归档本体（备份频道里的消息）不受影响


## 备用工具

`tdl-sync` 容器保留给 Pyrogram 无法覆盖的场景（如按时间窗口批量导出整个频道）：

```bash
docker compose run --rm tdl-sync tdl -n archiver login -T qr
docker compose run --rm tdl-sync tdl -n archiver chat ls
```

不随 `docker compose up` 自动启动，需要时用 `docker compose run --rm` 手动执行

## 运维

```bash
docker compose logs -f stash-listener    # 实时日志
docker compose restart stash-listener    # 重启
docker compose down                      # 停止
```
