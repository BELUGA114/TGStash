# Telegram 个人归档系统

把 Telegram 上的媒体文件归档到私有备份频道，支持全文搜索。

- **路径一（转发媒体）**：转发到接收频道 → 自动下载、去重、上传到备份频道，原消息打上 `✅ 已归档`
- **路径二（转发链接）**：禁止转发的消息，复制链接发到接收频道 → 自动下载、去重、上传

两条路径共享同一套去重管道（file_unique_id + SHA-256），路径二自动处理媒体组。

## 准备工作

1. 去 <https://my.telegram.org> 申请 `api_id` / `api_hash`
2. 建两个频道：
   - **接收频道**：转发内容和链接进来的地方
   - **私有备份频道**：最终归档存放的地方
3. 把闲置账号加进这两个频道，都设为**管理员**
4. 路径二的频道也加进去（只需要普通成员权限）

## 部署

```bash
# 1. 配置
cp .env.example .env
编辑 .env 文件，填 TG_API_ID / TG_API_HASH，其余先留空

# 2. 构建
docker compose build

# 3. 登录（交互式输入手机号 + 验证码）
docker compose run --rm stash-listener python login.py

# 4. 获取频道 ID（登录后运行）
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

# 5. 把接收频道和备份频道的 ID 填回 .env（RECEIVE_CHAT_ID / ARCHIVE_CHAT_ID）

# 6. 启动
docker compose up -d
docker compose logs -f
```

## 使用

### 路径一：转发媒体

往接收频道转发带媒体的消息，等待扫描间隔后自动归档，原消息打上 `✅ 已归档`。

### 路径二：转发链接

禁止转发的消息 → 复制链接 → 把链接作为文本发到接收频道。支持两种格式：

```
https://t.me/username/123       公开用户名
https://t.me/c/123456/123       私有频道
```

自动处理媒体组——链接指向相册中的某张照片时，整组一起归档，各自 caption 保留。原链接消息打上 `✅ 已归档`。

## 搜索

```bash
docker compose exec stash-listener python search.py 关键词
```

FTS5 + trigram 分词器，**关键词至少 3 个字符**。

## 备用工具

`tdl-sync` 容器保留用于 Pyrogram 无法覆盖的特殊场景（如按时间窗口批量导出整个频道）：

```bash
# 先登录一次
docker compose run --rm tdl-sync tdl -n archiver login -T qr

# 列出对话
docker compose run --rm tdl-sync tdl -n archiver chat ls
```

`tdl-sync` 不会随 `docker compose up` 自动启动，需要时用 `docker compose run --rm` 手动执行。

## 目录结构

```
TGStash/
├── .env                  # 配置文件
├── docker-compose.yml
├── stash-listener/       # 主服务（路径一 + 路径二）
├── tdl-sync/             # 备用工具
└── data/                 # 运行时数据
    ├── session/          #   Kurigram 登录 session
    ├── db/
    │   └── archive.db    #   共享数据库（唯一需要备份的文件）
    └── tmp/              #   临时目录，处理完自动清空
```

## 运维

```bash
docker compose logs -f stash-listener
docker compose restart stash-listener
docker compose build; if ($?) { docker compose up -d }
```
