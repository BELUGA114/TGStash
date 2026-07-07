# Telegram 个人归档系统

把 Telegram 上的媒体文件自动归档到私有备份频道，支持全文搜索。两条采集路径覆盖不同的使用场景。

- **路径一（手动转发）**：转发到"接收频道" → 自动下载、去重、上传到"私有备份频道"，原消息打上 `✅ 已归档` 标记
- **路径二（禁止转发频道）**：`tdl` 定时批量导出 → 下载、去重、上传到同一个私有备份频道

## 准备工作

1. 去 <https://my.telegram.org> 申请 `api_id` / `api_hash`
2. 建两个频道：
   - **接收频道**：手动转发内容进来的地方
   - **私有备份频道**：最终归档存放的地方
3. 把闲置账号加进这两个频道，都设为**管理员**（接收频道需要管理员权限才能编辑别人转发的消息；备份频道需要权限才能发消息）
4. 把"禁止转发重点频道"也加进去（只需要普通成员权限，能看到消息即可）

## 部署

```bash
# 1. 配置
cp .env.example .env
编辑 .env 文件，填 TG_API_ID / TG_API_HASH，其余先留空

# 2. 构建
docker compose build

# 3. 登录（两个服务各自一次）
#    路径一：交互式输入手机号 + 验证码
docker compose run --rm stash-listener python login.py
#    路径二：二选一
docker compose run --rm tdl-sync tdl -n archiver login -T code   # 验证码登录
docker compose run --rm tdl-sync tdl -n archiver login -T qr     # 二维码登录

# 4. 获取频道 ID
docker compose run --rm tdl-sync tdl -n archiver chat ls

# 5. 把频道 ID 填回 .env（RECEIVE_CHAT_ID / ARCHIVE_CHAT_ID / PRIORITY_CHANNELS）

# 6. 启动
docker compose up -d
docker compose logs -f
```

## 搜索归档

```bash
docker compose exec stash-listener python search.py 关键词
```

搜索基于 SQLite FTS5 + trigram 分词器，**关键词至少 3 个字符**（2 字词搜不到）。trigram 是对中文的折中方案——SQLite 默认分词器依赖空格分词，对中文无效。

## 目录结构

```
TGStash/
├── .env                  # 配置文件
├── docker-compose.yml
├── stash-listener/       # 路径一
├── tdl-sync/             # 路径二
└── data/                 # 运行时数据（挂载到容器）
    ├── session/          #   Kurigram 登录 session
    ├── tdl-session/      #   tdl 登录 session
    ├── db/
    │   └── archive.db    #   共享数据库（唯一需要备份的文件）
    └── tmp/              #   下载临时目录，处理完自动清空
```

`data/db/archive.db` 是唯一需要长期备份的文件。它不是归档本体（本体是私有备份频道里的消息），但丢了这个库会失去去重能力和全文索引，需要重新扫描才能重建。

## 已知限制

- **路径二缺少元数据**：tdl 导出 JSON 未解析，`messages` 表里路径二的记录搜不到 caption 和发送者，只能按文件名和来源频道定位。后续可以用 Kurigram 的 `get_messages()` 回填
- **全文搜索是 FTS5**：内容多了想要更好的中文分词可以接入 Meilisearch，`messages` 表数据直接同步，不用改采集逻辑

## 运维命令

```bash
# 看日志
docker compose logs -f stash-listener
docker compose logs -f tdl-sync

# 重启服务
docker compose restart stash-listener

# 排查 tdl 问题
docker compose run --rm tdl-sync tdl -n archiver chat ls
```
