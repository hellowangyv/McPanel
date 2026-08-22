# McPanel - 我的世界(Minecraft)开服面板

> ## ⚠️ 重要声明（务必先读）
>
> **这是一个不完善、未完成、存在大量已知问题的个人实验项目。**
>
> - 本项目**不建议、也不应被直接用于生产环境**，请勿直接部署到公网提供服务。
> - 代码中多处为"能用就行"的实现，缺少完善的异常处理、权限校验、并发保护与安全加固。
> - 已知问题列表见下文 [已知问题与局限](#已知问题与局限)，远非完整清单。
> - 若你有开服需求，请优先考虑成熟方案（如 Pterodactyl、MCSManager 等）。
> - 若你希望基于本项目二次开发，请**自行全面审查每一处代码**后再决定是否复用。
>
> **本项目以 MIT License 开源，详情见 [LICENSE](LICENSE)（英文）/[LICENSE_CN](LICENSE_CN)（中文）。** 使用前请阅读并遵守许可证条款。

---

## 项目简介

McPanel 是一个基于 Flask 的 Minecraft 服务器管理面板，提供：

- **Web 面板**：服务器生命周期管理（创建 / 启动 / 停止 / 重启 / 强制结束）
- **实时控制台**：基于 Flask-SocketIO 的 WebSocket 控制台，可向服务器发送指令
- **文件管理**：服务器目录浏览、上传、删除（有基本路径穿越防护）
- **配置管理**：MOTD、端口、玩家上限、难度等服务器设置
- **Java 自动管理**：按 MC 版本自动下载对应 JRE（8 / 17 / 21 多版本共存）
- **核心自动下载**：Paper / Spigot / Fabric / Vanilla 等核心按版本自动下载
- **分布式节点**：主控（Master）+ 节点（Agent）架构，可跨机器部署服务器
- **管理后台**：用户管理、服务器管理、分布式节点管理、全局统计
- **Dashboard**：配额 / 资源 / 图表监控（ECharts）

## 技术栈

- 后端：Python 3 + Flask + Flask-SocketIO + Flask-SQLAlchemy + Flask-Login
- 存储：SQLite（`data/panel.db`）
- 采集：psutil
- 前端：原生 HTML/CSS/JS + ECharts（CDN，离线时降级为纯 DOM 展示）
- 并发服务器：eventlet（socketio）

## 架构

```
┌─────────────────────────────┐        ┌──────────────────────────────┐
│  主控 Master (app.py)        │  HTTP  │  节点 Agent (node_agent.py)   │
│  端口 8765                   │ ─────► │  端口 58765 (默认)            │
│  · Web 面板 / 管理后台        │  RPC   │  · 在节点机上启停 MC 服务器    │
│  · 节点心跳监控 (5s)          │        │  · 本地下载 Java / 核心        │
│  · 服务器配置同步             │        │  · 本地文件管理               │
└─────────────────────────────┘        └──────────────────────────────┘
```

- 主控通过 HTTP API（`Authorization: Bearer <NODE_API_TOKEN>`）调用节点。
- 节点每 5 秒向主控上报心跳（`/heartbeat`），主控据心跳判定节点在线状态。

## 目录结构

```
├── app.py               # 主控面板（Flask 应用、全部页面与 API 路由）
├── node_agent.py        # 分布式节点 Agent（独立 Flask 进程）
├── server_manager.py    # 本地 Minecraft 服务器进程管理
├── node_manager.py      # 分布式节点管理（心跳线程、RPC 客户端）
├── java_manager.py      # Java 自动下载 / 多版本管理
├── models.py            # SQLAlchemy 数据模型
├── config.py            # 集中配置（环境变量、密钥、路径）
├── requirements.txt     # Python 依赖
├── static/              # 前端静态资源（css / js）
├── templates/           # Jinja2 模板
└── data/                # 运行时数据（自动创建）
    ├── panel.db         # SQLite 数据库
    ├── servers/         # 本地服务器目录（server_<id>/）
    ├── runtime/         # 自动下载的 JRE 运行时（jre-8 / jre-17 / jre-21）
    └── uploads/         # 上传文件临时目录
```

## 环境要求

- Python **3.9+**（在 3.13 下开发调试）
- 操作系统：Windows / Linux（主要在 Windows 下测试，Linux 未充分验证）
- 网络：需要能访问以下地址
  - BellSoft JRE 下载（Java 自动下载）
  - Mojang / Paper 等核心下载源
  - jsdelivr CDN（前端 ECharts，离线时自动降级）

## 安装与运行

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

> 注意：`requests` 是**分布式节点功能的硬依赖**。若未安装，节点 RPC 会静默降级，
> 表现为"节点永远离线 / 远程服务器无法启动"，且不会直接报错退出。

### 2. 启动主控（Master）

```bash
# 生产/多人使用务必先设置密钥，否则重启后所有登录态失效
export PANEL_SECRET="$(python -c "import secrets;print(secrets.token_hex(32))")"
# 分布式场景：所有主控与节点必须使用同一个 token
export NODE_API_TOKEN="$(python -c "import secrets;print(secrets.token_urlsafe(32))")"

python app.py
```

访问 http://127.0.0.1:8765

默认账号（**务必修改，见下方安全警告**）：

| 账号 | 密码 | 角色 |
|---|---|---|
| admin | admin123 | 管理员 |
| demo | demo1234 | 演示用户 |

### 3. 启动分布式节点（Agent，可选）

在另一台机器上（或本机测试）：

```bash
# 必须与主控使用完全相同的 NODE_API_TOKEN
export NODE_API_TOKEN="<与主控相同的 token>"

python node_agent.py --host 0.0.0.0 --port 58765 \
    --token "$NODE_API_TOKEN" \
    --data-dir ./node_data
```

然后在主控面板「管理后台 → 分布式节点 → 添加节点」中填写：

- 节点地址：`http://<节点IP>:58765`
- 节点 Token：`<NODE_API_TOKEN>`

主控心跳检测到节点在线后，即可在创建服务器时选择该节点。

## 环境变量

| 变量 | 默认值 | 说明 |
|---|---|---|
| `PANEL_SECRET` | 随机（每次启动变化） | 主控 Flask 密钥；不设置则重启后登录态全部失效 |
| `NODE_API_TOKEN` | 随机（每次启动变化） | 主控与节点间通信 Token；**多节点部署必须显式设置且一致** |
| `NODE_ROLE` | `master` | `master`（主控）或 `agent`（纯节点模式） |
| `PANEL_PORT` | `8765` | 主控端口 |
| `PANEL_HOST` | `0.0.0.0` | 主控监听地址 |
| `NODE_HOST` | `0.0.0.0` | 节点监听地址 |
| `NODE_PORT` | `58765` | 节点端口 |
| `NODE_DATA_DIR` | `./node_data` | 节点数据目录 |
| `NODE_ALLOW_FROM` | 空 | 节点允许来源 IP（建议设置，配合防火墙使用） |
| `NODE_HEARTBEAT_INTERVAL` | `5` | 节点心跳间隔（秒） |
| `NODE_HEARTBEAT_TIMEOUT` | `30` | 心跳超时判定离线阈值（秒） |
| `INSECURE_SSL` | 空 | 设为 `1` 禁用 SSL 证书校验（仅内网离线环境，不推荐） |
| `PANEL_CORS_ORIGINS` | 空（严格同源） | 逗号分隔的额外 CORS 来源 |
| `MAX_UPLOAD_MB` | `128` | 文件上传大小上限（MB） |

## 安全警告

- 默认账号 `admin/admin123`、`demo/demo1234` 硬编码在代码中，**任何公网暴露前必须修改**。
- 节点仅靠一个静态 Token 认证，`NODE_ALLOW_FROM` 默认不限制来源，**节点端口不应暴露到公网**。
- 面板未实现 HTTPS；如需公网访问必须自行加反代（Nginx/Caddy）+ TLS。
- CSRF 防护基于 session token，已实现；但整体安全模型未经专业审计。

---

## 已知问题与局限

> 以下为开发与实测过程中**真实遇到**的问题，仅列出主要项，不代表全部。

### 分布式 / 节点相关

1. **`requests` 缺失时静默降级**：主控未安装 `requests` 时，节点 RPC 全部失败，
   节点会显示离线、远程服务器无法启动，且控制台只有一句警告，不容易发现。
2. **Agent 曾不生成 `server.properties`**：早期版本节点启动服务器前不写入
   `server.properties`，导致 MC 使用默认端口 25565，同机多服必然端口冲突崩溃。
   已修复（启动前自动写入），但旧版本部署的历史服务器需注意。
3. **节点心跳可能被 psutil 卡死**：在慢盘 / 异常系统上，`psutil.virtual_memory()` /
   `disk_usage()` 可能长时间阻塞，导致 `/heartbeat` 超时、节点被误判离线。
   已加"超时降级返回缓存值"保护，但未在极端环境充分验证。
4. **主控-节点 Token 不一致不会报错**：若主控与节点 `NODE_API_TOKEN` 不同，
   节点表现为连不上/401，需要检查两端环境变量。
5. **远程服务器状态同步依赖心跳**：主控通过轮询同步远程服务器状态，
   存在最长一个心跳周期的状态延迟。

### Java / 核心下载

6. **Java 自动下载依赖 BellSoft 网络**：国内网络实测约 60KB/s，
   下载 56MB 的 JRE 需要十几分钟；下载中断会残留临时文件（下次启动重下）。
7. **核心下载源可能被墙/限速**：Paper/Mojang 源不稳定时，首次启动会长时间卡在
   "自动下载核心"阶段。
8. **首次启动体验差**：首次启动需先下载 Java、再下载核心，均无断点续传。

### 本地服务器管理

9. **无进程守护**：面板进程退出后，由其拉起的 MC 服务器进程会被一并终止；
   面板无法接管/恢复外部启动的服务器进程。
10. **强制结束依赖 psutil**：`kill` 使用 psutil 遍历子进程，极端情况下可能杀不干净。
11. **端口冲突仅启动时暴露**：启动前不做本地端口预检，冲突时 MC 直接崩溃，
    通过控制台日志才能发现。
12. **watchdog 只同步状态**：不负责自动拉起崩溃的服务器。

### 功能缺失（未实现）

13. **未实现 RCON**（`server.properties` 中 `enable-rcon=false`）。
14. **未实现备份/快照**、**插件/模组商店**、**多语言**。
15. **无任务调度**：没有计划任务 / 定时重启 / 定时备份能力。
16. **前端部分功能在弱网下体验一般**：控制台轮询、图表依赖 CDN。

### 代码质量

17. 存在调试用临时文件残留（如 `_test_perf.py`、`_smoke_*.log`）。
18. 错误处理风格不一：部分路径静默吞异常，部分路径直接抛 500。
19. 仅在小规模场景下测试，**未做过并发、安全、性能压测**。

---

## 免责声明

- 本项目按现状（AS-IS）提供，作者不对其可用性、安全性、稳定性做任何担保。
- 使用本项目产生的一切后果（数据丢失、机器被入侵、服务不可用等）由使用者自行承担。
- 请勿在未充分理解代码的前提下，将本项目部署到公网或用于商业用途。
- 本项目以 [MIT License](LICENSE)（英文）/ [LICENSE_CN](LICENSE_CN)（中文）开源，二次分发/复用请遵守许可证条款。
