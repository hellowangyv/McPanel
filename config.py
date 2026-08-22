"""
McPanel 集中配置模块
- 所有环境变量读取、密钥生成、安全选项集中在此
- 生产环境必须设置 PANEL_SECRET 环境变量
"""
from __future__ import annotations

import os
import secrets
import sys
from pathlib import Path
from typing import List


def _warn(msg: str):
    """打印醒目的警告（黄色）"""
    YELLOW = '\033[93m'
    RESET = '\033[0m'
    print(f'{YELLOW}[WARN] {msg}{RESET}', file=sys.stderr)


# ======== 基础目录 ========
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / 'data'
SERVERS_DIR = DATA_DIR / 'servers'
UPLOAD_DIR = DATA_DIR / 'uploads'
DB_PATH = DATA_DIR / 'panel.db'

for d in (DATA_DIR, SERVERS_DIR, UPLOAD_DIR):
    d.mkdir(parents=True, exist_ok=True)


# ======== 安全配置 ========

# SECRET_KEY: 生产必须设置环境变量，否则启动时生成临时随机密钥（重启后session失效）
ENV_SECRET = os.environ.get('PANEL_SECRET', '').strip()
if ENV_SECRET and len(ENV_SECRET) >= 16:
    SECRET_KEY = ENV_SECRET
    SECRET_RANDOM = False
else:
    if ENV_SECRET:
        _warn('PANEL_SECRET 长度不足 16 位, 已忽略并使用临时密钥')
    else:
        _warn('未设置 PANEL_SECRET 环境变量, 正在生成一次性临时密钥. '
              '设置 export PANEL_SECRET=$(python -c "import secrets;print(secrets.token_hex(32))") '
              '以避免服务重启后用户全部掉线.')
    SECRET_KEY = secrets.token_hex(32)
    SECRET_RANDOM = True

# SocketIO CORS: 默认严格同源, 环境变量可覆盖（逗号分隔多个源）
_cors_raw = os.environ.get('PANEL_CORS_ORIGINS', '').strip()
if _cors_raw:
    CORS_ALLOWED_ORIGINS: List[str] = [o.strip() for o in _cors_raw.split(',') if o.strip()]
else:
    # 默认严格同源
    CORS_ALLOWED_ORIGINS = []


# ======== SSL / HTTPS 下载 ========
# INSECURE_SSL=1 时禁用证书验证（仅用于内网自签/离线环境，不推荐生产）
INSECURE_SSL = os.environ.get('INSECURE_SSL', '').strip() in ('1', 'true', 'TRUE', 'yes')
if INSECURE_SSL:
    _warn('INSECURE_SSL 已启用, SSL 证书验证被禁用, 仅用于离线/测试环境!')


# ======== 分布式节点 ========
# 当前节点在分布式中的角色: 'master' (主面板) 或 'agent' (仅节点, 不开面板)
NODE_ROLE = os.environ.get('NODE_ROLE', 'master').strip().lower()
if NODE_ROLE not in ('master', 'agent'):
    NODE_ROLE = 'master'

# 节点通信 Token (用于 Agent 验证 Master 身份, Master 部署时可作为默认节点 Token)
NODE_API_TOKEN = os.environ.get('NODE_API_TOKEN', '').strip()
if not NODE_API_TOKEN:
    NODE_API_TOKEN = secrets.token_urlsafe(32)
    _warn(f'未设置 NODE_API_TOKEN, 已生成默认值. 若要部署多节点请设置环境变量以保持一致. '
          f'当前值: {NODE_API_TOKEN[:8]}...')

# 节点心跳间隔 (秒) - 默认 5s 满足"节点信息更新延迟不超过 5 秒"的 SLA
NODE_HEARTBEAT_INTERVAL = int(os.environ.get('NODE_HEARTBEAT_INTERVAL', '5'))
NODE_HEARTBEAT_TIMEOUT = int(os.environ.get('NODE_HEARTBEAT_TIMEOUT', '30'))


# ======== 面板配置 ========
MAX_UPLOAD_MB = int(os.environ.get('MAX_UPLOAD_MB', '128'))
PANEL_PORT = int(os.environ.get('PANEL_PORT', '8765'))
PANEL_HOST = os.environ.get('PANEL_HOST', '0.0.0.0')
