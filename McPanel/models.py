"""
数据库模型定义
"""
from datetime import datetime
from functools import wraps
from flask_sqlalchemy import SQLAlchemy
from flask_login import UserMixin, current_user
from werkzeug.security import generate_password_hash, check_password_hash
from flask import abort

db = SQLAlchemy()


def admin_required(f):
    """管理员权限装饰器: 必须登录 + is_admin=True, 否则 403"""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_user.is_authenticated:
            abort(401)
        if not getattr(current_user, 'is_admin', False):
            abort(403)
        return f(*args, **kwargs)
    return decorated


class User(UserMixin, db.Model):
    """用户表"""
    __tablename__ = 'users'

    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False, index=True)
    email = db.Column(db.String(120), unique=True, nullable=False)
    password_hash = db.Column(db.String(256), nullable=False)
    is_admin = db.Column(db.Boolean, default=False)
    # 资源配额
    max_servers = db.Column(db.Integer, default=2)        # 最大服务器数量
    total_memory = db.Column(db.Integer, default=4096)    # 总内存配额 MB
    total_cores = db.Column(db.Integer, default=2)        # 总核心配额
    balance = db.Column(db.Float, default=0.0)            # 余额
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    servers = db.relationship('ServerInstance', backref='owner', lazy=True, cascade='all, delete-orphan')

    def set_password(self, password: str):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password: str) -> bool:
        return check_password_hash(self.password_hash, password)

    def used_memory(self) -> int:
        return sum(s.memory for s in self.servers)

    def used_cores(self) -> int:
        return sum(s.cores for s in self.servers)


class DistributedNode(db.Model):
    """分布式节点服务器 (分支服务器)"""
    __tablename__ = 'nodes'

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), unique=True, nullable=False)
    api_url = db.Column(db.String(300), nullable=False)          # http(s)://host:port
    api_token = db.Column(db.String(300), nullable=False)        # Bearer Token
    description = db.Column(db.String(300), default='')
    location = db.Column(db.String(100), default='')             # 机房位置描述

    # 状态
    status = db.Column(db.String(20), default='offline')         # online / offline / error
    last_error = db.Column(db.String(500), nullable=True)

    # 资源总览 (从节点心跳更新)
    host_cpu = db.Column(db.Float, default=0.0)                  # %
    host_cpu_count = db.Column(db.Integer, default=0)           # 逻辑核数
    host_memory_total = db.Column(db.BigInteger, default=0)      # GB
    host_memory_used = db.Column(db.BigInteger, default=0)       # GB
    host_disk_total = db.Column(db.BigInteger, default=0)        # GB
    host_disk_used = db.Column(db.BigInteger, default=0)         # GB
    running_servers = db.Column(db.Integer, default=0)
    total_servers = db.Column(db.Integer, default=0)
    # 实际在线玩家 (来自节点每台服务器心跳汇总, 解决"在线玩家显示不准")
    online_players = db.Column(db.Integer, default=0)
    # 管理员可设置的最大服务器数配额 (0 = 不限); 心跳不覆盖此值
    max_servers = db.Column(db.Integer, default=0)

    last_heartbeat_at = db.Column(db.DateTime, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    servers = db.relationship('ServerInstance', backref='node', lazy=True)

    @property
    def status_display(self) -> str:
        mapping = {'online': '在线', 'offline': '离线', 'error': '异常'}
        return mapping.get(self.status, self.status)

    @property
    def status_color(self) -> str:
        mapping = {'online': '#22c55e', 'offline': '#64748b', 'error': '#ef4444'}
        return mapping.get(self.status, '#64748b')

    @property
    def memory_percent(self) -> float:
        if not self.host_memory_total:
            return 0.0
        return round(self.host_memory_used / self.host_memory_total * 100, 1)

    @property
    def disk_percent(self) -> float:
        if not self.host_disk_total:
            return 0.0
        return round(self.host_disk_used / self.host_disk_total * 100, 1)

    # ---------- 面板视角的资源统计 (跨主从同步) ----------
    # 这些 helper 用来解决"管理员看到的剩余资源不符合实际"的问题:
    # host_memory_used 是物理占用 (含其他进程), 而"面板已分配"是所有绑定此节点的 ServerInstance.memory 之和
    # 实际剩余 = 物理总 - max(物理已用, 面板已分配)

    def assigned_memory_mb(self) -> int:
        """该节点上所有服务器分配的内存总和 (MB)"""
        return sum(s.memory for s in self.servers)

    def assigned_cores(self) -> int:
        """该节点上所有服务器分配的 CPU 核心总和"""
        return sum(s.cores for s in self.servers)

    def assigned_server_count(self) -> int:
        return len(self.servers)

    def running_server_count(self) -> int:
        return sum(1 for s in self.servers if s.status == 'running')

    @property
    def available_memory_mb(self) -> int:
        """实际可用内存 (MB): 物理剩余 - 面板已分配, 不允许小于 0"""
        phys_total_mb = (self.host_memory_total or 0) * 1024
        phys_used_mb = (self.host_memory_used or 0) * 1024
        assigned = self.assigned_memory_mb()
        # 实际占用 = max(物理已用, 面板已分配); 剩余 = 总 - 实际占用
        actual_used = max(phys_used_mb, assigned)
        return max(0, int(phys_total_mb - actual_used))

    @property
    def available_cores(self) -> int:
        """实际可用 CPU 核数 = 节点逻辑核数 - 已分配核数 (不小于 0)"""
        total_logical = self.host_cpu_count or 0
        if total_logical <= 0:
            # Agent 未上报时, 用已分配 + 4 兜底, 避免显示为 0
            return max(0, 4 - self.assigned_cores())
        return max(0, total_logical - self.assigned_cores())

    @property
    def load_score(self) -> float:
        """综合负载评分 (0-100, 越高越繁忙), 用于排序"""
        cpu = self.host_cpu or 0
        mem_pct = self.memory_percent
        disk_pct = self.disk_percent
        return round(cpu * 0.5 + mem_pct * 0.3 + disk_pct * 0.2, 1)

    def can_accept_more_servers(self) -> bool:
        """是否还能接受新服务器 (max_servers=0 表示不限)"""
        if not self.max_servers:
            return True
        return self.assigned_server_count() < self.max_servers

    @property
    def available_server_slots(self) -> int:
        """还可分配的服务器数 (0 表示已满或不限的负数表示无限)"""
        if not self.max_servers:
            return -1  # 不限
        return max(0, self.max_servers - self.assigned_server_count())


class ServerInstance(db.Model):
    """Minecraft 服务器实例"""
    __tablename__ = 'servers'

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)
    # 分布式部署: NULL 表示本地节点, 否则指向 DistributedNode
    node_id = db.Column(db.Integer, db.ForeignKey('nodes.id'), nullable=True)
    # 服务器配置
    server_type = db.Column(db.String(50), default='paper')  # paper, vanilla, forge, fabric, spigot
    version = db.Column(db.String(20), default='1.20.4')
    memory = db.Column(db.Integer, default=2048)            # MB
    cores = db.Column(db.Integer, default=1)
    port = db.Column(db.Integer, unique=True, nullable=False)
    motd = db.Column(db.String(200), default='A Minecraft Server')
    max_players = db.Column(db.Integer, default=20)
    # 运行状态
    status = db.Column(db.String(20), default='stopped')    # running, stopped, starting, stopping, crashed
    pid = db.Column(db.Integer, nullable=True)
    # 统计
    player_count = db.Column(db.Integer, default=0)
    uptime_seconds = db.Column(db.BigInteger, default=0)
    last_started = db.Column(db.DateTime, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    server_path = db.Column(db.String(500), nullable=True)   # 服务器目录绝对路径
    jar_file = db.Column(db.String(200), default='server.jar')
    java_args = db.Column(db.String(500), default='')         # 额外 JVM 参数
    auto_start = db.Column(db.Boolean, default=False)
    online_mode = db.Column(db.Boolean, default=True)         # 正版验证
    pvp_enabled = db.Column(db.Boolean, default=True)
    difficulty = db.Column(db.String(10), default='normal')   # peaceful, easy, normal, hard
    level_name = db.Column(db.String(100), default='world')
    whitelist_enabled = db.Column(db.Boolean, default=False)

    @property
    def status_display(self) -> str:
        mapping = {
            'running': '运行中',
            'stopped': '已停止',
            'starting': '启动中',
            'stopping': '关闭中',
            'crashed': '已崩溃',
            'installing': '安装中'
        }
        return mapping.get(self.status, self.status)

    @property
    def status_color(self) -> str:
        mapping = {
            'running': '#22c55e',
            'stopped': '#64748b',
            'starting': '#eab308',
            'stopping': '#f97316',
            'crashed': '#ef4444',
            'installing': '#3b82f6'
        }
        return mapping.get(self.status, '#64748b')
