"""
MC 开服面板 - Flask 主入口
===================================
功能:
  - 用户注册 / 登录 (Flask-Login)
  - 仪表盘: 资源概览, 服务器列表 (主机资源监控仅管理员可见)
  - 服务器管理: 新建 / 删除 / 启停 / 重启 / 强制终止
  - 实时控制台 (SocketIO 订阅 room=server_<id>)
  - 命令输入、配置编辑、文件管理
  - [Admin] 管理员中心: 用户管理 / 节点管理 / 全局服务器 / 全局监控
  - [Distributed] 分支节点管理: HTTP+Token 调用远端 Node Agent

启动:
    python app.py       # 默认 http://127.0.0.1:8765
管理员账号 (自动初始化): admin / admin123

安全增强:
  - 主机资源监控 (API + 页面 + SocketIO) 仅管理员可见
  - SECRET_KEY 默认生成随机临时值; 生产通过 PANEL_SECRET 环境变量设置
  - SocketIO CORS 默认严格同源, 由 PANEL_CORS_ORIGINS 覆盖
  - SSL 下载默认严格校验证书, 仅当 INSECURE_SSL=1 时放宽
"""
from __future__ import annotations

import os
import random
import threading
import time
import traceback
import secrets
from datetime import datetime
from pathlib import Path
from typing import Dict

import psutil
from flask import (Flask, render_template, request, redirect, url_for,
                   flash, jsonify, abort, send_from_directory, session)
from flask_login import (LoginManager, login_user, logout_user, login_required,
                         current_user)
from flask_socketio import SocketIO, join_room, leave_room, emit

import config as cfg
from models import db, User, ServerInstance, DistributedNode, admin_required
from server_manager import MinecraftServer
from java_manager import JavaManager
from node_manager import NodeManager, NodeRPC, NodeRPCError

# ========================= 基础配置 =========================

BASE_DIR = cfg.BASE_DIR
DATA_DIR = cfg.DATA_DIR
SERVERS_DIR = cfg.SERVERS_DIR
UPLOAD_DIR = cfg.UPLOAD_DIR
DB_PATH = cfg.DB_PATH

app = Flask(__name__, template_folder='templates', static_folder='static')
app.config['SECRET_KEY'] = cfg.SECRET_KEY
app.config['SQLALCHEMY_DATABASE_URI'] = f'sqlite:///{DB_PATH}'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['MAX_CONTENT_LENGTH'] = cfg.MAX_UPLOAD_MB * 1024 * 1024
# Session Cookie 安全属性: HttpOnly + SameSite=Lax (防 CSRF 与 XSS 偷 cookie)
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
# HTTPS 部署时启用 Secure (生产推荐强制 HTTPS)
app.config['SESSION_COOKIE_SECURE'] = os.environ.get('SESSION_COOKIE_SECURE', '').strip() in ('1', 'true', 'TRUE', 'yes')
# REMEMBER_COOKIE 同样加固 (Flask-Login "记住我"功能)
app.config['REMEMBER_COOKIE_HTTPONLY'] = True
app.config['REMEMBER_COOKIE_SAMESITE'] = 'Lax'

db.init_app(app)


# ---- CSRF 防护: 会话级 token + before_request 校验 ----
import hmac as _hmac
import secrets as _secrets


def _generate_csrf_token() -> str:
    """生成会话级 CSRF token, 跨请求保持"""
    if '_csrf_token' not in session:
        session['_csrf_token'] = _secrets.token_urlsafe(32)
    return session['_csrf_token']


@app.before_request
def _csrf_protect():
    """对所有状态变更请求 (POST/PUT/DELETE/PATCH) 校验 CSRF token

    豁免:
      - GET / HEAD / OPTIONS
      - 携带 Authorization: Bearer ... 头的 API 请求 (Agent RPC, 不依赖 cookie)
      - 响应 content-type 为 application/json 且显式 X-Requested-With (可选)
    """
    method = request.method.upper()
    if method in ('GET', 'HEAD', 'OPTIONS'):
        return
    # Bearer token 鉴权的 API 请求 (不依赖 cookie, 不存在 CSRF 风险)
    if request.headers.get('Authorization', '').lower().startswith('bearer '):
        return
    # Agent 反向上报路径 (无 cookie, 不存在 CSRF)
    if request.path.startswith('/api/agent/') or request.path.startswith('/api/upload/'):
        return
    token = (request.form.get('csrf_token') or
             request.headers.get('X-CSRFToken') or
             (request.get_json(silent=True) or {}).get('csrf_token') or '')
    expected = session.get('_csrf_token')
    if not expected or not token or not _hmac.compare_digest(token, expected):
        if request.path.startswith('/api/') or request.is_json:
            return jsonify({'ok': False, 'msg': 'CSRF token 校验失败'}), 403
        abort(403, description='CSRF token 校验失败, 请重新加载页面后重试')


@app.context_processor
def _inject_csrf_token():
    """模板全局可访问 csrf_token()"""
    return {'csrf_token': _generate_csrf_token}

# ---- SocketIO: CORS 严格化 ----
# engineio 需要 list/set/str; [] 表示严格同源（不允许任何跨域）
_socketio_cors = cfg.CORS_ALLOWED_ORIGINS
if not _socketio_cors:
    # 空列表 = 严格同源 (engineio 不会放行任何跨域)
    _socketio_cors_origins = []
else:
    _socketio_cors_origins = list(_socketio_cors)

socketio = SocketIO(
    app,
    cors_allowed_origins=_socketio_cors_origins,
    async_mode='threading',
    logger=False,
    engineio_logger=False,
)

login_manager = LoginManager(app)
login_manager.login_view = 'auth_login'
login_manager.login_message = '请先登录后再访问此页面'
login_manager.login_message_category = 'warning'

# ========================= 全局控制器注册表 =========================

_SERVER_CONTROLLERS: Dict[int, MinecraftServer] = {}
_controllers_lock = threading.Lock()


def get_controller(inst: ServerInstance):
    """
    根据服务器所属节点选择控制器:
      - node_id 为空 -> 本地 MinecraftServer
      - 有 node_id   -> 返回 RemoteServerProxy (占位, 方法名与本地对齐)
    """
    if inst.node_id is not None:
        # 远程节点: 用 RPC 代理 (此处简单实现, 后续可扩展)
        return RemoteServerProxy(inst)
    with _controllers_lock:
        c = _SERVER_CONTROLLERS.get(inst.id)
        if not c:
            c = MinecraftServer(inst, socketio, str(SERVERS_DIR))
            _SERVER_CONTROLLERS[inst.id] = c
        return c


class RemoteServerProxy:
    """远程节点服务器代理, 接口与 MinecraftServer 对齐 (部分实现)"""

    def __init__(self, inst: ServerInstance):
        self.inst = inst
        self.id = inst.id
        self.name = inst.name
        # 懒加载 RPC (访问 config 时自动取最新的 node)
        self._last_rpc = None

    def _rpc(self):
        node = db.session.get(DistributedNode, self.inst.node_id)
        if not node:
            raise RuntimeError(f'服务器所属节点 #{self.inst.node_id} 不存在')
        return NODE_MANAGER.get_rpc(node)

    def _rpc_long(self):
        """长超时 RPC: 首次启动需要下载 Java / 核心, 单个请求可能耗时数分钟

        默认 RPC 超时 15s 对"首次启动"不够, 会导致主控误报"节点通信失败"
        """
        node = db.session.get(DistributedNode, self.inst.node_id)
        if not node:
            raise RuntimeError(f'服务器所属节点 #{self.inst.node_id} 不存在')
        return NodeRPC(node, timeout=1800)

    # ---------- 常用动作 ----------
    def start(self):
        try:
            # 重新从 DB 读取最新配置, 避免 detached session; 同时把配置传给 Agent
            sv = db.session.get(ServerInstance, self.id)
            if not sv:
                return {'ok': False, 'msg': '服务器不存在'}
            payload = {
                'server_type': sv.server_type,
                'version': sv.version,
                'memory': sv.memory,
                'cores': sv.cores,
                'port': sv.port,
                'jar_file': sv.jar_file or 'server.jar',
                'motd': sv.motd or 'A Minecraft Server',
                'max_players': sv.max_players,
                'difficulty': sv.difficulty,
                'level_name': sv.level_name,
                'online_mode': sv.online_mode,
                'pvp_enabled': sv.pvp_enabled,
                'whitelist_enabled': sv.whitelist_enabled,
                'java_args': sv.java_args or '',
            }
            return self._rpc_long().server_start(self.id, payload)
        except NodeRPCError as e:
            return {'ok': False, 'msg': f'节点通信失败: {e}'}

    def stop(self):
        try:
            return self._rpc().server_stop(self.id)
        except NodeRPCError as e:
            return {'ok': False, 'msg': f'节点通信失败: {e}'}

    def restart(self):
        try:
            # 重启内部包含 start, 首次可能需要下载 Java/核心, 同样用长超时
            return self._rpc_long().server_restart(self.id)
        except NodeRPCError as e:
            return {'ok': False, 'msg': f'节点通信失败: {e}'}

    def kill(self):
        try:
            return self._rpc().server_kill(self.id)
        except NodeRPCError as e:
            return {'ok': False, 'msg': f'节点通信失败: {e}'}

    def send_command(self, cmd: str):
        try:
            r = self._rpc().server_command(self.id, cmd)
            return bool(r.get('ok'))
        except NodeRPCError:
            return False

    def get_stats(self):
        try:
            return self._rpc().server_stats(self.id)
        except NodeRPCError:
            return {'status': 'unknown', 'error': '节点不可达'}

    @property
    def console(self):
        class _C:
            def all(s):
                try:
                    return self._rpc().server_console(self.id)
                except NodeRPCError:
                    return []
        return _C()

    # ---- 以下方法在节点场景下不适用, 用本地占位兜底 ----
    def ensure_directory(self):
        pass  # 节点端负责

    def write_server_properties(self):
        pass


# ========================= Java 运行时管理器 =========================
java_manager = JavaManager(runtime_dir=DATA_DIR / 'runtime', socketio=socketio, app=app)

# ========================= 依赖注入 (解决 detached ORM session 问题) =========================
MinecraftServer.inject_deps(db, ServerInstance, app, java_manager)

# ========================= 节点管理器 =========================
NODE_MANAGER = NodeManager(
    db=db, node_model=DistributedNode, app=app,
    heartbeat_interval=cfg.NODE_HEARTBEAT_INTERVAL,
    heartbeat_timeout=cfg.NODE_HEARTBEAT_TIMEOUT,
)


# ========================= DB 初始化 & 启动后恢复 =========================

def _port_in_use(port: int) -> bool:
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(('0.0.0.0', port))
            return False
        except OSError:
            return True


def allocate_port(preferred: int | None = None) -> int:
    if preferred and not _port_in_use(preferred):
        return preferred
    base = 25565
    for i in range(200):
        p = base + i * 10 + random.randint(0, 9)
        if not _port_in_use(p):
            # 数据库内也不能重复
            if not ServerInstance.query.filter_by(port=p).first():
                return p
    raise RuntimeError('无法分配可用端口')


def _ensure_db_columns():
    """
    SQLite 不支持完整 ALTER, 这里做增量兼容:
      - servers.node_id
      - nodes 表 (由 create_all 完成)
      - nodes.host_cpu_count / nodes.online_players (心跳返回结构升级)
    """
    try:
        from sqlalchemy import text
        with db.engine.connect() as conn:
            cols = [r[1] for r in conn.execute(text("PRAGMA table_info(servers)"))]
            if 'node_id' not in cols:
                conn.execute(text("ALTER TABLE servers ADD COLUMN node_id INTEGER REFERENCES nodes(id)"))
                conn.commit()
                app.logger.info('[DB] 已为 servers 表新增 node_id 列')
            # nodes 表新字段兼容 (旧库可能没有)
            node_cols = [r[1] for r in conn.execute(text("PRAGMA table_info(nodes)"))]
            for col, sqltype in (('host_cpu_count', 'INTEGER DEFAULT 0'),
                                 ('online_players', 'INTEGER DEFAULT 0'),
                                 ('max_servers', 'INTEGER DEFAULT 0')):
                if col not in node_cols:
                    conn.execute(text(f"ALTER TABLE nodes ADD COLUMN {col} {sqltype}"))
                    conn.commit()
                    app.logger.info(f'[DB] 已为 nodes 表新增 {col} 列')
    except Exception as e:
        app.logger.warning(f'[DB] 列兼容性检查跳过: {e}')


def init_db():
    with app.app_context():
        db.create_all()
        _ensure_db_columns()
        # 管理员种子
        if not User.query.filter_by(username='admin').first():
            u = User(
                username='admin',
                email='admin@mcpanel.local',
                is_admin=True,
                max_servers=50,
                total_memory=1024 * 32,
                total_cores=32,
            )
            u.set_password('admin123')
            db.session.add(u)
        # 演示用户
        if not User.query.filter_by(username='demo').first():
            u = User(
                username='demo',
                email='demo@mcpanel.local',
                is_admin=False,
                max_servers=3,
                total_memory=8192,
                total_cores=4,
            )
            u.set_password('demo1234')
            db.session.add(u)
        db.session.commit()
        # 重置上次运行中的本地服务器状态为 stopped (节点服务器由 agent 管理)
        ServerInstance.query.filter(
            ServerInstance.status.in_(['running', 'starting', 'stopping']),
            ServerInstance.node_id.is_(None),
        ).update({ServerInstance.status: 'stopped',
                   ServerInstance.pid: None, ServerInstance.player_count: 0})
        db.session.commit()


# ========================= Login Loader =========================

@login_manager.user_loader
def load_user(uid: str):
    try:
        return db.session.get(User, int(uid))
    except Exception:
        return None


# ========================= 上下文注入 =========================

@app.context_processor
def inject_globals():
    return {
        'now_year': datetime.utcnow().year,
        'panel_name': 'McPanel 我的世界开服面板',
        'simpfun_like': True,
    }


# 注册常用内置函数到 Jinja 环境
app.jinja_env.globals.update({
    'min': min, 'max': max, 'len': len, 'range': range,
    'round': round, 'int': int, 'float': float, 'str': str,
})


# ========================= 首页与认证 =========================

@app.route('/')
def index():
    if current_user.is_authenticated:
        return redirect(url_for('dashboard'))
    return redirect(url_for('landing'))


@app.route('/landing')
def landing():
    """类似简幻欢的宣传落地页"""
    stats = {
        'users': User.query.count(),
        'servers': ServerInstance.query.count(),
        'running': ServerInstance.query.filter_by(status='running').count(),
    }
    return render_template('landing.html', stats=stats)


@app.route('/login', methods=['GET', 'POST'])
def auth_login():
    if current_user.is_authenticated:
        return redirect(url_for('dashboard'))
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '')
        user = User.query.filter(db.or_(User.username == username, User.email == username)).first()
        if user and user.check_password(password):
            login_user(user, remember=bool(request.form.get('remember')))
            # 管理员加入 admin SocketIO 房间 (用于后续接收主机监控广播)
            if user.is_admin:
                session['_is_admin'] = '1'
            else:
                session.pop('_is_admin', None)
            # 检测管理员是否仍使用默认口令 (admin123), 提醒强制改密
            if user.is_admin and user.check_password('admin123'):
                session['_must_change_admin_password'] = '1'
                flash('⚠ 您正在使用默认管理员口令, 请立即修改以避免安全风险', 'warning')
            else:
                session.pop('_must_change_admin_password', None)
            flash(f'欢迎回来, {user.username}!', 'success')
            next_url = request.args.get('next') or ''
            # 仅允许站内相对跳转, 防 open redirect 钓鱼
            if next_url and next_url.startswith('/') and not next_url.startswith('//'):
                return redirect(next_url)
            return redirect(url_for('dashboard'))
        flash('用户名或密码错误', 'error')
    # 登录页: 仅在管理员仍使用默认口令时给出警告 (不显示明文)
    show_default_creds_hint = False
    admin_user = User.query.filter_by(username='admin').first()
    if admin_user and admin_user.check_password('admin123'):
        show_default_creds_hint = True
    return render_template('login.html', show_default_creds_hint=show_default_creds_hint)


@app.route('/register', methods=['GET', 'POST'])
def auth_register():
    if current_user.is_authenticated:
        return redirect(url_for('dashboard'))
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        email = request.form.get('email', '').strip()
        pwd = request.form.get('password', '')
        pwd2 = request.form.get('password2', '')
        if len(username) < 3 or len(username) > 20:
            flash('用户名长度需在 3-20 之间', 'error')
        elif '@' not in email or '.' not in email:
            flash('邮箱格式不正确', 'error')
        elif len(pwd) < 6:
            flash('密码长度至少 6 位', 'error')
        elif pwd != pwd2:
            flash('两次输入的密码不一致', 'error')
        elif User.query.filter_by(username=username).first():
            flash('用户名已存在', 'error')
        elif User.query.filter_by(email=email).first():
            flash('邮箱已被注册', 'error')
        else:
            u = User(username=username, email=email)
            u.set_password(pwd)
            db.session.add(u)
            db.session.commit()
            login_user(u)
            flash('注册成功, 已自动登录!', 'success')
            return redirect(url_for('dashboard'))
    return render_template('register.html')


@app.route('/logout')
@login_required
def auth_logout():
    logout_user()
    flash('您已安全退出', 'info')
    return redirect(url_for('landing'))


# ========================= 仪表盘 =========================

@app.route('/dashboard')
@login_required
def dashboard():
    servers = ServerInstance.query.filter_by(user_id=current_user.id).order_by(ServerInstance.created_at.desc()).all()

    # ---- 仅管理员取主机资源数据 ----
    host = None
    if current_user.is_admin:
        host = {
            'cpu': psutil.cpu_percent(interval=0.2),
            'memory_total': round(psutil.virtual_memory().total / (1024 ** 3), 1),
            'memory_used': round(psutil.virtual_memory().used / (1024 ** 3), 1),
            'memory_percent': psutil.virtual_memory().percent,
            'disk_total': round(psutil.disk_usage(str(DATA_DIR)).total / (1024 ** 3), 1),
            'disk_used': round(psutil.disk_usage(str(DATA_DIR)).used / (1024 ** 3), 1),
            'disk_percent': psutil.disk_usage(str(DATA_DIR)).percent,
        }

    # 资源使用汇总
    used_mem = sum(s.memory for s in servers)
    used_cores = sum(s.cores for s in servers)
    running_cnt = sum(1 for s in servers if s.status == 'running')
    stats = {
        'running': running_cnt,
        'total': len(servers),
        'used_mem': used_mem,
        'used_mem_pct': round(used_mem / max(1, current_user.total_memory) * 100, 1),
        'used_cores': used_cores,
        'used_cores_pct': round(used_cores / max(1, current_user.total_cores) * 100, 1),
        'players_online': sum(s.player_count for s in servers),
    }
    return render_template('dashboard.html',
                           servers=servers,
                           host=host,  # 普通用户为 None
                           stats=stats,
                           java_status=java_manager.get_status())


# ========================= 服务器 CRUD =========================

@app.route('/servers/new', methods=['GET', 'POST'])
@login_required
def server_new():
    user_servers = ServerInstance.query.filter_by(user_id=current_user.id).all()
    used_mem = sum(s.memory for s in user_servers)
    used_cores = sum(s.cores for s in user_servers)
    remain_mem = current_user.total_memory - used_mem
    remain_cores = current_user.total_cores - used_cores

    # 节点列表 (普通用户也能选, 但仅显示在线节点)
    all_nodes = DistributedNode.query.filter_by(status='online').all()

    if request.method == 'POST':
        if len(user_servers) >= current_user.max_servers and not current_user.is_admin:
            flash(f'已达到最大服务器数量 {current_user.max_servers}', 'error')
            return redirect(url_for('server_new'))
        name = request.form.get('name', '').strip()
        s_type = request.form.get('server_type', 'paper')
        version = request.form.get('version', '1.20.4')
        memory = int(request.form.get('memory', 2048))
        cores = int(request.form.get('cores', 1))
        motd = request.form.get('motd', 'A Minecraft Server').strip() or 'A Minecraft Server'
        max_players = int(request.form.get('max_players', 20))
        prefer_location = request.form.get('prefer_location', '').strip()

        # ---- 节点选择 ----
        # node_id=0  -> 主节点本地 (默认)
        # node_id=N  -> 远程节点 N
        # auto       -> 让系统自动从 recommend_nodes 选最优
        # fallback   -> 若所选节点不可用, 自动推荐备选
        node_choice = request.form.get('node_choice', 'auto').strip()
        chosen_node_id: Optional[int] = None  # None=本地, 否则远程

        try:
            picked_nid = int(request.form.get('node_id', '0') or '0')
        except (TypeError, ValueError):
            picked_nid = 0

        if node_choice == 'manual' and picked_nid > 0:
            # 用户手动指定节点: 校验在线 + 资源 + max_servers 配额
            n = db.session.get(DistributedNode, picked_nid)
            if not n or n.status != 'online':
                # 节点不可用 -> 自动备选
                flash(f'所选节点 #{picked_nid} 不可用, 已自动切换到备选节点', 'warn')
                chosen_node_id = _try_auto_pick(memory, cores, prefer_location)
            elif n.available_memory_mb < memory * 1.5:
                flash(f'节点 {n.name} 剩余内存不足, 已自动切换到备选节点', 'warn')
                chosen_node_id = _try_auto_pick(memory, cores, prefer_location)
            elif not n.can_accept_more_servers():
                # 节点配额已满 -> 自动备选
                flash(f'节点 {n.name} 已达最大服务器数 {n.max_servers}, 已切换到备选节点', 'warn')
                chosen_node_id = _try_auto_pick(memory, cores, prefer_location)
            else:
                chosen_node_id = n.id
        else:
            # auto: 智能推荐
            chosen_node_id = _try_auto_pick(memory, cores, prefer_location)

        # 校验
        if len(name) < 2:
            flash('服务器名称至少 2 个字符', 'error')
        elif memory < 512:
            flash('内存至少 512 MB', 'error')
        elif memory > remain_mem:
            flash(f'超出可用内存配额 (剩余 {remain_mem}MB)', 'error')
        elif cores < 1 or cores > remain_cores:
            flash(f'核心数不合法 (剩余 {remain_cores})', 'error')
        else:
            try:
                port = allocate_port()
            except RuntimeError as e:
                flash(str(e), 'error')
                return redirect(url_for('server_new'))
            sv = ServerInstance(
                name=name, user_id=current_user.id,
                server_type=s_type, version=version,
                memory=memory, cores=cores, port=port,
                motd=motd, max_players=max_players,
                status='stopped',
                node_id=chosen_node_id,
            )
            db.session.add(sv)
            db.session.commit()
            # 本地服务器才预先初始化目录
            if chosen_node_id is None:
                ctl = get_controller(sv)
                ctl.ensure_directory()
                ctl.write_server_properties()
            node_label = '主节点' if chosen_node_id is None else f'节点 #{chosen_node_id}'

            # 部署即下载所需 Java: 后台执行, 不阻塞创建请求
            required_java = JavaManager.java_for_mc(version)
            try:
                if chosen_node_id is None:
                    # 本地节点: 后台下载到主控 runtime
                    java_manager.ensure_installed_async(required_java)
                else:
                    # 远程节点: 通知 Agent 预下载 (长超时, 后台线程执行)
                    threading.Thread(
                        target=_preinstall_node_java,
                        args=(chosen_node_id, required_java),
                        daemon=True).start()
            except Exception as e:
                app.logger.warning(f'部署时预装 Java 失败: {e}')

            flash(f'服务器已创建! 端口: {port} · 部署在 {node_label} · 已开始后台准备 Java {required_java}', 'success')
            return redirect(url_for('server_detail', sid=sv.id))
    return render_template('server_new.html',
                           remain_mem=remain_mem,
                           remain_cores=remain_cores,
                           max_servers=current_user.max_servers,
                           used_count=len(user_servers),
                           nodes=all_nodes)


def _try_auto_pick(memory_mb: int, cores: int, prefer_location: str = '') -> Optional[int]:
    """自动推荐最佳节点; 返回 node_id (None 表示主节点本地)."""
    recs = NODE_MANAGER.recommend_nodes(
        required_memory_mb=memory_mb, required_cores=cores,
        prefer_location=prefer_location, limit=3,
    )
    if recs:
        return recs[0]['node'].id
    return None


def _preinstall_node_java(node_id: int, version: int):
    """通知远程节点 Agent 预下载 Java (后台线程, 失败不影响部署)"""
    try:
        with app.app_context():
            node = db.session.get(DistributedNode, node_id)
            if not node or node.status != 'online':
                return
            # Agent 的 /java/install 会等待其他任务并串行下载, 用长超时等待其完成
            rpc = NodeRPC(node, timeout=3600)
            res = rpc.java_install(version)
            app.logger.info(f'[Deploy] 节点 #{node_id} Java {version} 预装结果: {res}')
    except Exception as e:
        app.logger.warning(f'[Deploy] 节点 #{node_id} 预装 Java {version} 失败 (启动时会自动重试): {e}')


def _own_server(sid: int) -> ServerInstance:
    sv = db.session.get(ServerInstance, sid)
    if not sv:
        abort(404)
    if sv.user_id != current_user.id and not current_user.is_admin:
        abort(403)
    return sv


@app.route('/servers/<int:sid>')
@login_required
def server_detail(sid: int):
    sv = _own_server(sid)
    ctl = get_controller(sv)
    return render_template('server_detail.html', s=sv, sid=sid, ctl=ctl)


@app.route('/servers/<int:sid>/delete', methods=['POST'])
@login_required
def server_delete(sid: int):
    sv = _own_server(sid)
    # 仅本地 kill, 远程节点暂不删文件
    if sv.node_id is None:
        ctl = get_controller(sv)
        try:
            ctl.kill()
        except Exception:
            pass
        try:
            import shutil
            p = SERVERS_DIR / f'server_{sv.id}'
            if p.exists():
                shutil.rmtree(p)
        except Exception as e:
            app.logger.warning(f'删除目录失败 {sv.id}: {e}')
    with _controllers_lock:
        _SERVER_CONTROLLERS.pop(sv.id, None)
    db.session.delete(sv)
    db.session.commit()
    flash('服务器已删除', 'info')
    return redirect(url_for('dashboard'))


# ------ 动作 API (返回 JSON, 方便前端) ------

@app.post('/api/servers/<int:sid>/start')
@login_required
def api_start(sid: int):
    sv = _own_server(sid)
    ctl = get_controller(sv)
    res = ctl.start()
    db.session.commit()
    return jsonify(res)


@app.post('/api/servers/<int:sid>/stop')
@login_required
def api_stop(sid: int):
    sv = _own_server(sid)
    ctl = get_controller(sv)
    res = ctl.stop()
    db.session.commit()
    return jsonify(res)


@app.post('/api/servers/<int:sid>/restart')
@login_required
def api_restart(sid: int):
    sv = _own_server(sid)
    ctl = get_controller(sv)
    res = ctl.restart()
    db.session.commit()
    return jsonify(res)


@app.post('/api/servers/<int:sid>/kill')
@login_required
def api_kill(sid: int):
    sv = _own_server(sid)
    ctl = get_controller(sv)
    res = ctl.kill()
    db.session.commit()
    return jsonify(res)


@app.post('/api/servers/<int:sid>/command')
@login_required
def api_command(sid: int):
    sv = _own_server(sid)
    cmd = request.get_json(silent=True) or {}
    cmd_text = (cmd.get('command') or '').strip()
    if not cmd_text:
        return jsonify({'ok': False, 'msg': '命令为空'})
    ctl = get_controller(sv)
    ok = ctl.send_command(cmd_text)
    return jsonify({'ok': ok})


@app.get('/api/servers/<int:sid>/stats')
@login_required
def api_stats(sid: int):
    sv = _own_server(sid)
    ctl = get_controller(sv)
    return jsonify(ctl.get_stats())


@app.get('/api/servers/<int:sid>/console')
@login_required
def api_console(sid: int):
    sv = _own_server(sid)
    ctl = get_controller(sv)
    return jsonify(ctl.console.all())


# ========================= 配置页面 =========================

@app.route('/servers/<int:sid>/settings', methods=['GET', 'POST'])
@login_required
def server_settings(sid: int):
    sv = _own_server(sid)
    ctl = get_controller(sv)
    user_servers = ServerInstance.query.filter_by(user_id=current_user.id).all()
    used_mem = sum(s.memory for s in user_servers if s.id != sv.id)
    used_cores = sum(s.cores for s in user_servers if s.id != sv.id)
    remain_mem = current_user.total_memory - used_mem
    remain_cores = current_user.total_cores - used_cores

    if request.method == 'POST':
        sv.name = request.form.get('name', sv.name).strip() or sv.name
        sv.motd = request.form.get('motd', sv.motd).strip() or sv.motd
        sv.max_players = int(request.form.get('max_players', sv.max_players))
        mem = int(request.form.get('memory', sv.memory))
        cores = int(request.form.get('cores', sv.cores))
        if 512 <= mem <= remain_mem:
            sv.memory = mem
        if 1 <= cores <= remain_cores:
            sv.cores = cores
        sv.version = request.form.get('version', sv.version)
        sv.server_type = request.form.get('server_type', sv.server_type)
        sv.java_args = request.form.get('java_args', sv.java_args).strip()
        sv.online_mode = 'online_mode' in request.form
        sv.pvp_enabled = 'pvp_enabled' in request.form
        sv.whitelist_enabled = 'whitelist_enabled' in request.form
        sv.auto_start = 'auto_start' in request.form
        sv.difficulty = request.form.get('difficulty', sv.difficulty)
        sv.level_name = request.form.get('level_name', sv.level_name).strip() or 'world'
        db.session.commit()
        if sv.node_id is None:
            ctl.write_server_properties()
        flash('设置已保存, 需重启服务器生效', 'success')
        return redirect(url_for('server_settings', sid=sv.id))
    return render_template('server_settings.html', s=sv, sid=sv.id,
                           remain_mem=remain_mem, remain_cores=remain_cores)


# ========================= 文件管理 =========================

@app.route('/servers/<int:sid>/files', defaults={'subpath': ''})
@app.route('/servers/<int:sid>/files/<path:subpath>')
@login_required
def server_files(sid: int, subpath: str):
    sv = _own_server(sid)
    ctl = get_controller(sv)
    try:
        files = ctl.list_files(subpath) if hasattr(ctl, 'list_files') else []
    except Exception as e:
        files = []
        flash(f'读取目录失败: {e}', 'error')
    return render_template('server_files.html', s=sv, sid=sv.id, files=files,
                           current_path=subpath or '/')


@app.post('/api/servers/<int:sid>/files/delete')
@login_required
def api_delete_file(sid: int):
    sv = _own_server(sid)
    ctl = get_controller(sv)
    data = request.get_json(silent=True) or {}
    rel = data.get('path', '')
    if not hasattr(ctl, 'delete_path'):
        return jsonify({'ok': False, 'msg': '节点文件删除暂未实现'})
    res = ctl.delete_path(rel)
    return jsonify(res)


# ========================= 主机 & 全局 API (仅管理员) =========================

@app.get('/api/host/stats')
@login_required
@admin_required
def api_host_stats():
    mem = psutil.virtual_memory()
    disk = psutil.disk_usage(str(DATA_DIR))
    net = psutil.net_io_counters()
    return jsonify({
        'cpu': psutil.cpu_percent(interval=0.2),
        'cpu_count': psutil.cpu_count(logical=True),
        'memory_total': round(mem.total / (1024 ** 3), 1),
        'memory_used': round(mem.used / (1024 ** 3), 1),
        'memory_percent': mem.percent,
        'disk_total': round(disk.total / (1024 ** 3), 1),
        'disk_used': round(disk.used / (1024 ** 3), 1),
        'disk_percent': disk.percent,
        'net_sent_mb': round(net.bytes_sent / (1024 ** 2), 2),
        'net_recv_mb': round(net.bytes_recv / (1024 ** 2), 2),
        'uptime_seconds': int(time.time() - psutil.boot_time()),
    })


# ========================= Java 运行时 API =========================

@app.get('/api/java/status')
@login_required
def api_java_status():
    """获取 Java 安装状态"""
    return jsonify(java_manager.get_status())


@app.post('/api/java/install')
@login_required
@admin_required
def api_java_install():
    """触发 Java JRE 下载安装 (异步, 仅管理员)"""
    data = request.get_json(silent=True) or {}
    java_version = int(data.get('version', 17))
    if java_version not in (8, 11, 17, 21):
        return jsonify({'ok': False, 'msg': '不支持的 Java 版本, 请选择 8/11/17/21'}), 400
    res = java_manager.install_async(java_version)
    return jsonify(res)


# ============================================================
# 管理员中心路由 (全部 @admin_required)
# ============================================================

@app.route('/admin')
@login_required
@admin_required
def admin_center():
    users_total = User.query.count()
    servers_total = ServerInstance.query.count()
    servers_running = ServerInstance.query.filter_by(status='running').count()
    nodes_total = DistributedNode.query.count()
    nodes_online = DistributedNode.query.filter_by(status='online').count()

    mem = psutil.virtual_memory()
    disk = psutil.disk_usage(str(DATA_DIR))
    local_host = {
        'cpu': psutil.cpu_percent(interval=0.1),
        'memory_total_gb': round(mem.total / (1024 ** 3), 1),
        'memory_used_gb': round(mem.used / (1024 ** 3), 1),
        'memory_percent': mem.percent,
        'disk_total_gb': round(disk.total / (1024 ** 3), 1),
        'disk_used_gb': round(disk.used / (1024 ** 3), 1),
        'disk_percent': disk.percent,
    }
    nodes = DistributedNode.query.order_by(DistributedNode.created_at.desc()).all()
    recent_users = User.query.order_by(User.created_at.desc()).limit(8).all()
    recent_servers = ServerInstance.query.order_by(ServerInstance.created_at.desc()).limit(8).all()
    # 本地节点 (node_id=NULL) 的服务器和在线玩家, 用于展示已分配 / 实际可用
    servers_local_default = ServerInstance.query.filter_by(node_id=None).all()
    local_online_players = sum(s.player_count for s in
                               [sv for sv in servers_local_default if sv.status == 'running'])
    # 主节点本地已分配 / 实际可用内存 (MB) - 与 DistributedNode.available_memory_mb 同算法
    local_assigned_mb = sum(s.memory for s in servers_local_default)
    local_phys_total_mb = int(psutil.virtual_memory().total // (1024 * 1024))
    local_phys_used_mb = int(psutil.virtual_memory().used // (1024 * 1024))
    local_actual_used_mb = max(local_phys_used_mb, local_assigned_mb)
    local_available_memory_mb = max(0, local_phys_total_mb - local_actual_used_mb)
    local_assigned_cores = sum(s.cores for s in servers_local_default)
    local_phys_cores = psutil.cpu_count(logical=True) or 0
    local_available_cores = max(0, local_phys_cores - local_assigned_cores)
    return render_template(
        'admin_center.html',
        users_total=users_total, servers_total=servers_total,
        servers_running=servers_running,
        nodes_total=nodes_total, nodes_online=nodes_online,
        local_host=local_host, nodes=nodes,
        recent_users=recent_users, recent_servers=recent_servers,
        servers_local_default=servers_local_default,
        local_online_players=local_online_players,
        local_assigned_mb=local_assigned_mb,
        local_available_memory_mb=local_available_memory_mb,
        local_assigned_cores=local_assigned_cores,
        local_available_cores=local_available_cores,
    )


@app.route('/admin/users')
@login_required
@admin_required
def admin_users():
    q = request.args.get('q', '').strip()
    query = User.query
    if q:
        like = f'%{q}%'
        query = query.filter(db.or_(User.username.like(like), User.email.like(like)))
    users = query.order_by(User.created_at.desc()).limit(500).all()
    return render_template('admin_users.html', users=users, q=q)


@app.route('/admin/servers')
@login_required
@admin_required
def admin_servers():
    servers = ServerInstance.query.order_by(ServerInstance.created_at.desc()).all()
    return render_template('admin_servers.html', servers=servers)


@app.route('/admin/nodes')
@login_required
@admin_required
def admin_nodes():
    nodes = DistributedNode.query.order_by(DistributedNode.created_at.desc()).all()
    return render_template('admin_nodes.html', nodes=nodes)


# ---------- 管理员 API ----------

@app.post('/api/admin/users/<int:uid>/update')
@login_required
@admin_required
def api_admin_update_user(uid: int):
    u = db.session.get(User, uid)
    if not u:
        return jsonify({'ok': False, 'msg': '用户不存在'}), 404
    data = request.get_json(silent=True) or {}
    # 防止管理员把自己降级 (至少留一个 admin)
    if u.id == current_user.id and not bool(data.get('is_admin', True)):
        admins_left = User.query.filter_by(is_admin=True).filter(User.id != u.id).count()
        if admins_left == 0:
            return jsonify({'ok': False, 'msg': '不能取消最后一个管理员'}), 400
    if 'max_servers' in data:
        try:
            u.max_servers = max(0, int(data['max_servers']))
        except (TypeError, ValueError):
            pass
    if 'total_memory' in data:
        try:
            u.total_memory = max(512, int(data['total_memory']))
        except (TypeError, ValueError):
            pass
    if 'total_cores' in data:
        try:
            u.total_cores = max(1, int(data['total_cores']))
        except (TypeError, ValueError):
            pass
    if 'balance' in data:
        try:
            u.balance = float(data['balance'])
        except (TypeError, ValueError):
            pass
    if 'is_admin' in data:
        u.is_admin = bool(data['is_admin'])
    if 'password' in data:
        pwd = str(data['password']).strip()
        if len(pwd) >= 6:
            u.set_password(pwd)
            # 管理员改密后清除 "默认口令未改" 警告标记 (针对当前操作者本人或修改了 admin 账号)
            if u.username == 'admin' or u.id == current_user.id:
                session.pop('_must_change_admin_password', None)
    db.session.commit()
    app.logger.info(f'[Admin] 管理员 {current_user.username} 修改了用户 #{u.id} ({u.username})')
    return jsonify({'ok': True})


@app.post('/api/admin/nodes/add')
@login_required
@admin_required
def api_admin_add_node():
    data = request.get_json(silent=True) or {}
    name = (data.get('name') or '').strip()
    api_url = (data.get('api_url') or '').strip().rstrip('/')
    api_token = (data.get('api_token') or '').strip()
    if len(name) < 2:
        return jsonify({'ok': False, 'msg': '节点名称至少 2 个字符'}), 400
    if not api_url.startswith(('http://', 'https://')):
        return jsonify({'ok': False, 'msg': 'API URL 需要 http(s):// 前缀'}), 400
    if len(api_token) < 8:
        return jsonify({'ok': False, 'msg': 'Token 长度至少 8 位'}), 400
    if DistributedNode.query.filter_by(name=name).first():
        return jsonify({'ok': False, 'msg': '节点名称已存在'}), 400
    n = DistributedNode(
        name=name, api_url=api_url, api_token=api_token,
        description=(data.get('description') or '').strip(),
        location=(data.get('location') or '').strip(),
    )
    # 可选: 创建时同时设置最大服务器数配额
    try:
        ms = int(data.get('max_servers', 0) or 0)
        if ms >= 0:
            n.max_servers = ms
    except (TypeError, ValueError):
        pass
    db.session.add(n)
    db.session.commit()
    app.logger.info(f'[Admin] 管理员 {current_user.username} 添加节点 {name} -> {api_url}')
    return jsonify({'ok': True, 'id': n.id})


@app.post('/api/admin/nodes/<int:nid>/update')
@login_required
@admin_required
def api_admin_update_node(nid: int):
    """更新节点配置 (管理员可设置最大服务器数 / 位置 / 描述 等)"""
    n = db.session.get(DistributedNode, nid)
    if not n:
        return jsonify({'ok': False, 'msg': '节点不存在'}), 404
    data = request.get_json(silent=True) or {}
    if 'max_servers' in data:
        try:
            ms = int(data['max_servers'])
            if ms < 0:
                return jsonify({'ok': False, 'msg': '最大服务器数不能为负数'}), 400
            n.max_servers = ms
            app.logger.info(f'[Admin] 节点 #{nid} max_servers -> {ms}')
        except (TypeError, ValueError):
            return jsonify({'ok': False, 'msg': 'max_servers 必须是整数'}), 400
    if 'description' in data:
        n.description = str(data['description']).strip()[:300]
    if 'location' in data:
        n.location = str(data['location']).strip()[:100]
    if 'name' in data:
        new_name = str(data['name']).strip()
        if len(new_name) >= 2 and new_name != n.name:
            if DistributedNode.query.filter_by(name=new_name).first():
                return jsonify({'ok': False, 'msg': '节点名称已存在'}), 400
            n.name = new_name
    db.session.commit()
    return jsonify({'ok': True, 'max_servers': n.max_servers,
                    'available_slots': n.available_server_slots})


@app.post('/api/admin/nodes/<int:nid>/delete')
@login_required
@admin_required
def api_admin_delete_node(nid: int):
    n = db.session.get(DistributedNode, nid)
    if not n:
        return jsonify({'ok': False, 'msg': '节点不存在'}), 404
    # 若还有服务器分配到此节点则不允许删除 (保护数据)
    linked = ServerInstance.query.filter_by(node_id=nid).count()
    if linked > 0:
        return jsonify({'ok': False, 'msg': f'该节点仍有 {linked} 台服务器, 请先迁移或删除'}), 400
    db.session.delete(n)
    db.session.commit()
    app.logger.info(f'[Admin] 管理员 {current_user.username} 删除节点 #{nid} {n.name}')
    return jsonify({'ok': True})


@app.get('/api/admin/nodes/<int:nid>/ping')
@login_required
@admin_required
def api_admin_ping_node(nid: int):
    n = db.session.get(DistributedNode, nid)
    if not n:
        return jsonify({'ok': False, 'msg': '节点不存在'}), 404
    try:
        info = NODE_MANAGER.get_rpc(n).ping()
        # 立即更新一次状态
        n.status = 'online'
        n.last_error = None
        n.last_heartbeat_at = datetime.utcnow()
        host = info.get('host') or {}
        n.host_cpu = float(host.get('cpu', 0.0))
        n.host_memory_total = float(host.get('memory_total_gb', 0.0))
        n.host_memory_used = float(host.get('memory_used_gb', 0.0))
        n.host_disk_total = float(host.get('disk_total_gb', 0.0))
        n.host_disk_used = float(host.get('disk_used_gb', 0.0))
        db.session.commit()
        return jsonify({'ok': True, 'info': info})
    except NodeRPCError as e:
        n.status = 'error'
        n.last_error = str(e)[:500]
        n.last_heartbeat_at = datetime.utcnow()
        db.session.commit()
        return jsonify({'ok': False, 'msg': str(e)})


@app.get('/api/admin/global_stats')
@login_required
@admin_required
def api_admin_global_stats():
    nodes = DistributedNode.query.all()
    total_mem = psutil.virtual_memory().total / (1024 ** 3)
    used_mem = psutil.virtual_memory().used / (1024 ** 3)
    total_disk = psutil.disk_usage(str(DATA_DIR)).total / (1024 ** 3)
    used_disk = psutil.disk_usage(str(DATA_DIR)).used / (1024 ** 3)
    running_local = ServerInstance.query.filter_by(status='running', node_id=None).count()
    for n in nodes:
        total_mem += n.host_memory_total
        used_mem += n.host_memory_used
        total_disk += n.host_disk_total
        used_disk += n.host_disk_used
    return jsonify({
        'users': User.query.count(),
        'servers': ServerInstance.query.count(),
        'servers_running': running_local + sum(n.running_servers for n in nodes),
        'nodes_total': len(nodes),
        'nodes_online': sum(1 for n in nodes if n.status == 'online'),
        'memory_total_gb': round(total_mem, 1),
        'memory_used_gb': round(used_mem, 1),
        'memory_percent': round(used_mem / max(0.01, total_mem) * 100, 1),
        'disk_total_gb': round(total_disk, 1),
        'disk_used_gb': round(used_disk, 1),
        'disk_percent': round(used_disk / max(0.01, total_disk) * 100, 1),
    })


# ========================= 分布式节点查询 API (登录用户可用) =========================
# 普通用户也能查看可用节点 / 推荐节点 / 节点详情, 用于创建服务器时选择;
# 仅暴露必要字段, 不泄露 Token 等敏感信息.

def _node_public_dict(n) -> dict:
    """对外可发布的节点信息 (剔除 api_token 等敏感字段)"""
    return {
        'id': n.id,
        'name': n.name,
        'description': n.description or '',
        'location': n.location or '',
        'status': n.status,
        'status_display': n.status_display,
        'status_color': n.status_color,
        'host_cpu': round(n.host_cpu or 0.0, 1),
        'host_cpu_count': n.host_cpu_count or 0,
        'host_memory_total': round(n.host_memory_total or 0.0, 1),
        'host_memory_used': round(n.host_memory_used or 0.0, 1),
        'host_disk_total': round(n.host_disk_total or 0.0, 1),
        'host_disk_used': round(n.host_disk_used or 0.0, 1),
        'memory_percent': n.memory_percent,
        'disk_percent': n.disk_percent,
        'load_score': n.load_score,
        # 面板视角的资源统计 (修复"剩余资源不实际")
        'assigned_memory_mb': n.assigned_memory_mb(),
        'available_memory_mb': n.available_memory_mb,
        'assigned_cores': n.assigned_cores(),
        'available_cores': n.available_cores,
        'assigned_servers': n.assigned_server_count(),
        'running_servers': n.running_server_count(),
        'online_players': n.online_players or 0,
        'max_servers': n.max_servers or 0,
        'available_slots': n.available_server_slots,
        'last_heartbeat_at': n.last_heartbeat_at.strftime('%Y-%m-%d %H:%M:%S')
                            if n.last_heartbeat_at else None,
        'last_error': n.last_error if current_user.is_admin else None,
    }


@app.get('/api/nodes/available')
@login_required
def api_nodes_available():
    """列出所有可用 (在线) 节点 + 主节点本地占位. 前端 5s 自动刷新. 普通用户可访问."""
    nodes = DistributedNode.query.filter_by(status='online').all()
    out = [_node_public_dict(n) for n in nodes]
    # 主节点 (本地) 一并返回, 作为可选目标
    local_mem = psutil.virtual_memory()
    local_disk = psutil.disk_usage(str(DATA_DIR))
    local_assigned_mb = sum(s.memory for s in
                           ServerInstance.query.filter_by(node_id=None).all())
    local_assigned_cores = sum(s.cores for s in
                               ServerInstance.query.filter_by(node_id=None).all())
    local_cpu_count = psutil.cpu_count(logical=True) or 0
    local_avail_mb = max(0, int(local_mem.total // (1024 * 1024)) - local_assigned_mb)
    local_avail_cores = max(0, local_cpu_count - local_assigned_cores)
    out.insert(0, {
        'id': 0,
        'name': '主节点 (本地)',
        'description': '主控面板所在服务器',
        'location': 'localhost',
        'status': 'online',
        'status_display': '在线',
        'status_color': '#22c55e',
        'host_cpu': round(psutil.cpu_percent(interval=None), 1),
        'host_cpu_count': local_cpu_count,
        'host_memory_total': round(local_mem.total / (1024 ** 3), 1),
        'host_memory_used': round(local_mem.used / (1024 ** 3), 1),
        'host_disk_total': round(local_disk.total / (1024 ** 3), 1),
        'host_disk_used': round(local_disk.used / (1024 ** 3), 1),
        'memory_percent': local_mem.percent,
        'disk_percent': local_disk.percent,
        'load_score': round(psutil.cpu_percent(interval=None) * 0.5
                            + local_mem.percent * 0.3
                            + local_disk.percent * 0.2, 1),
        'assigned_memory_mb': local_assigned_mb,
        'available_memory_mb': local_avail_mb,
        'assigned_cores': local_assigned_cores,
        'available_cores': local_avail_cores,
        'assigned_servers': ServerInstance.query.filter_by(node_id=None).count(),
        'running_servers': ServerInstance.query.filter_by(status='running', node_id=None).count(),
        'online_players': sum(s.player_count for s in
                              ServerInstance.query.filter_by(status='running', node_id=None).all()),
        'last_heartbeat_at': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
        'last_error': None,
    })
    return jsonify({'ok': True, 'nodes': out})


@app.get('/api/nodes/recommend')
@login_required
def api_nodes_recommend():
    """基于用户需求推荐候选节点 (含评分理由). 普通用户可访问."""
    try:
        mem = int(request.args.get('memory', '0') or '0')
    except (TypeError, ValueError):
        mem = 0
    try:
        cores = int(request.args.get('cores', '0') or '0')
    except (TypeError, ValueError):
        cores = 0
    loc = (request.args.get('location', '') or '').strip()
    try:
        limit = max(1, min(10, int(request.args.get('limit', '5') or '5')))
    except (TypeError, ValueError):
        limit = 5
    recs = NODE_MANAGER.recommend_nodes(
        required_memory_mb=mem, required_cores=cores,
        prefer_location=loc, limit=limit,
    )
    return jsonify({
        'ok': True,
        'recommendations': [{
            'node': _node_public_dict(r['node']),
            'score': r['score'],
            'reasons': r['reasons'],
            'tags': r['tags'],
            'metrics': r['metrics'],
        } for r in recs],
    })


@app.get('/api/nodes/<int:nid>/detail')
@login_required
def api_node_detail(nid: int):
    """单节点详情 (含其上所有服务器状态). 普通用户可访问, 但只能看到自己服务器."""
    if nid == 0:
        return jsonify({'ok': False, 'msg': '主节点无需查询详情, 请用 /api/nodes/available'}), 400
    n = db.session.get(DistributedNode, nid)
    if not n:
        return jsonify({'ok': False, 'msg': '节点不存在'}), 404
    # 节点上服务器列表 (普通用户只看自己的, 管理员看全部)
    q = ServerInstance.query.filter_by(node_id=nid)
    if not current_user.is_admin:
        q = q.filter_by(user_id=current_user.id)
    servers = [{
        'id': s.id,
        'name': s.name,
        'status': s.status,
        'status_display': s.status_display,
        'memory': s.memory,
        'cores': s.cores,
        'port': s.port,
        'player_count': s.player_count,
        'max_players': s.max_players,
        'mine': s.user_id == current_user.id,
    } for s in q.order_by(ServerInstance.id.desc()).all()]
    return jsonify({
        'ok': True,
        'node': _node_public_dict(n),
        'servers': servers,
    })


@app.post('/api/nodes/<int:nid>/check')
@login_required
def api_node_check(nid: int):
    """用户提交前实时检查节点是否仍可分配所需资源 (防止创建瞬间节点变满)."""
    if nid == 0:
        # 主节点检查: 用户配额 + 主节点本地剩余
        try:
            mem = int(request.get_json(silent=True).get('memory', '0') or '0')
        except Exception:
            mem = 0
        used = sum(s.memory for s in ServerInstance.query.filter_by(user_id=current_user.id).all())
        remain = current_user.total_memory - used
        local_mem = psutil.virtual_memory()
        local_phys_free_mb = max(0, int(local_mem.total // (1024 * 1024)) - int(local_mem.used // (1024 * 1024)))
        if mem > remain:
            return jsonify({'ok': False, 'reason': 'over_user_quota',
                            'msg': f'超出用户配额 (剩余 {remain}MB)'})
        if mem > local_phys_free_mb:
            return jsonify({'ok': False, 'reason': 'node_full',
                            'msg': f'主节点物理内存不足 (剩余 {local_phys_free_mb}MB)'})
        return jsonify({'ok': True, 'msg': '主节点可分配'})

    n = db.session.get(DistributedNode, nid)
    if not n:
        return jsonify({'ok': False, 'reason': 'not_found', 'msg': '节点不存在'}), 404
    if n.status != 'online':
        return jsonify({'ok': False, 'reason': 'offline',
                        'msg': f'节点当前 {n.status_display}, 不可分配'})
    try:
        mem = int((request.get_json(silent=True) or {}).get('memory', '0') or '0')
    except (TypeError, ValueError):
        mem = 0
    if mem and mem > n.available_memory_mb:
        return jsonify({'ok': False, 'reason': 'node_full',
                        'msg': f'节点剩余内存 {n.available_memory_mb}MB 不足 {mem}MB'})
    # 检查 max_servers 配额
    if not n.can_accept_more_servers():
        return jsonify({'ok': False, 'reason': 'quota_full',
                        'msg': f'节点已达最大服务器数 {n.max_servers} (已用 {n.assigned_server_count()})'})
    return jsonify({'ok': True, 'msg': '节点可分配',
                    'available_memory_mb': n.available_memory_mb,
                    'available_cores': n.available_cores,
                    'available_slots': n.available_server_slots})


# ========================= SocketIO 事件 =========================
# 说明: 管理员房间使用 SocketIO room='admin_room'
#      每次连接时根据登录态加入

def _join_admin_room_if_needed():
    """在 SocketIO 事件中判断当前连接用户是否管理员, 是则加入管理员房间"""
    uid = session.get('_user_id')
    if not uid:
        return
    try:
        user = db.session.get(User, int(uid))
    except Exception:
        return
    if user and user.is_admin:
        join_room('admin_room')


@socketio.on('connect')
def on_connect():
    _join_admin_room_if_needed()


@socketio.on('join_server')
def on_join(data):
    sid = (data or {}).get('server_id')
    if not sid:
        return
    # 管理员也同时加入 admin 房间 (确保能收到主机广播)
    _join_admin_room_if_needed()
    # 权限校验: 从 session 取 user
    uid = session.get('_user_id')
    if not uid:
        return False
    try:
        user = db.session.get(User, int(uid))
    except Exception:
        return False
    if not user:
        return False
    sv = db.session.get(ServerInstance, int(sid))
    if not sv:
        return False
    if sv.user_id != user.id and not user.is_admin:
        return False
    join_room(f'server_{sid}')
    # 把现有 console 推过去
    ctl = get_controller(sv)
    emit('console_hist', {'lines': ctl.console.all()})
    return True


@socketio.on('leave_server')
def on_leave(data):
    sid = (data or {}).get('server_id')
    if sid:
        leave_room(f'server_{sid}')


@socketio.on('send_command')
def on_cmd(data):
    data = data or {}
    sid = data.get('server_id')
    cmd = (data.get('command') or '').strip()
    if not sid or not cmd:
        return
    uid = session.get('_user_id')
    if not uid:
        return
    sv = db.session.get(ServerInstance, int(sid))
    if not sv:
        return
    try:
        user = db.session.get(User, int(uid))
    except Exception:
        return
    if not user or (sv.user_id != user.id and not user.is_admin):
        return
    ctl = get_controller(sv)
    ctl.send_command(cmd)


# ========================= 错误页面 =========================

@app.errorhandler(404)
def _404(e):
    return render_template('error.html', code=404, title='页面不存在',
                           desc='你访问的页面走丢了, 回首页看看吧 ~'), 404


@app.errorhandler(403)
def _403(e):
    return render_template('error.html', code=403, title='无权访问',
                           desc='你没有权限访问该资源, 请确认账号后重试'), 403


@app.errorhandler(500)
def _500(e):
    traceback.print_exc()
    return render_template('error.html', code=500, title='服务器异常',
                           desc='服务器处理请求时出现问题, 请稍后再试'), 500


# ========================= 启动 =========================

def _background_host_broadcast():
    """
    每 3 秒采集一次主机状态, 仅向 admin_room 广播 (只有管理员能收到).
    普通用户不再接收任何主机级监控数据.
    """
    while True:
        try:
            with app.app_context():
                mem = psutil.virtual_memory()
                disk = psutil.disk_usage(str(DATA_DIR))
                payload = {
                    'cpu': psutil.cpu_percent(interval=None),
                    'memory_percent': mem.percent,
                    'disk_percent': disk.percent,
                }
                socketio.emit('host_stats', payload, room='admin_room', namespace='/')
        except Exception:
            pass
        time.sleep(3)


if __name__ == '__main__':
    init_db()
    # 启动节点心跳线程
    NODE_MANAGER.start_heartbeat()
    print('=' * 50)
    print(' McPanel 我的世界开服面板')
    print(f' 数据目录: {DATA_DIR}')
    print(' 管理账号: admin / admin123')
    print(' 演示账号: demo / demo1234')
    print(f' 面板地址: http://127.0.0.1:{cfg.PANEL_PORT}')
    if cfg.SECRET_RANDOM:
        print('[!] 警告: 未设置 PANEL_SECRET, 当前为随机一次性密钥 (重启将使登录态失效)')
    if cfg.INSECURE_SSL:
        print('[!] 警告: INSECURE_SSL 已启用, 下载时不验证 SSL 证书')
    print('=' * 50)
    t = threading.Thread(target=_background_host_broadcast, daemon=True)
    t.start()
    socketio.run(app, host=cfg.PANEL_HOST, port=cfg.PANEL_PORT, debug=False, allow_unsafe_werkzeug=True)
