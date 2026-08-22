"""
分布式节点管理器 (Master 端)
负责:
  - 节点 RPC 调用封装 (HTTP + Bearer Token, Session 连接池复用)
  - 后台心跳线程: 默认每 5s ping 所有节点, 更新状态与资源数据 (SLA: ≤5s)
  - 节点调度: 为新服务器推荐候选节点 (基于负载评分 + 用户需求约束)
"""
from __future__ import annotations

import threading
import time
from datetime import datetime
from typing import Optional, Dict, List, Tuple
from urllib.parse import urljoin

# requests 为可选依赖: 仅在有远程节点时需要
try:
    import requests  # type: ignore
    from requests.adapters import HTTPAdapter
    try:
        from urllib3.util.retry import Retry
        _HAS_RETRY = True
    except ImportError:
        _HAS_RETRY = False
    _HAS_REQUESTS = True
except ImportError:
    _HAS_REQUESTS = False
    _HAS_RETRY = False


class NodeRPCError(Exception):
    """节点调用失败"""
    pass


class NodeRPC:
    """对单个 DistributedNode 的 RPC 客户端封装

    优化:
    - 使用 requests.Session 复用 TCP 连接, 单次心跳 RPC 从 ~100-300ms 降至 ~10-30ms
    - HTTPAdapter 连接池: pool_connections=10, pool_maxsize=20
    - 自动重试: 连接失败 / 5xx 时重试 2 次, 间隔 0.5s
    """

    AGENT_VERSION = '1.0'

    def __init__(self, node, timeout: int = 30):
        self.node = node
        self.timeout = timeout
        self.base_url = node.api_url.rstrip('/') + '/'
        self.token = node.api_token
        # 每个 NodeRPC 实例独占一个 Session (per-node 连接池)
        self._session: Optional['requests.Session'] = None
        self._session_lock = threading.Lock()

    def _get_session(self) -> 'requests.Session':
        """懒加载 Session, 含连接池 + 重试策略"""
        if self._session is None:
            with self._session_lock:
                if self._session is None:
                    s = requests.Session()
                    adapter = HTTPAdapter(
                        pool_connections=10,    # 跨主机连接缓存数
                        pool_maxsize=20,        # 单主机最大连接数
                        max_retries=Retry(
                            total=2, backoff_factor=0.5,
                            status_forcelist=[502, 503, 504],
                            allowed_methods=frozenset(['GET', 'POST', 'DELETE', 'PUT'])
                        ) if _HAS_RETRY else 2,
                    )
                    s.mount('http://', adapter)
                    s.mount('https://', adapter)
                    s.headers.update(self._headers())
                    self._session = s
        return self._session

    # ---------- 内部 ----------
    def _headers(self) -> Dict[str, str]:
        return {
            'Authorization': f'Bearer {self.token}',
            'User-Agent': f'McPanel-Master/{self.AGENT_VERSION}',
            'Accept': 'application/json',
        }

    def _request(self, method: str, path: str, json_body: Optional[dict] = None) -> dict:
        if not _HAS_REQUESTS:
            raise NodeRPCError('缺少 requests 依赖, 请先 `pip install requests` 以启用分布式节点功能')
        url = urljoin(self.base_url, path.lstrip('/'))
        # 使用 per-node Session 复用 TCP 连接 (keepalive), 大幅降低 RPC 延迟
        session = self._get_session()
        try:
            r = session.request(
                method, url,
                json=json_body,
                timeout=self.timeout,
            )
        except Exception as e:
            raise NodeRPCError(f'网络请求失败: {e}') from e
        if r.status_code == 401:
            raise NodeRPCError('节点 Token 无效 (401 Unauthorized)')
        if r.status_code == 403:
            raise NodeRPCError('节点拒绝访问 (403 Forbidden)')
        if 400 <= r.status_code < 600:
            raise NodeRPCError(f'节点返回错误 HTTP {r.status_code}: {r.text[:200]}')
        try:
            return r.json()
        except Exception as e:
            raise NodeRPCError(f'节点响应非 JSON: {e}') from e

    # ---------- 公开 API ----------
    def ping(self) -> dict:
        """心跳 + 获取节点资源情况。返回 {'ok':bool, 'host':{...}, 'servers':{...}}"""
        return self._request('GET', '/heartbeat')

    def server_start(self, server_id: int, payload: Optional[dict] = None) -> dict:
        """启动远端服务器; payload 可包含 server_type/version/memory/cores/port/jar_file
        Agent 会按这些配置自动下载 Java + 核心 jar"""
        return self._request('POST', f'/server/{server_id}/start', payload or {})

    def server_stop(self, server_id: int) -> dict:
        return self._request('POST', f'/server/{server_id}/stop')

    def server_restart(self, server_id: int) -> dict:
        return self._request('POST', f'/server/{server_id}/restart')

    def server_kill(self, server_id: int) -> dict:
        return self._request('POST', f'/server/{server_id}/kill')

    def server_stats(self, server_id: int) -> dict:
        return self._request('GET', f'/server/{server_id}/stats')

    def server_command(self, server_id: int, command: str) -> dict:
        return self._request('POST', f'/server/{server_id}/command', {'command': command})

    def server_console(self, server_id: int) -> dict:
        return self._request('GET', f'/server/{server_id}/console')

    def files_list(self, server_id: int, subpath: str = '') -> dict:
        return self._request('GET', f'/server/{server_id}/files', {'path': subpath or '/'})

    def files_delete(self, server_id: int, rel: str) -> dict:
        return self._request('DELETE', f'/server/{server_id}/files', {'path': rel})

    # ---------- 远端 Java 管理 ----------
    def java_status(self) -> dict:
        """获取远端节点 Java 安装状态"""
        return self._request('GET', '/java/status')

    def java_install(self, version: int = 17) -> dict:
        """触发远端节点下载 Java (同步阻塞直到完成)"""
        return self._request('POST', '/java/install', {'version': version})


class NodeManager:
    """全局节点管理器（单例风格）"""

    def __init__(self, db, node_model, app,
                 heartbeat_interval: int = 30,
                 heartbeat_timeout: int = 120):
        self.db = db
        self.NodeModel = node_model
        self.app = app
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_timeout = heartbeat_timeout
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        # RPC 缓存 (按 node.id)
        self._rpc_cache: Dict[int, NodeRPC] = {}
        self._lock = threading.Lock()

    # ---------- 生命周期 ----------
    def start_heartbeat(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True, name='node-heartbeat')
        self._thread.start()

    def stop(self):
        self._stop.set()

    # ---------- RPC 工厂 ----------
    def get_rpc(self, node) -> NodeRPC:
        with self._lock:
            rpc = self._rpc_cache.get(node.id)
            if rpc is None or rpc.token != node.api_token or rpc.base_url.rstrip('/') != node.api_url.rstrip('/'):
                rpc = NodeRPC(node)
                self._rpc_cache[node.id] = rpc
            return rpc

    # ---------- 心跳循环 ----------
    def _run(self):
        while not self._stop.is_set():
            try:
                self._tick_once()
            except Exception as e:
                print(f'[NodeManager] 心跳轮询异常: {e}')
            # 分片 sleep, 方便快速退出
            for _ in range(max(1, self.heartbeat_interval)):
                if self._stop.is_set():
                    break
                time.sleep(1)

    def _tick_once(self):
        with self.app.app_context():
            nodes = self.NodeModel.query.all()
        for n in nodes:
            try:
                rpc = self.get_rpc(n)
                info = rpc.ping()
                self._apply_heartbeat(n, info, error=None)
            except NodeRPCError as e:
                self._apply_heartbeat(n, None, error=str(e))
            except Exception as e:
                self._apply_heartbeat(n, None, error=f'未知错误: {e}')

    def _apply_heartbeat(self, node, info: Optional[dict], error: Optional[str]):
        """把心跳结果写回 DB, 并同步每台远端服务器的状态 (解决在线玩家不准问题)"""
        with self.app.app_context():
            obj = self.db.session.get(self.NodeModel, node.id)
            if obj is None:
                return
            obj.last_heartbeat_at = datetime.utcnow()
            online_players_total = 0
            if error:
                obj.status = 'error'
                obj.last_error = error[:500] if error else None
                obj.online_players = 0
            else:
                obj.status = 'online'
                obj.last_error = None
                host = (info or {}).get('host') or {}
                obj.host_cpu = float(host.get('cpu', 0.0))
                obj.host_cpu_count = int(host.get('cpu_count', 0) or 0)
                obj.host_memory_total = float(host.get('memory_total_gb', 0.0))
                obj.host_memory_used = float(host.get('memory_used_gb', 0.0))
                obj.host_disk_total = float(host.get('disk_total_gb', 0.0))
                obj.host_disk_used = float(host.get('disk_used_gb', 0.0))
                servers = (info or {}).get('servers') or {}
                obj.running_servers = int(servers.get('running', 0))
                obj.total_servers = int(servers.get('total', 0))
                # 心跳返回每台服务器的状态 -> 同步到 ServerInstance (修正远端玩家数)
                for s_info in (servers.get('list') or []):
                    try:
                        sid = int(s_info.get('id') or 0)
                        if not sid:
                            continue
                        # 只同步本节点上的服务器 (避免误更新)
                        from models import ServerInstance
                        sv = self.db.session.get(ServerInstance, sid)
                        if sv is None or sv.node_id != obj.id:
                            continue
                        # 玩家数 / 状态同步 (远端权威)
                        sv.player_count = int(s_info.get('players') or 0)
                        # 若远端报告运行中而本地 DB 不是, 同步状态
                        remote_status = (s_info.get('status') or '').lower()
                        if remote_status and sv.status != remote_status:
                            sv.status = remote_status
                        online_players_total += sv.player_count
                    except Exception:
                        continue
                obj.online_players = online_players_total
            try:
                self.db.session.commit()
            except Exception:
                self.db.session.rollback()

    # ---------- 调度: 智能推荐候选节点 ----------

    def pick_best_node(self, required_memory_mb: int = 0) -> Optional:
        """[兼容旧调用] 返回单个最佳节点对象; 找不到返回 None"""
        recs = self.recommend_nodes(required_memory_mb=required_memory_mb, limit=1)
        return recs[0]['node'] if recs else None

    def recommend_nodes(self,
                        required_memory_mb: int = 0,
                        required_cores: int = 0,
                        prefer_location: str = '',
                        limit: int = 5) -> List[Dict]:
        """
        智能推荐候选节点 (返回多个, 带评分理由).
        评分维度 (满分 100):
          - 资源适配 (35): 可用内存 / 核数是否充足, 越宽裕分越高
          - 负载低 (30): CPU/内存/磁盘占用越低分越高
          - 服务器稀疏度 (15): 该节点上服务器数越少分越高 (分散负载)
          - 地理位置匹配 (10): 用户偏好位置匹配则加分
          - 玩家拥挤度 (10): 当前在线玩家越少分越高
        不可用节点 (内存不足 / 已分配超额) 直接被过滤, 不进入候选.

        返回: [{'node': obj, 'score': float, 'reasons': [str,...], 'tags': [str,...]}, ...]
              按 score 降序, 至多 limit 个
        """
        out: List[Dict] = []
        # 关键: 在 app context 内完成所有 ORM 关系访问 (n.servers / n.assigned_memory_mb 等)
        # 否则出了 with 块后 node 变成 detached, 访问 .servers 会抛 DetachedInstanceError
        with self.app.app_context():
            nodes = self.NodeModel.query.filter_by(status='online').all()
            for n in nodes:
                reasons: List[str] = []
                tags: List[str] = []

                # ---- 配额检查: 节点 max_servers 已满 -> 直接过滤 ----
                if not n.can_accept_more_servers():
                    continue

                # ---- 资源适配检查 ----
                avail_mem_mb = n.available_memory_mb
                avail_cores = n.available_cores
                # 要求 1.5 倍内存余量, 防止启动后 OOM
                if required_memory_mb and avail_mem_mb < required_memory_mb * 1.5:
                    continue  # 内存不足, 不进入候选
                if required_cores and avail_cores < required_cores:
                    continue  # 核数不足

                # ---- 资源适配评分 (35) ----
                # 可用内存余量越大越好, 折算到 0-35
                mem_slack = avail_mem_mb - (required_memory_mb or 0)
                mem_score = min(35.0, mem_slack / 1024.0 * 3.5)  # 每剩余 1GB +3.5, 上限 35
                if required_memory_mb:
                    reasons.append(f'可用内存 {avail_mem_mb}MB 满足需求 {required_memory_mb}MB')
                else:
                    reasons.append(f'可用内存 {avail_mem_mb}MB')
                if avail_cores > 0:
                    tags.append(f'空闲 {avail_cores} 核')

                # ---- 负载评分 (30) ----
                cpu = n.host_cpu or 0
                mem_pct = n.memory_percent
                disk_pct = n.disk_percent
                # 100 - load_score 即"轻松度"
                lightness = max(0.0, 100.0 - (n.load_score or 0.0))
                load_score = lightness * 0.30  # 0-30
                reasons.append(f'负载 {n.load_score}/100 (CPU {cpu:.0f}% · 内存 {mem_pct:.0f}% · 磁盘 {disk_pct:.0f}%)')

                # ---- 服务器稀疏度评分 (15) ----
                assigned_cnt = n.assigned_server_count()
                # 节点服务器越少越稀疏; 0台=满分15, 5台以上=0
                sparse_score = max(0.0, 15.0 - assigned_cnt * 3.0)
                reasons.append(f'已部署 {assigned_cnt} 台服务器')

                # ---- 地理位置匹配 (10) ----
                loc_score = 0.0
                if prefer_location and n.location:
                    if prefer_location.lower() in n.location.lower():
                        loc_score = 10.0
                        tags.append('位置匹配')
                        reasons.append(f'位置匹配: {n.location}')
                    else:
                        loc_score = 0.0
                elif n.location:
                    loc_score = 3.0  # 有位置信息但未指定偏好, 给一点基础分

                # ---- 玩家拥挤度 (10) ----
                online = n.online_players or 0
                # 0 玩家 = 满分 10, 每 10 人扣 1
                crowd_score = max(0.0, 10.0 - online / 10.0)
                if online:
                    reasons.append(f'当前在线 {online} 玩家')

                total = round(mem_score + load_score + sparse_score + loc_score + crowd_score, 1)
                out.append({
                    'node': n,
                    'score': total,
                    'reasons': reasons,
                    'tags': tags,
                    'metrics': {
                        'avail_mem_mb': avail_mem_mb,
                        'avail_cores': avail_cores,
                        'cpu': cpu,
                        'mem_pct': mem_pct,
                        'disk_pct': disk_pct,
                        'load_score': n.load_score,
                        'assigned_servers': assigned_cnt,
                        'online_players': online,
                    },
                })

        # 评分降序, 取 limit 个, 第一个标"推荐"
        out.sort(key=lambda x: x['score'], reverse=True)
        out = out[:limit]
        if out:
            out[0]['tags'].insert(0, '⭐ 推荐')
        return out
