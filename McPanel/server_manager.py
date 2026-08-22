"""
Minecraft 服务器进程管理器
负责:
  - 服务器的启动 / 停止 / 重启
  - 进程监控
  - 控制台输出缓冲与广播 (SocketIO)
  - 服务器文件管理 (server.properties 写入)
  - 核心服务端下载 (模拟实现, 实际生产需调用 MC 版本 API)
"""
from __future__ import annotations

import os
import re
import ssl
import subprocess
import threading
import time
import shutil
import platform
import queue
import json
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict
from urllib.request import urlopen, Request

import psutil

# SSL 上下文: 默认严格验证, 环境变量 INSECURE_SSL=1 时放宽（仅离线/测试）
try:
    from config import INSECURE_SSL
except ImportError:
    import os as _os
    INSECURE_SSL = _os.environ.get('INSECURE_SSL', '').strip() in ('1', 'true', 'TRUE', 'yes')

_SSL_CTX = ssl.create_default_context()
if INSECURE_SSL:
    _SSL_CTX.check_hostname = False
    _SSL_CTX.verify_mode = ssl.CERT_NONE
else:
    # Windows 下 Python 默认 SSL 上下文可能拿不到系统根证书, 优先用 certifi CA 包
    try:
        import certifi
        _SSL_CTX.load_verify_locations(cafile=certifi.where())
    except ImportError:
        try:
            _SSL_CTX.load_default_certs()
        except Exception:
            pass


def _java_version_for_mc(mc_version: str) -> int:
    """根据 MC 版本推断所需 Java 大版本 (与 java_manager.java_for_mc 一致)"""
    try:
        parts = str(mc_version).split('.')
        major = int(parts[0])
        minor = int(parts[1]) if len(parts) > 1 else 0
        patch = int(parts[2]) if len(parts) > 2 else 0
    except (ValueError, IndexError):
        return 17
    if major > 1:
        return 21
    if major == 1:
        if minor > 20:
            return 21
        if minor == 20:
            return 21 if patch >= 5 else 17
        if minor >= 17:
            return 17
        return 8
    return 8


class _JavaPreparing(Exception):
    """启动时所需 Java 缺失, 已转入后台下载, 下载完成后自动启动"""
    pass


class ConsoleBuffer:
    """环形日志缓冲, 保存最近 N 行"""

    def __init__(self, max_lines: int = 500):
        self.max_lines = max_lines
        self._lines: List[Dict] = []
        self._lock = threading.Lock()

    def append(self, line: str, level: str = 'info'):
        entry = {
            'time': datetime.now().strftime('%H:%M:%S'),
            'level': level,
            'text': line.rstrip('\n')
        }
        with self._lock:
            self._lines.append(entry)
            if len(self._lines) > self.max_lines:
                self._lines = self._lines[-self.max_lines:]

    def all(self) -> List[Dict]:
        with self._lock:
            return list(self._lines)

    def clear(self):
        with self._lock:
            self._lines.clear()


class MinecraftServer:
    """单个 MC 服务器实例的控制器。

    注意: 不长期持有 SQLAlchemy ORM 实例引用, 避免 detached session 错误。
    通过 `_db` 属性在请求上下文中按需重新读取、每次修改后 commit。
    """

    # 在 app.py 中实例化时注入这些全局引用 (避免循环 import)
    _db_ref = None        # SQLAlchemy db 对象
    _model_ref = None     # ServerInstance model 类
    _app_ref = None       # Flask app (用于推送 app context)
    _java_manager_ref = None  # JavaManager 实例

    @classmethod
    def inject_deps(cls, db, ServerInstanceModel, flask_app, java_manager=None):
        cls._db_ref = db
        cls._model_ref = ServerInstanceModel
        cls._app_ref = flask_app
        cls._java_manager_ref = java_manager

    def __init__(self, server_instance, socketio, base_dir: str):
        self.id: int = server_instance.id
        self._name: str = server_instance.name
        self.socketio = socketio
        self.base_dir = Path(base_dir)
        self.server_dir: Path = self.base_dir / f'server_{self.id}'
        self.console = ConsoleBuffer(max_lines=800)
        self._process: Optional[subprocess.Popen] = None
        self._watchdog: Optional[threading.Thread] = None
        self._output_thread: Optional[threading.Thread] = None
        self._input_queue: 'queue.Queue[str]' = queue.Queue()
        self._input_thread: Optional[threading.Thread] = None
        self._status_lock = threading.Lock()
        # 本地缓存的可变状态 (供非请求上下文线程修改并与 DB 同步)
        self._status: str = server_instance.status
        self._pid: Optional[int] = server_instance.pid
        self._player_count: int = server_instance.player_count
        self._last_started: Optional[datetime] = server_instance.last_started

    # ---------- DB 辅助 ----------

    @property
    def config(self):
        """始终返回一个当前 session 绑定的 ServerInstance。"""
        if not self._db_ref or not self._model_ref:
            raise RuntimeError('MinecraftServer 依赖未注入, 请调用 MinecraftServer.inject_deps()')
        # 尝试从当前 session 中获取, 过期则重新查
        try:
            obj = self._db_ref.session.get(self._model_ref, self.id)
        except Exception:
            obj = None
        if obj is None:
            # 推一个 app context 并重新查询 (非请求线程使用)
            app = self._app_ref
            with app.app_context():
                obj = self._db_ref.session.get(self._model_ref, self.id)
        if obj is None:
            raise KeyError(f'Server #{self.id} 不存在于数据库')
        return obj

    def _save_to_db(self, **fields):
        """安全地把字段写入到 DB (自带 session 管理, 线程 & 上下文安全)。"""
        app = self._app_ref
        db = self._db_ref
        with app.app_context():
            obj = db.session.get(self._model_ref, self.id)
            if obj is None:
                return
            for k, v in fields.items():
                setattr(obj, k, v)
            try:
                db.session.commit()
            except Exception:
                db.session.rollback()
                raise

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, val):
        self._name = val

    # -------- 基础工具 --------

    def _emit_event(self, event: str, data=None):
        """向前端广播事件"""
        try:
            self.socketio.emit(event, {'server_id': self.id, 'data': data or {}},
                               room=f'server_{self.id}', namespace='/')
        except Exception as e:
            print(f'[emit-error] {e}')

    def _append_log(self, line: str, level: str = 'info'):
        self.console.append(line, level)
        self._emit_event('console_line', {
            'time': datetime.now().strftime('%H:%M:%S'),
            'level': level,
            'text': line.rstrip('\n')
        })

    # -------- 目录与安装 --------

    def ensure_directory(self):
        self.server_dir.mkdir(parents=True, exist_ok=True)
        # eula.txt 默认同意 (面板环境)
        eula = self.server_dir / 'eula.txt'
        if not eula.exists():
            eula.write_text('# Generated by MC Server Panel\neula=true\n', encoding='utf-8')

    def write_server_properties(self):
        """根据当前配置写入 server.properties"""
        self.ensure_directory()
        props = {
            'server-port': str(self.config.port),
            'server-ip': '0.0.0.0',
            'motd': self.config.motd,
            'max-players': str(self.config.max_players),
            'online-mode': str(self.config.online_mode).lower(),
            'pvp': str(self.config.pvp_enabled).lower(),
            'difficulty': self.config.difficulty,
            'level-name': self.config.level_name,
            'white-list': str(self.config.whitelist_enabled).lower(),
            'enable-query': 'true',
            'enable-rcon': 'false',
            'gamemode': 'survival',
            'view-distance': '10',
            'simulation-distance': '10',
            'allow-nether': 'true',
            'spawn-protection': '16',
            'max-world-size': '29999984',
            'resource-pack': '',
            'enable-command-block': 'true',
        }
        lines = [f'# Minecraft server properties - generated {datetime.now()}', '']
        for k, v in props.items():
            lines.append(f'{k}={v}')
        (self.server_dir / 'server.properties').write_text('\n'.join(lines) + '\n', encoding='utf-8')

    def ensure_jar(self) -> bool:
        """确保服务器 jar 存在, 不存在则自动下载真实核心"""
        jar_path = self.server_dir / self.config.jar_file
        if jar_path.exists() and jar_path.stat().st_size > 1000:
            return True
        # 下载真实核心
        try:
            self._download_core()
            return True
        except Exception as e:
            self._append_log(f'[错误] 核心下载失败: {e}', 'error')
            return False

    def _download_core(self):
        """下载真实的 Minecraft 服务器核心"""
        s_type = self.config.server_type
        version = self.config.version
        jar_name = self.config.jar_file
        jar_path = self.server_dir / jar_name

        self._append_log(f'[系统] 正在下载 {s_type} {version} 核心...', 'info')
        self._set_status('installing')

        if s_type == 'vanilla':
            url, size = self._get_vanilla_url(version)
        elif s_type == 'paper':
            url, size = self._get_paper_url(version)
        elif s_type == 'spigot':
            # Spigot 需要编译, 暂时回退到 vanilla
            self._append_log('[系统] Spigot 核心需要 BuildTools 编译, 暂时使用 Vanilla 核心', 'warn')
            url, size = self._get_vanilla_url(version)
        else:
            # 其他类型回退到 vanilla
            url, size = self._get_vanilla_url(version)

        self._append_log(f'[系统] 下载地址: {url}', 'debug')
        self._append_log(f'[系统] 文件大小: {round(size / 1048576, 2)} MB', 'debug')

        # 下载
        req = Request(url, headers={'User-Agent': 'McPanel/1.0'})
        with urlopen(req, timeout=120, context=_SSL_CTX) as resp:
            total = int(resp.headers.get('Content-Length', size))
            downloaded = 0
            chunk_size = 1024 * 64
            with open(jar_path, 'wb') as f:
                while True:
                    chunk = resp.read(chunk_size)
                    if not chunk:
                        break
                    f.write(chunk)
                    downloaded += len(chunk)
                    pct = (downloaded / total * 100) if total else 0
                    if downloaded % (1024 * 512) < chunk_size:  # 每 512KB 报告一次
                        self._emit_event('core_progress', {
                            'downloaded': downloaded, 'total': total,
                            'percent': round(pct, 1),
                            'msg': f'核心下载中... {downloaded // 1048576}MB / {total // 1048576}MB ({pct:.1f}%)',
                        })

        self._append_log(f'[系统] 核心下载完成: {jar_name} ({round(jar_path.stat().st_size / 1048576, 2)} MB)', 'success')

    @staticmethod
    def _get_vanilla_url(version: str):
        """通过 Mojang API 获取 Vanilla server.jar 下载链接"""
        # 1. 获取版本清单
        req = Request('https://piston-meta.mojang.com/mc/game/version_manifest_v2.json',
                      headers={'User-Agent': 'McPanel/1.0'})
        with urlopen(req, timeout=30, context=_SSL_CTX) as r:
            manifest = json.loads(r.read().decode('utf-8'))
        # 2. 找到目标版本
        ver_info = None
        for v in manifest.get('versions', []):
            if v['id'] == version:
                ver_info = v
                break
        if not ver_info:
            raise ValueError(f'Mojang 版本清单中找不到 {version}')
        # 3. 获取版本详情
        req2 = Request(ver_info['url'], headers={'User-Agent': 'McPanel/1.0'})
        with urlopen(req2, timeout=30, context=_SSL_CTX) as r2:
            ver_detail = json.loads(r2.read().decode('utf-8'))
        server = ver_detail.get('downloads', {}).get('server', {})
        if not server:
            raise ValueError(f'{version} 没有 server.jar 下载')
        return server['url'], server.get('size', 0)

    @staticmethod
    def _get_paper_url(version: str):
        """获取 Paper 核心下载链接"""
        # 尝试 PaperMC API (v2 可能已弃用, 直接用已知 build 号)
        # PaperMC 下载 URL 格式:
        # https://api.papermc.io/v2/projects/paper/versions/{ver}/builds/{build}/downloads/paper-{ver}-{build}.jar
        try:
            api_url = f'https://api.papermc.io/v2/projects/paper/versions/{version}'
            req = Request(api_url, headers={'User-Agent': 'McPanel/1.0', 'Accept': 'application/json'})
            with urlopen(req, timeout=30, context=_SSL_CTX) as r:
                data = json.loads(r.read().decode('utf-8'))
            builds = data.get('builds', [])
            if builds:
                latest_build = builds[-1]
                url = f'https://api.papermc.io/v2/projects/paper/versions/{version}/builds/{latest_build}/downloads/paper-{version}-{latest_build}.jar'
                return url, 0
        except Exception:
            pass
        # 回退: 使用 vanilla
        return MinecraftServer._get_vanilla_url(version)

    # -------- 进程控制 --------

    def _get_java_path(self) -> str:
        """获取 java 可执行路径: 优先项目自带 JRE, 其次系统 Java"""
        if self._java_manager_ref:
            p = self._java_manager_ref.get_java_path()
            if p:
                return p
        return 'java'

    def _required_java_version(self) -> int:
        """当前服务器 MC 版本所需的 Java 大版本"""
        try:
            return _java_version_for_mc(self.config.version)
        except Exception:
            return 17

    def _ensure_java(self) -> str:
        """确保所需版本 Java 就绪并返回其可执行路径

        已安装 -> 直接返回; 未安装 -> 转入后台下载, 完成后自动启动 (抛出 _JavaPreparing)
        """
        if not self._java_manager_ref:
            return 'java'
        ver = self._required_java_version()
        path = self._java_manager_ref.get_java_path(ver)
        if path:
            return path
        self._append_log(f'[系统] 本服务器需要 Java {ver}, 未检测到, 转入后台自动下载...', 'warn')
        self._set_status('installing')
        threading.Thread(target=self._download_java_then_start, args=(ver,), daemon=True,
                         name=f'java-auto-{self.id}').start()
        raise _JavaPreparing(f'正在后台自动下载所需 Java {ver}, 下载完成后将自动启动')

    def _download_java_then_start(self, ver: int):
        """后台下载指定版本 Java, 完成后自动启动服务器"""
        try:
            res = self._java_manager_ref.ensure_installed(
                ver, progress_cb=lambda d, t, msg: self._append_log(f'[Java] {msg}', 'info'))
            if not res.get('ok'):
                self._set_status('crashed')
                self._append_log(f'[错误] Java {ver} 自动下载失败: {res.get("msg")}', 'error')
                return
            self._append_log(f'[系统] Java {ver} 下载完成, 正在自动启动服务器...', 'success')
            self.start()
        except _JavaPreparing:
            pass  # 再次触发后台下载? 已在下载中, 忽略
        except Exception as e:
            self._set_status('crashed')
            self._append_log(f'[错误] Java 自动下载流程异常: {e}', 'error')

    def _build_cmd(self, java: str) -> List[str]:
        mem_mb = self.config.memory
        jar = self.server_dir / self.config.jar_file
        cmd = [
            java,
            f'-Xmx{mem_mb}M', f'-Xms{max(256, mem_mb // 2)}M',
            '-XX:+UseG1GC', '-XX:+ParallelRefProcEnabled',
            '-jar', str(jar), 'nogui'
        ]
        if self.config.java_args:
            extra = self.config.java_args.split()
            cmd[1:1] = extra
        return cmd

    @staticmethod
    def _python_bin() -> str:
        import sys
        return sys.executable

    def start(self) -> Dict:
        with self._status_lock:
            if self._process and self._process.poll() is None:
                return {'ok': False, 'msg': '服务器已经在运行中'}
            self._set_status('starting')
            # 启动前清零玩家数 (上一次会话残留)
            self._player_count = 0
            try:
                self._save_to_db(player_count=0)
            except Exception:
                pass
        try:
            self.ensure_directory()
            self.write_server_properties()
            # 下载核心 (如果没有)
            if not self.ensure_jar():
                return {'ok': False, 'msg': '核心下载失败, 请检查网络或手动上传 server.jar'}
            self._set_status('starting')
            self.console.clear()
            self._append_log(f'[系统] 正在启动服务器: {self.name}', 'info')
            self._append_log(f'[系统] 端口: {self.config.port}  内存: {self.config.memory}MB  核心: {self.config.cores}', 'info')
            self._append_log(f'[系统] 类型: {self.config.server_type}  版本: {self.config.version}  核心: {self.config.jar_file}', 'info')

            # 确保所需版本 Java 就绪 (缺失则转入后台下载, 完成后自动启动)
            try:
                java = self._ensure_java()
            except _JavaPreparing as e:
                return {'ok': False, 'msg': str(e), 'java_preparing': True}
            except Exception as e:
                self._set_status('crashed')
                self._append_log(f'[错误] Java 准备失败: {e}', 'error')
                return {'ok': False, 'msg': str(e)}

            cmd = self._build_cmd(java)
            self._append_log(f'[系统] 命令: {" ".join(cmd)}', 'debug')
            self._process = subprocess.Popen(
                cmd,
                cwd=str(self.server_dir),
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                creationflags=_get_creation_flags()
            )
            self._pid = self._process.pid
            self._last_started = datetime.utcnow()
            # 同步 PID / last_started 到 DB (后台异常容忍)
            try:
                self._save_to_db(pid=self._pid, last_started=self._last_started, status='starting')
            except Exception as e:
                print(f'[db-save] pid/last_started 失败: {e}')
            self._start_output_watcher()
            self._start_input_watcher()
            self._start_watchdog()
            return {'ok': True, 'msg': '服务器已启动', 'pid': self._process.pid}
        except Exception as e:
            self._set_status('crashed')
            self._append_log(f'[错误] 启动失败: {e}', 'error')
            return {'ok': False, 'msg': f'启动失败: {e}'}

    def stop(self, force: bool = False) -> Dict:
        with self._status_lock:
            if not self._process or self._process.poll() is not None:
                self._set_status('stopped')
                # 已停止状态也要确保玩家清零
                self._player_count = 0
                try:
                    self._save_to_db(player_count=0, status='stopped')
                except Exception:
                    pass
                return {'ok': True, 'msg': '服务器未运行'}
            self._set_status('stopping')
        try:
            if force:
                self._process.kill()
                self._append_log('[系统] 服务器被强制终止', 'warn')
            else:
                self._append_log('[系统] 正在发送 stop 指令...', 'info')
                self.send_command('stop')
                # 给 30s 优雅关闭
                try:
                    self._process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    self._append_log('[系统] 优雅关闭超时, 强制终止', 'warn')
                    self._process.kill()
            # 停止完成后立即清零玩家数
            self._player_count = 0
            try:
                self._save_to_db(player_count=0, status='stopped')
            except Exception:
                pass
            self._emit_event('stats', {'players': 0, 'reason': '服务器已停止'})
            return {'ok': True, 'msg': '服务器已关闭'}
        except Exception as e:
            return {'ok': False, 'msg': f'关闭失败: {e}'}

    def restart(self) -> Dict:
        res = self.stop()
        time.sleep(2)
        return self.start()

    def kill(self) -> Dict:
        return self.stop(force=True)

    # -------- 命令输入 / 输出监听 --------

    def send_command(self, cmd: str):
        if not self._process or self._process.poll() is not None:
            self._append_log('[错误] 服务器未运行, 无法执行命令', 'error')
            return False
        try:
            self._input_queue.put(cmd)
            return True
        except Exception as e:
            self._append_log(f'[错误] 命令发送失败: {e}', 'error')
            return False

    def _start_input_watcher(self):
        def worker():
            proc = self._process
            while proc and proc.poll() is None:
                try:
                    cmd = self._input_queue.get(timeout=0.5)
                except queue.Empty:
                    continue
                try:
                    proc.stdin.write(cmd + '\n')
                    proc.stdin.flush()
                    self._append_log(f'[> ] {cmd}', 'cmd')
                except Exception as e:
                    self._append_log(f'[错误] 写入 stdin 失败: {e}', 'error')
                    break
        self._input_thread = threading.Thread(target=worker, daemon=True)
        self._input_thread.start()

    def _start_output_watcher(self):
        def worker():
            proc = self._process
            try:
                for line in proc.stdout:  # type: ignore[union-attr]
                    if not line:
                        continue
                    # 识别日志级别
                    line_clean = line.rstrip()
                    level = 'info'
                    low = line_clean.lower()
                    if '/error' in low or 'exception' in low:
                        level = 'error'
                    elif '/warn' in low:
                        level = 'warn'
                    self._append_log(line_clean, level)
                    self._parse_line(line_clean)
            except Exception as e:
                self._append_log(f'[监听器异常] {e}', 'error')
        self._output_thread = threading.Thread(target=worker, daemon=True)
        self._output_thread.start()

    def _parse_line(self, line: str):
        """根据日志行更新状态 / 人数等

        玩家数刷新策略:
          1. 'joined the game' / 'logged in' -> +1
          2. 'left the game' / 'lost connection' -> -1
          3. 'There are X of a max of Y players' (list 命令响应) -> 校准为权威值
        启动/停止/崩溃时统一清零，避免显示陈旧数据。
        """
        if 'Done (' in line and ')! For help, type "help"' in line:
            self._set_status('running')
            self._emit_event('state_changed', {'status': 'running'})
            self._append_log('[系统] 服务器完全启动成功!', 'success')
            # 启动完成时清零玩家数 (避免上一次会话的残留)
            self._update_player_count(0, reason='启动完成')
            return

        # joined / left 增量更新
        if ' joined the game' in line or '] logged in with entity id' in line:
            self._update_player_count(self._player_count + 1, reason='玩家加入')
        if ' left the game' in line or 'lost connection' in line:
            self._update_player_count(max(0, self._player_count - 1), reason='玩家离开')

        # list 命令响应 -> 校准 (权威值)
        m = re.search(r'There are (\d+) of a max of (\d+) players', line)
        if m:
            self._update_player_count(int(m.group(1)), reason='list 校准', max_players=int(m.group(2)))

        if 'Stopping server' in line or 'Closing Server' in line:
            self._set_status('stopping')
            # 关服中清零玩家
            self._update_player_count(0, reason='关服中')
        if self._status != 'running':
            if 'Preparing level' in line or 'Preparing start region' in line:
                self._set_status('starting')

    def _update_player_count(self, new_count: int, reason: str = '', max_players: Optional[int] = None):
        """安全更新玩家计数: 同步内存 + DB + 前端事件"""
        if new_count < 0:
            new_count = 0
        if new_count == self._player_count and max_players is None:
            return  # 无变化不广播
        self._player_count = new_count
        try:
            fields = {'player_count': new_count}
            if max_players is not None:
                fields['max_players'] = max_players
            self._save_to_db(**fields)
        except Exception:
            pass
        self._emit_event('stats', {
            'players': new_count,
            'max_players': max_players if max_players is not None else self._get_db_max_players(),
            'reason': reason,
        })

    def _get_db_max_players(self) -> int:
        try:
            return self.config.max_players
        except Exception:
            return 20

    def _start_watchdog(self):
        def worker():
            while True:
                proc = self._process
                if not proc:
                    break
                ret = proc.poll()
                if ret is not None:
                    # 进程退出
                    with self._status_lock:
                        was_running = self._status in ('running', 'starting', 'stopping')
                    final_status = 'stopped'
                    if was_running:
                        if ret == 0 or self._status == 'stopping':
                            self._set_status('stopped')
                            self._append_log(f'[系统] 服务器已正常退出 (code={ret})', 'info')
                        else:
                            self._set_status('crashed')
                            final_status = 'crashed'
                            self._append_log(f'[错误] 服务器异常退出 (code={ret})', 'error')
                    else:
                        self._set_status('stopped')
                    self._pid = None
                    # 进程退出时强制清零玩家 (防止显示陈旧人数)
                    self._player_count = 0
                    try:
                        self._save_to_db(pid=None, player_count=0, status=final_status)
                    except Exception:
                        pass
                    self._emit_event('state_changed', {'status': self._status})
                    self._emit_event('stats', {'players': 0, 'reason': '进程退出'})
                    break
                time.sleep(1.0)
        self._watchdog = threading.Thread(target=worker, daemon=True)
        self._watchdog.start()

        # 后台定时 list 刷新线程: 每 45s 发送一次 list 命令校准玩家数
        def list_refresher():
            time.sleep(15)  # 等 15s 让服务器完全启动
            while True:
                try:
                    proc_ref = self._process
                    if not proc_ref or proc_ref.poll() is not None:
                        break
                    if self._status == 'running':
                        # 静默发送 list 命令 (不显示在控制台)
                        try:
                            proc_ref.stdin.write('list\n')  # type: ignore[union-attr]
                            proc_ref.stdin.flush()           # type: ignore[union-attr]
                        except Exception:
                            break
                except Exception:
                    break
                time.sleep(45)
        t_list = threading.Thread(target=list_refresher, daemon=True, name=f'list-refresh-{self.id}')
        t_list.start()

    def _set_status(self, status: str):
        self._status = status
        try:
            self._save_to_db(status=status)
        except Exception:
            pass
        self._emit_event('state_changed', {'status': status})

    # -------- 资源监控 --------

    def get_stats(self) -> Dict:
        proc = self._process
        cpu = 0.0
        rss_mb = 0
        proc_alive = proc is not None and proc.poll() is None
        if proc_alive:
            try:
                p = psutil.Process(proc.pid)
                with p.oneshot():
                    cpu = p.cpu_percent(interval=0.2)
                    rss_mb = p.memory_info().rss // (1024 * 1024)
            except Exception:
                pass
        # 通过 config 属性取 DB 中最新元信息 (不会 detached, 因为 config 内部已重取)
        try:
            cfg = self.config
            max_players = cfg.max_players
            memory_limit = cfg.memory
            port = cfg.port
            status_display = cfg.status_display
            status_color = cfg.status_color
            # 玩家数: 若进程不在运行, 强制读 0 (避免陈旧缓存); 否则读 DB 最新值
            if proc_alive:
                db_player_count = cfg.player_count or 0
                # 若内存值与 DB 不同步, 用 DB 权威值校准
                if db_player_count != self._player_count:
                    self._player_count = db_player_count
            else:
                self._player_count = 0
                db_player_count = 0
        except Exception:
            # 兜底: 使用本地缓存 + 默认值
            cfg = None
            max_players = 20
            memory_limit = getattr(self, '_mem_fallback', 2048)
            port = None
            db_player_count = self._player_count if proc_alive else 0
            _meta = {
                'running': ('运行中', '#22c55e'), 'stopped': ('已停止', '#64748b'),
                'starting': ('启动中', '#eab308'), 'stopping': ('关闭中', '#f97316'),
                'crashed': ('已崩溃', '#ef4444'), 'installing': ('安装中', '#3b82f6'),
            }
            label, color = _meta.get(self._status, (self._status, '#64748b'))
            status_display, status_color = label, color

        return {
            'status': self._status,
            'status_display': status_display,
            'status_color': status_color,
            'cpu': round(cpu, 1),
            'memory_mb': rss_mb,
            'memory_limit': memory_limit,
            'players': db_player_count,
            'max_players': max_players,
            'port': port,
            'pid': proc.pid if proc_alive else None,
            'uptime': self._uptime_str(),
        }

    def _uptime_str(self) -> str:
        if not self._last_started or self._status not in ('running', 'stopping'):
            return '0s'
        delta = datetime.utcnow() - self._last_started
        total = int(delta.total_seconds())
        h, rem = divmod(total, 3600)
        m, s = divmod(rem, 60)
        if h:
            return f'{h}h {m}m {s}s'
        if m:
            return f'{m}m {s}s'
        return f'{s}s'

    # -------- 文件操作 --------

    def list_files(self, sub: str = '') -> List[Dict]:
        # 拒绝包含路径穿越的子路径
        if '\x00' in (sub or '') or '..' in (sub or '').replace('\\', '/').split('/'):
            return []
        base = self.server_dir / (sub or '')
        if not base.exists():
            return []
        # 解析后必须仍在 server_dir 下 (防 symlink / 前缀逃逸)
        try:
            base.resolve().relative_to(self.server_dir.resolve())
        except ValueError:
            return []
        files = []
        for p in base.iterdir():
            try:
                st = p.stat()
                files.append({
                    'name': p.name,
                    'is_dir': p.is_dir(),
                    'size': st.st_size if p.is_file() else 0,
                    'modified': datetime.fromtimestamp(st.st_mtime).strftime('%Y-%m-%d %H:%M'),
                    'path': str(p.relative_to(self.server_dir)).replace('\\', '/')
                })
            except OSError:
                continue
        return sorted(files, key=lambda x: (not x['is_dir'], x['name']))

    def delete_path(self, rel: str) -> Dict:
        # 规范化路径并检查路径遍历 / 符号链接
        if not rel:
            return {'ok': False, 'msg': '路径为空'}
        # 拒绝包含风险字符
        if '\x00' in rel or ('..' in rel.replace('\\', '/').split('/')):
            return {'ok': False, 'msg': '路径非法 (包含危险字符)'}
        base_resolved = self.server_dir.resolve()
        target = (base_resolved / rel).resolve()
        # 双重校验: 解析后必须仍在 base 下 (防 symlink 逃逸)
        try:
            target.relative_to(base_resolved)
        except ValueError:
            return {'ok': False, 'msg': '路径非法 (越出服务器目录)'}
        if not target.exists():
            return {'ok': False, 'msg': '文件不存在'}
        # 禁止删除服务器根目录本身
        if target == base_resolved:
            return {'ok': False, 'msg': '不能删除服务器根目录'}
        try:
            if target.is_dir() and not target.is_symlink():
                shutil.rmtree(target)
            else:
                target.unlink()
            return {'ok': True}
        except Exception as e:
            return {'ok': False, 'msg': str(e)}


def _get_creation_flags() -> int:
    if platform.system() == 'Windows':
        # 不创建控制台窗口
        return 0x08000000  # CREATE_NO_WINDOW
    return 0


# ============================================================
# Mock 服务器脚本: 让没有真实 server.jar 时面板也能演示
# ============================================================
MOCK_SERVER_PY = r'''#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Minecraft 服务器模拟脚本 - 用于面板功能演示
会读取 stdin 命令, 向 stdout 打印仿真日志, 并响应 list/stop/op 等常用命令。
"""
import argparse
import random
import sys
import time
import threading

PORT = 25565
NAME = 'MockServer'

# 随机玩家名池
NAME_POOL = ['Steve', 'Alex', 'Notch', 'Herobrine', 'Dream', 'xM_Jx', 'Sasha', 'Lily',
             'Kaito', 'Creeper_lover', 'Diamond_hunter', 'Redstone_king']

_players = set()
_running = True


def log(msg: str):
    ts = time.strftime('%H:%M:%S')
    print(f'[{ts}] [Server thread/INFO]: {msg}', flush=True)


def stdin_reader():
    global _running
    while True:
        try:
            line = sys.stdin.readline()
        except EOFError:
            break
        if not line:
            break
        line = line.strip()
        if not line:
            continue
        handle_cmd(line)
        if not _running:
            break


def handle_cmd(line: str):
    global _running
    low = line.lower()
    log(f'issued server command: {line}')
    if low == 'stop':
        log('Stopping the server')
        log('Saving players')
        log('Saving worlds')
        log('Closing Server')
        _running = False
    elif low == 'list':
        n = len(_players)
        names = ', '.join(sorted(_players))
        log(f'There are {n} of a max of 20 players online: {names}')
    elif low.startswith('say '):
        log(f'[Server] {line[4:]}')
    elif low.startswith('op '):
        who = line.split()[1]
        log(f'Made {who} a server operator')
    elif low.startswith('deop '):
        who = line.split()[1]
        log(f'Made {who} no longer a server operator')
    elif low.startswith('kick '):
        who = line.split()[1]
        if who in _players:
            _players.remove(who)
            log(f'{who} left the game')
    elif low.startswith('whitelist '):
        log(f'Whitelist command accepted (mock)')
    elif low in ('help', '?'):
        log('--- Mock Help ---')
        log('/help /stop /list /say <msg> /op <p> /deop <p> /kick <p>')
    else:
        log(f'Unknown or unsupported command "{line}" in mock mode')


def random_player_events():
    while _running:
        time.sleep(random.randint(8, 25))
        if not _running:
            break
        action = random.random()
        if action < 0.5 and len(_players) < 6:
            candidate = random.choice(NAME_POOL)
            if candidate not in _players:
                _players.add(candidate)
                log(f'{candidate}[/127.0.0.1:{random.randint(50000, 60000)}] logged in with entity id '
                    f'{random.randint(1, 999)} at (0.5, 64, 0.5)')
                log(f'{candidate} joined the game')
        elif len(_players) > 0:
            victim = random.choice(list(_players))
            _players.remove(victim)
            log(f'{victim} lost connection: Disconnected')
            log(f'{victim} left the game')


def boot_sequence():
    global PORT, NAME
    ap = argparse.ArgumentParser()
    ap.add_argument('--port', type=int, default=25565)
    ap.add_argument('--name', type=str, default='MockServer')
    args, _ = ap.parse_known_args()
    PORT = args.port
    NAME = args.name

    log(f'Starting minecraft server version 1.20.4 (Mock mode)')
    log(f'Loading properties')
    log(f'Default game type: SURVIVAL')
    time.sleep(0.5)
    log('Generating keypair')
    log(f'Starting Minecraft server on 0.0.0.0:{PORT}')
    time.sleep(0.8)
    log('Preparing level "world"')
    log('Preparing start region for dimension minecraft:overworld')
    time.sleep(1.2)
    log('Preparing spawn area: 25%')
    time.sleep(0.4)
    log('Preparing spawn area: 60%')
    time.sleep(0.4)
    log('Preparing spawn area: 100%')
    time.sleep(0.3)
    log(f'Done ({random.uniform(2, 6):.2f}s)! For help, type "help"')
    log(f'There are 0 of a max of 20 players online: ')


def main():
    boot_sequence()
    t_in = threading.Thread(target=stdin_reader, daemon=True)
    t_in.start()
    t_rp = threading.Thread(target=random_player_events, daemon=True)
    t_rp.start()
    while _running:
        time.sleep(1)
    time.sleep(0.5)


if __name__ == '__main__':
    main()
'''
