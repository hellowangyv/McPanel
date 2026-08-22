"""
McPanel 分布式节点 Agent (分支服务器上独立运行的脚本)
-----------------------------------------------------------------
用途:  部署在其他服务器上, 接受主面板 (Master) 的 HTTP 指令来实际启停 MC 服务器
启动:
    # 最简单: 用默认 Token (从环境变量读取, 否则生成并打印)
    python node_agent.py

    # 指定参数
    python node_agent.py --host 0.0.0.0 --port 58765 --token 你的共享密钥 --data-dir ./node_data

依赖: Flask, psutil (同主面板 requirements; 可选 requests 用于反向上报)
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import re
import secrets
import ssl
import subprocess
import sys
import tarfile
import threading
import time
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Callable, Tuple
from urllib.request import urlopen, Request
from urllib.error import URLError

import psutil
from flask import Flask, request, jsonify, abort


# ============================================================
# 注意: Agent 设计为轻量进程，不依赖主面板的 models / server_manager
#       以下是最小化复刻，便于独立分发部署
# ============================================================

# SSL 上下文: 默认严格验证, INSECURE_SSL=1 时放宽（仅离线/测试）
# Windows 下 Python 默认 SSL 上下文可能拿不到系统根证书, 优先用 certifi 提供的 CA 包
_INSECURE_SSL = os.environ.get('INSECURE_SSL', '').strip() in ('1', 'true', 'TRUE', 'yes')
_SSL_CTX = ssl.create_default_context()
if _INSECURE_SSL:
    _SSL_CTX.check_hostname = False
    _SSL_CTX.verify_mode = ssl.CERT_NONE
else:
    try:
        import certifi
        _SSL_CTX.load_verify_locations(cafile=certifi.where())
    except ImportError:
        # certifi 未安装: 尝试加载系统证书 (Windows 下可能仍失败)
        try:
            _SSL_CTX.load_default_certs()
        except Exception:
            pass


def _detect_java_major(java_exe) -> Optional[int]:
    """执行 java -version 探测大版本号 (目录名无法推断时使用)"""
    try:
        r = subprocess.run(
            [str(java_exe), '-version'],
            capture_output=True, text=True, timeout=10,
            creationflags=0x08000000 if platform.system() == 'Windows' else 0,
        )
        out = (r.stderr or '') + (r.stdout or '')
        m = re.search(r'version "(\d+)(?:\.(\d+))?', out)
        if m:
            major = int(m.group(1))
            if major == 1 and m.group(2):
                major = int(m.group(2))
            return major
    except Exception:
        pass
    return None


# ============================================================
# 心跳采集保护: psutil 在某些环境 (慢盘/异常系统) 可能长时间阻塞,
# 用带超时的工作线程采集, 超时返回上次成功值, 保证 /heartbeat 永远快速响应
# ============================================================
_host_stats_cache = {'data': {}, 'lock': threading.Lock()}


def _safe_host_stats(data_dir: Path, timeout: float = 6.0) -> dict:
    def collect() -> dict:
        mem = psutil.virtual_memory()
        disk = psutil.disk_usage(str(data_dir))
        return {
            'cpu': psutil.cpu_percent(interval=0.2),
            'cpu_count': psutil.cpu_count(logical=True),
            'memory_total_gb': round(mem.total / (1024 ** 3), 2),
            'memory_used_gb': round(mem.used / (1024 ** 3), 2),
            'memory_percent': mem.percent,
            'disk_total_gb': round(disk.total / (1024 ** 3), 2),
            'disk_used_gb': round(disk.used / (1024 ** 3), 2),
            'disk_percent': disk.percent,
        }
    from concurrent.futures import ThreadPoolExecutor
    try:
        with ThreadPoolExecutor(max_workers=1) as ex:
            data = ex.submit(collect).result(timeout=timeout)
        with _host_stats_cache['lock']:
            _host_stats_cache['data'] = data
        return data
    except Exception:
        with _host_stats_cache['lock']:
            return dict(_host_stats_cache['data'])


def _safe_server_stats_list(servers: dict, servers_lock: threading.Lock, timeout: float = 8.0) -> list:
    """采集所有远端服务器的状态; 单个服务器 psutil 卡住时整体降级为空列表"""
    def collect() -> list:
        out = []
        with servers_lock:
            items = list(servers.items())
        for sid, s in items:
            try:
                out.append({'id': sid, **s.get_stats()})
            except Exception:
                continue
        return out
    from concurrent.futures import ThreadPoolExecutor
    try:
        with ThreadPoolExecutor(max_workers=1) as ex:
            return ex.submit(collect).result(timeout=timeout)
    except Exception:
        return []


class _JavaManagerLite:
    """Agent 端 Java 自动管理器 (移植自主面板 java_manager, 独立无依赖版本)

    职责:
      - 检测系统已安装的 Java
      - 自动下载 BellSoft Liberica JRE 到 Agent runtime 目录 (多版本共存)
      - 提供 java 可执行路径给 _MinecraftServerLite
    """

    BELLSOFT_BASE = 'https://download.bell-sw.com/java'
    BELLSOFT_API = 'https://api.bell-sw.com/v1/liberica/releases'
    DEFAULT_VERSION = 17
    # 兜底用的已知版本号 (在线 API 查询失败时使用; BellSoft 发布新版后可能下架)
    VERSIONS = {
        17: '17.0.20+10',
        21: '21.0.12+10',
        11: '11.0.32+11',
        8:  '8u504+1',
    }
    # 最新版本缓存: {feature_version: (full_version_str, fetched_at_ts)}
    _latest_cache: Dict[int, tuple] = {}
    _latest_cache_ttl = 3600  # 1 小时

    _instance: Optional['_JavaManagerLite'] = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
            return cls._instance

    def __init__(self, runtime_dir: Path, log_fn: Optional[Callable[[str, str], None]] = None):
        if getattr(self, '_initialized', False):
            if log_fn and not getattr(self, '_log_fn', None):
                self._log_fn = log_fn
            return
        self.runtime_dir = Path(runtime_dir)
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        self._log_fn = log_fn or (lambda msg, level: print(f'[JavaAgent] {msg}'))
        self._install_lock = threading.Lock()
        self._installing = False
        self._initialized = True

    def _log(self, msg: str, level: str = 'info'):
        try:
            self._log_fn(msg, level)
        except Exception:
            pass

    @staticmethod
    def detect_system_java() -> Optional[Dict]:
        """检测系统已安装的 Java, 返回 {'path':..., 'version':...} 或 None"""
        java_cmd = 'java'
        if platform.system() == 'Windows':
            for p in os.environ.get('JAVA_HOME', '').split(os.pathsep):
                if p:
                    exe = Path(p) / 'bin' / 'java.exe'
                    if exe.exists():
                        java_cmd = str(exe)
                        break
        try:
            r = subprocess.run(
                [java_cmd, '-version'],
                capture_output=True, text=True, timeout=10,
                creationflags=0x08000000 if platform.system() == 'Windows' else 0,
            )
            out = (r.stderr or '') + (r.stdout or '')
            if r.returncode != 0 and not out:
                return None
            m = re.search(r'version "(\d+)(?:\.(\d+))?', out)
            if m:
                major = int(m.group(1))
                if major == 1 and m.group(2):
                    major = int(m.group(2))
                return {'path': java_cmd, 'version': major,
                        'raw': out.strip().splitlines()[0] if out.strip() else ''}
        except Exception:
            pass
        return None

    def _scan_bundled(self) -> Dict[int, Path]:
        """扫描 runtime 目录下所有已安装的 JRE, 返回 {版本号: java 可执行路径}"""
        out: Dict[int, Path] = {}
        if not self.runtime_dir.exists():
            return out
        for d in self.runtime_dir.iterdir():
            if not d.is_dir():
                continue
            bin_dir = d / 'bin'
            if not bin_dir.exists():
                sub = list(d.iterdir())
                if len(sub) == 1 and sub[0].is_dir():
                    bin_dir = sub[0] / 'bin'
            java_name = 'java.exe' if platform.system() == 'Windows' else 'java'
            java_exe = bin_dir / java_name if bin_dir.exists() else None
            if not (java_exe and java_exe.exists()):
                continue
            m = re.search(r'[a-z]+[-_]?(\d+)', d.name.lower())
            v = int(m.group(1)) if m else None
            if v is None:
                v = _detect_java_major(java_exe)
            if v:
                out.setdefault(v, java_exe)
        return out

    def get_bundled_java(self, version: Optional[int] = None) -> Optional[Path]:
        """获取 Agent runtime 目录下的 java 可执行文件路径 (可指定大版本)"""
        scan = self._scan_bundled()
        if not scan:
            return None
        if version is not None:
            return scan.get(version)
        return scan[max(scan)]

    def installed_versions(self) -> list:
        return sorted(self._scan_bundled().keys())

    def is_installed(self, version: int) -> bool:
        return self.get_bundled_java(version) is not None

    def get_java_path(self, version: Optional[int] = None) -> Optional[str]:
        """优先使用 Agent 自带 JRE (可指定版本), 其次系统 Java"""
        bundled = self.get_bundled_java(version)
        if bundled:
            return str(bundled)
        sys_java = self.detect_system_java()
        if sys_java:
            return sys_java['path']
        return None

    def get_status(self) -> Dict:
        bundled = self.get_bundled_java()
        sys_java = self.detect_system_java()
        java_path = self.get_java_path()
        version = None
        version_raw = ''
        if java_path:
            try:
                r = subprocess.run(
                    [java_path, '-version'],
                    capture_output=True, text=True, timeout=10,
                    creationflags=0x08000000 if platform.system() == 'Windows' else 0,
                )
                out = (r.stderr or '') + (r.stdout or '')
                m = re.search(r'version "(\d+)(?:\.(\d+))?', out)
                if m:
                    version = int(m.group(1))
                    if version == 1 and m.group(2):
                        version = int(m.group(2))
                if out.strip():
                    version_raw = out.strip().splitlines()[0]
            except Exception:
                pass
        return {
            'installed': java_path is not None,
            'java_path': java_path or '',
            'version': version,
            'version_raw': version_raw,
            'source': 'bundled' if bundled else ('system' if sys_java else 'none'),
            'installed_versions': self.installed_versions(),
            'installing': self._installing,
        }

    def _get_download_info(self, java_version: int) -> Optional[Dict]:
        sys_os = platform.system().lower()
        arch = platform.machine().lower()
        os_map = {'windows': 'windows', 'linux': 'linux', 'darwin': 'macos'}
        arch_map = {'x86_64': 'amd64', 'amd64': 'amd64',
                    'arm64': 'aarch64', 'aarch64': 'aarch64',
                    'x86': 'i386', 'i386': 'i386'}
        bell_os = os_map.get(sys_os, 'linux')
        bell_arch = arch_map.get(arch, 'amd64')
        # 优先查询 BellSoft API 获取最新版本, 失败则回退到硬编码 VERSIONS
        full_version = self._fetch_latest_version(java_version, bell_os, bell_arch) \
            or self.VERSIONS.get(java_version)
        if not full_version:
            self._log(f'不支持的 Java 版本: {java_version}', 'error')
            return None
        ext = 'tar.gz' if bell_os == 'linux' else 'zip'
        filename = f'bellsoft-jre{full_version}-{bell_os}-{bell_arch}.{ext}'
        url = f'{self.BELLSOFT_BASE}/{full_version}/{filename}'
        return {'url': url, 'name': filename, 'version': java_version, 'ext': ext}

    def _fetch_latest_version(self, feature_version: int,
                              bell_os: str, bell_arch: str) -> Optional[str]:
        """查询 BellSoft API 获取指定主版本的最新 JRE 完整版本号

        - 命中缓存且未过期 -> 直接返回
        - API 不可达 -> 返回 None (调用方回退到 VERSIONS)
        - 缓存 TTL 1 小时, 避免 API 请求过频
        """
        cache_key = feature_version
        now = time.time()
        cached = self._latest_cache.get(cache_key)
        if cached and (now - cached[1]) < self._latest_cache_ttl:
            return cached[0]
        # ext / package_type 映射
        package_type = 'tar.gz' if bell_os == 'linux' else 'zip'
        # bitness 推断: i386 -> 32, 其他 (amd64/aarch64) -> 64
        bitness = 32 if bell_arch == 'i386' else 64
        api_url = (f'{self.BELLSOFT_API}'
                   f'?bundle-type=jre&os={bell_os}&bitness={bitness}'
                   f'&package-type={package_type}')
        try:
            req = Request(api_url, headers={
                'User-Agent': 'McPanel-NodeAgent/1.0',
                'Accept': 'application/json'})
            with urlopen(req, timeout=15, context=_SSL_CTX) as r:
                data = json.loads(r.read().decode('utf-8'))
            if not isinstance(data, list):
                return None
            # 过滤: featureVersion 匹配 + filename 含目标 arch
            cands = [d for d in data
                     if d.get('featureVersion') == feature_version
                     and f'-{bell_arch}.' in (d.get('filename') or '')]
            if not cands:
                return None
            # 取最新 (按 updateVersion + buildVersion 排序)
            latest = max(cands, key=lambda d: (
                d.get('updateVersion', 0),
                d.get('buildVersion', 0),
                d.get('patchVersion', 0)))
            full = latest.get('version')
            if full:
                self._latest_cache[cache_key] = (full, now)
                self._log(f'[Agent] BellSoft API: Java {feature_version} 最新版 {full}', 'info')
                return full
        except Exception as e:
            self._log(f'[Agent] BellSoft API 查询失败, 回退到内置版本: {e}', 'warn')
        return None

    def install(self, java_version: int = None,
                progress_cb: Optional[Callable[[int, int, str], None]] = None) -> Dict:
        """下载并安装 JRE 到 runtime 目录"""
        java_version = java_version or self.DEFAULT_VERSION
        with self._install_lock:
            if self._installing:
                return {'ok': False, 'msg': '已有安装任务在进行中'}
            self._installing = True
        try:
            self._log(f'开始下载 Java {java_version} JRE...', 'info')
            # 目标目录: 按版本隔离, 多版本共存
            target_dir = self.runtime_dir / f'jre-{java_version}'
            if target_dir.exists():
                import shutil
                shutil.rmtree(target_dir, ignore_errors=True)
            target_dir.mkdir(parents=True, exist_ok=True)
            info = self._get_download_info(java_version)
            if not info:
                return {'ok': False, 'msg': '无法获取下载链接'}
            url = info['url']
            self._log(f'下载地址: {url}', 'info')
            tmp_file = self.runtime_dir / info['name']
            try:
                req = Request(url, headers={'User-Agent': 'McPanel-NodeAgent/1.0'})
                with urlopen(req, timeout=120, context=_SSL_CTX) as resp:
                    total = int(resp.headers.get('Content-Length', 0))
                    downloaded = 0
                    chunk_size = 1024 * 64
                    with open(tmp_file, 'wb') as f:
                        while True:
                            chunk = resp.read(chunk_size)
                            if not chunk:
                                break
                            f.write(chunk)
                            downloaded += len(chunk)
                            pct = (downloaded / total * 100) if total else 0
                            msg = f'Java 下载中... {downloaded//(1024*1024)}MB / {total//(1024*1024) if total else "?"}MB ({pct:.1f}%)'
                            if progress_cb:
                                progress_cb(downloaded, total, msg)
                self._log(f'下载完成: {tmp_file}', 'success')
            except Exception as e:
                return {'ok': False, 'msg': f'下载失败: {e}'}

            self._log('正在解压...', 'info')
            ext = info.get('ext', 'zip')
            try:
                if ext == 'tar.gz' or tmp_file.name.endswith('.tar.gz'):
                    with tarfile.open(tmp_file, 'r:gz') as tf:
                        tf.extractall(target_dir)
                else:
                    with zipfile.ZipFile(tmp_file, 'r') as zf:
                        zf.extractall(target_dir)
            except Exception as e:
                return {'ok': False, 'msg': f'解压失败: {e}'}
            finally:
                try:
                    tmp_file.unlink()
                except Exception:
                    pass

            java_exe = self.get_bundled_java(java_version)
            if not java_exe:
                return {'ok': False, 'msg': '解压后未找到 java 可执行文件'}
            self._log(f'Java 安装完成: {java_exe}', 'success')
            return {'ok': True, 'msg': 'Java 安装成功', 'java_path': str(java_exe)}
        finally:
            self._installing = False

    def ensure_installed(self, java_version: int,
                         progress_cb: Optional[Callable[[int, int, str], None]] = None,
                         wait_timeout: int = 1800) -> Dict:
        """确保指定版本 JRE 已安装; 未安装自动下载

        与 install() 的区别:
          - 已安装 -> 直接返回 (不重复下载)
          - 有其他安装任务进行中 -> 等待其完成 (预热线程 / 其他服务器启动)
          - 竞争失败 -> 自动重试, 而非直接返回"已有安装任务"
        """
        if self.is_installed(java_version):
            return {'ok': True, 'msg': f'Java {java_version} 已安装',
                    'java_path': str(self.get_bundled_java(java_version))}
        # 等待其他进行中的安装任务结束
        waited = 0
        while self._installing and waited < wait_timeout:
            time.sleep(2)
            waited += 2
            if self.is_installed(java_version):
                return {'ok': True, 'msg': f'Java {java_version} 已安装 (等待其他任务完成)',
                        'java_path': str(self.get_bundled_java(java_version))}
        if self._installing:
            return {'ok': False, 'msg': f'等待其他 Java 安装任务超时 (目标 {java_version})'}
        # 尝试抢占安装, 竞争失败则等待后重试
        for _ in range(10):
            if not self._installing:
                r = self.install(java_version, progress_cb=progress_cb)
                if r.get('ok') or '进行中' not in r.get('msg', ''):
                    return r
            time.sleep(2)
            if self.is_installed(java_version):
                return {'ok': True, 'msg': f'Java {java_version} 已安装 (其他任务完成)',
                        'java_path': str(self.get_bundled_java(java_version))}
        return {'ok': False, 'msg': 'Java 安装任务持续冲突, 请稍后重试'}


class _ConsoleBuffer:
    def __init__(self, max_lines: int = 500):
        self.max_lines = max_lines
        self._lines = []
        self._lock = threading.Lock()

    def append(self, line: str, level: str = 'info'):
        entry = {
            'time': datetime.now().strftime('%H:%M:%S'),
            'level': level,
            'text': line.rstrip('\n'),
        }
        with self._lock:
            self._lines.append(entry)
            if len(self._lines) > self.max_lines:
                self._lines = self._lines[-self.max_lines:]

    def all(self):
        with self._lock:
            return list(self._lines)

    def clear(self):
        with self._lock:
            self._lines.clear()


class _MinecraftServerLite:
    """最小化服务器控制器 (与主面板 server_manager 逻辑一致但去除 ORM 依赖)

    支持自动下载 Java (BellSoft JRE) + 自动下载 Minecraft 服务器核心

    性能优化:
    - URL 解析缓存 (TTL 6h): 跳过 Mojang/PaperMC API 调用
    - 跨服务器共享核心缓存: 命中后用硬链接秒级部署 (<100ms)
    - Agent 启动时后台预热常见 Java 版本 + 核心版本
    """

    # 类级别共享 (JavaManager 单例由 create_app 注入)
    _java_manager_ref: Optional[_JavaManagerLite] = None
    # 跨服务器共享的核心缓存目录 (由 create_app 设置)
    _core_cache_dir: Optional[Path] = None
    # URL 解析缓存: {(server_type, version): (url, size, fetched_at_ts)}
    _url_cache: Dict[tuple, tuple] = {}
    _URL_CACHE_TTL = 6 * 3600  # 6 小时
    _url_cache_lock = threading.Lock()

    @classmethod
    def set_java_manager(cls, jm: '_JavaManagerLite'):
        cls._java_manager_ref = jm

    @classmethod
    def set_core_cache_dir(cls, path: Path):
        """设置共享核心缓存目录 (跨服务器复用已下载的 server.jar)"""
        cls._core_cache_dir = Path(path)
        cls._core_cache_dir.mkdir(parents=True, exist_ok=True)

    @classmethod
    def _cache_key(cls, server_type: str, version: str, jar_file: str) -> str:
        """生成缓存文件名: paper_1.20.4_server.jar"""
        safe_type = re.sub(r'[^a-z0-9]+', '_', (server_type or 'vanilla').lower())
        safe_ver = re.sub(r'[^a-z0-9.+]+', '_', (version or '1.20.4').lower())
        return f'{safe_type}_{safe_ver}_{jar_file}'

    @classmethod
    def _get_cached_core_path(cls, server_type: str, version: str, jar_file: str) -> Optional[Path]:
        """查询缓存中是否存在该核心, 返回路径或 None"""
        if not cls._core_cache_dir:
            return None
        p = cls._core_cache_dir / cls._cache_key(server_type, version, jar_file)
        if p.exists() and p.stat().st_size > 1000:
            return p
        return None

    def __init__(self, server_id: int, base_dir: Path,
                 memory_mb: int = 2048, cores: int = 1,
                 port: int = 25565, jar_file: str = 'server.jar',
                 server_type: str = 'paper', version: str = '1.20.4',
                 motd: str = 'A Minecraft Server', max_players: int = 20,
                 difficulty: str = 'normal', level_name: str = 'world',
                 online_mode: bool = True, pvp_enabled: bool = True,
                 whitelist_enabled: bool = False, java_args: str = ''):
        self.server_id = server_id
        self.base_dir = Path(base_dir)
        self.server_dir = self.base_dir / f'server_{server_id}'
        self.server_dir.mkdir(parents=True, exist_ok=True)
        self.memory_mb = memory_mb
        self.cores = cores
        self.port = port
        self.jar_file = jar_file
        self.server_type = server_type
        self.version = version
        self.motd = motd
        self.max_players = max_players
        self.difficulty = difficulty
        self.level_name = level_name
        self.online_mode = online_mode
        self.pvp_enabled = pvp_enabled
        self.whitelist_enabled = whitelist_enabled
        self.java_args = java_args or ''
        self.console = _ConsoleBuffer(max_lines=500)
        self._process: Optional[object] = None
        self._status: str = 'stopped'
        self._player_count: int = 0
        self._last_started: Optional[datetime] = None
        self._lock = threading.Lock()
        # 保证 eula=true
        eula = self.server_dir / 'eula.txt'
        if not eula.exists():
            eula.write_text('# Generated by McPanel Node Agent\neula=true\n', encoding='utf-8')

    # ---------- server.properties ----------
    def write_server_properties(self):
        """按当前配置写入 server.properties (端口/MOTD/玩家上限等)

        缺失会导致 MC 使用默认配置: 端口固定 25565, 与其他服务器撞车直接崩溃
        """
        props = {
            'server-port': str(self.port),
            'server-ip': '0.0.0.0',
            'motd': self.motd,
            'max-players': str(self.max_players),
            'online-mode': str(self.online_mode).lower(),
            'pvp': str(self.pvp_enabled).lower(),
            'difficulty': self.difficulty,
            'level-name': self.level_name,
            'white-list': str(self.whitelist_enabled).lower(),
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

    # ---------- 工具 ----------
    def _append_log(self, line: str, level: str = 'info'):
        self.console.append(line, level)

    def _java(self, version: Optional[int] = None) -> str:
        """获取 java 路径: 优先 Agent runtime 自带 JRE (可指定版本), 其次系统 java"""
        if self._java_manager_ref:
            p = self._java_manager_ref.get_java_path(version)
            if p:
                return p
        # 兜底: 系统 java 或 JAVA_HOME
        p = os.environ.get('JAVA_HOME')
        if p:
            cand = Path(p) / 'bin' / ('java.exe' if os.name == 'nt' else 'java')
            if cand.exists():
                return str(cand)
        return 'java'

    def _ensure_java(self, required_version: int = 17) -> bool:
        """确保指定版本的 Java 已安装; 未安装自动下载 (带等待, 避免并发冲突)"""
        if not self._java_manager_ref:
            self._append_log('[Agent] Java 管理器未注入, 跳过 Java 检测', 'warn')
            return True  # 兜底让 _java() 用系统 java
        if self._java_manager_ref.is_installed(required_version):
            self._append_log(f'[Agent] Java {required_version} 已就绪', 'debug')
            return True
        # 未安装 -> 自动下载对应版本 (等待其他安装任务 / 预热线程结束后再装)
        self._append_log(f'[Agent] 未检测到 Java {required_version}, 自动下载 JRE...', 'info')
        r = self._java_manager_ref.ensure_installed(
            required_version,
            progress_cb=lambda d, t, msg: self._append_log(f'[Agent] {msg}', 'info'))
        if not r.get('ok'):
            self._append_log(f'[Agent] Java 下载失败: {r.get("msg")}', 'error')
            return False
        self._append_log(f'[Agent] Java {required_version} 安装成功: {r.get("java_path")}', 'success')
        return True

    def _required_java_version(self) -> int:
        """根据 MC 版本推断所需 Java 大版本

        Mojang 对应关系:
          - 1.20.5+ : Java 21
          - 1.17 - 1.20.4: Java 17 (1.17 实际需 16, BellSoft 无, 用 17 兜底)
          - 1.12 - 1.16.5: Java 8
          - 1.11 及更早: Java 8
        """
        try:
            parts = self.version.split('.')
            major = int(parts[0])
            minor = int(parts[1]) if len(parts) > 1 else 0
            patch = int(parts[2]) if len(parts) > 2 else 0
        except (ValueError, IndexError):
            return 17
        if major > 1:
            return 21  # 未来版本 (2.x+) 兜底 21
        if major == 1:
            if minor > 20:
                return 21  # 1.21+
            if minor == 20:
                # 1.20.5+ -> 21, 1.20 - 1.20.4 -> 17
                return 21 if patch >= 5 else 17
            if minor >= 17:
                return 17
            if minor >= 12:
                return 8
            return 8
        return 8

    # ---------- 核心 jar 下载 ----------
    def _ensure_jar(self) -> bool:
        """确保 server.jar 存在

        优先级:
        1. server_dir/jar_file 已存在 -> 直接用 (最快, ~0ms)
        2. 共享核心缓存命中 -> 硬链接到 server_dir (<100ms)
        3. 缓存未命中 -> 下载到缓存, 再硬链接 (慢, 仅首次)
        """
        jar_path = self.server_dir / self.jar_file
        if jar_path.exists() and jar_path.stat().st_size > 1000:
            return True

        # 检查共享核心缓存
        cached = self._get_cached_core_path(self.server_type, self.version, self.jar_file)
        if cached:
            try:
                # 优先硬链接 (秒级, 节省磁盘), 失败则复制
                if jar_path.exists():
                    jar_path.unlink()
                try:
                    os.link(cached, jar_path)
                except (OSError, AttributeError):
                    import shutil
                    shutil.copy2(cached, jar_path)
                self._append_log(
                    f'[Agent] 核心已从缓存复用: {self.jar_file} '
                    f'({round(jar_path.stat().st_size/1048576, 2)} MB)', 'success')
                return True
            except Exception as e:
                self._append_log(f'[Agent] 缓存复用失败, 转为下载: {e}', 'warn')

        # 缓存未命中 -> 下载到缓存目录, 然后硬链接到 server_dir
        try:
            self._download_core()
            return True
        except Exception as e:
            self._append_log(f'[Agent] 核心下载失败: {e}', 'error')
            return False

    def _download_core(self):
        """下载 Minecraft 服务器核心到共享缓存, 再硬链接到 server_dir"""
        self._append_log(
            f'[Agent] 正在下载 {self.server_type} {self.version} 核心...', 'info')
        self._status = 'installing'

        url, size = self._resolve_core_url()

        # 下载目标: 优先写入缓存目录, 没有缓存目录则直接写到 server_dir
        cache_dir = self._core_cache_dir or self.server_dir
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache_path = cache_dir / self._cache_key(self.server_type, self.version, self.jar_file)
        # 临时文件, 防止中途失败留下半文件
        tmp_path = cache_path.with_suffix(cache_path.suffix + '.part')

        req = Request(url, headers={'User-Agent': 'McPanel-NodeAgent/1.0'})
        with urlopen(req, timeout=180, context=_SSL_CTX) as resp:
            total = int(resp.headers.get('Content-Length', 0) or size or 0)
            downloaded = 0
            chunk_size = 1024 * 64
            last_log = 0
            with open(tmp_path, 'wb') as f:
                while True:
                    chunk = resp.read(chunk_size)
                    if not chunk:
                        break
                    f.write(chunk)
                    downloaded += len(chunk)
                    # 每下载 1MB 输出一次进度
                    if total and downloaded - last_log >= 1024 * 1024:
                        last_log = downloaded
                        pct = downloaded / total * 100
                        self._append_log(
                            f'[Agent] 核心下载 {downloaded//(1024*1024)}MB / '
                            f'{total//(1024*1024)}MB ({pct:.1f}%)', 'info')
        # 下载完成: 原子重命名 .part -> 最终
        os.replace(tmp_path, cache_path)
        self._append_log(
            f'[Agent] 核心下载完成: {self.jar_file} '
            f'({round(cache_path.stat().st_size/1048576, 2)} MB)', 'success')

        # 若缓存目录与 server_dir 不同, 硬链接到 server_dir
        jar_path = self.server_dir / self.jar_file
        if cache_path != jar_path:
            try:
                if jar_path.exists():
                    jar_path.unlink()
                try:
                    os.link(cache_path, jar_path)
                except (OSError, AttributeError):
                    import shutil
                    shutil.copy2(cache_path, jar_path)
            except Exception as e:
                self._append_log(f'[Agent] 缓存复用链接失败 (但下载已成功): {e}', 'warn')

    def _resolve_core_url(self) -> Tuple[str, int]:
        """解析核心下载 URL, 带 6h TTL 缓存 (避免每次启动都查 Mojang/PaperMC API)"""
        key = (self.server_type, self.version)
        now = time.time()
        with self._url_cache_lock:
            cached = self._url_cache.get(key)
            if cached and (now - cached[2]) < self._URL_CACHE_TTL:
                return cached[0], cached[1]
        # 缓存未命中, 调用 API 解析
        if self.server_type == 'vanilla':
            url, sz = self._get_vanilla_url(self.version)
        elif self.server_type == 'paper':
            try:
                url, sz = self._get_paper_url(self.version)
            except Exception:
                url, sz = self._get_vanilla_url(self.version)
        elif self.server_type == 'spigot':
            self._append_log('[Agent] Spigot 需要 BuildTools 编译, 暂用 Vanilla 核心', 'warn')
            url, sz = self._get_vanilla_url(self.version)
        else:
            url, sz = self._get_vanilla_url(self.version)
        with self._url_cache_lock:
            self._url_cache[key] = (url, sz, now)
        return url, sz

    @staticmethod
    def _get_vanilla_url(version: str):
        """通过 Mojang API 获取 Vanilla server.jar 下载链接"""
        req = Request('https://piston-meta.mojang.com/mc/game/version_manifest_v2.json',
                      headers={'User-Agent': 'McPanel-NodeAgent/1.0'})
        with urlopen(req, timeout=30, context=_SSL_CTX) as r:
            manifest = json.loads(r.read().decode('utf-8'))
        ver_info = None
        for v in manifest.get('versions', []):
            if v['id'] == version:
                ver_info = v
                break
        if not ver_info:
            raise ValueError(f'Mojang 版本清单中找不到 {version}')
        req2 = Request(ver_info['url'], headers={'User-Agent': 'McPanel-NodeAgent/1.0'})
        with urlopen(req2, timeout=30, context=_SSL_CTX) as r2:
            ver_detail = json.loads(r2.read().decode('utf-8'))
        server = ver_detail.get('downloads', {}).get('server', {})
        if not server:
            raise ValueError(f'{version} 没有 server.jar 下载')
        return server['url'], server.get('size', 0)

    @staticmethod
    def _get_paper_url(version: str):
        """获取 Paper 核心下载链接"""
        try:
            api_url = f'https://api.papermc.io/v2/projects/paper/versions/{version}'
            req = Request(api_url, headers={
                'User-Agent': 'McPanel-NodeAgent/1.0',
                'Accept': 'application/json'})
            with urlopen(req, timeout=30, context=_SSL_CTX) as r:
                data = json.loads(r.read().decode('utf-8'))
            builds = data.get('builds', [])
            if builds:
                latest = builds[-1]
                url = (f'https://api.papermc.io/v2/projects/paper/versions/{version}'
                       f'/builds/{latest}/downloads/paper-{version}-{latest}.jar')
                return url, 0
        except Exception:
            pass
        return _MinecraftServerLite._get_vanilla_url(version)

    # ---------- 控制 ----------
    def start(self) -> Dict:
        import subprocess
        import platform
        with self._lock:
            if self._process and self._process.poll() is None:  # type: ignore[union-attr]
                return {'ok': False, 'msg': '服务器已在运行中'}
            self._status = 'starting'
            self.console.clear()
            self._append_log(f'[Agent] 启动服务器 #{self.server_id} port={self.port}', 'info')

            # 1. 确保 Java 已安装 (按 MC 版本推断所需版本)
            required_java = self._required_java_version()
            if not self._ensure_java(required_java):
                self._status = 'crashed'
                return {'ok': False, 'msg': 'Java 安装失败'}

            # 2. 确保 server.jar 存在 (按 server_type/version 下载)
            jar = self.server_dir / self.jar_file
            if not jar.exists() or jar.stat().st_size < 1000:
                self._append_log(f'[Agent] 未找到 {self.jar_file}, 自动下载核心...', 'warn')
                if not self._ensure_jar():
                    self._status = 'crashed'
                    return {'ok': False, 'msg': '核心下载失败'}

            # 2.5 写入 server.properties (端口/MOTD 等, 缺失会默认 25565 端口撞车)
            try:
                self.write_server_properties()
            except Exception as e:
                self._append_log(f'[Agent] 写入 server.properties 失败: {e}', 'warn')

            # 3. 启动 Java 进程 (用所需版本对应的 java)
            try:
                cmd = [
                    self._java(required_java),
                    f'-Xmx{self.memory_mb}M',
                    f'-Xms{max(256, self.memory_mb // 2)}M',
                    '-XX:+UseG1GC',
                    '-jar', str(jar), 'nogui',
                ]
                flags = 0x08000000 if platform.system() == 'Windows' else 0
                self._process = subprocess.Popen(
                    cmd, cwd=str(self.server_dir),
                    stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    text=True, bufsize=1, creationflags=flags,
                )
            except Exception as e:
                self._status = 'crashed'
                return {'ok': False, 'msg': f'启动失败: {e}'}
            self._last_started = datetime.utcnow()
            self._pump_output()
            self._status = 'running'
            return {'ok': True, 'msg': '已启动', 'pid': self._process.pid}

    def _start_mock(self) -> Dict:
        """使用内嵌的 python mock 脚本"""
        import subprocess
        import platform
        import tempfile
        # 写 mock 脚本到临时 py
        mock_py = self.server_dir / '_mc_mock.py'
        from server_manager import MOCK_SERVER_PY  # 复用主面板 mock
        mock_py.write_text(MOCK_SERVER_PY, encoding='utf-8')
        cmd = [sys.executable, str(mock_py), '--port', str(self.port), '--name', f'AgentSrv{self.server_id}']
        flags = 0x08000000 if platform.system() == 'Windows' else 0
        try:
            self._process = subprocess.Popen(
                cmd, cwd=str(self.server_dir),
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, bufsize=1, creationflags=flags,
            )
        except Exception as e:
            return {'ok': False, 'msg': f'mock 启动失败: {e}'}
        self._last_started = datetime.utcnow()
        self._pump_output()
        self._status = 'running'
        return {'ok': True, 'msg': '已启动 (mock模式)', 'pid': self._process.pid}

    def _pump_output(self):
        """后台监听 stdout"""
        def worker():
            proc = self._process
            try:
                for line in proc.stdout:  # type: ignore[union-attr]
                    if not line:
                        continue
                    self.console.append(line)
                    self._parse(line)
            except Exception as e:
                self._append_log(f'[Agent] stdout 监听异常: {e}', 'error')
        t = threading.Thread(target=worker, daemon=True)
        t.start()

    def _parse(self, line: str):
        if 'Done (' in line and 'help' in line:
            self._status = 'running'
        if 'players online' in line:
            import re
            m = re.search(r'There are (\d+)', line)
            if m:
                self._player_count = int(m.group(1))
        if 'Stopping server' in line or 'Closing Server' in line:
            self._status = 'stopping'

    def stop(self, force: bool = False) -> Dict:
        import subprocess
        with self._lock:
            proc = self._process
            if not proc or proc.poll() is not None:
                self._status = 'stopped'
                return {'ok': True, 'msg': '服务器未运行'}
            try:
                if force:
                    proc.kill()
                else:
                    try:
                        proc.stdin.write('stop\n')  # type: ignore[union-attr]
                        proc.stdin.flush()          # type: ignore[union-attr]
                        proc.wait(timeout=30)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                self._status = 'stopped'
                return {'ok': True, 'msg': '已停止'}
            except Exception as e:
                return {'ok': False, 'msg': f'停止失败: {e}'}

    def restart(self) -> Dict:
        self.stop()
        time.sleep(1.5)
        return self.start()

    def kill(self) -> Dict:
        return self.stop(force=True)

    def send_command(self, cmd: str) -> Dict:
        proc = self._process
        if not proc or proc.poll() is not None:
            return {'ok': False, 'msg': '服务器未运行'}
        try:
            proc.stdin.write(cmd + '\n')  # type: ignore[union-attr]
            proc.stdin.flush()            # type: ignore[union-attr]
            self._append_log(f'[> ] {cmd}', 'cmd')
            return {'ok': True}
        except Exception as e:
            return {'ok': False, 'msg': f'发送失败: {e}'}

    def get_stats(self) -> Dict:
        proc = self._process
        cpu = 0.0
        rss_mb = 0
        pid = None
        if proc and proc.poll() is None:
            pid = proc.pid
            try:
                p = psutil.Process(proc.pid)
                with p.oneshot():
                    cpu = p.cpu_percent(interval=0.15)
                    rss_mb = p.memory_info().rss // (1024 * 1024)
            except Exception:
                pass
        if self._last_started and self._status in ('running', 'stopping'):
            delta = int((datetime.utcnow() - self._last_started).total_seconds())
            h, rem = divmod(delta, 3600)
            m, s = divmod(rem, 60)
            uptime = f'{h}h {m}m {s}s' if h else (f'{m}m {s}s' if m else f'{s}s')
        else:
            uptime = '0s'
        return {
            'status': self._status,
            'cpu': round(cpu, 1),
            'memory_mb': rss_mb,
            'memory_limit': self.memory_mb,
            'players': self._player_count,
            'pid': pid,
            'uptime': uptime,
            'port': self.port,
        }


# ============================================================
# Agent Flask 应用
# ============================================================

def create_app(args) -> Flask:
    app = Flask(__name__)
    data_dir = Path(args.data_dir).resolve()
    data_dir.mkdir(parents=True, exist_ok=True)
    servers_dir = data_dir / 'servers'
    servers_dir.mkdir(parents=True, exist_ok=True)

    # 配置校验
    expected_token = args.token
    if not expected_token:
        print('[WARN] 未设置 --token 或 NODE_API_TOKEN, 将使用随机生成的 Token:')
        expected_token = secrets.token_urlsafe(24)
        print(f'       ===== TOKEN: {expected_token} =====')
    # 允许的主服务器 IP 白名单 (可选)
    allowed_master_ips = set()
    if args.allow_from:
        allowed_master_ips = {ip.strip() for ip in args.allow_from.split(',') if ip.strip()}

    # 注册表 server_id -> controller
    servers: Dict[int, _MinecraftServerLite] = {}
    servers_lock = threading.Lock()

    # Java 管理器单例 (Agent runtime 目录)
    runtime_dir = data_dir / 'runtime'
    java_manager = _JavaManagerLite(runtime_dir, log_fn=lambda msg, level: print(f'[JavaAgent] {msg}'))
    _MinecraftServerLite.set_java_manager(java_manager)

    def get_server(sid: int, create: bool = False,
                   server_type: str = 'paper', version: str = '1.20.4',
                   memory_mb: int = 2048, cores: int = 1,
                   port: int = 25565, jar_file: str = 'server.jar',
                   motd: str = 'A Minecraft Server', max_players: int = 20,
                   difficulty: str = 'normal', level_name: str = 'world',
                   online_mode: bool = True, pvp_enabled: bool = True,
                   whitelist_enabled: bool = False, java_args: str = ''
                   ) -> Optional[_MinecraftServerLite]:
        with servers_lock:
            s = servers.get(sid)
            if s is None and create:
                # 由 master 启动时同步过来的配置创建实例
                s = _MinecraftServerLite(
                    sid, servers_dir,
                    memory_mb=memory_mb, cores=cores, port=port, jar_file=jar_file,
                    server_type=server_type, version=version,
                    motd=motd, max_players=max_players,
                    difficulty=difficulty, level_name=level_name,
                    online_mode=online_mode, pvp_enabled=pvp_enabled,
                    whitelist_enabled=whitelist_enabled, java_args=java_args,
                )
                servers[sid] = s
            elif s is not None:
                # 已存在的实例, 更新配置
                with s._lock:
                    s.server_type = server_type or s.server_type
                    s.version = version or s.version
            return s

    # ---------- 鉴权中间件 ----------
    @app.before_request
    def _auth():
        # 允许 GET /health 匿名
        if request.path == '/health':
            return
        auth = request.headers.get('Authorization', '')
        token = ''
        if auth.lower().startswith('bearer '):
            token = auth[7:].strip()
        if not token or token != expected_token:
            abort(401)
        if allowed_master_ips:
            remote = request.remote_addr
            if remote not in allowed_master_ips:
                abort(403)

    # ---------- 路由 ----------
    @app.get('/health')
    def health():
        return jsonify({'ok': True, 'agent': 'McPanel-Node', 'version': '1.0'})

    @app.get('/heartbeat')
    def heartbeat():
        # 资源采集带超时保护: 慢盘/异常系统下也保证快速响应
        host = _safe_host_stats(data_dir, timeout=6)
        per_server_raw = _safe_server_stats_list(servers, servers_lock, timeout=8)
        per_server = [{
            'id': s.get('id'),
            'status': s.get('status', 'unknown'),
            'players': s.get('players', 0),
            'max_players': s.get('max_players', 0),
            'memory_mb': s.get('memory_mb', 0),
            'memory_limit': s.get('memory_limit', 0),
            'cpu': s.get('cpu', 0.0),
            'port': s.get('port', None),
            'uptime': s.get('uptime', '0s'),
        } for s in per_server_raw]
        with servers_lock:
            running = sum(1 for s in servers.values() if s._status == 'running')
        return jsonify({
            'ok': True,
            'agent_version': '1.0',
            'timestamp': datetime.utcnow().isoformat(),
            'host': host,
            'servers': {
                'total': len(per_server),
                'running': running,
                'list': per_server,
            },
        })

    @app.post('/server/<int:sid>/start')
    def srv_start(sid: int):
        cfg = request.get_json(silent=True) or {}
        # 若 master 传了配置, 创建/更新时一并应用
        s = get_server(
            sid, create=True,
            server_type=cfg.get('server_type', 'paper'),
            version=cfg.get('version', '1.20.4'),
            memory_mb=int(cfg.get('memory', 2048)),
            cores=int(cfg.get('cores', 1)),
            port=int(cfg.get('port', 25565 + sid)),
            jar_file=cfg.get('jar_file', 'server.jar'),
            motd=cfg.get('motd', 'A Minecraft Server'),
            max_players=int(cfg.get('max_players', 20)),
            difficulty=cfg.get('difficulty', 'normal'),
            level_name=cfg.get('level_name', 'world'),
            online_mode=bool(cfg.get('online_mode', True)),
            pvp_enabled=bool(cfg.get('pvp_enabled', True)),
            whitelist_enabled=bool(cfg.get('whitelist_enabled', False)),
            java_args=cfg.get('java_args', ''),
        )
        if s is None:
            return jsonify({'ok': False, 'msg': '服务器实例化失败'})
        if cfg:
            with s._lock:
                s.port = int(cfg.get('port', s.port))
                s.memory_mb = int(cfg.get('memory', s.memory_mb))
                s.cores = int(cfg.get('cores', s.cores))
                s.jar_file = cfg.get('jar_file', s.jar_file)
                s.server_type = cfg.get('server_type', s.server_type)
                s.version = cfg.get('version', s.version)
                s.motd = cfg.get('motd', s.motd)
                s.max_players = int(cfg.get('max_players', s.max_players))
                s.difficulty = cfg.get('difficulty', s.difficulty)
                s.level_name = cfg.get('level_name', s.level_name)
                s.online_mode = bool(cfg.get('online_mode', s.online_mode))
                s.pvp_enabled = bool(cfg.get('pvp_enabled', s.pvp_enabled))
                s.whitelist_enabled = bool(cfg.get('whitelist_enabled', s.whitelist_enabled))
                s.java_args = cfg.get('java_args', s.java_args)
        return jsonify(s.start())

    @app.post('/server/<int:sid>/stop')
    def srv_stop(sid: int):
        s = get_server(sid)
        return jsonify(s.stop() if s else {'ok': False, 'msg': '服务器不存在'})

    @app.post('/server/<int:sid>/restart')
    def srv_restart(sid: int):
        s = get_server(sid)
        return jsonify(s.restart() if s else {'ok': False, 'msg': '服务器不存在'})

    @app.post('/server/<int:sid>/kill')
    def srv_kill(sid: int):
        s = get_server(sid)
        return jsonify(s.kill() if s else {'ok': False, 'msg': '服务器不存在'})

    @app.get('/server/<int:sid>/stats')
    def srv_stats(sid: int):
        s = get_server(sid)
        return jsonify(s.get_stats() if s else {'status': 'missing'})

    @app.post('/server/<int:sid>/command')
    def srv_command(sid: int):
        s = get_server(sid)
        if not s:
            return jsonify({'ok': False, 'msg': '不存在'})
        cmd = ((request.get_json(silent=True) or {}).get('command') or '').strip()
        if not cmd:
            return jsonify({'ok': False, 'msg': '命令为空'})
        return jsonify(s.send_command(cmd))

    @app.get('/server/<int:sid>/console')
    def srv_console(sid: int):
        s = get_server(sid)
        return jsonify(s.console.all() if s else [])

    # ---------- 文件 API (最小实现, 同样做防目录穿越) ----------
    @app.get('/server/<int:sid>/files')
    def srv_files(sid: int):
        s = get_server(sid, create=True)
        sub = request.args.get('path', '') or ''
        base = s.server_dir.resolve()
        target = (base / sub).resolve()
        try:
            target.relative_to(base)
        except ValueError:
            return jsonify({'ok': False, 'msg': '路径非法'}), 400
        if not target.exists():
            return jsonify([])
        out = []
        for p in target.iterdir():
            try:
                st = p.stat()
                out.append({
                    'name': p.name,
                    'is_dir': p.is_dir(),
                    'size': st.st_size if p.is_file() else 0,
                    'modified': datetime.fromtimestamp(st.st_mtime).strftime('%Y-%m-%d %H:%M'),
                    'path': str(p.relative_to(base)).replace('\\', '/'),
                })
            except OSError:
                continue
        return jsonify(out)

    @app.delete('/server/<int:sid>/files')
    def srv_files_delete(sid: int):
        s = get_server(sid)
        if not s:
            return jsonify({'ok': False, 'msg': '不存在'})
        rel = ((request.get_json(silent=True) or {}).get('path') or '').strip()
        if not rel:
            return jsonify({'ok': False, 'msg': '路径为空'})
        if '\x00' in rel or ('..' in rel.replace('\\', '/').split('/')):
            return jsonify({'ok': False, 'msg': '路径非法'})
        base = s.server_dir.resolve()
        target = (base / rel).resolve()
        try:
            target.relative_to(base)
        except ValueError:
            return jsonify({'ok': False, 'msg': '路径非法'})
        if target == base:
            return jsonify({'ok': False, 'msg': '不能删除根目录'})
        import shutil
        try:
            if target.is_dir() and not target.is_symlink():
                shutil.rmtree(target)
            elif target.exists():
                target.unlink()
            return jsonify({'ok': True})
        except Exception as e:
            return jsonify({'ok': False, 'msg': str(e)})

    # ---------- Agent 端 Java 管理 API ----------
    @app.get('/java/status')
    def java_status():
        return jsonify({'ok': True, 'status': java_manager.get_status()})

    @app.post('/java/install')
    def java_install():
        """确保远端节点安装指定版本 Java

        采用 ensure_installed: 已装则秒回; 有其他安装任务(预热/其他服)则等待其完成后串行下载
        """
        data = request.get_json(silent=True) or {}
        ver = int(data.get('version', _JavaManagerLite.DEFAULT_VERSION))
        if ver not in _JavaManagerLite.VERSIONS:
            return jsonify({'ok': False, 'msg': f'不支持的 Java 版本 {ver}, 可选: '
                                                f'{list(_JavaManagerLite.VERSIONS.keys())}'}), 400
        r = java_manager.ensure_installed(ver, wait_timeout=1800)
        return jsonify(r)

    # 记录一下
    app.logger.info(f'[NodeAgent] 数据目录={data_dir}, IP白名单={allowed_master_ips or "ANY"}')

    # 设置共享核心缓存目录 + 启动后台预热线程 (不阻塞 HTTP 服务启动)
    core_cache_dir = data_dir / 'core_cache'
    _MinecraftServerLite.set_core_cache_dir(core_cache_dir)
    prewarm_thread = threading.Thread(
        target=prewarm_pool,
        args=(java_manager, core_cache_dir, _agent_log),
        daemon=True,
        name='McPanel-Prewarm',
    )
    prewarm_thread.start()

    return app


def _agent_log(msg: str, level: str = 'info'):
    """Agent 全局日志: 输出到 stdout 与 app.logger (供预热线程使用)"""
    print(f'[Prewarm] {msg}')
    try:
        from flask import current_app
        if current_app:
            current_app.logger.info(f'[Prewarm] {msg}')
    except Exception:
        pass


def prewarm_pool(java_manager: '_JavaManagerLite',
                 core_cache_dir: Path,
                 log_fn: Optional[Callable[[str, str], None]] = None):
    """Agent 启动时后台预热: 预下载常用 Java 版本 + MC 核心

    幂等: 已存在的文件直接跳过
    并发: Java 与核心分别用线程池并行下载
    目标: 首次启动服务器时, Java + 核心均命中缓存, 启动 < 1s
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    log = log_fn or (lambda msg, level='info': print(f'[Prewarm] {msg}'))

    # Java 预热版本: 仅预下载最常用的 17 (MC 1.17-1.20.4) + 21 (MC 1.20.5+)
    # 8 / 11 留给按需下载 (老版本 MC 用户少)
    java_versions = [17, 21]

    # 核心预热清单: 当前主流版本 (paper 与 vanilla 都覆盖)
    core_targets = [
        ('paper', '1.20.4'),
        ('paper', '1.21'),
        ('vanilla', '1.21'),
    ]

    log(f'预热池启动: Java {java_versions} + 核心 {[f"{t[0]}/{t[1]}" for t in core_targets]}', 'info')

    # 阶段 1: 并行预下载 Java
    def _java_task(ver: int):
        try:
            if java_manager.is_installed(ver):
                log(f'Java {ver} 已安装, 跳过', 'info')
                return
            log(f'预下载 Java {ver}...', 'info')
            # ensure_installed 会自动等待其他版本安装完成, 避免并发冲突
            r = java_manager.ensure_installed(ver)
            if r.get('ok'):
                log(f'Java {ver} 预热成功: {r.get("java_path")}', 'info')
            else:
                log(f'Java {ver} 预热失败: {r.get("msg")}', 'warn')
        except Exception as e:
            log(f'Java {ver} 预热异常: {e}', 'warn')

    with ThreadPoolExecutor(max_workers=2, thread_name_prefix='prewarm-java') as ex:
        futures = [ex.submit(_java_task, v) for v in java_versions]
        for f in as_completed(futures):
            try:
                f.result()
            except Exception as e:
                log(f'Java 预热 future 异常: {e}', 'warn')

    # 阶段 2: 并行预下载 MC 核心
    _MinecraftServerLite.set_core_cache_dir(core_cache_dir)

    def _core_task(server_type: str, version: str):
        try:
            cached = _MinecraftServerLite._get_cached_core_path(server_type, version, 'server.jar')
            if cached:
                log(f'核心 {server_type} {version} 已缓存 ({round(cached.stat().st_size/1048576, 1)}MB), 跳过', 'info')
                return
            log(f'预下载核心 {server_type} {version}...', 'info')
            # 创建临时实例只为触发缓存下载; 用 throwaway server_id 避免冲突
            import tempfile
            with tempfile.TemporaryDirectory(prefix='prewarm_') as tmp:
                s = _MinecraftServerLite(
                    server_id=-9999,
                    base_dir=Path(tmp),
                    server_type=server_type, version=version,
                    jar_file='server.jar',
                )
                ok = s._ensure_jar()
                if ok:
                    log(f'核心 {server_type} {version} 预热成功', 'info')
                else:
                    log(f'核心 {server_type} {version} 预热失败', 'warn')
        except Exception as e:
            log(f'核心 {server_type} {version} 预热异常: {e}', 'warn')

    with ThreadPoolExecutor(max_workers=3, thread_name_prefix='prewarm-core') as ex:
        futures = [ex.submit(_core_task, st, v) for st, v in core_targets]
        for f in as_completed(futures):
            try:
                f.result()
            except Exception as e:
                log(f'核心预热 future 异常: {e}', 'warn')

    log('预热池任务结束', 'info')


def main():
    ap = argparse.ArgumentParser('McPanel Node Agent')
    ap.add_argument('--host', default=os.environ.get('NODE_HOST', '0.0.0.0'))
    ap.add_argument('--port', type=int, default=int(os.environ.get('NODE_PORT', '58765')))
    ap.add_argument('--token', default=os.environ.get('NODE_API_TOKEN', ''))
    ap.add_argument('--data-dir', default=os.environ.get('NODE_DATA_DIR', './node_data'))
    ap.add_argument('--allow-from', default=os.environ.get('NODE_ALLOW_FROM', ''),
                    help='仅允许这些主服务器 IP 访问 (逗号分隔); 留空=允许任意 IP (配合 Token 使用)')
    ap.add_argument('--debug', action='store_true')
    args = ap.parse_args()

    app = create_app(args)
    print('=' * 50)
    print(' McPanel Node Agent (分支服务器)')
    print(f' 监听地址: http://{args.host}:{args.port}')
    print(f' 数据目录: {Path(args.data_dir).resolve()}')
    print(' 使用 --token <共享密钥> 与主面板保持一致')
    print(' 主面板添加节点时, API URL 填写上面的监听地址')
    print('=' * 50)
    # 生产请用 gunicorn / waitress, 这里用 werkzeug 单进程足够
    app.run(host=args.host, port=args.port, debug=args.debug, threaded=True)


if __name__ == '__main__':
    main()
