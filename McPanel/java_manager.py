"""
Java 运行时自动管理器
- 检测系统已安装的 Java
- 自动下载 BellSoft Liberica JRE 到项目 runtime 目录
- 提供 java 可执行路径
"""
from __future__ import annotations

import io
import json
import os
import platform
import re
import ssl
import subprocess
import tarfile
import threading
import time
import zipfile
from pathlib import Path
from typing import Optional, Dict, Callable
from urllib.request import urlopen, Request
from urllib.error import URLError

# BellSoft 下载基础
BELLSOFT_BASE = 'https://download.bell-sw.com/java'
BELLSOFT_API = 'https://api.bell-sw.com/v1/liberica/releases'
# 默认 JRE 版本 (MC 1.17+ 需要 Java 17, 1.20.5+ 需要 Java 21)
DEFAULT_JAVA_VERSION = 17

# 兜底用的已知版本号 (API 查询失败时使用; BellSoft 发布新版后可能下架)
BELLSOFT_VERSIONS = {
    17: '17.0.20+10',
    21: '21.0.12+10',
    11: '11.0.32+11',
    8:  '8u504+1',
}

# 最新版本缓存: {feature_version: (full_version_str, fetched_at_ts)}
_latest_cache: Dict[int, tuple] = {}
_latest_cache_ttl = 3600  # 1 小时

# 已安装 JRE 的目录名前缀
VERSION_DIR_PREFIX = 'jre-'


def _detect_java_major(java_exe) -> Optional[int]:
    """执行 java -version 探测大版本号 (目录名无法推断时使用)"""
    try:
        r = subprocess.run(
            [str(java_exe), '-version'],
            capture_output=True, text=True, timeout=10,
            creationflags=0x08000000 if platform.system() == 'Windows' else 0,
        )
        out = (r.stderr or '') + (r.stdout or '')
        import re
        m = re.search(r'version "(\d+)(?:\.(\d+))?', out)
        if m:
            major = int(m.group(1))
            if major == 1 and m.group(2):
                major = int(m.group(2))
            return major
    except Exception:
        pass
    return None

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


class JavaManager:
    """Java 运行时管理器 (单例)"""

    _instance: Optional['JavaManager'] = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
            return cls._instance

    def __init__(self, runtime_dir: str | Path = None, socketio=None, app=None):
        if hasattr(self, '_initialized') and self._initialized:
            # 允许后续注入 socketio / app
            if socketio and not self.socketio:
                self.socketio = socketio
            if app and not self.app:
                self.app = app
            return
        self.runtime_dir = Path(runtime_dir) if runtime_dir else Path('data') / 'runtime'
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        self.socketio = socketio
        self.app = app
        self._install_lock = threading.Lock()
        self._installing = False
        self._initialized = True

    # ------------------ 工具 ------------------

    def _emit(self, event: str, data: dict):
        """向前端推送事件"""
        if not self.socketio:
            return
        try:
            self.socketio.emit(event, data, namespace='/')
        except Exception:
            pass

    def _log(self, msg: str, level: str = 'info'):
        print(f'[JavaManager] {msg}')
        self._emit('java_log', {'msg': msg, 'level': level})

    # ------------------ 系统 Java 检测 ------------------

    @staticmethod
    def detect_system_java() -> Optional[Dict]:
        """检测系统已安装的 Java, 返回 {'path':..., 'version':...} 或 None"""
        java_cmd = 'java'
        # Windows 下也找 javaw
        if platform.system() == 'Windows':
            # 尝试常见路径
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
            # java -version 输出到 stderr
            out = (r.stderr or '') + (r.stdout or '')
            if r.returncode != 0 and not out:
                return None
            # 解析版本: 'openjdk version "17.0.8"' 或 'java version "1.8.0_361"'
            import re
            m = re.search(r'version "(\d+)(?:\.(\d+))?', out)
            if m:
                major = int(m.group(1))
                # 处理 1.8 旧格式
                if major == 1 and m.group(2):
                    major = int(m.group(2))
                return {'path': java_cmd, 'version': major, 'raw': out.strip().splitlines()[0] if out.strip() else ''}
        except (FileNotFoundError, subprocess.TimeoutExpired, Exception):
            pass
        return None

    # ------------------ 项目自带 JRE 检测 ------------------

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
                # 有些压缩包解压后有额外子目录
                sub = list(d.iterdir())
                if len(sub) == 1 and sub[0].is_dir():
                    bin_dir = sub[0] / 'bin'
            java_name = 'java.exe' if platform.system() == 'Windows' else 'java'
            java_exe = bin_dir / java_name if bin_dir.exists() else None
            if not (java_exe and java_exe.exists()):
                continue
            # 优先从目录名推断版本 (jre-17 / jre-17.0.13 / jdk-21 等)
            m = re.search(r'[a-z]+[-_]?(\d+)', d.name.lower())
            v = int(m.group(1)) if m else None
            if v is None:
                v = _detect_java_major(java_exe)
            if v:
                out.setdefault(v, java_exe)
        return out

    def get_bundled_java(self, version: Optional[int] = None) -> Optional[Path]:
        """获取项目 runtime 目录下的 java 可执行文件路径

        Args:
            version: 指定 Java 大版本 (8/11/17/21); 为 None 时返回已安装的最高版本
        """
        scan = self._scan_bundled()
        if not scan:
            return None
        if version is not None:
            return scan.get(version)
        return scan[max(scan)]

    def installed_versions(self) -> List[int]:
        """返回所有已安装的 JRE 大版本号 (升序)"""
        return sorted(self._scan_bundled().keys())

    def is_installed(self, version: int) -> bool:
        """指定版本的 JRE 是否已安装"""
        return self.get_bundled_java(version) is not None

    # ------------------ 获取 Java 路径 ------------------

    def get_java_path(self, version: Optional[int] = None) -> Optional[str]:
        """优先使用项目自带 JRE (可指定版本), 其次系统 Java"""
        bundled = self.get_bundled_java(version)
        if bundled:
            return str(bundled)
        sys_java = self.detect_system_java()
        if sys_java:
            return sys_java['path']
        return None

    # ------------------ MC 版本 -> Java 版本 映射 ------------------

    @staticmethod
    def java_for_mc(mc_version: str) -> int:
        """根据 Minecraft 版本推断所需 Java 大版本

        Mojang 官方对应关系:
          - 1.20.5+       -> Java 21
          - 1.17 ~ 1.20.4 -> Java 17
          - 1.12 ~ 1.16.5 -> Java 8 (1.13+ 也可用 11, 统一用 8 兼容性最好)
          - 1.11 及更早    -> Java 8
        """
        try:
            parts = str(mc_version).split('.')
            major = int(parts[0])
            minor = int(parts[1]) if len(parts) > 1 else 0
            patch = int(parts[2]) if len(parts) > 2 else 0
        except (ValueError, IndexError):
            return 17
        if major > 1:
            return 21  # 未来版本 (2.x+) 兜底
        if major == 1:
            if minor > 20:
                return 21
            if minor == 20:
                return 21 if patch >= 5 else 17
            if minor >= 17:
                return 17
            return 8
        return 8

    # ------------------ 确保已安装 (部署/启动时自动下载) ------------------

    def ensure_installed(self, java_version: int,
                         progress_cb: Optional[Callable[[int, int, str], None]] = None,
                         wait_timeout: int = 900) -> Dict:
        """确保指定版本的 JRE 已安装; 未安装则自动下载

        - 已安装           -> 直接返回
        - 有其他安装任务中  -> 等待其完成后再检查 (同版本则复用, 不同版本串行下载)
        - 超时仍未就绪      -> 返回失败
        """
        if self.is_installed(java_version):
            return {'ok': True, 'msg': f'Java {java_version} 已安装',
                    'java_path': str(self.get_bundled_java(java_version))}
        waited = 0
        while self._installing and waited < wait_timeout:
            time.sleep(2)
            waited += 2
            if self.is_installed(java_version):
                return {'ok': True, 'msg': f'Java {java_version} 已安装 (等待其他任务完成)',
                        'java_path': str(self.get_bundled_java(java_version))}
        if self._installing:
            return {'ok': False, 'msg': f'等待其他 Java 安装任务超时, 请稍后重试 (目标 {java_version})'}
        return self.install(java_version, progress_cb=progress_cb)

    def ensure_installed_async(self, java_version: int = DEFAULT_JAVA_VERSION) -> Dict:
        """后台线程确保指定版本 JRE 已安装 (部署时调用, 不阻塞请求)"""
        if self.is_installed(java_version):
            return {'ok': True, 'msg': f'Java {java_version} 已安装'}
        if self._installing:
            return {'ok': False, 'msg': '已有安装任务在进行中'}

        def _worker():
            try:
                with self.app.app_context() if self.app else _noop_ctx():
                    self.ensure_installed(java_version)
            except Exception as e:
                self._log(f'自动安装 Java {java_version} 异常: {e}', 'error')
                self._emit('java_install_error', {'msg': str(e)})
        t = threading.Thread(target=_worker, daemon=True)
        t.start()
        return {'ok': True, 'msg': f'已开始自动下载 Java {java_version}'}

    # ------------------ Java 状态 ------------------

    def get_status(self) -> Dict:
        """获取 Java 安装状态"""
        bundled = self.get_bundled_java()
        sys_java = self.detect_system_java()
        java_path = self.get_java_path()
        # 检测版本
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
                import re
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
            'runtime_dir': str(self.runtime_dir),
            'installed_versions': self.installed_versions(),
            'installing': self._installing,
        }

    # ------------------ 下载安装 JRE ------------------

    def _get_download_info(self, java_version: int = DEFAULT_JAVA_VERSION) -> Optional[Dict]:
        """构建 BellSoft Liberica JRE 下载链接"""
        sys_os = platform.system().lower()
        arch = platform.machine().lower()
        # 平台 & 架构映射
        os_map = {'windows': 'windows', 'linux': 'linux', 'darwin': 'macos'}
        arch_map = {'x86_64': 'amd64', 'amd64': 'amd64',
                    'arm64': 'aarch64', 'aarch64': 'aarch64',
                    'x86': 'i386', 'i386': 'i386'}
        bell_os = os_map.get(sys_os, 'linux')
        bell_arch = arch_map.get(arch, 'amd64')
        # 优先查询 BellSoft API 获取最新版本, 失败则回退到 BELLSOFT_VERSIONS
        full_version = self._fetch_latest_version(java_version, bell_os, bell_arch) \
            or BELLSOFT_VERSIONS.get(java_version)
        if not full_version:
            self._log(f'不支持的 Java 版本: {java_version}', 'error')
            return None
        # 构建下载 URL
        # Windows/macOS: .zip, Linux: .tar.gz
        if bell_os == 'linux':
            ext = 'tar.gz'
        else:
            ext = 'zip'
        filename = f'bellsoft-jre{full_version}-{bell_os}-{bell_arch}.{ext}'
        url = f'{BELLSOFT_BASE}/{full_version}/{filename}'
        return {
            'url': url,
            'name': filename,
            'size': 0,
            'checksum': '',
            'version': java_version,
            'ext': ext,
        }

    def _fetch_latest_version(self, feature_version: int,
                              bell_os: str, bell_arch: str) -> Optional[str]:
        """查询 BellSoft API 获取指定主版本的最新 JRE 完整版本号

        - 命中缓存且未过期 -> 直接返回
        - API 不可达 -> 返回 None (调用方回退到 BELLSOFT_VERSIONS)
        - 缓存 TTL 1 小时, 避免 API 请求过频
        """
        now = time.time()
        cached = _latest_cache.get(feature_version)
        if cached and (now - cached[1]) < _latest_cache_ttl:
            return cached[0]
        package_type = 'tar.gz' if bell_os == 'linux' else 'zip'
        bitness = 32 if bell_arch == 'i386' else 64
        api_url = (f'{BELLSOFT_API}'
                   f'?bundle-type=jre&os={bell_os}&bitness={bitness}'
                   f'&package-type={package_type}')
        try:
            req = Request(api_url, headers={
                'User-Agent': 'McPanel-JavaManager/1.0',
                'Accept': 'application/json'})
            with urlopen(req, timeout=15, context=_SSL_CTX) as r:
                data = json.loads(r.read().decode('utf-8'))
            if not isinstance(data, list):
                return None
            cands = [d for d in data
                     if d.get('featureVersion') == feature_version
                     and f'-{bell_arch}.' in (d.get('filename') or '')]
            if not cands:
                return None
            latest = max(cands, key=lambda d: (
                d.get('updateVersion', 0),
                d.get('buildVersion', 0),
                d.get('patchVersion', 0)))
            full = latest.get('version')
            if full:
                _latest_cache[feature_version] = (full, now)
                self._log(f'BellSoft API: Java {feature_version} 最新版 {full}')
                return full
        except Exception as e:
            self._log(f'BellSoft API 查询失败, 回退到内置版本: {e}', 'warn')
        return None

    def install(self, java_version: int = DEFAULT_JAVA_VERSION,
                progress_cb: Optional[Callable[[int, int, str], None]] = None) -> Dict:
        """下载并安装 JRE 到 runtime 目录

        Args:
            java_version: Java 大版本号 (17 或 21)
            progress_cb: 进度回调 (downloaded, total, msg)
        Returns:
            {'ok': bool, 'msg': str, 'java_path': str}
        """
        with self._install_lock:
            if self._installing:
                return {'ok': False, 'msg': '已有安装任务在进行中'}
            self._installing = True
        try:
            self._log(f'开始下载 Java {java_version} JRE...', 'info')
            self._emit('java_install_start', {'version': java_version})

            # 目标目录: 按版本隔离存放, 支持多版本共存 (jre-17 / jre-21 / ...)
            target_dir = self.runtime_dir / f'{VERSION_DIR_PREFIX}{java_version}'
            if target_dir.exists():
                import shutil
                shutil.rmtree(target_dir, ignore_errors=True)
            target_dir.mkdir(parents=True, exist_ok=True)

            info = self._get_download_info(java_version)
            if not info:
                return {'ok': False, 'msg': '无法获取下载链接'}

            url = info['url']
            self._log(f'下载地址: {url}', 'info')

            # 下载到临时文件
            tmp_file = self.runtime_dir / info['name']
            try:
                req = Request(url, headers={'User-Agent': 'McPanel/1.0'})
                with urlopen(req, timeout=120, context=_SSL_CTX) as resp:
                    total = int(resp.headers.get('Content-Length', 0))
                    downloaded = 0
                    chunk_size = 1024 * 64  # 64KB
                    with open(tmp_file, 'wb') as f:
                        while True:
                            chunk = resp.read(chunk_size)
                            if not chunk:
                                break
                            f.write(chunk)
                            downloaded += len(chunk)
                            pct = (downloaded / total * 100) if total else 0
                            msg = f'下载中... {downloaded // (1024*1024)}MB / {total // (1024*1024) if total else "?"}MB ({pct:.1f}%)'
                            self._emit('java_progress', {
                                'downloaded': downloaded, 'total': total,
                                'percent': round(pct, 1), 'msg': msg,
                            })
                            if progress_cb:
                                progress_cb(downloaded, total, msg)
                self._log(f'下载完成: {tmp_file}', 'success')
            except Exception as e:
                return {'ok': False, 'msg': f'下载失败: {e}'}

            # 解压到该版本专属目录
            self._log('正在解压...', 'info')
            self._emit('java_progress', {'percent': 100, 'msg': '正在解压...'})
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

            # 验证
            java_exe = self.get_bundled_java(java_version)
            if not java_exe:
                return {'ok': False, 'msg': '解压后未找到 java 可执行文件, 请检查压缩包格式'}
            self._log(f'安装完成: {java_exe}', 'success')
            self._emit('java_install_done', {'java_path': str(java_exe)})

            # 测试 java -version
            try:
                r = subprocess.run(
                    [str(java_exe), '-version'],
                    capture_output=True, text=True, timeout=15,
                    creationflags=0x08000000 if platform.system() == 'Windows' else 0,
                )
                out = (r.stderr or '').strip()
                if out:
                    self._log(f'版本验证: {out.splitlines()[0]}', 'info')
            except Exception:
                pass

            return {'ok': True, 'msg': 'Java 安装成功', 'java_path': str(java_exe)}
        finally:
            self._installing = False

    def install_async(self, java_version: int = DEFAULT_JAVA_VERSION) -> Dict:
        """异步下载安装"""
        if self._installing:
            return {'ok': False, 'msg': '已有安装任务在进行中'}
        def _worker():
            try:
                with self.app.app_context() if self.app else _noop_ctx():
                    self.install(java_version)
            except Exception as e:
                self._log(f'安装异常: {e}', 'error')
                self._emit('java_install_error', {'msg': str(e)})
        t = threading.Thread(target=_worker, daemon=True)
        t.start()
        return {'ok': True, 'msg': '下载任务已启动'}


class _noop_ctx:
    def __enter__(self):
        return self
    def __exit__(self, *a):
        pass
