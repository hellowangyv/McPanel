"""分布式服务器重构 - 性能基准测试

测试维度:
1. URL 缓存命中 (跳过外部 API)
2. 核心缓存命中 (硬链接, <100ms)
3. _ensure_jar 三级优先级正确工作
4. Agent 预热线程能正确启动 + 幂等
5. Master→Node Session 连接池复用 (mock 测试)
6. 端到端: 冷启动 vs 暖启动 耗时对比 (mock 下载)
"""
import os
import shutil
import sys
import tempfile
import time
import threading
from pathlib import Path
from unittest.mock import MagicMock, patch

os.environ['PANEL_SECRET'] = 'test-benchmark'
os.environ['NODE_API_TOKEN'] = 'test-token'

import node_agent
from node_agent import (
    _JavaManagerLite, _MinecraftServerLite, prewarm_pool, _agent_log
)
import node_manager

passed = 0
failed = 0

def check(name, cond, detail=''):
    global passed, failed
    if cond:
        passed += 1
        print(f'  ✓ {name}')
    else:
        failed += 1
        print(f'  ✗ {name} -- {detail}')


# ===================== 测试 1: URL 缓存 =====================
print('\n[1] URL 解析缓存 (TTL 6h)')

# 准备: 清空缓存
_MinecraftServerLite._url_cache.clear()

tmp = Path(tempfile.mkdtemp(prefix='bench_'))
cache_dir = tmp / 'core_cache'
_MinecraftServerLite.set_core_cache_dir(cache_dir)

# 用 mock 让 _get_vanilla_url 返回固定值, 不实际访问网络
call_count = {'vanilla': 0, 'paper': 0}

orig_vanilla = _MinecraftServerLite._get_vanilla_url
orig_paper = _MinecraftServerLite._get_paper_url

def mock_vanilla(version):
    call_count['vanilla'] += 1
    return f'https://example.com/vanilla/{version}.jar', 50000000

def mock_paper(version):
    call_count['paper'] += 1
    return f'https://example.com/paper/{version}.jar', 50000000

_MinecraftServerLite._get_vanilla_url = staticmethod(mock_vanilla)
_MinecraftServerLite._get_paper_url = staticmethod(mock_paper)

# 创建测试实例
s = _MinecraftServerLite(
    server_id=1, base_dir=tmp / 'servers',
    server_type='paper', version='1.20.4',
    jar_file='server.jar',
)

# 第一次解析 (应调用 API)
url1, sz1 = s._resolve_core_url()
check('首次 URL 解析调用 API', call_count['paper'] == 1, f'调用次数 {call_count["paper"]}')

# 第二次解析 (应命中缓存, 不调用 API)
url2, sz2 = s._resolve_core_url()
check('第二次 URL 解析命中缓存', call_count['paper'] == 1, f'调用次数 {call_count["paper"]}')
check('两次返回相同 URL', url1 == url2)

# 另一版本应触发新 API 调用
s2 = _MinecraftServerLite(
    server_id=2, base_dir=tmp / 'servers',
    server_type='paper', version='1.21',
    jar_file='server.jar',
)
s2._resolve_core_url()
check('不同版本触发新 API 调用', call_count['paper'] == 2, f'调用次数 {call_count["paper"]}')

# 恢复
_MinecraftServerLite._get_vanilla_url = orig_vanilla
_MinecraftServerLite._get_paper_url = orig_paper
_MinecraftServerLite._url_cache.clear()


# ===================== 测试 2: 核心缓存硬链接 =====================
print('\n[2] 核心缓存硬链接 (<100ms)')

# 准备: 在缓存目录放一个"已下载"的 fake jar
cache_file = cache_dir / _MinecraftServerLite._cache_key('paper', '1.20.4', 'server.jar')
cache_file.write_bytes(b'\x00' * 50000000)  # 50MB fake jar

s3 = _MinecraftServerLite(
    server_id=3, base_dir=tmp / 'servers3',
    server_type='paper', version='1.20.4',
    jar_file='server.jar',
)
# 确保 server_dir 中没有 jar
jar_path = s3.server_dir / 'server.jar'
if jar_path.exists():
    jar_path.unlink()

# 调用 _ensure_jar, 应从缓存硬链接
t0 = time.perf_counter()
ok = s3._ensure_jar()
elapsed_ms = (time.perf_counter() - t0) * 1000

check('_ensure_jar 从缓存命中返回 True', ok)
check('server_dir 中 jar 已创建', jar_path.exists())
check(f'硬链接耗时 < 100ms (实际 {elapsed_ms:.1f}ms)', elapsed_ms < 100, f'实际 {elapsed_ms:.1f}ms')
# 验证硬链接 (相同 inode)
import os as _os
check('硬链接 inode 一致 (节省磁盘)', _os.stat(cache_file).st_ino == _os.stat(jar_path).st_ino,
      f'cache={_os.stat(cache_file).st_ino}, jar={_os.stat(jar_path).st_ino}')


# ===================== 测试 3: 三级优先级 =====================
print('\n[3] _ensure_jar 三级优先级')

# 已存在 server_dir/jar -> 直接返回
s4 = _MinecraftServerLite(
    server_id=4, base_dir=tmp / 'servers4',
    server_type='paper', version='1.20.4',
    jar_file='server.jar',
)
(s4.server_dir / 'server.jar').write_bytes(b'\x00' * 5000)
t0 = time.perf_counter()
ok = s4._ensure_jar()
elapsed_ms = (time.perf_counter() - t0) * 1000
check('已存在 jar 直接返回 True', ok)
check(f'已存在场景耗时 < 5ms (实际 {elapsed_ms:.2f}ms)', elapsed_ms < 5, f'实际 {elapsed_ms:.2f}ms')


# ===================== 测试 4: 预热线程 (mock) =====================
print('\n[4] 预热线程 prewarm_pool (mock install/download)')

# 清空缓存目录
shutil.rmtree(cache_dir, ignore_errors=True)
cache_dir.mkdir(parents=True, exist_ok=True)

# Mock Java manager: install 立即返回 ok (避免真下载)
mock_jm = MagicMock()
mock_jm.get_status.return_value = {'installed': False, 'version': 0}
mock_jm.install.return_value = {'ok': True, 'java_path': '/fake/java'}

# Mock _MinecraftServerLite._ensure_jar
orig_ensure = _MinecraftServerLite._ensure_jar
def mock_ensure(self):
    # 模拟下载到缓存
    cf = self._core_cache_dir / self._cache_key(self.server_type, self.version, self.jar_file)
    cf.write_bytes(b'\x00' * 1000000)  # 1MB fake
    return True
_MinecraftServerLite._ensure_jar = mock_ensure

# 恢复 _get_vanilla_url/_get_paper_url (mock 版本)
_MinecraftServerLite._get_vanilla_url = staticmethod(mock_vanilla)
_MinecraftServerLite._get_paper_url = staticmethod(mock_paper)

t0 = time.perf_counter()
# 在主线程同步执行 (实际是 daemon 后台)
logs = []
prewarm_pool(mock_jm, cache_dir, log_fn=lambda msg, level='info': logs.append(msg))
elapsed = time.perf_counter() - t0

check(f'prewarm 完成 (实际 {elapsed:.2f}s)', elapsed < 30)
check('Java 17 预下载调用', mock_jm.install.call_count >= 1, f'调用次数 {mock_jm.install.call_count}')
# 验证核心缓存文件已生成
check('paper 1.20.4 缓存文件已生成', (cache_dir / 'paper_1.20.4_server.jar').exists())
check('paper 1.21 缓存文件已生成', (cache_dir / 'paper_1.21_server.jar').exists())
check('vanilla 1.21 缓存文件已生成', (cache_dir / 'vanilla_1.21_server.jar').exists())

# 预热线程幂等性: 再跑一次应跳过所有
mock_jm.install.reset_mock()
mock_jm.get_status.return_value = {'installed': True, 'version': 21}  # 都已装
_MinecraftServerLite._ensure_jar = orig_ensure  # 恢复 (会从缓存命中)
logs2 = []
prewarm_pool(mock_jm, cache_dir, log_fn=lambda msg, level='info': logs2.append(msg))
check('幂等: Java 全部跳过 (install 不被调用)', mock_jm.install.call_count == 0)
check('幂等: 核心全部跳过 (日志含 "已缓存")', any('已缓存' in m for m in logs2))


# ===================== 测试 5: Master→Node Session 复用 =====================
print('\n[5] NodeRPC Session 连接池复用')

# Mock requests.Session 验证 _get_session 只创建一次
class MockSession:
    def __init__(self):
        self.request_count = 0
        self.headers = {}
    def request(self, method, url, **kwargs):
        self.request_count += 1
        class R: pass
        r = R()
        r.status_code = 200
        r.json = lambda: {'ok': True, 'host': {'cpu': 50}}
        r.text = '{}'
        return r
    def mount(self, *args, **kwargs): pass

with patch.object(node_manager, 'requests') as mock_requests_mod:
    if hasattr(node_manager, 'HTTPAdapter'):
        node_manager.HTTPAdapter = MagicMock()
    mock_requests_mod.Session = MockSession
    mock_requests_mod.request = MagicMock()  # 不应该被调用
    
    # 创建 NodeRPC (用 mock node)
    mock_node = MagicMock()
    mock_node.api_url = 'http://fake-node:58765'
    mock_node.api_token = 'fake-token'
    
    rpc = node_manager.NodeRPC(mock_node, timeout=5)
    
    # 第一次 ping
    r1 = rpc.ping()
    check('第一次 ping 成功', r1.get('ok'))
    session1 = rpc._session
    check('Session 已创建', session1 is not None)
    
    # 第二次 ping 应复用同一 Session
    r2 = rpc.ping()
    session2 = rpc._session
    check('第二次 ping 复用同一 Session', session1 is session2)
    check('Session request 被调用 2 次', session1.request_count == 2)


# ===================== 测试 6: 冷启动 vs 暖启动 耗时对比 =====================
print('\n[6] 冷启动 vs 暖启动 耗时对比 (真实下载延迟模拟)')

# 清空缓存
shutil.rmtree(cache_dir, ignore_errors=True)
cache_dir.mkdir(parents=True, exist_ok=True)

# Cold start: 模拟真实 50MB jar 下载 (写 50MB + 模拟 1s 网络)
def mock_ensure_slow(self):
    """模拟真实下载: 写 50MB 文件 + 模拟 1s 网络"""
    cf = self._core_cache_dir / self._cache_key(self.server_type, self.version, self.jar_file)
    tmp_p = cf.with_suffix('.part')
    # 模拟网络下载延迟
    time.sleep(1.0)
    # 写 50MB (反映磁盘 I/O 成本)
    with open(tmp_p, 'wb') as f:
        f.write(b'\x00' * (50 * 1024 * 1024))
    os.replace(tmp_p, cf)
    # 硬链接到 server_dir
    jar = self.server_dir / self.jar_file
    try:
        os.link(cf, jar)
    except OSError:
        import shutil as _sh
        _sh.copy2(cf, jar)
    return True

_MinecraftServerLite._ensure_jar = mock_ensure_slow

s_cold = _MinecraftServerLite(
    server_id=99, base_dir=tmp / 'cold',
    server_type='paper', version='1.20.4', jar_file='server.jar',
)
t0 = time.perf_counter()
ok = s_cold._ensure_jar()
cold_ms = (time.perf_counter() - t0) * 1000
check(f'冷启动 (模拟下载 50MB + 1s 网络): {cold_ms:.0f}ms', ok)

# 暖启动: 恢复真实 _ensure_jar 实现, 此时缓存已命中 (冷启动刚写入), 走硬链接路径 (~2ms)
_MinecraftServerLite._ensure_jar = orig_ensure
s_warm = _MinecraftServerLite(
    server_id=100, base_dir=tmp / 'warm',
    server_type='paper', version='1.20.4', jar_file='server.jar',
)
# 确保 warm 实例的 server_dir 中没有 jar (强制触发缓存命中 -> 硬链接)
jar_warm = s_warm.server_dir / 'server.jar'
if jar_warm.exists():
    jar_warm.unlink()
t0 = time.perf_counter()
ok = s_warm._ensure_jar()
warm_ms = (time.perf_counter() - t0) * 1000
check(f'暖启动 (缓存命中, 硬链接 50MB): {warm_ms:.1f}ms', ok)

speedup = cold_ms / warm_ms if warm_ms > 0 else 999
check(f'提速倍数 >= 100x (实际 {speedup:.0f}x)', speedup >= 100,
      f'冷 {cold_ms:.0f}ms / 暖 {warm_ms:.1f}ms = {speedup:.0f}x')
print(f'  ⚡ 提速: 冷 {cold_ms:.0f}ms → 暖 {warm_ms:.1f}ms ({speedup:.0f}x)')


# ===================== 清理 =====================
shutil.rmtree(tmp, ignore_errors=True)
_MinecraftServerLite._url_cache.clear()

print(f'\n=== 基准测试结束: {passed} 通过 / {failed} 失败 ===')
sys.exit(0 if failed == 0 else 1)
