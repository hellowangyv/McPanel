/* ============================================================
   McPanel 前端主脚本
   - 通用工具: $, $$, toast, copyText
   - 全局 Socket.IO 客户端工厂
   - Tab 切换 / 仪表盘轮询 / 控制台实时交互
   - ECharts 数据可视化: 配额环形图 / 资源趋势折线 / 主机监控
   ============================================================ */

(function (win) {
  'use strict';

  /* ---------- 基础工具 ---------- */
  const $ = (sel, root = document) => root.querySelector(sel);
  const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));

  /* ---------- CSRF: 拦截原生 fetch, 状态变更请求自动注入 X-CSRFToken 头 ---------- */
  const _origFetch = win.fetch.bind(win);
  win.fetch = function (input, init) {
    init = init || {};
    const method = (init.method || (input && input.method) || 'GET').toUpperCase();
    if (['POST', 'PUT', 'DELETE', 'PATCH'].includes(method)) {
      let url = '';
      try { url = (typeof input === 'string') ? input : (input.url || ''); } catch (_) { url = ''; }
      const isSameOrigin = !url || url.startsWith('/') || url.startsWith(location.origin + '/');
      const hasAuthHeader = init.headers && (
        (init.headers.Authorization) ||
        (init.headers.authorization) ||
        typeof init.headers === 'object' && (init.headers['Authorization'] || init.headers['authorization'])
      );
      if (isSameOrigin && !hasAuthHeader) {
        const meta = document.querySelector('meta[name="csrf-token"]');
        if (meta && meta.content) {
          const headers = new Headers(init.headers || {});
          headers.set('X-CSRFToken', meta.content);
          init.headers = headers;
        }
      }
    }
    return _origFetch(input, init);
  };

  function toast(msg, type = 'ok', dur = 3000) {
    const host = $('#toast-host');
    if (!host) return;
    const el = document.createElement('div');
    el.className = 'toast ' + type;
    el.textContent = msg;
    host.appendChild(el);
    setTimeout(() => {
      el.style.transition = 'opacity .3s, transform .3s';
      el.style.opacity = '0';
      el.style.transform = 'translateX(40px)';
      setTimeout(() => el.remove(), 320);
    }, dur);
  }

  function copyText(text) {
    const cb = navigator.clipboard;
    const done = () => toast('已复制: ' + text, 'ok');
    if (cb && cb.writeText) {
      cb.writeText(text).then(done).catch(() => _fallbackCopy(text, done));
    } else {
      _fallbackCopy(text, done);
    }
  }
  function _fallbackCopy(text, cb) {
    const ta = document.createElement('textarea');
    ta.value = text; document.body.appendChild(ta);
    ta.select(); document.execCommand('copy'); ta.remove();
    cb && cb();
  }

  /* ---------- Socket 工厂（自动重连） ---------- */
  let _globalSocket = null;
  function getSocket() {
    if (_globalSocket && _globalSocket.connected) return _globalSocket;
    if (typeof io === 'undefined') {
      console.warn('[McPanel] Socket.IO 客户端未加载, 实时功能不可用');
      return null;
    }
    _globalSocket = io({
      transports: ['websocket', 'polling'],
      reconnection: true,
      reconnectionAttempts: 20,
      reconnectionDelay: 1000,
    });
    _globalSocket.on('connect', () => console.log('[McPanel] Socket.IO 已连接'));
    _globalSocket.on('disconnect', () => console.warn('[McPanel] Socket.IO 断开, 等待重连...'));
    return _globalSocket;
  }

  /* ============================================================
     ECharts 数据可视化模块
     所有图表都在 echarts 可用时初始化, 否则静默降级为纯 DOM 展示
     ============================================================ */
  const charts = {};

  // 深色主题下的通用配置
  const AXIS = {
    axisLine: { lineStyle: { color: 'rgba(148,163,184,.2)' } },
    axisLabel: { color: '#6B7A90', fontSize: 11, fontFamily: 'JetBrains Mono' },
    splitLine: { lineStyle: { color: 'rgba(148,163,184,.08)' } },
  };

  charts.available = function () { return typeof win.echarts !== 'undefined'; };

  // 初始化实例, 容器不存在或 echarts 缺失时返回 null
  charts.init = function (el) {
    if (!el || !charts.available()) return null;
    const c = win.echarts.init(el, null, { renderer: 'canvas' });
    const ro = new ResizeObserver(() => c.resize());
    ro.observe(el);
    return c;
  };

  // 环形进度图 (配额)
  charts.donut = function (el, opts) {
    const c = charts.init(el);
    if (!c) return null;
    opts = opts || {};
    const used = Number(opts.used) || 0;
    const total = Number(opts.total) || 1;
    const pct = Math.min(100, total > 0 ? used / total * 100 : 0);
    const color = opts.color || '#10B981';
    const label = opts.label || '';
    const unit = opts.unit || '';
    c.setOption({
      animationDuration: 800,
      series: [{
        type: 'pie',
        radius: ['72%', '88%'],
        center: ['50%', '50%'],
        silent: true,
        label: { show: false },
        data: [
          { value: pct, itemStyle: { color, shadowBlur: 18, shadowColor: color + '55' } },
          { value: 100 - pct, itemStyle: { color: 'rgba(148,163,184,.12)' } },
        ],
      }],
      graphic: [
        { type: 'text', left: 'center', top: '38%', style: { text: pct.toFixed(1) + '%', fill: '#E8EEF7', fontSize: 22, fontWeight: 800, fontFamily: 'JetBrains Mono' } },
        { type: 'text', left: 'center', top: '56%', style: { text: label, fill: '#6B7A90', fontSize: 11, fontFamily: 'JetBrains Mono' } },
      ],
    });
    return c;
  };

  // 双指标滚动趋势折线图 (CPU / 内存)
  charts.trend = function (el, opts) {
    const c = charts.init(el);
    if (!c) return null;
    opts = opts || {};
    const max = Number(opts.max) || 100;
    const series = (opts.series || [
      { name: 'CPU', color: '#10B981' },
      { name: '内存', color: '#4E8DF5' },
    ]).map(s => ({
      name: s.name,
      type: 'line',
      smooth: true,
      showSymbol: false,
      lineStyle: { width: 2.5, color: s.color },
      itemStyle: { color: s.color },
      areaStyle: {
        color: new (win.echarts.graphic.LinearGradient)(0, 0, 0, 1, [
          { offset: 0, color: s.color + '2e' },
          { offset: 1, color: s.color + '00' },
        ]),
      },
      data: [],
    }));
    c.setOption({
      animation: false,
      grid: { left: 8, right: 10, top: 26, bottom: 8, containLabel: true },
      tooltip: {
        trigger: 'axis',
        backgroundColor: 'rgba(20,27,37,.95)',
        borderColor: 'rgba(148,163,184,.2)',
        textStyle: { color: '#E8EEF7', fontSize: 12, fontFamily: 'JetBrains Mono' },
      },
      legend: {
        top: 0, right: 0,
        textStyle: { color: '#A8B4C6', fontSize: 11, fontFamily: 'JetBrains Mono' },
        itemWidth: 14, itemHeight: 3,
      },
      xAxis: Object.assign({ type: 'category', boundaryGap: false }, AXIS),
      yAxis: Object.assign({ type: 'value', max }, AXIS),
      series,
    });
    return c;
  };

  // 向趋势图追加数据点
  charts.push = function (chart, values) {
    if (!chart) return;
    const data = chart.getOption().series.map(s => s.data);
    values.forEach((v, i) => {
      const arr = data[i] || [];
      arr.push(v);
      if (arr.length > 60) arr.shift();
    });
    const x = chart.getOption().xAxis[0].data || [];
    x.push(new Date().toLocaleTimeString('zh-CN', { hour12: false }));
    if (x.length > 60) x.shift();
    chart.setOption({ xAxis: [{ data: x }], series: data.map((d, i) => ({ data: d })) });
  };

  /* ---------- Tab 切换 ---------- */
  function initTabs() {
    $$('.tab-nav').forEach(nav => {
      nav.addEventListener('click', e => {
        const t = e.target.closest('.tab');
        if (!t || !t.dataset.tab || t.tagName === 'A') return;
        const tabId = t.dataset.tab;
        $$('.tab', nav).forEach(x => x.classList.remove('active'));
        t.classList.add('active');
        const parent = nav.parentElement;
        $$('.tab-panel', parent).forEach(p => p.classList.remove('active'));
        $('#' + tabId, parent)?.classList.add('active');
        // 切到图表 tab 时 resize
        if (charts.available()) {
          setTimeout(() => win.echarts.getInstanceByDom($('.chart-box', parent))?.resize(), 60);
        }
      });
    });
  }

  /* ---------- 仪表盘服务器卡片状态轮询 ---------- */
  function pollServers() {
    const cards = $$('.server-card[data-sid]');
    if (cards.length === 0) return;

    async function refresh() {
      for (const card of cards) {
        const sid = card.dataset.sid;
        try {
          const r = await fetch(`/api/servers/${sid}/stats`);
          if (!r.ok) continue;
          const s = await r.json();
          const status = $('.sv-status', card);
          if (status) {
            status.style.setProperty('--c', s.status_color);
            const dot = $('.dot', status);
            status.lastChild.textContent = s.status_display;
            if (dot) dot.style.background = s.status_color;
          }
          const pl = $('.sv-players', card);
          if (pl) pl.textContent = `${s.players}/${s.max_players}`;
          const cpu = $('.sv-cpu-text', card);
          if (cpu) cpu.textContent = `CPU ${s.cpu}%`;
          const memBar = $('.sv-mem-bar', card);
          const memPct = s.memory_limit ? Math.min(100, s.memory_mb / s.memory_limit * 100) : 0;
          if (memBar) memBar.style.width = memPct + '%';
          const memTxt = $('.sv-mem-text', card);
          if (memTxt) memTxt.textContent = `内存 ${s.memory_mb} / ${s.memory_limit}MB`;
        } catch (_) { /* ignore */ }
      }
    }
    refresh();
    setInterval(refresh, 4000);
  }

  /* ---------- 仪表盘卡片按钮：快速启动/停止/重启 ---------- */
  function bindQuickActions() {
    document.addEventListener('click', async e => {
      const btn = e.target.closest('[data-act]');
      if (!btn || !btn.dataset.sid) return;
      const sid = btn.dataset.sid;
      const act = btn.dataset.act;
      const URLS = { start: 'start', stop: 'stop', restart: 'restart', kill: 'kill' };
      if (!URLS[act]) return;
      btn.disabled = true;
      try {
        const r = await fetch(`/api/servers/${sid}/${URLS[act]}`, { method: 'POST' });
        const d = await r.json();
        if (d.ok) {
          toast(d.msg || `${act} 成功`, 'ok');
          setTimeout(async () => {
            try {
              const rs = await fetch(`/api/servers/${sid}/stats`);
              const st = await rs.json();
              const ev = new CustomEvent('sv-state', { detail: { sid, stats: st } });
              document.dispatchEvent(ev);
            } catch (_) {}
          }, 2000);
        } else {
          toast(d.msg || `${act} 失败`, 'err');
        }
      } catch (err) {
        toast('请求失败: ' + err.message, 'err');
      } finally {
        btn.disabled = false;
      }
    });
  }

  /* ---------- 服务器详情页 ---------- */
  const consoleScroll = {
    _auto: true,
    _userDisabled: false,
    toggle() {
      this._userDisabled = !this._userDisabled;
      this._auto = !this._userDisabled;
      toast('自动滚动: ' + (this._auto ? '开' : '关'));
    },
    ensure(box) {
      if (this._auto && box) box.scrollTop = box.scrollHeight;
    }
  };

  function toggleQuickCmds() {
    $('#quick-cmds')?.classList.toggle('hidden');
  }

  function appendConsoleLine(box, entry) {
    const div = document.createElement('div');
    div.className = 'cl cl-' + (entry.level || 'info');
    const time = document.createElement('span'); time.className = 'cl-time'; time.textContent = `[${entry.time || '--:--:--'}]`;
    const lvl = document.createElement('span'); lvl.className = 'cl-level';
    lvl.textContent = ({ info: 'INFO', warn: 'WARN', error: 'ERROR', success: 'OK', cmd: 'CMD', debug: 'DBG' })[entry.level || 'info'] || 'INFO';
    const text = document.createElement('span'); text.className = 'cl-text';
    text.textContent = entry.text ?? '';
    if (/https?:\/\//.test(entry.text || '')) {
      text.innerHTML = escapeHtml(entry.text).replace(/(https?:\/\/[^\s]+)/g, '<a href="$1" target="_blank" style="color:#60a5fa">$1</a>');
    }
    div.append(time, lvl, text);
    box.appendChild(div);
    while (box.childNodes.length > 1200) box.removeChild(box.firstChild);
  }
  function escapeHtml(s) {
    const d = document.createElement('div'); d.textContent = s; return d.innerHTML;
  }

  function initServerDetail(sid) {
    if (sid == null || sid === '' || sid === 'undefined' || isNaN(Number(sid))) {
      return;
    }
    const socket = getSocket();
    const box = $('#console-box');
    const statusDot = $('#sv-status-dot');
    const statusChip = $('#sv-status-chip');
    const pidText = $('#sv-pid');

    // 资源趋势图
    const trendChart = charts.trend($('#trend-chart'), {
      max: 100,
      series: [
        { name: 'CPU %', color: '#10B981' },
        { name: '内存 %', color: '#4E8DF5' },
      ],
    });

    if (socket) {
      socket.emit('join_server', { server_id: sid });
      socket.on('console_hist', data => {
        if (!box) return;
        box.innerHTML = '';
        (data.lines || []).forEach(l => appendConsoleLine(box, l));
        consoleScroll.ensure(box);
      });
      socket.on('console_line', msg => {
        if (!box) return;
        const d = msg?.data || msg;
        appendConsoleLine(box, d);
        consoleScroll.ensure(box);
      });
      socket.on('state_changed', msg => {
        const s = msg?.data?.status || msg?.status;
        if (s) applyStatus(s);
      });
      socket.on('stats', msg => {
        const d = msg?.data || msg;
        if (d?.players != null) updatePlayers(d.players, d.max_players);
      });
      window.addEventListener('beforeunload', () => socket.emit('leave_server', { server_id: sid }));
    } else {
      fetch(`/api/servers/${sid}/console`).then(r => r.json()).then(lines => {
        box.innerHTML = '';
        lines.forEach(l => appendConsoleLine(box, l));
      });
    }

    let lastCpu = null, lastMemPct = null;
    async function refreshStats() {
      try {
        const r = await fetch(`/api/servers/${sid}/stats`);
        const s = await r.json();
        applyStatus(s.status, s.status_color);
        $('#st-cpu') && ($('#st-cpu').textContent = s.cpu + '%');
        $('#bar-cpu') && ($('#bar-cpu').style.width = s.cpu + '%');
        const memTxt = `${s.memory_mb} / ${s.memory_limit} MB`;
        $('#st-mem') && ($('#st-mem').textContent = memTxt);
        const memPct = s.memory_limit ? Math.min(100, s.memory_mb / s.memory_limit * 100) : 0;
        $('#bar-mem') && ($('#bar-mem').style.width = memPct + '%');
        $('#st-players') && updatePlayers(s.players, s.max_players);
        $('#st-uptime') && ($('#st-uptime').textContent = s.uptime);
        $('#st-pid') && ($('#st-pid').textContent = s.pid || '-');
        pidText && (pidText.textContent = s.pid || '-');
        // 推送趋势图数据 (服务器运行中才记录)
        if (s.status === 'running') {
          charts.push(trendChart, [s.cpu || 0, memPct]);
        }
      } catch (_) {}
    }
    refreshStats();
    setInterval(refreshStats, 3000);

    const STATUS_META = {
      running: { color: '#10B981', label: '运行中' },
      stopped: { color: '#64748B', label: '已停止' },
      starting: { color: '#F5A623', label: '启动中' },
      stopping: { color: '#F97316', label: '关闭中' },
      crashed: { color: '#F4575C', label: '已崩溃' },
      installing: { color: '#4E8DF5', label: '安装中' }
    };
    function applyStatus(status, color) {
      const meta = STATUS_META[status] || {};
      const c = color || meta.color || '#64748b';
      if (statusDot) statusDot.style.background = c;
      if (statusChip) {
        statusChip.style.borderColor = c;
        statusChip.style.color = c;
        statusChip.textContent = meta.label || status;
      }
      const running = status === 'running' || status === 'stopping' || status === 'starting';
      const startBtn = $('#btn-start'); const stopBtn = $('#btn-stop');
      const restartBtn = $('#btn-restart'); const killBtn = $('#btn-kill');
      if (startBtn) startBtn.disabled = running;
      if (stopBtn) stopBtn.disabled = !running;
      if (restartBtn) restartBtn.disabled = status !== 'running';
      if (killBtn) killBtn.disabled = !(status === 'running' || status === 'starting' || status === 'stopping');
    }

    function updatePlayers(n, max) {
      $('#st-players') && ($('#st-players').textContent = n);
      const av = $('#st-player-avatars');
      if (!av) return;
      if (av.dataset.lastN == n) return;
      av.dataset.lastN = n;
      av.innerHTML = '';
      const palette = ['#10B981', '#4E8DF5', '#A78BFA', '#F5A623', '#F4575C', '#2DD4BF', '#EC4899', '#F97316'];
      const names = ['Steve','Alex','Notch','Dream','Lily','Sasha','Kaito','Jack','Amy','Mia','Leo','Zoe','Max','Ivy'];
      for (let i = 0; i < Math.min(n, 12); i++) {
        const d = document.createElement('div');
        const name = names[i % names.length];
        const c = palette[i % palette.length];
        d.style.cssText = `width:28px;height:28px;border-radius:50%;background:${c};color:#fff;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700;border:2px solid var(--bg-1);margin-left:-6px;box-shadow:0 2px 6px rgba(0,0,0,.4)`;
        d.title = name;
        d.textContent = name.slice(0, 2);
        av.appendChild(d);
      }
    }

    $$('.btn-action[data-act]').forEach(btn => {
      btn.addEventListener('click', async () => {
        const act = btn.dataset.act;
        btn.disabled = true;
        try {
          const r = await fetch(`/api/servers/${sid}/${act}`, { method: 'POST' });
          const d = await r.json();
          if (d.ok) toast(d.msg || `${act} 命令已发出`, 'ok');
          else toast(d.msg || `${act} 失败`, 'err');
          setTimeout(refreshStats, 1500);
        } catch (e) {
          toast('请求失败: ' + e.message, 'err');
        } finally {
          setTimeout(() => btn.disabled = false, 1200);
        }
      });
    });

    const form = $('#console-form');
    const input = $('#console-cmd');
    const cmdHistory = []; let histIdx = -1;

    form && form.addEventListener('submit', e => {
      e.preventDefault();
      sendCmd(input.value);
    });
    input && input.addEventListener('keydown', e => {
      if (e.key === 'ArrowUp' && cmdHistory.length) {
        histIdx = Math.max(0, histIdx === -1 ? cmdHistory.length - 1 : histIdx - 1);
        input.value = cmdHistory[histIdx] || '';
      } else if (e.key === 'ArrowDown' && histIdx >= 0) {
        histIdx++;
        input.value = cmdHistory[histIdx] || '';
        if (histIdx >= cmdHistory.length) histIdx = -1;
      }
    });

    function sendCmd(raw) {
      const text = (raw || '').trim();
      if (!text) return;
      cmdHistory.push(text); histIdx = -1;
      if (socket) {
        socket.emit('send_command', { server_id: sid, command: text });
      } else {
        fetch(`/api/servers/${sid}/command`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ command: text })
        });
      }
      input.value = '';
      input.focus();
    }
    $$('.qc-btn').forEach(b => b.addEventListener('click', () => sendCmd(b.dataset.cmd)));

    if (box) {
      box.addEventListener('scroll', () => {
        if (consoleScroll._userDisabled) return;
        const near = box.scrollHeight - box.scrollTop - box.clientHeight < 40;
        consoleScroll._auto = near;
      });
    }
  }

  /* ---------- 仪表盘: 主机监控实时趋势图 (管理员) ---------- */
  function initHostTrend() {
    const el = $('#host-trend-chart');
    if (!el) return;
    const chart = charts.trend(el, {
      max: 100,
      series: [
        { name: 'CPU %', color: '#10B981' },
        { name: '内存 %', color: '#4E8DF5' },
        { name: '磁盘 %', color: '#F5A623' },
      ],
    });
    const socket = getSocket();
    if (socket && chart) {
      socket.on('host_stats', d => {
        charts.push(chart, [d.cpu || 0, d.memory_percent || 0, d.disk_percent || 0]);
      });
    }
  }

  /* ---------- 仪表盘: 配额环形图 ---------- */
  function initQuotaDonuts() {
    $$('[data-donut]').forEach(el => {
      const used = Number(el.dataset.donutUsed) || 0;
      const total = Number(el.dataset.donutTotal) || 1;
      charts.donut(el, {
        used, total,
        color: el.dataset.donutColor || '#10B981',
        label: el.dataset.donutLabel || '',
        unit: el.dataset.donutUnit || '',
      });
    });
  }

  /* ---------- 初始化 ---------- */
  function init() {
    initTabs();
    bindQuickActions();

    // 图表
    initQuotaDonuts();
    initHostTrend();

    // 主机状态实时推送 (仪表盘使用)
    try {
      const socket = getSocket();
      if (socket) {
        socket.on('host_stats', d => {
          const cpu = document.getElementById('host-cpu');
          const bar = document.getElementById('host-cpu-bar');
          const mem = document.getElementById('host-mem');
          const mbar = document.getElementById('host-mem-bar');
          if (cpu) cpu.textContent = (d.cpu || 0).toFixed(1) + '%';
          if (bar) bar.style.width = (d.cpu || 0) + '%';
          if (mem) mem.textContent = `实时 ${d.memory_percent}%`;
          if (mbar) mbar.style.width = (d.memory_percent || 0) + '%';
        });
      }
    } catch (_) {}

    setTimeout(() => {
      $$('.flash:not(.flash-error):not(.flash-danger)').forEach(f => {
        if (!f.querySelector('.flash-close:hover')) {
          f.style.transition = 'opacity .5s, transform .5s';
          f.style.opacity = '0';
          setTimeout(() => f.remove(), 550);
        }
      });
    }, 4500);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

  /* ---------- 对外 API ---------- */
  win.$ = $; win.$$ = $$;
  win.toast = toast; win.copyText = copyText;
  win.socket = getSocket;
  win.pollServers = pollServers;
  win.initServerDetail = initServerDetail;
  win.consoleScroll = consoleScroll;
  win.toggleQuickCmds = toggleQuickCmds;
  win.charts = charts;
})(window);
