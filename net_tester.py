#!/usr/bin/env python3
"""网络测速工具 - 绿色版

测速方案（参考 speedtest-cli 核心逻辑，针对国内优化）：
1. 探测选源：小文件下载测延迟，选最快的 CDN
2. 下载测速：多线程持续下载 10 秒，取总吞吐量
3. 上传测速：POST 大数据 10 秒，取吞吐量
4. 延迟：TCP 连接测延迟

支持高带宽（千兆）：按时间截断而非固定大小，TCP 充分爬坡。
"""

import tkinter as tk
from tkinter import ttk
import threading
import time
import os
import sys
import ssl
import socket
import math
import urllib.request
from queue import Queue


# ─── 测速核心 ───

_ctx = ssl.create_default_context()
_ctx.check_hostname = False
_ctx.verify_mode = ssl.CERT_NONE

# 强制 IPv4
_orig = socket.getaddrinfo
socket.getaddrinfo = lambda *a, **k: [r for r in _orig(*a, **k) if r[0] == socket.AF_INET]

# 下载测速源
DOWNLOAD_SOURCES = [
    ("华为云镜像", "https://mirrors.huaweicloud.com/nodejs/v20.17.0/node-v20.17.0-x64.msi"),
    ("华为云-Python", "https://mirrors.huaweicloud.com/python/3.12.5/Python-3.12.5.tgz"),
    ("腾讯-微信", "https://dldir1.qq.com/weixin/Windows/WeChatSetup.exe"),
]

# 上传端点
UPLOAD_URL = "https://speed.cloudflare.com/__up"

# 测速持续时间（秒）
DOWNLOAD_DURATION = 10
UPLOAD_DURATION = 10
# 探测超时（秒）
PROBE_TIMEOUT = 3


class ByteAccumulator:
    """线程安全的字节累计器，供仪表盘读取近窗口速度"""

    # 近窗口：反映“此刻吞吐”，不会像总均速那样从 0 慢慢爬
    WINDOW_SEC = 1.2
    MIN_WINDOW_SEC = 0.35
    # 下载开局极短窗口容易虚高，要求至少攒到这么久才报窗口速度
    DOWNLOAD_MIN_ELAPSED = 0.5

    def __init__(self):
        self._lock = threading.Lock()
        self.total = 0
        self._window = []  # (timestamp, bytes)
        self._t0 = None            # reset 时刻（含建连空窗）
        self._first_byte_t = None  # 真正开始传数据的时刻
        self._last_byte_t = None   # 最后一次记账时刻

    def reset(self):
        with self._lock:
            self.total = 0
            self._window = []
            self._t0 = time.time()
            self._first_byte_t = None
            self._last_byte_t = None

    def add(self, n):
        if n <= 0:
            return
        now = time.time()
        with self._lock:
            if self._t0 is None:
                self._t0 = now
            if self._first_byte_t is None:
                self._first_byte_t = now
            self._last_byte_t = now
            self.total += n
            self._window.append((now, n))
            cutoff = now - self.WINDOW_SEC
            self._window = [(t, b) for t, b in self._window if t >= cutoff]

    def elapsed(self):
        with self._lock:
            if self._t0 is None:
                return 0.0
            return max(time.time() - self._t0, 0.0)

    def transfer_elapsed(self):
        """首字节→末字节的传输时长（排除建连空窗与收尾等待）"""
        with self._lock:
            if self._first_byte_t is None or self._last_byte_t is None:
                return 0.0
            return max(self._last_byte_t - self._first_byte_t, 0.0)

    def average_mbps(self):
        """从首字节到现在的平均速度（Mbps），不含建连等待"""
        now = time.time()
        with self._lock:
            if self._first_byte_t is None or self.total <= 0:
                return 0.0
            elapsed = max(now - self._first_byte_t, 0.001)
            return self.total / elapsed * 8 / 1_000_000

    def window_mbps(self):
        """近 1.2 秒窗口吞吐（Mbps）——仪表盘主数据源"""
        now = time.time()
        with self._lock:
            if not self._window:
                return 0.0
            cutoff = now - self.WINDOW_SEC
            recent = [(t, b) for t, b in self._window if t >= cutoff]
            if not recent:
                return 0.0
            w_elapsed = max(now - recent[0][0], self.MIN_WINDOW_SEC)
            return sum(b for _, b in recent) / w_elapsed * 8 / 1_000_000

    def display_mbps(self, prefer_average=False):
        """仪表盘速度：默认近窗口；下载开局略加均速抑制虚高"""
        win = self.window_mbps()
        avg = self.average_mbps()
        te = self.transfer_elapsed()

        if prefer_average:
            # 上传：近窗口为主；均速在极短时长会虚高，前 0.8s 只用窗口
            if win <= 0:
                return 0.0
            if te < 0.8:
                return win
            return win * 0.85 + avg * 0.15

        # 下载：还没传够半秒先报 0，避免指针虚冲
        if te < self.DOWNLOAD_MIN_ELAPSED:
            return 0.0
        if win <= 0:
            return avg
        if te < 2.0:
            return win * 0.45 + avg * 0.55
        return win * 0.7 + avg * 0.3


def measure_ping(host, port=443, count=5, timeout=5):
    """TCP 连接测延迟"""
    latencies = []
    for _ in range(count):
        try:
            start = time.time()
            sock = socket.create_connection((host, port), timeout=timeout)
            elapsed = (time.time() - start) * 1000
            sock.close()
            latencies.append(elapsed)
        except Exception:
            pass
    if not latencies:
        return 0
    return sum(latencies) / len(latencies)


def probe_source(name, url, timeout=PROBE_TIMEOUT):
    """探测一个源：TCP 连接测延迟 + 下载 1MB 确认可达

    返回 (name, 下载字节数, TCP延迟ms)
    """
    try:
        # 从 URL 提取 host
        from urllib.parse import urlparse
        parsed = urlparse(url)
        host = parsed.hostname
        port = parsed.port or (443 if parsed.scheme == "https" else 80)

        # TCP 连接测纯延迟
        tcp_latencies = []
        for _ in range(3):
            try:
                start = time.time()
                sock = socket.create_connection((host, port), timeout=timeout)
                lat = (time.time() - start) * 1000
                sock.close()
                tcp_latencies.append(lat)
            except Exception:
                pass
        latency_ms = sum(tcp_latencies) / len(tcp_latencies) if tcp_latencies else 99999

        # 下载 1MB 确认可达
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", "Range": "bytes=0-1048575"})
        with urllib.request.urlopen(req, timeout=timeout, context=_ctx) as resp:
            data = resp.read(1048576)
        return name, len(data), latency_ms
    except Exception:
        return name, 0, 99999


def select_best_source(sources, callback=None):
    """探测所有源，返回所有可达源 [(name, url, latency_ms), ...]"""
    results = []
    for name, url in sources:
        if callback:
            callback(f"探测 {name}...")
        _, size, latency_ms = probe_source(name, url)
        if size > 0:
            results.append((name, url, latency_ms))
            if callback:
                callback(f"{name}: 延迟 {latency_ms:.0f}ms")
        else:
            if callback:
                callback(f"{name}: 不可达")

    return results


class TimedDownloader(threading.Thread):
    """按时间下载——持续下载指定秒数，统计总字节数"""

    def __init__(self, url, duration, result_queue, thread_id, accumulator=None):
        super().__init__(daemon=True)
        self.url = url
        self.duration = duration
        self.result_queue = result_queue
        self.thread_id = thread_id
        self.accumulator = accumulator
        self._should_stop = False

    def stop(self):
        self._should_stop = True

    def run(self):
        total_bytes = 0
        start = time.time()
        try:
            req = urllib.request.Request(self.url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=self.duration + 5, context=_ctx) as resp:
                while not self._should_stop and (time.time() - start) < self.duration:
                    chunk = resp.read(131072)  # 128KB
                    if not chunk:
                        break
                    total_bytes += len(chunk)
                    if self.accumulator:
                        self.accumulator.add(len(chunk))
        except Exception:
            pass
        elapsed = time.time() - start
        self.result_queue.put((self.thread_id, total_bytes, elapsed))


def measure_download_timed(urls, duration=DOWNLOAD_DURATION, threads_per_url=8,
                           callback=None, progress_callback=None):
    """多 URL 多线程持续下载，返回 (总字节, 速度Mbps)

    urls 可以是单个 URL 字符串或 URL 列表。
    每个 URL 开 threads_per_url 个线程，总线程数 = len(urls) * threads_per_url。
    自适应：单 URL 时开 12 线程，多 URL 时每 URL 开 8 线程。
    progress_callback(mbps) 约每 100ms 回调一次瞬时速度。
    """
    if isinstance(urls, str):
        urls = [urls]

    # 自适应线程数：确保总线程 16-24 之间
    if len(urls) == 1:
        threads_per_url = 12
    total_threads = len(urls) * threads_per_url

    acc = ByteAccumulator()
    acc.reset()
    q = Queue()
    workers = []
    for url in urls:
        for i in range(threads_per_url):
            t = TimedDownloader(url, duration, q, len(workers), accumulator=acc)
            workers.append(t)
            t.start()

    if callback:
        callback(f"下载中: {len(urls)}个源 × {threads_per_url}线程 = {total_threads}连接")

    # 近窗口速度 + 轻度 EMA；下载开局由 display_mbps 自己抑制虚高
    deadline = time.time() + duration + 10
    ema = 0.0
    ema_ready = False
    while any(t.is_alive() for t in workers) and time.time() < deadline:
        if progress_callback:
            raw = acc.display_mbps(prefer_average=False)
            if raw <= 0 and not ema_ready:
                progress_callback(0.0)
            else:
                if not ema_ready:
                    ema = raw
                    ema_ready = True
                else:
                    ema = ema * 0.55 + raw * 0.45
                progress_callback(ema)
        time.sleep(0.1)

    for t in workers:
        t.join(timeout=2)

    total_bytes = 0
    max_elapsed = 0
    while not q.empty():
        _, bytes_down, elapsed = q.get()
        total_bytes += bytes_down
        max_elapsed = max(max_elapsed, elapsed)

    if max_elapsed < 0.5:
        return 0, 0

    speed_mbps = total_bytes / max_elapsed * 8 / 1_000_000
    return total_bytes, speed_mbps


class UploadStream:
    """可被 urllib 边读边发的上传体：按块记账，到点停止（避免大包拖死时长）"""

    def __init__(self, total_size, chunk_size, accumulator, end_time=None):
        self.total_size = total_size
        self.sent = 0
        self.chunk = b"0" * chunk_size
        self.accumulator = accumulator
        self.end_time = end_time

    def read(self, size=-1):
        # 到点立即停，宁可连接报错，也不让单请求拖到十几秒
        if self.end_time is not None and time.time() >= self.end_time:
            return b""
        if self.sent >= self.total_size:
            return b""
        if size is None or size < 0:
            size = len(self.chunk)
        n = min(size, self.total_size - self.sent, len(self.chunk))
        if n <= 0:
            return b""
        self.sent += n
        if self.accumulator:
            self.accumulator.add(n)
        return self.chunk[:n]


class TimedUploader(threading.Thread):
    """按时间上传——1MB 流式 POST，边发边记账，到点截断"""

    # 实测：4MB 包在中低上行会拖过测试时长且出字节更晚；1MB 更稳
    PAYLOAD_SIZE = 1 * 1024 * 1024
    READ_CHUNK = 64 * 1024

    def __init__(self, url, duration, result_queue, thread_id, accumulator=None):
        super().__init__(daemon=True)
        self.url = url
        self.duration = duration
        self.result_queue = result_queue
        self.thread_id = thread_id
        self.accumulator = accumulator
        self._should_stop = False

    def stop(self):
        self._should_stop = True

    def run(self):
        total_bytes = 0
        start = time.time()
        end_time = start + self.duration
        try:
            while not self._should_stop and time.time() < end_time:
                remain = end_time - time.time()
                # 剩余不够发完一整包就停：中途截断会和 Content-Length 打架导致空等超时
                if remain < 2.0:
                    break
                body = UploadStream(
                    self.PAYLOAD_SIZE, self.READ_CHUNK,
                    self.accumulator, end_time=None,
                )
                req = urllib.request.Request(
                    self.url, data=body,
                    headers={
                        "User-Agent": "Mozilla/5.0",
                        "Content-Type": "application/octet-stream",
                        "Content-Length": str(self.PAYLOAD_SIZE),
                    },
                    method="POST",
                )
                try:
                    with urllib.request.urlopen(req, timeout=15, context=_ctx) as resp:
                        resp.read()
                    total_bytes += body.sent
                except Exception:
                    total_bytes += body.sent
                if time.time() >= end_time or self._should_stop:
                    break
        except Exception:
            pass
        elapsed = time.time() - start
        self.result_queue.put((self.thread_id, total_bytes, elapsed))


def measure_upload_timed(url, duration=UPLOAD_DURATION, threads=8,
                         callback=None, progress_callback=None):
    """多线程持续上传，返回速度 Mbps"""
    acc = ByteAccumulator()
    acc.reset()
    q = Queue()
    workers = []
    t_start = time.time()
    t_end = t_start + duration
    for i in range(threads):
        t = TimedUploader(url, duration, q, i, accumulator=acc)
        workers.append(t)
        t.start()

    ema = 0.0
    ema_ready = False
    last_good = 0.0
    last_good_t = 0.0
    # 进度只跟到正式时长；之后保峰值，不再因窗口清空掉到 0
    while time.time() < t_end:
        if progress_callback:
            raw = acc.display_mbps(prefer_average=True)
            now = time.time()
            # 请求间隙窗口可能短暂为 0：短时保持上次好值，避免指针掉零
            if raw > 0.5:
                last_good = raw
                last_good_t = now
                sample = raw
            elif last_good_t and (now - last_good_t) < 0.7:
                sample = last_good
            else:
                sample = 0.0

            if sample <= 0 and not ema_ready:
                progress_callback(0.0)
            else:
                if not ema_ready:
                    ema = sample
                    ema_ready = True
                else:
                    # 跟上峰值要快，不能那种“从 0 慢慢爬”
                    ema = ema * 0.5 + sample * 0.5
                progress_callback(ema)
        time.sleep(0.1)

    # 到点：钉住最后读数，并通知线程收工
    if progress_callback and ema_ready:
        progress_callback(ema)
    for t in workers:
        t.stop()
    join_deadline = time.time() + 3
    for t in workers:
        t.join(timeout=max(join_deadline - time.time(), 0.05))

    total_bytes = 0
    max_elapsed = 0
    while not q.empty():
        _, bytes_up, elapsed = q.get()
        total_bytes += bytes_up
        max_elapsed = max(max_elapsed, elapsed)

    # 最终成绩按首字节→末字节，排除建连空窗
    transfer_secs = acc.transfer_elapsed()
    if transfer_secs >= 0.5 and total_bytes > 0:
        speed_mbps = total_bytes / transfer_secs * 8 / 1_000_000
    elif max_elapsed >= 0.5 and total_bytes > 0:
        speed_mbps = total_bytes / max_elapsed * 8 / 1_000_000
    else:
        return 0, 0

    return total_bytes, speed_mbps


# ─── 动态省略号动画 ───

class DotAnimator:
    def __init__(self, label_var, base_text, root):
        self.label_var = label_var
        self.base_text = base_text
        self.root = root
        self.running = False
        self._job = None

    def start(self):
        self.running = True
        self._animate(0)

    def stop(self):
        self.running = False
        if self._job:
            self.root.after_cancel(self._job)

    def _animate(self, dots):
        if not self.running:
            return
        self.label_var.set(self.base_text + "." * dots)
        self._job = self.root.after(500, self._animate, (dots + 1) % 4)


# ─── 汽车仪表盘网速表 ───

class SpeedGauge:
    """汽车仪表盘式网速表：指针平滑扫动，显示实时 Mbps"""

    # 默认按家用带宽常见区间铺满；超过再抬档
    SCALE_TIERS = (1000, 2000, 5000)
    DEFAULT_MAX = 1000.0
    # 0~500Mbps 占表盘 72%（刻度疏），500~满量程占 28%（刻度密）
    LOW_BAND = 500.0
    LOW_SHARE = 0.72
    # 指针扫过角度：从左下 225° 到右下 -45°（共 270°）
    START_ANGLE = 225.0
    SWEEP = 270.0

    def __init__(self, parent, scale_fn, width, height):
        self.scale = scale_fn
        self.width = width
        self.height = height
        self.bg = "#0b1020"
        self.canvas = tk.Canvas(
            parent, width=width, height=height,
            highlightthickness=0, bg=self.bg, bd=0,
        )
        self.canvas.pack(fill="both", expand=True)

        self.max_mbps = self.DEFAULT_MAX
        self.target = 0.0
        self.current = 0.0
        self.mode = ""  # 下载 / 上传 / 空
        self._anim_job = None
        self._cx = width / 2
        self._cy = height / 2 + self.scale(8)
        self._radius = min(width, height) * 0.38

        self._draw_static()
        self._draw_dynamic(0.0)

    def _tier_for(self, speed):
        for t in self.SCALE_TIERS:
            if speed <= t * 0.92:
                return t
        return self.SCALE_TIERS[-1]

    def reset(self):
        self.max_mbps = self.DEFAULT_MAX
        self.target = 0.0
        self.current = 0.0
        self.mode = ""
        self._draw_static()
        self._draw_dynamic(0.0)

    def set_mode(self, mode):
        self.mode = mode or ""
        self._draw_dynamic(self.current)

    def set_speed(self, mbps, mode=None):
        """设置目标速度；指针用动画追赶"""
        if mode is not None:
            self.mode = mode
        mbps = max(0.0, float(mbps))
        # 量程随峰值抬升，但不频繁回落，避免指针乱跳
        need = self._tier_for(mbps)
        if need > self.max_mbps:
            self.max_mbps = float(need)
            self._draw_static()
        self.target = mbps
        if self._anim_job is None:
            self._tick()

    def speed_to_ratio(self, mbps):
        """速度 → 表盘比例（非线性）：常用低速区占更大弧长"""
        mbps = max(0.0, float(mbps))
        max_s = self.max_mbps
        if max_s <= self.LOW_BAND:
            return min(mbps / max_s, 1.0) if max_s > 0 else 0.0

        if mbps <= self.LOW_BAND:
            return (mbps / self.LOW_BAND) * self.LOW_SHARE

        high = max_s - self.LOW_BAND
        if high <= 0:
            return 1.0
        return self.LOW_SHARE + min((mbps - self.LOW_BAND) / high, 1.0) * (1.0 - self.LOW_SHARE)

    def _speed_to_angle(self, mbps):
        return self.START_ANGLE - self.speed_to_ratio(mbps) * self.SWEEP

    def _major_tick_values(self):
        """主刻度数值：0-500 疏，500 以上密"""
        max_s = int(self.max_mbps)
        ticks = [0, 50, 100, 200, 300, 400, 500]
        if max_s <= 500:
            return [t for t in ticks if t <= max_s] + ([max_s] if max_s not in ticks else [])

        # 500 以上按量程补点，间距更紧
        if max_s <= 1000:
            ticks += [750, 1000]
        elif max_s <= 2000:
            ticks += [750, 1000, 1500, 2000]
        else:
            ticks += [750, 1000, 2000, 3500, 5000]
        # 去重并裁到量程
        out = []
        for t in ticks:
            if t <= max_s and t not in out:
                out.append(t)
        if out[-1] != max_s:
            out.append(max_s)
        return out

    def _polar(self, angle_deg, r):
        rad = math.radians(angle_deg)
        return self._cx + r * math.cos(rad), self._cy - r * math.sin(rad)

    def _draw_static(self):
        c = self.canvas
        c.delete("static")
        cx, cy, r = self._cx, self._cy, self._radius

        # 外圈光晕
        c.create_oval(
            cx - r - self.scale(14), cy - r - self.scale(14),
            cx + r + self.scale(14), cy + r + self.scale(14),
            outline="#1e2a44", width=self.scale(3), tags="static",
        )
        c.create_oval(
            cx - r - self.scale(6), cy - r - self.scale(6),
            cx + r + self.scale(6), cy + r + self.scale(6),
            outline="#2a3d5c", width=self.scale(2), tags="static",
        )

        # 背景弧轨
        self._create_arc(
            r, self.START_ANGLE, self.SWEEP,
            outline="#1a2336", width=self.scale(14), tags="static",
        )

        # 色带按真实速度分段，再映射到非线性弧长
        # 0-200 青绿 · 200-500 琥珀 · 500+ 玫红
        speed_zones = [
            (0.0, min(200.0, self.max_mbps), "#1ec8a5"),
            (min(200.0, self.max_mbps), min(500.0, self.max_mbps), "#f0a202"),
            (min(500.0, self.max_mbps), self.max_mbps, "#ff4d6d"),
        ]
        for s0, s1, color in speed_zones:
            if s1 <= s0:
                continue
            a0 = self.speed_to_ratio(s0)
            a1 = self.speed_to_ratio(s1)
            start = self.START_ANGLE - a0 * self.SWEEP
            extent = (a1 - a0) * self.SWEEP
            if extent <= 0:
                continue
            self._create_arc(
                r, start, extent,
                outline=color, width=self.scale(10), tags="static",
            )

        # 主刻度（按速度值非线性落位）
        majors = self._major_tick_values()
        for i, val in enumerate(majors):
            ratio = self.speed_to_ratio(val)
            ang = self.START_ANGLE - ratio * self.SWEEP
            outer = self._polar(ang, r + self.scale(2))
            inner = self._polar(ang, r - self.scale(16))
            c.create_line(
                *inner, *outer,
                fill="#dce6f5", width=self.scale(2), tags="static",
            )
            label_pos = self._polar(ang, r - self.scale(30))
            c.create_text(
                *label_pos, text=str(int(val)),
                fill="#9fb3d1", font=("Consolas", self.scale(8)),
                tags="static",
            )
            # 相邻主刻度之间插小刻度
            if i < len(majors) - 1:
                v0, v1 = majors[i], majors[i + 1]
                steps = 4 if (v1 - v0) >= 100 else 2
                for j in range(1, steps):
                    vv = v0 + (v1 - v0) * j / steps
                    rr = self.speed_to_ratio(vv)
                    aang = self.START_ANGLE - rr * self.SWEEP
                    o2 = self._polar(aang, r + self.scale(1))
                    i2 = self._polar(aang, r - self.scale(8))
                    c.create_line(
                        *i2, *o2, fill="#3d5270",
                        width=self.scale(1), tags="static",
                    )

        # 中心盖
        hub = self.scale(16)
        c.create_oval(
            cx - hub, cy - hub, cx + hub, cy + hub,
            fill="#151c2e", outline="#4a6fa5", width=self.scale(2), tags="static",
        )

        # 标题
        c.create_text(
            cx, cy - r - self.scale(28),
            text="NET SPEED", fill="#5b7a9d",
            font=("Consolas", self.scale(9), "bold"), tags="static",
        )

    def _create_arc(self, r, start_angle, extent, **kwargs):
        """按数学角度画弧（Canvas 的 start 是 3 点钟方向逆时针）"""
        # start_angle 是数学角，extent 是顺时针扫过的度数 → Canvas 用负 extent
        cx, cy = self._cx, self._cy
        self.canvas.create_arc(
            cx - r, cy - r, cx + r, cy + r,
            start=start_angle, extent=-extent,
            style="arc", **kwargs,
        )

    def _draw_dynamic(self, mbps):
        c = self.canvas
        c.delete("dyn")
        cx, cy, r = self._cx, self._cy, self._radius
        ang = self._speed_to_angle(mbps)

        # 已走过的高亮弧（跟随非线性比例）
        if mbps > 0.5:
            ratio = self.speed_to_ratio(mbps)
            self._create_arc(
                r, self.START_ANGLE, ratio * self.SWEEP,
                outline="#7ee8ff", width=self.scale(3), tags="dyn",
            )

        # 指针
        tip = self._polar(ang, r - self.scale(22))
        base_l = self._polar(ang + 90, self.scale(5))
        base_r = self._polar(ang - 90, self.scale(5))
        tail = self._polar(ang + 180, self.scale(22))
        c.create_polygon(
            tip[0], tip[1], base_l[0], base_l[1],
            tail[0], tail[1], base_r[0], base_r[1],
            fill="#ff6b35", outline="#ffd1bf", width=1, tags="dyn",
        )
        # 针尖光点
        c.create_oval(
            tip[0] - 3, tip[1] - 3, tip[0] + 3, tip[1] + 3,
            fill="#fff0e6", outline="", tags="dyn",
        )
        # 中心钉
        pin = self.scale(7)
        c.create_oval(
            cx - pin, cy - pin, cx + pin, cy + pin,
            fill="#ff6b35", outline="#ffffff", width=1, tags="dyn",
        )

        # 模式与数字读数（与指针同一 current，保证一致）
        mode_text = self.mode if self.mode else "待机"
        mode_color = "#7ee8ff" if self.mode == "下载" else ("#f0a202" if self.mode == "上传" else "#6b7c93")
        c.create_text(
            cx, cy + self.scale(42),
            text=mode_text, fill=mode_color,
            font=("Microsoft YaHei", self.scale(11), "bold"), tags="dyn",
        )
        c.create_text(
            cx, cy + self.scale(72),
            text=f"{mbps:.1f}", fill="#ffffff",
            font=("Consolas", self.scale(22), "bold"), tags="dyn",
        )
        c.create_text(
            cx, cy + self.scale(96),
            text="Mbps", fill="#8aa0bc",
            font=("Consolas", self.scale(10)), tags="dyn",
        )

    def _tick(self):
        # 指数平滑追赶目标；上行稍快、下行稍慢，减少“先冲高再回落”的观感
        diff = self.target - self.current
        if abs(diff) < 0.08:
            self.current = self.target
            self._draw_dynamic(self.current)
            self._anim_job = None
            return
        factor = 0.22 if diff > 0 else 0.32
        self.current += diff * factor
        self._draw_dynamic(self.current)
        self._anim_job = self.canvas.after(33, self._tick)


# ─── UI ───

class SpeedTestApp:
    def __init__(self, root):
        self.root = root
        self.root.title("网速测试工具")

        self._setup_dpi()
        self.W = self.scale(580)
        self.H = self.scale(760)
        self.root.geometry(f"{self.W}x{self.H}")
        self.root.resizable(False, False)

        self.root.update_idletasks()
        # 用实际可用屏幕区域居中（排除任务栏）
        sw = self.root.winfo_screenwidth()
        sh = self.root.winfo_screenheight()
        x = max(0, (sw - self.W) // 2)
        y = max(0, (sh - self.H) // 2 - self.scale(30))  # 略偏上，但不超出屏幕
        self.root.geometry(f"{self.W}x{self.H}+{x}+{y}")

        self.is_testing = False
        self.dots_anim = None
        self._build_ui()

    def _setup_dpi(self):
        self.dpi_scale = 1.0
        try:
            import ctypes
            # 让 Windows 感知 DPI（否则 tkinter 会被虚拟化导致模糊）
            try:
                ctypes.windll.shcore.SetProcessDpiAwareness(2)  # PER_MONITOR_AWARE
            except Exception:
                try:
                    ctypes.windll.user32.SetProcessDPIAware()
                except Exception:
                    pass

            hdc = ctypes.windll.user32.GetDC(0)
            dpi = ctypes.windll.gdi32.GetDeviceCaps(hdc, 88)
            ctypes.windll.user32.ReleaseDC(0, hdc)
            if dpi > 96:
                self.dpi_scale = dpi / 96.0

            # 让 tkinter 也按 DPI 缩放
            self.root.tk.call("tk", "scaling", self.dpi_scale * 96 / 72)
        except Exception:
            pass

    def scale(self, val):
        return int(val * self.dpi_scale)

    def _font(self, size, weight="normal"):
        return ("Microsoft YaHei", self.scale(size), weight)

    def _build_ui(self):
        ttk.Label(self.root, text="网络测速工具", font=self._font(18, "bold")).pack(pady=(self.scale(16), self.scale(4)))
        ttk.Label(self.root, text="智能选源 · 定时测速 · 多线程", font=self._font(10), foreground="gray").pack(pady=(0, self.scale(6)))

        self.status_var = tk.StringVar(value="就绪 — 点击开始测速")
        ttk.Label(self.root, textvariable=self.status_var, font=self._font(11)).pack(pady=self.scale(2))

        self.progress = ttk.Progressbar(self.root, orient="horizontal", length=self.scale(460), mode="determinate")
        self.progress.pack(pady=self.scale(6))

        frame = ttk.LabelFrame(self.root, text="测试现况", padding=self.scale(10))
        frame.pack(fill="x", padx=self.scale(30), pady=self.scale(4))

        self.download_var = tk.StringVar(value="下载速度：—")
        ttk.Label(frame, textvariable=self.download_var, font=self._font(12)).pack(anchor="w", pady=self.scale(1))

        self.upload_var = tk.StringVar(value="上传速度：—")
        ttk.Label(frame, textvariable=self.upload_var, font=self._font(12)).pack(anchor="w", pady=self.scale(1))

        self.realtime_var = tk.StringVar(value="")
        ttk.Label(self.root, textvariable=self.realtime_var, font=self._font(9), foreground="blue").pack(pady=self.scale(2))

        self.btn_start = ttk.Button(self.root, text="开始测速", command=self.start_test)
        self.btn_start.pack(pady=self.scale(6))

        # 结果区容器：底层仪表盘，上层结果报告（完成后覆盖）
        self.result_area = tk.Frame(self.root, height=self.scale(340), bg="#0b1020")
        self.result_area.pack(fill="both", expand=True, padx=self.scale(20), pady=(self.scale(2), self.scale(12)))
        self.result_area.pack_propagate(False)

        self.gauge = SpeedGauge(
            self.result_area, self.scale,
            width=self.scale(520), height=self.scale(340),
        )

        # 结果覆盖层（默认隐藏，测完后盖住仪表盘）
        self.report_frame = tk.Frame(self.result_area, bg=self.root.cget("background"))
        self.report_text = tk.Text(
            self.report_frame, height=15, width=66,
            font=("Consolas", self.scale(10)),
            state="disabled", wrap="word", relief="flat",
            background=self.root.cget("background"),
        )
        self.report_text.pack(fill="both", expand=True, padx=self.scale(4), pady=self.scale(4))
        # 启动时只显示仪表盘
        self._show_gauge()

    def _show_gauge(self):
        """显示网速仪表盘，隐藏结果框"""
        self.report_frame.place_forget()
        self.gauge.reset()

    def _show_report(self):
        """结果框覆盖在仪表盘上"""
        self.report_frame.place(relx=0, rely=0, relwidth=1, relheight=1)
        self.report_frame.lift()

    def start_test(self):
        if self.is_testing:
            return
        self.is_testing = True
        self.btn_start.config(state="disabled", text="测试中...")
        self.progress["value"] = 0
        self.download_var.set("下载速度：等待中")
        self.upload_var.set("上传速度：等待中")
        self.realtime_var.set("")
        self._set_report("")
        # 再次测速：重新露出仪表，藏起结果
        self._show_gauge()
        threading.Thread(target=self._run_test, daemon=True).start()

    def _gauge_speed(self, mbps, mode):
        """线程安全地更新仪表盘指针"""
        self.root.after(0, lambda m=mbps, md=mode: self.gauge.set_speed(m, md))

    def _run_test(self):
        results = {}
        try:
            # 1. 探测所有源
            self._update_status("正在探测测速节点")
            self._start_dots("探测中")
            self._update_progress(5)
            self._gauge_speed(0, "探测")

            reachable = select_best_source(DOWNLOAD_SOURCES, callback=self._update_realtime)

            if not reachable:
                self._update_status("所有节点不可达，请检查网络")
                self._stop_dots()
                self._gauge_speed(0, "")
                return

            # 显示节点延迟到实时区域
            node_info = " / ".join(f"{n} {lat:.0f}ms" for n, _, lat in reachable)
            self._update_realtime(f"节点延迟：{node_info}")
            results["nodes"] = node_info
            self._update_progress(15)
            self._stop_dots()

            # 2. 下载测速——多 URL 多线程持续 10 秒
            self._update_status(f"正在测试下载速度（{DOWNLOAD_DURATION}秒）")
            self._update("download", "下载速度：测试中")
            self._start_dots("下载测速中")
            self._gauge_speed(0, "下载")

            # 用所有可达源（不只最优源），多 URL 并行突破 CDN 单 IP 限速
            all_urls = [url for _, url in DOWNLOAD_SOURCES]
            total_bytes, dl_speed = measure_download_timed(
                all_urls, duration=DOWNLOAD_DURATION, threads_per_url=8,
                callback=None,
                progress_callback=lambda mbps: self._gauge_speed(mbps, "下载"),
            )

            self._stop_dots()
            results["download"] = dl_speed
            self._update("download", f"下载速度：{dl_speed:.2f} Mbps")
            self._gauge_speed(dl_speed, "下载")
            self._update_progress(60)

            # 4. 上传测速——持续 10 秒
            self._update_status(f"正在测试上传速度（{UPLOAD_DURATION}秒）")
            self._update("upload", "上传速度：测试中")
            self._start_dots("上传测速中")
            self._gauge_speed(0, "上传")

            total_up, ul_speed = measure_upload_timed(
                UPLOAD_URL, duration=UPLOAD_DURATION, threads=8,
                callback=None,
                progress_callback=lambda mbps: self._gauge_speed(mbps, "上传"),
            )

            # 如果 8 线程上传偏低，自动加到 12 线程重试一次
            if ul_speed < 10:
                self._stop_dots()
                self._update_realtime(f"上传 {ul_speed:.0f} Mbps 偏低，12 线程重试...")
                self._gauge_speed(0, "上传")
                total_up, ul_speed = measure_upload_timed(
                    UPLOAD_URL, duration=UPLOAD_DURATION, threads=12,
                    callback=None,
                    progress_callback=lambda mbps: self._gauge_speed(mbps, "上传"),
                )

            self._stop_dots()
            results["upload"] = ul_speed
            self._update("upload", f"上传速度：{ul_speed:.2f} Mbps")
            self._gauge_speed(ul_speed, "上传")
            self._update_realtime(f"上传完成: {total_up/1_000_000:.1f}MB / {UPLOAD_DURATION}s = {ul_speed:.2f} Mbps")
            self._update_progress(95)

            # 5. 完成——结果框覆盖仪表盘
            self._update_progress(100)
            self._update_status("测速完成！")
            report = self._generate_report(results)
            self._set_report(report)
            self.root.after(0, self._show_report)

        except Exception as e:
            self._update_status(f"测速失败：{e}")
            self._stop_dots()
            self._gauge_speed(0, "")
        finally:
            self.is_testing = False
            self.root.after(0, lambda: self.btn_start.config(state="normal", text="开始测速"))

    def _generate_report(self, r):
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        dl = r.get("download", 0)
        ul = r.get("upload", 0)
        nodes = r.get("nodes", "—")

        if dl >= 500:
            grade = "★★★★★ 极速"
        elif dl >= 200:
            grade = "★★★★★ 优秀"
        elif dl >= 100:
            grade = "★★★★ 良好"
        elif dl >= 50:
            grade = "★★★ 不错"
        elif dl >= 20:
            grade = "★★ 一般"
        elif dl >= 5:
            grade = "★ 较慢"
        else:
            grade = " 很慢"

        return f"""╔══════════════════════════════════════╗
║         网络测速总结报告
╠══════════════════════════════════════╣
  测试时间：{now}
  节点延迟：{nodes}

  下载速度：{dl:.2f} Mbps（{dl/8:.1f} MB/s）
  上传速度：{ul:.2f} Mbps（{ul/8:.1f} MB/s）

  综合评级：{grade}
╚══════════════════════════════════════╝"""

    def _update_status(self, text):
        self.root.after(0, lambda: self.status_var.set(text))

    def _update_progress(self, value):
        self.root.after(0, lambda: self.progress.config(value=value))

    def _update(self, key, text):
        m = {"download": self.download_var, "upload": self.upload_var}
        if key in m:
            self.root.after(0, lambda: m[key].set(text))

    def _update_realtime(self, text):
        if self.dots_anim:
            self.dots_anim.stop()
            self.dots_anim = None
        self.root.after(0, lambda: self.realtime_var.set(text))

    def _start_dots(self, base_text):
        self._stop_dots()
        self.dots_anim = DotAnimator(self.realtime_var, base_text, self.root)
        self.dots_anim.start()

    def _stop_dots(self):
        if self.dots_anim:
            self.dots_anim.stop()
            self.dots_anim = None

    def _set_report(self, text):
        def _do():
            self.report_text.config(state="normal")
            self.report_text.delete("1.0", "end")
            self.report_text.insert("1.0", text)
            self.report_text.config(state="disabled")
        self.root.after(0, _do)


def _app_icon_path():
    if getattr(sys, "frozen", False):
        base = sys._MEIPASS
    else:
        base = os.path.dirname(os.path.abspath(__file__))
    p = os.path.join(base, "icon.ico")
    return p if os.path.isfile(p) else None


def main():
    root = tk.Tk()
    icon = _app_icon_path()
    if icon:
        try:
            root.iconbitmap(icon)
        except Exception:
            pass
    app = SpeedTestApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
