#!/usr/bin/env python3
"""网络测速工具 - 绿色版

测速方案（参考 speedtest-cli / cfspeed 思路，针对国内优化）：
1. 地区源池 + 延迟够用即继续（静默补探）+ 轻量摸底定档（≥300Mbps→高带宽）
2. 下载测速：多源多连接（常态≥32，高带宽≥40）持续下载 10 秒，取总吞吐量
3. 上传测速：http.client 多连接连续 POST 大包（类 cfspeed），取吞吐量
4. 延迟：TCP 连接测延迟

仪表盘显示近窗真实吞吐（非假平滑）；最终成绩仍按首末字节累计。
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
import http.client
import urllib.request
from collections import deque
from urllib.parse import urlparse
from queue import Empty, Queue

from source_selection import (
    DEFAULT_REGION,
    REGION_LABELS,
    SAMPLE_DURATION_S,
    ProbeSession,
    RegionCatalog,
    load_settings,
    locate_region_by_ip,
    plan_download_concurrency,
    plan_sample_concurrency,
    resolve_region,
    save_settings,
)


# ─── 测速核心 ───

_ctx = ssl.create_default_context()
_ctx.check_hostname = False
_ctx.verify_mode = ssl.CERT_NONE

# 强制 IPv4
_orig = socket.getaddrinfo
socket.getaddrinfo = lambda *a, **k: [r for r in _orig(*a, **k) if r[0] == socket.AF_INET]

# 兼容旧引用：默认华南源池
DOWNLOAD_SOURCES = RegionCatalog.sources_for(DEFAULT_REGION)

# 上传端点（Cloudflare 公开测速，与 cfspeed / speed.cloudflare.com 相同）
UPLOAD_URL = "https://speed.cloudflare.com/__up"

# 测速持续时间（秒）
DOWNLOAD_DURATION = 10
UPLOAD_DURATION = 10


class ByteAccumulator:
    """线程安全的字节累计器。

    最终成绩用首末字节累计；仪表用「近窗吞吐 + 轻度 EMA」，
    避免累计均值那种「冲到一百多再拖十几秒慢慢掉到六十」的假掉速感。
    """

    LIVE_WINDOW_S = 2.0
    # EMA 时间常数（秒）：只做轻度跟上，不是假平滑把成绩拧到别处
    LIVE_EMA_TAU_S = 0.8

    def __init__(self):
        self._lock = threading.Lock()
        self.total = 0
        self._first_byte_t = None
        self._last_byte_t = None
        self._samples = deque()  # (t, cumulative_total)
        self._ema_mbps = None
        self._ema_t = None

    def reset(self):
        with self._lock:
            self.total = 0
            self._first_byte_t = None
            self._last_byte_t = None
            self._samples.clear()
            self._ema_mbps = None
            self._ema_t = None

    def add(self, n):
        if n <= 0:
            return
        now = time.time()
        with self._lock:
            if self._first_byte_t is None:
                self._first_byte_t = now
            self._last_byte_t = now
            self.total += n
            self._samples.append((now, self.total))
            cut = now - self.LIVE_WINDOW_S - 0.5
            while len(self._samples) > 2 and self._samples[0][0] < cut:
                self._samples.popleft()

    def transfer_elapsed(self):
        """首字节→末字节"""
        with self._lock:
            if self._first_byte_t is None or self._last_byte_t is None:
                return 0.0
            return max(self._last_byte_t - self._first_byte_t, 0.0)

    def average_mbps(self):
        """自首字节起的真实累计吞吐（Mbps）"""
        now = time.time()
        with self._lock:
            if self._first_byte_t is None or self.total <= 0:
                return 0.0
            elapsed = max(now - self._first_byte_t, 0.001)
            return self.total / elapsed * 8 / 1_000_000

    def _window_mbps_unlocked(self, now: float) -> float:
        elapsed = now - self._first_byte_t
        cut = now - self.LIVE_WINDOW_S
        t0 = self._first_byte_t
        b0 = 0
        for t, b in self._samples:
            if t <= cut:
                t0, b0 = t, b
            else:
                break
        dt = now - t0
        if dt < 0.2:
            return self.total / max(elapsed, 0.001) * 8 / 1_000_000
        return (self.total - b0) / dt * 8 / 1_000_000

    def live_mbps(self):
        """仪表读数：近窗真实吞吐再轻度 EMA。"""
        with self._lock:
            if self._first_byte_t is None or self.total <= 0:
                return 0.0
            now = time.time()
            elapsed = now - self._first_byte_t
            if elapsed < 0.35:
                return 0.0
            instant = self._window_mbps_unlocked(now)
            if self._ema_mbps is None:
                self._ema_mbps = instant
                self._ema_t = now
                return instant
            dt = max(now - (self._ema_t or now), 0.0)
            self._ema_t = now
            # alpha = 1 - e^(-dt/tau)：跟上真实变化，但不跟每个突发毛刺死磕
            alpha = 1.0 - math.exp(-dt / self.LIVE_EMA_TAU_S) if dt > 0 else 1.0
            alpha = min(max(alpha, 0.0), 1.0)
            self._ema_mbps = self._ema_mbps * (1.0 - alpha) + instant * alpha
            return self._ema_mbps


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


class TimedDownloader(threading.Thread):
    """按时间下载——持续下载指定秒数，统计总字节数"""

    def __init__(self, url, duration, result_queue, thread_id, accumulator=None, stop_event=None):
        super().__init__(daemon=True)
        self.url = url
        self.duration = duration
        self.result_queue = result_queue
        self.thread_id = thread_id
        self.accumulator = accumulator
        self.stop_event = stop_event
        self._should_stop = False
        self._resp = None

    def stop(self):
        self._should_stop = True
        # 立刻掐断连接，避免 join 超时后线程仍在读、拖累上传/卡 UI
        resp = self._resp
        if resp is not None:
            try:
                resp.close()
            except Exception:
                pass

    def _stopped(self):
        return self._should_stop or (self.stop_event is not None and self.stop_event.is_set())

    def run(self):
        total_bytes = 0
        start = time.time()
        try:
            req = urllib.request.Request(self.url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=self.duration + 5, context=_ctx) as resp:
                self._resp = resp
                try:
                    while not self._stopped() and (time.time() - start) < self.duration:
                        chunk = resp.read(131072)  # 128KB
                        if not chunk:
                            break
                        total_bytes += len(chunk)
                        if self.accumulator:
                            self.accumulator.add(len(chunk))
                finally:
                    self._resp = None
        except Exception:
            pass
        elapsed = time.time() - start
        self.result_queue.put((self.thread_id, total_bytes, elapsed))


def measure_download_timed(urls, duration=DOWNLOAD_DURATION, threads_per_url=None,
                           callback=None, progress_callback=None, stop_event=None,
                           high_bandwidth=False):
    """多 URL 多线程持续下载，返回 (总字节, 速度Mbps)"""
    if isinstance(urls, str):
        urls = [urls]

    if threads_per_url is None:
        threads_per_url, total_threads = plan_download_concurrency(
            len(urls), high_bandwidth=high_bandwidth,
        )
    else:
        total_threads = len(urls) * threads_per_url

    acc = ByteAccumulator()
    acc.reset()
    q = Queue()
    workers = []
    for url in urls:
        for _ in range(threads_per_url):
            t = TimedDownloader(
                url, duration, q, len(workers),
                accumulator=acc, stop_event=stop_event,
            )
            workers.append(t)
            t.start()

    if callback:
        callback(f"下载中: {len(urls)}个源 × {threads_per_url}线程 = {total_threads}连接")

    t_end = time.time() + duration
    while time.time() < t_end:
        if stop_event is not None and stop_event.is_set():
            break
        if progress_callback:
            # 真实累计吞吐，无 EMA / 假保持
            progress_callback(acc.live_mbps())
        time.sleep(0.1)

    for t in workers:
        t.stop()
    join_deadline = time.time() + 3
    for t in workers:
        t.join(timeout=max(join_deadline - time.time(), 0.05))

    total_bytes = acc.total
    transfer_secs = acc.transfer_elapsed()
    if transfer_secs >= 0.5 and total_bytes > 0:
        return total_bytes, total_bytes / transfer_secs * 8 / 1_000_000

    max_elapsed = 0
    queued = 0
    while not q.empty():
        _, bytes_down, elapsed = q.get()
        queued += bytes_down
        max_elapsed = max(max_elapsed, elapsed)
    if max_elapsed >= 0.5 and queued > 0:
        return queued, queued / max_elapsed * 8 / 1_000_000
    return 0, 0


class ContinuousUploader(threading.Thread):
    """并行连续上传（对齐 cfspeed：每流约 10MB，循环 POST，http.client 边发边记账）

    不用“整段测速预留数 GB Content-Length”——实测会导致首字节极慢。
    到点或停止时关连接；字节按实际 send 累计，仪表用真实累计吞吐。
    """

    CHUNK = 256 * 1024          # 256KB
    # 20MB：比 cfspeed 默认 10MB 更长连发，减少请求间隙；又远小于“数 GB 预留”那种首字节迟滞
    PAYLOAD_SIZE = 20 * 1024 * 1024

    def __init__(self, url, duration, result_queue, thread_id, accumulator=None, stop_event=None):
        super().__init__(daemon=True)
        self.url = url
        self.duration = duration
        self.result_queue = result_queue
        self.thread_id = thread_id
        self.accumulator = accumulator
        self.stop_event = stop_event
        self._should_stop = False
        self._conn = None

    def stop(self):
        self._should_stop = True
        conn = self._conn
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    def _stopped(self):
        return self._should_stop or (self.stop_event is not None and self.stop_event.is_set())

    def run(self):
        parsed = urlparse(self.url)
        host = parsed.hostname
        port = parsed.port or 443
        path = parsed.path or "/__up"
        if parsed.query:
            path = f"{path}?{parsed.query}"

        total_bytes = 0
        start = time.time()
        end_time = start + self.duration
        chunk = b"0" * self.CHUNK

        try:
            while not self._stopped() and time.time() < end_time:
                remain_time = end_time - time.time()
                # 剩余太短不再开新 10MB 请求
                if remain_time < 1.0:
                    break
                conn = None
                try:
                    conn = http.client.HTTPSConnection(
                        host, port, context=_ctx, timeout=min(20.0, remain_time + 5.0),
                    )
                    self._conn = conn
                    size = self.PAYLOAD_SIZE
                    conn.putrequest("POST", path)
                    conn.putheader("User-Agent", "Mozilla/5.0")
                    conn.putheader("Content-Type", "application/octet-stream")
                    conn.putheader("Content-Length", str(size))
                    conn.endheaders()

                    sent = 0
                    while not self._stopped() and time.time() < end_time and sent < size:
                        n = min(len(chunk), size - sent)
                        conn.send(chunk[:n])
                        sent += n
                        total_bytes += n
                        if self.accumulator:
                            self.accumulator.add(n)

                    # 正常发完则读掉响应，便于连接干净结束
                    if sent >= size and not self._stopped() and time.time() < end_time:
                        try:
                            resp = conn.getresponse()
                            resp.read()
                        except Exception:
                            pass
                except Exception:
                    if self._stopped() or time.time() >= end_time:
                        break
                    time.sleep(0.05)
                finally:
                    self._conn = None
                    if conn is not None:
                        try:
                            conn.close()
                        except Exception:
                            pass
        except Exception:
            pass

        self.result_queue.put((self.thread_id, total_bytes, time.time() - start))


def measure_upload_timed(url, duration=UPLOAD_DURATION, threads=8,
                         callback=None, progress_callback=None, stop_event=None):
    """多连接连续上传，返回 (总字节, 速度Mbps)"""
    acc = ByteAccumulator()
    acc.reset()
    q = Queue()
    workers = []
    t_end = time.time() + duration

    for i in range(threads):
        t = ContinuousUploader(
            url, duration, q, i,
            accumulator=acc, stop_event=stop_event,
        )
        workers.append(t)
        t.start()
        # 错开建连，减轻「开局八条一起冲」造成的仪表虚高
        if i + 1 < threads:
            time.sleep(0.08)

    while time.time() < t_end:
        if stop_event is not None and stop_event.is_set():
            break
        if progress_callback:
            # 真实累计吞吐，绝不做 hold-last / EMA 造假
            progress_callback(acc.live_mbps())
        time.sleep(0.1)

    for t in workers:
        t.stop()
    join_deadline = time.time() + 2
    for t in workers:
        t.join(timeout=max(join_deadline - time.time(), 0.05))

    # 成绩以累计器为准（与仪表同一数据源），避免和队列汇总不一致
    total_bytes = acc.total
    transfer_secs = acc.transfer_elapsed()
    if transfer_secs >= 0.5 and total_bytes > 0:
        return total_bytes, total_bytes / transfer_secs * 8 / 1_000_000

    # 兜底：线程回报
    max_elapsed = 0
    queued = 0
    while not q.empty():
        _, bytes_up, elapsed = q.get()
        queued += bytes_up
        max_elapsed = max(max_elapsed, elapsed)
    if max_elapsed >= 0.5 and queued > 0:
        return queued, queued / max_elapsed * 8 / 1_000_000
    return 0, 0


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
            try:
                self.root.after_cancel(self._job)
            except Exception:
                pass
            self._job = None

    def _animate(self, dots):
        if not self.running:
            return
        self.label_var.set(self.base_text + "." * dots)
        self._job = self.root.after(400, self._animate, (dots + 1) % 4)


# ─── 汽车仪表盘网速表 ───

class SpeedGauge:
    """汽车仪表盘式网速表：数字显示真实值，指针快速跟上"""

    SCALE_TIERS = (1000, 2000, 5000)
    DEFAULT_MAX = 1000.0
    LOW_BAND = 500.0
    LOW_SHARE = 0.72
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
        self.mode = ""
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
        if self._anim_job:
            try:
                self.canvas.after_cancel(self._anim_job)
            except Exception:
                pass
            self._anim_job = None
        self.max_mbps = self.DEFAULT_MAX
        self.target = 0.0
        self.current = 0.0
        self.mode = ""
        self._draw_static()
        self._draw_dynamic(0.0)

    def set_speed(self, mbps, mode=None, snap=False):
        """设置真实速度；数字直接显示该值。

        snap=True：换阶段时瞬间归零/就位，避免上传阶段还从下载峰值慢慢掉下来。
        """
        mode_changed = mode is not None and mode != self.mode
        if mode is not None:
            self.mode = mode
        mbps = max(0.0, float(mbps))
        need = self._tier_for(mbps)
        if need > self.max_mbps:
            self.max_mbps = float(need)
            self._draw_static()
        self.target = mbps
        if snap or mode_changed:
            # 阶段切换：取消旧动画，指针与数字立刻跟上
            if self._anim_job:
                try:
                    self.canvas.after_cancel(self._anim_job)
                except Exception:
                    pass
                self._anim_job = None
            self.current = mbps
            self._draw_dynamic(self.current, self.target)
            return
        # 数字用真实值；指针快速追上（不做慢爬阻尼）
        if self._anim_job is None:
            self._tick()

    def speed_to_ratio(self, mbps):
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
        max_s = int(self.max_mbps)
        ticks = [0, 50, 100, 200, 300, 400, 500]
        if max_s <= 500:
            return [t for t in ticks if t <= max_s] + ([max_s] if max_s not in ticks else [])
        if max_s <= 1000:
            ticks += [750, 1000]
        elif max_s <= 2000:
            ticks += [750, 1000, 1500, 2000]
        else:
            ticks += [750, 1000, 2000, 3500, 5000]
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
        self._create_arc(
            r, self.START_ANGLE, self.SWEEP,
            outline="#1a2336", width=self.scale(14), tags="static",
        )

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

        majors = self._major_tick_values()
        for i, val in enumerate(majors):
            ratio = self.speed_to_ratio(val)
            ang = self.START_ANGLE - ratio * self.SWEEP
            outer = self._polar(ang, r + self.scale(2))
            inner = self._polar(ang, r - self.scale(16))
            c.create_line(*inner, *outer, fill="#dce6f5", width=self.scale(2), tags="static")
            label_pos = self._polar(ang, r - self.scale(30))
            c.create_text(
                *label_pos, text=str(int(val)),
                fill="#9fb3d1", font=("Consolas", self.scale(8)), tags="static",
            )
            if i < len(majors) - 1:
                v0, v1 = majors[i], majors[i + 1]
                steps = 4 if (v1 - v0) >= 100 else 2
                for j in range(1, steps):
                    vv = v0 + (v1 - v0) * j / steps
                    rr = self.speed_to_ratio(vv)
                    aang = self.START_ANGLE - rr * self.SWEEP
                    o2 = self._polar(aang, r + self.scale(1))
                    i2 = self._polar(aang, r - self.scale(8))
                    c.create_line(*i2, *o2, fill="#3d5270", width=self.scale(1), tags="static")

        hub = self.scale(16)
        c.create_oval(
            cx - hub, cy - hub, cx + hub, cy + hub,
            fill="#151c2e", outline="#4a6fa5", width=self.scale(2), tags="static",
        )
        c.create_text(
            cx, cy - r - self.scale(28),
            text="NET SPEED", fill="#5b7a9d",
            font=("Consolas", self.scale(9), "bold"), tags="static",
        )

    def _create_arc(self, r, start_angle, extent, **kwargs):
        cx, cy = self._cx, self._cy
        self.canvas.create_arc(
            cx - r, cy - r, cx + r, cy + r,
            start=start_angle, extent=-extent,
            style="arc", **kwargs,
        )

    def _draw_dynamic(self, needle_mbps, text_mbps=None):
        if text_mbps is None:
            text_mbps = needle_mbps
        c = self.canvas
        c.delete("dyn")
        cx, cy, r = self._cx, self._cy, self._radius
        ang = self._speed_to_angle(needle_mbps)

        if needle_mbps > 0.5:
            ratio = self.speed_to_ratio(needle_mbps)
            self._create_arc(
                r, self.START_ANGLE, ratio * self.SWEEP,
                outline="#7ee8ff", width=self.scale(3), tags="dyn",
            )

        tip = self._polar(ang, r - self.scale(22))
        base_l = self._polar(ang + 90, self.scale(5))
        base_r = self._polar(ang - 90, self.scale(5))
        tail = self._polar(ang + 180, self.scale(22))
        c.create_polygon(
            tip[0], tip[1], base_l[0], base_l[1],
            tail[0], tail[1], base_r[0], base_r[1],
            fill="#ff6b35", outline="#ffd1bf", width=1, tags="dyn",
        )
        c.create_oval(
            tip[0] - 3, tip[1] - 3, tip[0] + 3, tip[1] + 3,
            fill="#fff0e6", outline="", tags="dyn",
        )
        pin = self.scale(7)
        c.create_oval(
            cx - pin, cy - pin, cx + pin, cy + pin,
            fill="#ff6b35", outline="#ffffff", width=1, tags="dyn",
        )

        mode_text = self.mode if self.mode else "待机"
        mode_color = "#7ee8ff" if self.mode == "下载" else ("#f0a202" if self.mode == "上传" else "#6b7c93")
        c.create_text(
            cx, cy + self.scale(42),
            text=mode_text, fill=mode_color,
            font=("Microsoft YaHei", self.scale(11), "bold"), tags="dyn",
        )
        # 数字始终是真实回调值
        c.create_text(
            cx, cy + self.scale(72),
            text=f"{text_mbps:.1f}", fill="#ffffff",
            font=("Consolas", self.scale(22), "bold"), tags="dyn",
        )
        c.create_text(
            cx, cy + self.scale(96),
            text="Mbps", fill="#8aa0bc",
            font=("Consolas", self.scale(10)), tags="dyn",
        )

    def _tick(self):
        diff = self.target - self.current
        if abs(diff) < 0.15:
            self.current = self.target
            self._draw_dynamic(self.current, self.target)
            self._anim_job = None
            return
        # 快速跟上真实值，不做慢爬
        self.current += diff * 0.55
        self._draw_dynamic(self.current, self.target)
        self._anim_job = self.canvas.after(33, self._tick)


# ─── UI ───

class SpeedTestApp:
    def __init__(self, root):
        self.root = root
        self.root.title("网速测试工具")

        self._setup_dpi()
        self.W = self.scale(580)
        self.H = self.scale(820)
        self.root.geometry(f"{self.W}x{self.H}")
        self.root.resizable(False, False)

        self.root.update_idletasks()
        sw = self.root.winfo_screenwidth()
        sh = self.root.winfo_screenheight()
        x = max(0, (sw - self.W) // 2)
        y = max(0, (sh - self.H) // 2 - self.scale(30))
        self.root.geometry(f"{self.W}x{self.H}+{x}+{y}")

        self.is_testing = False
        self._closing = False
        self.stop_event = threading.Event()
        self.dots = {"download": None, "upload": None, "realtime": None}
        self._workers_lock = threading.Lock()

        self.settings = load_settings()
        # 探测内轻量摸底 → high_bandwidth；必须接到正式下载
        self.probe_session = ProbeSession(
            sample_fn=self._bandwidth_sample,
            on_update=self._on_probe_session_updated,
        )
        self._probe_lock = threading.Lock()
        self._probing = False
        self._probe_pending = None
        self._ui_queue = Queue()
        # 仪表更新合并：避免下载 10s×10Hz 灌爆队列导致卡顿，以及上传阶段仍刷到旧下载值
        self._gauge_gen = 0
        self._gauge_pending = None  # (gen, mbps, mode, snap)
        self._gauge_flush_queued = False

        self._build_ui()
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)
        # 主线程消费 UI 队列（后台线程禁止直接 after）
        self.root.after(50, self._poll_ui)
        # 启动后台预热探测（会话复用）
        self.root.after(100, self._start_background_probe)

    def _setup_dpi(self):
        self.dpi_scale = 1.0
        try:
            import ctypes
            try:
                ctypes.windll.shcore.SetProcessDpiAwareness(2)
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
            self.root.tk.call("tk", "scaling", self.dpi_scale * 96 / 72)
        except Exception:
            pass

    def scale(self, val):
        return int(val * self.dpi_scale)

    def _font(self, size, weight="normal"):
        return ("Microsoft YaHei", self.scale(size), weight)

    def _build_ui(self):
        ttk.Label(self.root, text="网络测速工具", font=self._font(18, "bold")).pack(pady=(self.scale(12), self.scale(2)))
        ttk.Label(self.root, text="地区择优 · 定时测速 · 多线程", font=self._font(10), foreground="gray").pack(pady=(0, self.scale(4)))

        self.status_var = tk.StringVar(value="正在准备测速节点…")
        ttk.Label(self.root, textvariable=self.status_var, font=self._font(11)).pack(pady=self.scale(2))

        self.progress = ttk.Progressbar(self.root, orient="horizontal", length=self.scale(460), mode="determinate")
        self.progress.pack(pady=self.scale(4))

        # 地区选择（只能改地区；具体源由软件自动择优）
        src_frame = ttk.LabelFrame(self.root, text="测速地区", padding=self.scale(8))
        src_frame.pack(fill="x", padx=self.scale(30), pady=self.scale(2))

        row = ttk.Frame(src_frame)
        row.pack(fill="x")
        ttk.Label(row, text="地区", font=self._font(10)).pack(side="left", padx=(0, self.scale(6)))

        self._region_ids = RegionCatalog.region_ids()
        self._region_labels = [REGION_LABELS.get(r, r) for r in self._region_ids]
        cur = self.settings.get("region") or DEFAULT_REGION
        if cur not in self._region_ids:
            cur = DEFAULT_REGION
        self.region_var = tk.StringVar(value=REGION_LABELS.get(cur, "华南"))
        self.region_combo = ttk.Combobox(
            row, textvariable=self.region_var, values=self._region_labels,
            state="readonly", width=10, font=self._font(10),
        )
        self.region_combo.pack(side="left")
        self.region_combo.bind("<<ComboboxSelected>>", self._on_region_changed)

        self.auto_locate_var = tk.BooleanVar(value=bool(self.settings.get("auto_locate", False)))
        self.auto_locate_cb = ttk.Checkbutton(
            row, text="自动定位", variable=self.auto_locate_var,
            command=self._on_auto_locate_toggled,
        )
        self.auto_locate_cb.pack(side="left", padx=(self.scale(12), 0))

        self.btn_reprobe = ttk.Button(row, text="重新探测", command=self._on_reprobe_clicked)
        self.btn_reprobe.pack(side="right")

        # 延迟数字变大时单行会裁切：固定换行宽度，保证三源+高延迟也能看全
        self._text_wrap = self.scale(500)
        self.sources_var = tk.StringVar(value="测速源：探测中…")
        ttk.Label(
            src_frame, textvariable=self.sources_var, font=self._font(9),
            foreground="gray", wraplength=self._text_wrap, justify="left",
        ).pack(anchor="w", pady=(self.scale(4), 0), fill="x")

        frame = ttk.LabelFrame(self.root, text="测试现况", padding=self.scale(8))
        frame.pack(fill="x", padx=self.scale(30), pady=self.scale(2))

        self.download_var = tk.StringVar(value="下载速度：—")
        ttk.Label(frame, textvariable=self.download_var, font=self._font(12)).pack(anchor="w", pady=self.scale(1))

        self.upload_var = tk.StringVar(value="上传速度：—")
        ttk.Label(frame, textvariable=self.upload_var, font=self._font(12)).pack(anchor="w", pady=self.scale(1))

        self.realtime_var = tk.StringVar(value="")
        ttk.Label(
            self.root, textvariable=self.realtime_var, font=self._font(9),
            foreground="blue", wraplength=self._text_wrap, justify="left",
        ).pack(pady=self.scale(2), padx=self.scale(30), fill="x")

        self.btn_start = ttk.Button(self.root, text="开始测速", command=self.on_button)
        self.btn_start.pack(pady=self.scale(4))

        self.result_area = tk.Frame(self.root, height=self.scale(340), bg="#0b1020")
        self.result_area.pack(fill="both", expand=True, padx=self.scale(20), pady=(self.scale(2), self.scale(12)))
        self.result_area.pack_propagate(False)

        self.gauge = SpeedGauge(
            self.result_area, self.scale,
            width=self.scale(520), height=self.scale(340),
        )

        self.report_frame = tk.Frame(self.result_area, bg=self.root.cget("background"))
        self.report_text = tk.Text(
            self.report_frame, height=15, width=66,
            font=("Consolas", self.scale(10)),
            state="disabled", wrap="word", relief="flat",
            background=self.root.cget("background"),
        )
        self.report_text.pack(fill="both", expand=True, padx=self.scale(4), pady=self.scale(4))
        self._show_gauge()

    def _show_gauge(self):
        self.report_frame.place_forget()
        self.gauge.reset()

    def _show_report(self):
        self.report_frame.place(relx=0, rely=0, relwidth=1, relheight=1)
        self.report_frame.lift()

    def on_button(self):
        if self.is_testing:
            self.stop_test()
        else:
            self.start_test()

    def _bandwidth_sample(self, urls, stop_event=None):
        """探测会话注入的轻量摸底：约 2 秒、总连接约 8～12，不进最终成绩。"""
        if not urls:
            return 0.0
        tpu, _ = plan_sample_concurrency(len(urls))
        _, mbps = measure_download_timed(
            urls,
            duration=SAMPLE_DURATION_S,
            threads_per_url=tpu,
            stop_event=stop_event,
        )
        return float(mbps)

    def _on_probe_session_updated(self):
        """静默补探更新择优后刷新文案（测速中不改）。"""
        if self._closing or self.is_testing:
            return
        summary = self.probe_session.summary_text()
        hb = self.probe_session.high_bandwidth
        urls = self.probe_session.selected_urls()

        def _apply():
            if self._closing or self.is_testing:
                return
            if urls:
                self.sources_var.set(f"测速源：{summary}")
                extra = "（高带宽档）" if hb else ""
                if not self._probing:
                    self.status_var.set(f"就绪 — 点击开始测速{extra}")
            else:
                self.sources_var.set("测速源：无可用节点")

        self._ui(_apply)

    def start_test(self):
        if self.is_testing:
            return
        self.is_testing = True
        self.stop_event.clear()
        self.btn_start.config(state="normal", text="停止测试")
        # 测速中禁止换区 / 重探 / 自动定位，避免踩坏会话缓存
        self._set_region_controls_enabled(False)
        # 注意：静默补探要等节点就绪后再冻结，否则探测中开测会锁死在早期烂源→上下载 0
        self.progress["value"] = 0
        self.realtime_var.set("")
        self._set_report("")
        self._show_gauge()
        # 主线程快照地区，供后台测速线程使用（勿在子线程读 Tk 变量）
        self._test_region_id = self._current_region_id()

        # 现况：下载/上传都进入「等待中...」流转
        self._start_dots("download", "下载速度：等待中")
        self._start_dots("upload", "上传速度：等待中")

        threading.Thread(target=self._run_test, daemon=True).start()

    def stop_test(self):
        if not self.is_testing:
            return
        self.stop_event.set()
        self._update_status("正在停止...")
        # 必须在主线程改按钮；收工后也由主线程轮询复位（后台 after 在 Windows 上不可靠）
        try:
            self.btn_start.config(state="disabled", text="停止中...")
        except Exception:
            pass
        self._schedule_idle_button()

    def _schedule_idle_button(self):
        """主线程等待 is_testing 结束后恢复「开始测速」"""
        if self._closing:
            return
        if not self.is_testing:
            try:
                self.btn_start.config(state="normal", text="开始测速")
                self._set_region_controls_enabled(True)
                # 后台线程的状态更新若丢失，主线程补上
                if self.status_var.get() == "正在停止...":
                    self.status_var.set("测试已停止")
            except Exception:
                pass
            return
        self.root.after(100, self._schedule_idle_button)

    def _set_region_controls_enabled(self, enabled: bool):
        """测速期间锁定地区相关控件，防止并发探测踩会话。"""
        try:
            self.region_combo.config(state="readonly" if enabled else "disabled")
            self.btn_reprobe.config(state="normal" if enabled else "disabled")
            self.auto_locate_cb.config(state="normal" if enabled else "disabled")
        except Exception:
            pass

    def on_close(self):
        """右上角关闭：先停测速再销毁窗口，避免线程/动画悬挂"""
        if self._closing:
            return
        self._closing = True
        self.stop_event.set()
        self._stop_all_dots()
        try:
            self.gauge.reset()
        except Exception:
            pass
        try:
            self.status_var.set("正在退出...")
        except Exception:
            pass
        # 给工作线程一点时间收工关连接
        self.root.after(200, self._destroy_app)

    def _destroy_app(self):
        try:
            self._stop_all_dots()
            self.gauge.reset()
        except Exception:
            pass
        try:
            self.root.destroy()
        except Exception:
            pass

    def _gauge_speed(self, mbps, mode, snap=False, expect_gen=None):
        """线程安全更新仪表；同阶段只保留最新一帧，换阶段可 snap 归零。

        expect_gen：进度回调传入开测时的代际；若阶段已切换则直接丢弃，
        避免「检查通过后、写入前」被 snap 插队，把下载峰值刷进上传阶段。
        """
        if snap:
            self._gauge_gen += 1
        gen = self._gauge_gen
        if expect_gen is not None and expect_gen != gen:
            return
        self._gauge_pending = (gen, float(mbps), mode, bool(snap))
        # 队列里只挂一次 flush，后续进度只改 pending，减轻下载尾段卡顿
        if not self._gauge_flush_queued:
            self._gauge_flush_queued = True
            self._ui(self._flush_gauge)

    def _flush_gauge(self):
        pending = self._gauge_pending
        if pending is None:
            self._gauge_flush_queued = False
            return
        gen, mbps, mode, snap = pending
        if gen != self._gauge_gen:
            self._gauge_flush_queued = False
            return
        try:
            self.gauge.set_speed(mbps, mode, snap=snap)
        except Exception:
            pass
        # 绘制期间若又有新进度，补排一次；否则放开标志
        if self._gauge_pending is not pending:
            self._ui(self._flush_gauge)
        else:
            self._gauge_flush_queued = False

    def _stopped(self):
        return self.stop_event.is_set() or self._closing

    def _label_to_region_id(self, label: str) -> str:
        for rid, lab in REGION_LABELS.items():
            if lab == label:
                return rid
        return DEFAULT_REGION

    def _current_region_id(self) -> str:
        return self._label_to_region_id(self.region_var.get())

    def _persist_settings(self):
        self.settings["region"] = self._current_region_id()
        self.settings["auto_locate"] = bool(self.auto_locate_var.get())
        save_settings(self.settings)

    def _start_background_probe(self):
        """启动或换区后的后台探测。"""
        if self._closing or self.is_testing:
            return
        region = self._current_region_id()
        auto = bool(self.auto_locate_var.get())
        threading.Thread(
            target=self._probe_worker, args=(False, region, auto), daemon=True,
        ).start()

    def _on_reprobe_clicked(self):
        if self.is_testing or self._probing:
            return
        self.probe_session.invalidate()
        self.sources_var.set("测速源：重新探测中…")
        self.status_var.set("正在重新探测节点…")
        region = self._current_region_id()
        auto = bool(self.auto_locate_var.get())
        threading.Thread(
            target=self._probe_worker, args=(True, region, auto), daemon=True,
        ).start()

    def _on_region_changed(self, _event=None):
        if self.is_testing:
            return
        if self.auto_locate_var.get():
            # 手改地区时关闭自动定位，避免下次又被覆盖
            self.auto_locate_var.set(False)
        self._persist_settings()
        self.probe_session.invalidate()
        self.sources_var.set("测速源：探测中…")
        self.status_var.set("地区已切换，正在探测…")
        region = self._current_region_id()
        auto = bool(self.auto_locate_var.get())
        threading.Thread(
            target=self._probe_worker, args=(True, region, auto), daemon=True,
        ).start()

    def _on_auto_locate_toggled(self):
        if self.is_testing:
            # 控件已禁用；若仍触发则忽略，避免测速中改会话
            return
        self._persist_settings()
        if not self.auto_locate_var.get():
            return
        self.status_var.set("正在自动定位…")
        self.sources_var.set("测速源：定位中…")

        def _job():
            region = resolve_region(self.settings, locate_fn=locate_region_by_ip)
            label = REGION_LABELS.get(region, "华南")

            def _apply():
                if self._closing or self.is_testing:
                    return
                self.region_var.set(label)
                self.settings["region"] = region
                save_settings(self.settings)
                self.probe_session.invalidate()
                self._start_background_probe()

            self._ui(_apply)

        threading.Thread(target=_job, daemon=True).start()

    def _probe_worker(self, force: bool, region_id: str, auto_locate: bool):
        with self._probe_lock:
            # 合并并发探测请求；正在探测时记下待办，结束后再跑一轮
            if self._probing:
                self._probe_pending = (region_id, auto_locate, force)
                return
            self._probing = True
            self._probe_pending = None
        try:
            while True:
                if self._closing:
                    break
                # 测速已开始：非强制探测退出，交给测速线程 ensure_probed 等待/复用
                if self.is_testing and not force:
                    break
                region = resolve_region(
                    {"region": region_id, "auto_locate": auto_locate},
                    locate_fn=locate_region_by_ip if auto_locate else None,
                )
                label = REGION_LABELS.get(region, "华南")
                if not self.is_testing:
                    self._ui(lambda lab=label: self.region_var.set(lab))

                if not self.is_testing:
                    self._ui(lambda: self.status_var.set("正在探测节点…"))
                selected = self.probe_session.ensure_probed(region)
                summary = self.probe_session.summary_text()
                hb = self.probe_session.high_bandwidth

                def _done(sel=selected, summ=summary, high_bw=hb):
                    if self._closing:
                        return
                    if sel:
                        self.sources_var.set(f"测速源：{summ}")
                        if not self.is_testing:
                            extra = "（高带宽档）" if high_bw else ""
                            self.status_var.set(f"就绪 — 点击开始测速{extra}")
                    else:
                        self.sources_var.set("测速源：无可用节点")
                        if not self.is_testing:
                            self.status_var.set("节点不可达，请检查网络或换地区")

                self._ui(_done)

                with self._probe_lock:
                    pending = self._probe_pending
                    self._probe_pending = None
                    if not pending or self.is_testing:
                        break
                    region_id, auto_locate, force = pending
        finally:
            with self._probe_lock:
                self._probing = False

    def _wait_probe_ready(self, timeout: float = 8.0) -> bool:
        """开测前确保会话已探测+摸底；必要时补探。

        必须等摸底线程真正结束再开正式下载，否则会与摸底抢带宽出现 0 Mbps。
        """
        region = getattr(self, "_test_region_id", None) or DEFAULT_REGION
        need = not (
            self.probe_session.ready
            and self.probe_session.selected_urls()
            and self.probe_session.region == region
            and self.probe_session.sample_done
        )
        if need:
            self._update_status("正在探测节点并摸底带宽…")
            self.probe_session.ensure_probed(region, stop_event=self.stop_event)
        # 先短等；若仍在 sampling，再等到结束（防呆：与正式上/下载叠流会出现 0 Mbps）
        self.probe_session.wait_for_sample(1.0, stop_event=self.stop_event)
        if (not self.probe_session.sample_done) or self.probe_session.sampling:
            self.probe_session.wait_for_sample(5.0, stop_event=self.stop_event)
        # 源偏少时再等静默补探凑源（此时尚未 freeze silent_updates）
        if len(self.probe_session.selected_urls()) < 3:
            for _ in range(20):
                if self._stopped():
                    break
                if len(self.probe_session.selected_urls()) >= 3:
                    break
                time.sleep(0.1)
        return bool(self.probe_session.selected_urls())

    def _run_test(self):
        results = {}
        stopped = False
        try:
            self._update_status("正在确认测速节点")
            self._update_progress(5)
            self._gauge_speed(0, "探测", snap=True)

            if not self._wait_probe_ready():
                if self._stopped():
                    stopped = True
                    return
                self._update_status("所有节点不可达，请检查网络")
                self._gauge_speed(0, "", snap=True)
                return

            if self._stopped():
                stopped = True
                return

            # 节点已就绪：冻结静默换源，再快照择优列表供本轮上下载使用
            self.probe_session.set_silent_updates(False)
            selected = self.probe_session.selected_results()
            node_info = self.probe_session.summary_text()
            self._update_realtime(f"节点延迟：{node_info}")
            self._ui(lambda: self.sources_var.set(f"测速源：{node_info}"))
            results["nodes"] = node_info
            # 秒级延迟时下载会被建连吃掉大半窗口，成绩会明显偏低——直接说清楚
            best_lat = min((lat for _, _, lat in selected), default=0.0)
            if best_lat >= 800:
                self._update_realtime(
                    f"节点延迟偏高（最优 {best_lat:.0f}ms），下载可能严重偏低，建议换区或点「重新探测」"
                )
            self._update_progress(15)

            # 下载阶段：只用择优源
            self._update_status(f"正在测试下载速度（{DOWNLOAD_DURATION}秒）")
            self._start_dots("download", "下载速度：测试中")
            self._gauge_speed(0, "下载", snap=True)
            dl_gen = self._gauge_gen

            urls = [u for _, u, _ in selected]
            # 防呆：摸底未结束不开正式下载（叠流会出现下载/上传 0 Mbps）
            if self.probe_session.sampling or not self.probe_session.sample_done:
                self.probe_session.wait_for_sample(5.0, stop_event=self.stop_event)
            # 接线硬约束：会话摸底 high_bandwidth 必须进入正式下载规划
            high_bw = bool(self.probe_session.high_bandwidth)
            total_bytes, dl_speed = measure_download_timed(
                urls, duration=DOWNLOAD_DURATION,
                callback=lambda msg: self._update_realtime(msg),
                progress_callback=lambda mbps, g=dl_gen: self._gauge_speed(
                    mbps, "下载", expect_gen=g,
                ),
                stop_event=self.stop_event,
                high_bandwidth=high_bw,
            )

            # 探测中开测偶发叠流/烂源导致 0：歇一下用当前择优再测一轮
            if not self._stopped() and dl_speed < 1.0 and urls:
                self._update_realtime("下载接近 0，0.5s 后重试…")
                time.sleep(0.5)
                self._gauge_speed(0, "下载", snap=True)
                dl_gen = self._gauge_gen
                total_bytes, dl_speed = measure_download_timed(
                    urls, duration=DOWNLOAD_DURATION,
                    callback=lambda msg: self._update_realtime(msg),
                    progress_callback=lambda mbps, g=dl_gen: self._gauge_speed(
                        mbps, "下载", expect_gen=g,
                    ),
                    stop_event=self.stop_event,
                    high_bandwidth=high_bw,
                )

            if self._stopped():
                stopped = True
                if dl_speed > 0:
                    results["download"] = dl_speed
                    self._stop_dots("download")
                    self._set_status_text("download", f"下载速度：{dl_speed:.2f} Mbps")
                return

            self._stop_dots("download")
            results["download"] = dl_speed
            self._set_status_text("download", f"下载速度：{dl_speed:.2f} Mbps")
            self._gauge_speed(dl_speed, "下载", snap=True)
            self._update_progress(60)

            # 上传阶段：掐断下载残留 + 丢掉队列里旧的下载仪表帧 + 指针瞬间归零
            if self.probe_session.sampling:
                self.probe_session.wait_for_sample(5.0, stop_event=self.stop_event)
            if not self._stopped():
                time.sleep(0.5)
            self._update_status(f"正在测试上传速度（{UPLOAD_DURATION}秒）")
            self._start_dots("upload", "上传速度：测试中")
            self._gauge_speed(0, "上传", snap=True)
            ul_gen = self._gauge_gen

            total_up, ul_speed = measure_upload_timed(
                UPLOAD_URL, duration=UPLOAD_DURATION, threads=8,
                callback=None,
                progress_callback=lambda mbps, g=ul_gen: self._gauge_speed(
                    mbps, "上传", expect_gen=g,
                ),
                stop_event=self.stop_event,
            )

            # 偏低或 0：歇一下再加线程重测（探测中开测后更易踩中）
            if not self._stopped() and ul_speed < 10:
                self._update_realtime(f"上传 {ul_speed:.0f} Mbps 偏低，0.5s 后 12 线程重试...")
                time.sleep(0.5)
                self._gauge_speed(0, "上传", snap=True)
                ul_gen = self._gauge_gen
                total_up, ul_speed = measure_upload_timed(
                    UPLOAD_URL, duration=UPLOAD_DURATION, threads=12,
                    callback=None,
                    progress_callback=lambda mbps, g=ul_gen: self._gauge_speed(
                        mbps, "上传", expect_gen=g,
                    ),
                    stop_event=self.stop_event,
                )

            if self._stopped():
                stopped = True
                if ul_speed > 0:
                    results["upload"] = ul_speed
                    self._stop_dots("upload")
                    self._set_status_text("upload", f"上传速度：{ul_speed:.2f} Mbps")
                return

            self._stop_dots("upload")
            results["upload"] = ul_speed
            self._set_status_text("upload", f"上传速度：{ul_speed:.2f} Mbps")
            self._gauge_speed(ul_speed, "上传", snap=True)
            self._update_realtime(
                f"上传完成: {total_up/1_000_000:.1f}MB / {UPLOAD_DURATION}s = {ul_speed:.2f} Mbps"
            )
            self._update_progress(95)

            self._update_progress(100)
            self._update_status("测速完成！")
            report = self._generate_report(results)
            self._set_report(report)
            if not self._closing:
                self._ui(self._show_report)

        except Exception as e:
            if not self._closing:
                self._update_status(f"测速失败：{e}")
                self._gauge_speed(0, "")
        finally:
            self._stop_all_dots()
            # 若停止时现况还停在「等待中/测试中」动画文案，收成静默状态
            if stopped and not self._closing:
                self._finalize_stopped_status(results)
                self._update_status("测试已停止")
            self.is_testing = False
            self.probe_session.set_silent_updates(True)
            # 按钮复位交给主线程轮询（_schedule_idle_button）
            self._ui(self._schedule_idle_button)

    def _finalize_stopped_status(self, results):
        if "download" in results:
            self._set_status_text("download", f"下载速度：{results['download']:.2f} Mbps")
        else:
            cur = self.download_var.get()
            if "等待中" in cur or "测试中" in cur:
                self._set_status_text("download", "下载速度：—")
        if "upload" in results:
            self._set_status_text("upload", f"上传速度：{results['upload']:.2f} Mbps")
        else:
            cur = self.upload_var.get()
            if "等待中" in cur or "测试中" in cur:
                self._set_status_text("upload", "上传速度：—")

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

    def _ui(self, fn):
        """投递到主线程队列；由 _poll_ui 执行（线程安全）"""
        if self._closing:
            return
        try:
            self._ui_queue.put(fn)
        except Exception:
            pass

    def _poll_ui(self):
        if self._closing:
            return
        # 每拍最多处理一批，避免下载尾段队列堆积时主线程卡死
        budget = 40
        try:
            while budget > 0:
                fn = self._ui_queue.get_nowait()
                budget -= 1
                try:
                    fn()
                except Exception:
                    pass
        except Empty:
            pass
        try:
            self.root.after(50, self._poll_ui)
        except Exception:
            pass

    def _update_status(self, text):
        self._ui(lambda: self.status_var.set(text))

    def _update_progress(self, value):
        self._ui(lambda: self.progress.config(value=value))

    def _set_status_text(self, key, text):
        """设置现况文案（会先停掉对应省略号动画）"""
        self._stop_dots(key)
        var = {"download": self.download_var, "upload": self.upload_var}.get(key)
        if var is not None:
            self._ui(lambda v=var, t=text: v.set(t))

    def _update_realtime(self, text):
        self._stop_dots("realtime")
        self._ui(lambda: self.realtime_var.set(text))

    def _start_dots(self, key, base_text):
        """必须投递主线程：DotAnimator 内部会 root.after。"""
        def _do():
            self._stop_dots_main(key)
            var_map = {
                "download": self.download_var,
                "upload": self.upload_var,
                "realtime": self.realtime_var,
            }
            var = var_map.get(key)
            if var is None or self._closing:
                return
            anim = DotAnimator(var, base_text, self.root)
            self.dots[key] = anim
            anim.start()

        self._ui(_do)

    def _stop_dots_main(self, key):
        anim = self.dots.get(key)
        if anim:
            anim.stop()
            self.dots[key] = None

    def _stop_dots(self, key):
        self._ui(lambda k=key: self._stop_dots_main(k))

    def _stop_all_dots(self):
        def _do():
            for key in list(self.dots.keys()):
                self._stop_dots_main(key)

        self._ui(_do)

    def _set_report(self, text):
        def _do():
            try:
                self.report_text.config(state="normal")
                self.report_text.delete("1.0", "end")
                self.report_text.insert("1.0", text)
                self.report_text.config(state="disabled")
            except Exception:
                pass
        self._ui(_do)


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
    SpeedTestApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
