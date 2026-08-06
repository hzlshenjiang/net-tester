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

    def __init__(self, url, duration, result_queue, thread_id):
        super().__init__(daemon=True)
        self.url = url
        self.duration = duration
        self.result_queue = result_queue
        self.thread_id = thread_id
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
        except Exception:
            pass
        elapsed = time.time() - start
        self.result_queue.put((self.thread_id, total_bytes, elapsed))


def measure_download_timed(urls, duration=DOWNLOAD_DURATION, threads_per_url=8, callback=None):
    """多 URL 多线程持续下载，返回 (总字节, 速度Mbps)
    
    urls 可以是单个 URL 字符串或 URL 列表。
    每个 URL 开 threads_per_url 个线程，总线程数 = len(urls) * threads_per_url。
    自适应：单 URL 时开 12 线程，多 URL 时每 URL 开 8 线程。
    """
    if isinstance(urls, str):
        urls = [urls]
    
    # 自适应线程数：确保总线程 16-24 之间
    if len(urls) == 1:
        threads_per_url = 12
    total_threads = len(urls) * threads_per_url
    
    q = Queue()
    workers = []
    for url in urls:
        for i in range(threads_per_url):
            t = TimedDownloader(url, duration, q, len(workers))
            workers.append(t)
            t.start()

    if callback:
        callback(f"下载中: {len(urls)}个源 × {threads_per_url}线程 = {total_threads}连接")

    for t in workers:
        t.join(timeout=duration + 10)

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


class TimedUploader(threading.Thread):
    """按时间上传——持续 POST 数据，统计总字节数"""

    def __init__(self, url, duration, result_queue, thread_id, chunk_size=512*1024):
        super().__init__(daemon=True)
        self.url = url
        self.duration = duration
        self.result_queue = result_queue
        self.thread_id = thread_id
        self.chunk = b"0" * chunk_size
        self._should_stop = False

    def stop(self):
        self._should_stop = True

    def run(self):
        total_bytes = 0
        start = time.time()
        try:
            # 每次上传 512KB，持续 duration 秒
            while not self._should_stop and (time.time() - start) < self.duration:
                req = urllib.request.Request(
                    self.url, data=self.chunk,
                    headers={"User-Agent": "Mozilla/5.0", "Content-Type": "application/octet-stream"},
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=15, context=_ctx) as resp:
                    resp.read()
                total_bytes += len(self.chunk)
        except Exception:
            pass
        elapsed = time.time() - start
        self.result_queue.put((self.thread_id, total_bytes, elapsed))


def measure_upload_timed(url, duration=UPLOAD_DURATION, threads=8, callback=None):
    """多线程持续上传，返回速度 Mbps"""
    q = Queue()
    workers = []
    for i in range(threads):
        t = TimedUploader(url, duration, q, i)
        workers.append(t)
        t.start()

    for t in workers:
        t.join(timeout=duration + 10)

    total_bytes = 0
    max_elapsed = 0
    while not q.empty():
        _, bytes_up, elapsed = q.get()
        total_bytes += bytes_up
        max_elapsed = max(max_elapsed, elapsed)

    if max_elapsed < 0.5:
        return 0, 0

    speed_mbps = total_bytes / max_elapsed * 8 / 1_000_000
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


# ─── UI ───

class SpeedTestApp:
    def __init__(self, root):
        self.root = root
        self.root.title("网速测试工具")

        self._setup_dpi()
        self.W = self.scale(580)
        self.H = self.scale(700)
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
        ttk.Label(self.root, text="网络测速工具", font=self._font(18, "bold")).pack(pady=(self.scale(20), self.scale(5)))
        ttk.Label(self.root, text="智能选源 · 定时测速 · 多线程", font=self._font(10), foreground="gray").pack(pady=(0, self.scale(10)))

        self.status_var = tk.StringVar(value="就绪 — 点击开始测速")
        ttk.Label(self.root, textvariable=self.status_var, font=self._font(11)).pack(pady=self.scale(3))

        self.progress = ttk.Progressbar(self.root, orient="horizontal", length=self.scale(460), mode="determinate")
        self.progress.pack(pady=self.scale(8))

        frame = ttk.LabelFrame(self.root, text="测试现况", padding=self.scale(12))
        frame.pack(fill="x", padx=self.scale(30), pady=self.scale(5))

        self.download_var = tk.StringVar(value="下载速度：—")
        ttk.Label(frame, textvariable=self.download_var, font=self._font(13)).pack(anchor="w", pady=self.scale(2))

        self.upload_var = tk.StringVar(value="上传速度：—")
        ttk.Label(frame, textvariable=self.upload_var, font=self._font(13)).pack(anchor="w", pady=self.scale(2))

        self.realtime_var = tk.StringVar(value="")
        ttk.Label(self.root, textvariable=self.realtime_var, font=self._font(9), foreground="blue").pack(pady=self.scale(3))

        self.btn_start = ttk.Button(self.root, text="开始测速", command=self.start_test)
        self.btn_start.pack(pady=self.scale(8))

        self.report_text = tk.Text(
            self.root, height=15, width=66,
            font=("Consolas", self.scale(10)),
            state="disabled", wrap="word", relief="flat",
            background=self.root.cget("background"),
        )
        self.report_text.pack(padx=self.scale(20), pady=(self.scale(3), self.scale(10)))

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
        threading.Thread(target=self._run_test, daemon=True).start()

    def _run_test(self):
        results = {}
        try:
            # 1. 探测所有源
            self._update_status("正在探测测速节点")
            self._start_dots("探测中")
            self._update_progress(5)

            reachable = select_best_source(DOWNLOAD_SOURCES, callback=self._update_realtime)

            if not reachable:
                self._update_status("所有节点不可达，请检查网络")
                self._stop_dots()
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

            # 用所有可达源（不只最优源），多 URL 并行突破 CDN 单 IP 限速
            all_urls = [url for _, url in DOWNLOAD_SOURCES]
            total_bytes, dl_speed = measure_download_timed(all_urls, duration=DOWNLOAD_DURATION, threads_per_url=8, callback=None)

            self._stop_dots()
            results["download"] = dl_speed
            self._update("download", f"下载速度：{dl_speed:.2f} Mbps")
            self._update_progress(60)

            # 4. 上传测速——持续 10 秒
            self._update_status(f"正在测试上传速度（{UPLOAD_DURATION}秒）")
            self._update("upload", "上传速度：测试中")
            self._start_dots("上传测速中")

            total_up, ul_speed = measure_upload_timed(UPLOAD_URL, duration=UPLOAD_DURATION, threads=8, callback=None)

            # 如果 8 线程上传偏低，自动加到 12 线程重试一次
            if ul_speed < 10:
                self._stop_dots()
                self._update_realtime(f"上传 {ul_speed:.0f} Mbps 偏低，12 线程重试...")
                total_up, ul_speed = measure_upload_timed(UPLOAD_URL, duration=UPLOAD_DURATION, threads=12, callback=None)

            self._stop_dots()
            results["upload"] = ul_speed
            self._update("upload", f"上传速度：{ul_speed:.2f} Mbps")
            self._update_realtime(f"上传完成: {total_up/1_000_000:.1f}MB / {UPLOAD_DURATION}s = {ul_speed:.2f} Mbps")
            self._update_progress(95)

            # 5. 完成
            self._update_progress(100)
            self._update_status("测速完成！")
            report = self._generate_report(results)
            self._set_report(report)

        except Exception as e:
            self._update_status(f"测速失败：{e}")
            self._stop_dots()
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


def main():
    root = tk.Tk()
    app = SpeedTestApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
