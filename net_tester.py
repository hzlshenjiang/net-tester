#!/usr/bin/env python3
"""网络测速工具 - 绿色版

测速方案（针对国内优化）：
- 下载：国内 CDN 源（华为云/腾讯），20-30MB 文件，多源取最快
- 上传：Cloudflare speed test 端点，5MB 数据
- 延迟：TCP 连接测延迟
- 防呆：每步超时保护，自动跳过失败源
- 分辨率自适应：DPI 缩放
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


# ─── 测速核心 ───

_ctx = ssl.create_default_context()
_ctx.check_hostname = False
_ctx.verify_mode = ssl.CERT_NONE

# 强制 IPv4（避免 IPv6 DNS 解析问题）
_orig_getaddrinfo = socket.getaddrinfo
def _ipv4_only(*args, **kwargs):
    return [r for r in _orig_getaddrinfo(*args, **kwargs) if r[0] == socket.AF_INET]
socket.getaddrinfo = _ipv4_only

# 下载测速源（国内 CDN，文件大小 20-30MB 最准确）
DOWNLOAD_SOURCES = [
    ("华为云镜像", "https://mirrors.huaweicloud.com/nodejs/v20.17.0/node-v20.17.0-x64.msi"),
    ("华为云-Python", "https://mirrors.huaweicloud.com/python/3.12.5/Python-3.12.5.tgz"),
    ("腾讯-微信", "https://dldir1.qq.com/weixin/Windows/WeChatSetup.exe"),
]

# 上传测速端点
UPLOAD_URL = "https://speed.cloudflare.com/__up"
UPLOAD_DATA_SIZE = 5 * 1024 * 1024  # 5MB——够准确且不太慢


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


def try_download(url, timeout=20):
    """尝试下载，返回 (字节数, 耗时秒, 速度Mbps) 或 None"""
    try:
        start = time.time()
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=timeout, context=_ctx) as resp:
            data = resp.read()
        elapsed = time.time() - start
        if elapsed < 0.3:
            return None  # 太快不可靠
        speed_mbps = len(data) / elapsed * 8 / 1_000_000
        return len(data), elapsed, speed_mbps
    except Exception:
        return None


def measure_upload(url, data, timeout=30):
    """上传测速，返回 (字节数, 耗时秒, 速度Mbps) 或 None"""
    try:
        start = time.time()
        req = urllib.request.Request(
            url, data=data,
            headers={"User-Agent": "Mozilla/5.0", "Content-Type": "application/octet-stream"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=timeout, context=_ctx) as resp:
            resp.read()
        elapsed = time.time() - start
        if elapsed < 0.3:
            return None
        speed_mbps = len(data) / elapsed * 8 / 1_000_000
        return len(data), elapsed, speed_mbps
    except Exception:
        return None


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
        self.W = self.scale(560)
        self.H = self.scale(620)
        self.root.geometry(f"{self.W}x{self.H}")
        self.root.resizable(False, False)

        self.root.update_idletasks()
        x = (self.root.winfo_screenwidth() - self.W) // 2
        y = (self.root.winfo_screenheight() - self.H) // 2
        self.root.geometry(f"{self.W}x{self.H}+{x}+{y}")

        self.is_testing = False
        self.dots_anim = None
        self._build_ui()

    def _setup_dpi(self):
        self.dpi_scale = 1.0
        try:
            import ctypes
            hdc = ctypes.windll.user32.GetDC(0)
            dpi = ctypes.windll.gdi32.GetDeviceCaps(hdc, 88)
            ctypes.windll.user32.ReleaseDC(0, hdc)
            if dpi > 96:
                self.dpi_scale = dpi / 96.0
        except Exception:
            pass

    def scale(self, val):
        return int(val * self.dpi_scale)

    def _font(self, size, weight="normal"):
        return ("Microsoft YaHei", self.scale(size), weight)

    def _build_ui(self):
        ttk.Label(self.root, text="网络测速工具", font=self._font(18, "bold")).pack(pady=(self.scale(20), self.scale(5)))
        ttk.Label(self.root, text="多源 CDN 测速 · 自动选最优节点", font=self._font(10), foreground="gray").pack(pady=(0, self.scale(10)))

        self.status_var = tk.StringVar(value="就绪 — 点击开始测速")
        ttk.Label(self.root, textvariable=self.status_var, font=self._font(11)).pack(pady=self.scale(3))

        self.progress = ttk.Progressbar(self.root, orient="horizontal", length=self.scale(460), mode="determinate")
        self.progress.pack(pady=self.scale(8))

        frame = ttk.LabelFrame(self.root, text="测试结果", padding=self.scale(12))
        frame.pack(fill="x", padx=self.scale(30), pady=self.scale(5))

        self.download_var = tk.StringVar(value="下载速度：—")
        ttk.Label(frame, textvariable=self.download_var, font=self._font(13)).pack(anchor="w", pady=self.scale(2))

        self.upload_var = tk.StringVar(value="上传速度：—")
        ttk.Label(frame, textvariable=self.upload_var, font=self._font(13)).pack(anchor="w", pady=self.scale(2))

        self.ping_var = tk.StringVar(value="延迟：—")
        ttk.Label(frame, textvariable=self.ping_var, font=self._font(13)).pack(anchor="w", pady=self.scale(2))

        self.server_var = tk.StringVar(value="最佳节点：—")
        ttk.Label(frame, textvariable=self.server_var, font=self._font(10), foreground="gray").pack(anchor="w", pady=self.scale(2))

        self.realtime_var = tk.StringVar(value="")
        ttk.Label(self.root, textvariable=self.realtime_var, font=self._font(9), foreground="blue").pack(pady=self.scale(3))

        self.btn_start = ttk.Button(self.root, text="开始测速", command=self.start_test)
        self.btn_start.pack(pady=self.scale(8))

        # 报告区域——完整显示
        self.report_text = tk.Text(
            self.root, height=11, width=62,
            font=("Consolas", self.scale(9)),
            state="disabled", wrap="word", relief="flat",
            background=self.root.cget("background"),
        )
        self.report_text.pack(padx=self.scale(30), pady=(self.scale(3), self.scale(10)))

    def start_test(self):
        if self.is_testing:
            return
        self.is_testing = True
        self.btn_start.config(state="disabled", text="测试中...")
        self.progress["value"] = 0
        self.download_var.set("下载速度：等待中")
        self.upload_var.set("上传速度：等待中")
        self.ping_var.set("延迟：等待中")
        self.server_var.set("最佳节点：—")
        self.realtime_var.set("")
        self._set_report("")
        threading.Thread(target=self._run_test, daemon=True).start()

    def _run_test(self):
        results = {}
        try:
            # 1. 延迟——超时 5 秒
            self._update_status("正在测试网络延迟")
            self._start_dots("测试延迟中")
            self._update_progress(5)
            ping = measure_ping("mirrors.huaweicloud.com", 443, 5, timeout=5)
            results["ping"] = ping
            self._update("ping", f"延迟：{ping:.0f} ms" if ping > 0 else "延迟：超时")
            self._update_progress(10)
            self._stop_dots()

            # 2. 下载——每个源超时 20 秒
            self._update_status("正在测试下载速度")
            self._update("download", "下载速度：测试中")
            self._start_dots("下载测速中")

            best_dl = 0
            best_name = "—"
            for name, url in DOWNLOAD_SOURCES:
                self._update_realtime(f"正在从 {name} 下载")
                result = try_download(url, timeout=20)
                if result:
                    size, elapsed, speed = result
                    self._update_realtime(f"{name}: {size/1_000_000:.1f}MB / {elapsed:.1f}s = {speed:.2f} Mbps")
                    if speed > best_dl:
                        best_dl = speed
                        best_name = name
                else:
                    self._update_realtime(f"{name}: 跳过")
                time.sleep(0.3)

            self._stop_dots()
            results["download"] = best_dl
            results["download_source"] = best_name
            self._update("download", f"下载速度：{best_dl:.2f} Mbps" if best_dl > 0 else "下载速度：全部失败")
            self._update("server", f"最佳节点：{best_name}")
            self._update_progress(55)

            # 3. 上传——5MB 超时 30 秒
            self._update_status("正在测试上传速度")
            self._update("upload", "上传速度：测试中")
            self._start_dots("上传测速中")

            upload_data = b"0" * UPLOAD_DATA_SIZE
            self._update_realtime(f"上传 {UPLOAD_DATA_SIZE//1024//1024}MB 到 Cloudflare")
            result = measure_upload(UPLOAD_URL, upload_data, timeout=30)

            best_ul = 0
            if result:
                size, elapsed, speed = result
                best_ul = speed
                self._update_realtime(f"上传完成: {size/1_000_000:.1f}MB / {elapsed:.1f}s = {speed:.2f} Mbps")
            else:
                # 降级 2MB
                self._update_realtime("5MB 超时，降级 2MB")
                result = measure_upload(UPLOAD_URL, b"0" * (2 * 1024 * 1024), timeout=15)
                if result:
                    size, elapsed, speed = result
                    best_ul = speed
                    self._update_realtime(f"上传完成: {size/1_000_000:.1f}MB / {elapsed:.1f}s = {speed:.2f} Mbps")

            self._stop_dots()
            results["upload"] = best_ul
            self._update("upload", f"上传速度：{best_ul:.2f} Mbps" if best_ul > 0 else "上传速度：失败")
            self._update_progress(95)

            # 4. 完成
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
        ping = r.get("ping", 0)
        src = r.get("download_source", "—")

        if dl >= 100:
            grade = "★★★★★ 优秀"
        elif dl >= 50:
            grade = "★★★★ 良好"
        elif dl >= 20:
            grade = "★★★ 一般"
        elif dl >= 5:
            grade = "★★ 较慢"
        else:
            grade = "★ 很慢"

        return f"""╔══════════════════════════════════════╗
║         网络测速总结报告
╠══════════════════════════════════════╣
  测试时间：{now}
  最佳节点：{src}

  下载速度：{dl:.2f} Mbps
  上传速度：{ul:.2f} Mbps
  网络延迟：{ping:.0f} ms

  综合评级：{grade}
╚══════════════════════════════════════╝"""

    def _update_status(self, text):
        self.root.after(0, lambda: self.status_var.set(text))

    def _update_progress(self, value):
        self.root.after(0, lambda: self.progress.config(value=value))

    def _update(self, key, text):
        m = {"download": self.download_var, "upload": self.upload_var, "ping": self.ping_var, "server": self.server_var}
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
