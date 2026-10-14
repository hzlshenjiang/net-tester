# -*- coding: utf-8 -*-
"""地区源池、并行探测、会话缓存与动态择优（公共 seam）。"""

from __future__ import annotations

import json
import os
import socket
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, List, Optional, Sequence, Tuple
from urllib.parse import urlparse

# (显示名, URL)
Source = Tuple[str, str]
ProbeResult = Tuple[str, str, float]  # name, url, latency_ms

DEFAULT_REGION = "south_china"
UNREACHABLE_MS = 99999.0
PROBE_TIMEOUT = 2.0
PROBE_COUNT = 2

# 地区标签（UI 用）
REGION_LABELS = {
    "south_china": "华南",
    "east_china": "华东",
    "nationwide": "全国",
}

# 华南：广州/深圳方向友好的国内大厂 CDN 大文件
_SOUTH = [
    ("华为云镜像", "https://mirrors.huaweicloud.com/nodejs/v20.17.0/node-v20.17.0-x64.msi"),
    ("华为云-Python", "https://mirrors.huaweicloud.com/python/3.12.5/Python-3.12.5.tgz"),
    ("腾讯-微信", "https://dldir1.qq.com/weixin/Windows/WeChatSetup.exe"),
    ("阿里云镜像", "https://mirrors.aliyun.com/golang/go1.22.5.windows-amd64.zip"),
]

_EAST = [
    ("华为云镜像", "https://mirrors.huaweicloud.com/nodejs/v20.17.0/node-v20.17.0-x64.msi"),
    ("阿里云镜像", "https://mirrors.aliyun.com/golang/go1.22.5.windows-amd64.zip"),
    ("腾讯-微信", "https://dldir1.qq.com/weixin/Windows/WeChatSetup.exe"),
]

_NATIONWIDE = [
    ("华为云镜像", "https://mirrors.huaweicloud.com/nodejs/v20.17.0/node-v20.17.0-x64.msi"),
    ("阿里云镜像", "https://mirrors.aliyun.com/golang/go1.22.5.windows-amd64.zip"),
    ("腾讯-微信", "https://dldir1.qq.com/weixin/Windows/WeChatSetup.exe"),
    ("中科大镜像", "https://mirrors.ustc.edu.cn/golang/go1.22.5.windows-amd64.zip"),
]


class RegionCatalog:
    """按地区返回候选测速源；未知地区回落华南。"""

    _POOLS = {
        "south_china": _SOUTH,
        "east_china": _EAST,
        "nationwide": _NATIONWIDE,
    }

    @classmethod
    def sources_for(cls, region_id: str) -> List[Source]:
        pool = cls._POOLS.get(region_id) or cls._POOLS[DEFAULT_REGION]
        return list(pool)

    @classmethod
    def region_ids(cls) -> List[str]:
        return list(cls._POOLS.keys())


def pick_optimal_sources(
    results: Sequence[ProbeResult],
    min_n: int = 2,
    max_n: int = 4,
    slack_ms: float = 25.0,
) -> List[ProbeResult]:
    """延迟落在「最优 + slack」内的源；至少 min_n、最多 max_n。"""
    reachable = [
        (n, u, lat) for n, u, lat in results
        if lat < UNREACHABLE_MS and lat > 0
    ]
    if not reachable:
        return []

    reachable.sort(key=lambda x: x[2])
    best = reachable[0][2]
    within = [r for r in reachable if r[2] <= best + slack_ms]

    if len(within) >= min_n:
        return within[:max_n]

    # 不足 min_n：按延迟从全量可达列表补到 min_n（不超过 max_n）
    need = min(len(reachable), max(min_n, len(within)))
    need = min(need, max_n)
    return reachable[:need]


def resolve_region(settings: dict, locate_fn: Optional[Callable[[], Optional[str]]] = None) -> str:
    """自动定位关 → settings.region；开且成功 → 定位结果；失败 → 华南。"""
    if not settings.get("auto_locate"):
        return settings.get("region") or DEFAULT_REGION
    if locate_fn is None:
        return DEFAULT_REGION
    try:
        found = locate_fn()
        if found and found in RegionCatalog._POOLS:
            return found
    except Exception:
        pass
    return DEFAULT_REGION


def tcp_ping_url(url: str, count: int = PROBE_COUNT, timeout: float = PROBE_TIMEOUT) -> float:
    """对 URL 主机做 TCP 建连测延迟，失败返回 UNREACHABLE_MS。"""
    parsed = urlparse(url)
    host = parsed.hostname
    if not host:
        return UNREACHABLE_MS
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    lats = []
    # DNS 也可能卡住：用默认超时兜底
    old_to = socket.getdefaulttimeout()
    socket.setdefaulttimeout(timeout)
    try:
        for _ in range(count):
            try:
                t0 = time.time()
                sock = socket.create_connection((host, port), timeout=timeout)
                lat = (time.time() - t0) * 1000
                sock.close()
                lats.append(lat)
            except Exception:
                pass
    finally:
        socket.setdefaulttimeout(old_to)
    if not lats:
        return UNREACHABLE_MS
    return sum(lats) / len(lats)


def parallel_probe(
    sources: Sequence[Source],
    stop_event: Optional[threading.Event] = None,
    ping_one: Optional[Callable[[str], float]] = None,
) -> List[ProbeResult]:
    """并行探测一批源，返回 [(name, url, latency_ms), ...]。"""
    ping_one = ping_one or (lambda url: tcp_ping_url(url))
    results: List[ProbeResult] = []
    if not sources:
        return results

    def _one(item: Source) -> ProbeResult:
        if stop_event is not None and stop_event.is_set():
            return item[0], item[1], UNREACHABLE_MS
        name, url = item
        return name, url, float(ping_one(url))

    # 整批探测硬超时，避免 DNS 卡死拖垮启动预热
    batch_timeout = PROBE_COUNT * PROBE_TIMEOUT + 3.0
    workers = min(8, len(sources))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {pool.submit(_one, s): s for s in sources}
        try:
            for fut in as_completed(futs, timeout=batch_timeout):
                if stop_event is not None and stop_event.is_set():
                    break
                name, url = futs[fut]
                try:
                    results.append(fut.result(timeout=0.1))
                except Exception:
                    results.append((name, url, UNREACHABLE_MS))
        except TimeoutError:
            pass
        for fut, (name, url) in futs.items():
            if not any(r[1] == url for r in results):
                results.append((name, url, UNREACHABLE_MS))
    results.sort(key=lambda x: x[2])
    return results


class ProbeSession:
    """会话级探测缓存：同地区复用；换区 / invalidate 才重探。"""

    def __init__(
        self,
        ping_fn: Optional[Callable[..., List[ProbeResult]]] = None,
        catalog: type = RegionCatalog,
    ):
        self._ping_fn = ping_fn or (lambda sources, stop_event=None: parallel_probe(sources, stop_event))
        self._catalog = catalog
        self._lock = threading.Lock()
        self._region: Optional[str] = None
        self._results: List[ProbeResult] = []
        self._selected: List[ProbeResult] = []
        self._ready = False

    @property
    def ready(self) -> bool:
        with self._lock:
            return self._ready

    @property
    def region(self) -> Optional[str]:
        with self._lock:
            return self._region

    def invalidate(self) -> None:
        with self._lock:
            self._ready = False
            self._results = []
            self._selected = []
            # 保留 _region 无妨；ensure 时若同区也会因 _ready=False 重探

    def ensure_probed(
        self,
        region_id: str,
        stop_event: Optional[threading.Event] = None,
    ) -> List[ProbeResult]:
        with self._lock:
            if self._ready and self._region == region_id:
                return list(self._selected)

        sources = self._catalog.sources_for(region_id)
        results = self._ping_fn(sources, stop_event=stop_event)
        selected = pick_optimal_sources(results)

        with self._lock:
            self._region = region_id
            self._results = list(results)
            self._selected = list(selected)
            self._ready = True
            return list(self._selected)

    def selected_urls(self) -> List[str]:
        with self._lock:
            return [u for _, u, _ in self._selected]

    def selected_results(self) -> List[ProbeResult]:
        with self._lock:
            return list(self._selected)

    def all_results(self) -> List[ProbeResult]:
        with self._lock:
            return list(self._results)

    def summary_text(self) -> str:
        with self._lock:
            if not self._ready or not self._selected:
                return "尚未探测"
            parts = [f"{n} {lat:.0f}ms" for n, _, lat in self._selected]
            return " / ".join(parts)


# ─── 设置读写 ───

def settings_path() -> str:
    base = os.environ.get("APPDATA") or os.path.expanduser("~")
    folder = os.path.join(base, "NetTester")
    return os.path.join(folder, "settings.json")


def load_settings() -> dict:
    path = settings_path()
    defaults = {"region": DEFAULT_REGION, "auto_locate": False}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return defaults
        region = data.get("region") or DEFAULT_REGION
        if region not in RegionCatalog._POOLS:
            region = DEFAULT_REGION
        return {
            "region": region,
            "auto_locate": bool(data.get("auto_locate", False)),
        }
    except Exception:
        return defaults


def save_settings(settings: dict) -> None:
    path = settings_path()
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump({
                "region": settings.get("region") or DEFAULT_REGION,
                "auto_locate": bool(settings.get("auto_locate", False)),
            }, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def locate_region_by_ip(timeout: float = 3.0) -> Optional[str]:
    """可选 IP 归属 → 地区 id；失败返回 None。

    使用 ip-api.com（免 key）；国内偶发不通，调用方应回落华南。
    """
    try:
        req = urllib.request.Request(
            "http://ip-api.com/json/?fields=status,regionName,city,message",
            headers={"User-Agent": "NetTester/1.0"},
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="ignore"))
        if data.get("status") != "success":
            return None
        text = f"{data.get('regionName', '')} {data.get('city', '')}".lower()
        # 粗映射：广东/广西/海南/福建南部 → 华南；江浙沪皖 → 华东
        south_keys = ("guangdong", "guangzhou", "shenzhen", "guangxi", "hainan",
                      "广东", "广州", "深圳", "广西", "海南", "福建", "fujian")
        east_keys = ("shanghai", "zhejiang", "jiangsu", "hangzhou", "nanjing", "suzhou",
                     "安徽", "上海", "浙江", "江苏", "杭州", "南京")
        # ip-api 对国内常返回中文或拼音
        raw = f"{data.get('regionName', '')} {data.get('city', '')}"
        for k in south_keys:
            if k in text or k in raw:
                return "south_china"
        for k in east_keys:
            if k in text or k in raw:
                return "east_china"
        return "nationwide"
    except Exception:
        return None
