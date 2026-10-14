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
PROBE_COUNT = 1  # 相对排序够用；次数少才能「够用即继续」
# 延迟探测：够用即返回
EARLY_REACHABLE = 3
EARLY_WAIT_S = 2.5
PROBE_HARD_WAIT_S = PROBE_TIMEOUT * PROBE_COUNT + 3.0
# 轻量摸底定档（正式默认 ≥32 服务约 300M；过线才 ≥40）
HIGH_BW_SAMPLE_MBPS = 300.0
SAMPLE_DURATION_S = 2.0
SAMPLE_TARGET_CONNECTIONS = 10  # 约 8～12

# 地区标签（UI 用）
REGION_LABELS = {
    "south_china": "华南",
    "east_china": "华东",
    "nationwide": "全国",
}

# 大文件 HTTPS：同厂商最多 2 条；hostname 尽量分散，掉源时仍能凑满 Top3～4
_HUAWEI_NODE = "https://mirrors.huaweicloud.com/nodejs/v20.17.0/node-v20.17.0-x64.msi"
_HUAWEI_PY = "https://mirrors.huaweicloud.com/python/3.12.5/Python-3.12.5.tgz"
_TENCENT_WX = "https://dldir1.qq.com/weixin/Windows/WeChatSetup.exe"
_TENCENT_CDN = "https://dldir1v6.qq.com/weixin/Windows/WeChatSetup.exe"
_ALIYUN_GO = "https://mirrors.aliyun.com/golang/go1.22.5.windows-amd64.zip"
_NPM_MIRROR = "https://cdn.npmmirror.com/binaries/node/v20.17.0/node-v20.17.0-x64.msi"
_KERNEL_GCC = "https://mirrors.kernel.org/gnu/gcc/gcc-14.2.0/gcc-14.2.0.tar.xz"

# 华南 7：华为×2 + 腾讯×2 + 阿里 + npmmirror + kernel
_SOUTH = [
    ("华为云镜像", _HUAWEI_NODE),
    ("华为云-Python", _HUAWEI_PY),
    ("腾讯-微信", _TENCENT_WX),
    ("腾讯CDN", _TENCENT_CDN),
    ("阿里云镜像", _ALIYUN_GO),
    ("npmmirror", _NPM_MIRROR),
    ("Kernel镜像", _KERNEL_GCC),
]

# 华东 6：少一条华为 Python，其余复用
_EAST = [
    ("华为云镜像", _HUAWEI_NODE),
    ("腾讯-微信", _TENCENT_WX),
    ("腾讯CDN", _TENCENT_CDN),
    ("阿里云镜像", _ALIYUN_GO),
    ("npmmirror", _NPM_MIRROR),
    ("Kernel镜像", _KERNEL_GCC),
]

# 全国：与华南同池，覆盖跨区
_NATIONWIDE = list(_SOUTH)


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
    min_n: int = 3,
    max_n: int = 4,
    slack_ms: float = 80.0,
) -> List[ProbeResult]:
    """延迟落在「最优 + slack」内的源；至少 min_n、最多 max_n。

    默认 Top3～4 + 较宽余裕：多源并行才能打满家宽（2 源×8 连接易卡在 ~200Mbps）。
    """
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


def plan_download_concurrency(
    num_urls: int,
    high_bandwidth: bool = False,
) -> Tuple[int, int]:
    """按源数量规划下载连接数，返回 (每源线程数, 总连接数)。

    目标：常态总连接 ≥32；高带宽加连 ≥40。源越少，每源线程越高。
    """
    if num_urls <= 0:
        return 0, 0
    target = 40 if high_bandwidth else 32
    if num_urls == 1:
        tpu = 20 if high_bandwidth else 16
    elif num_urls == 2:
        tpu = 20 if high_bandwidth else 16
    elif num_urls == 3:
        tpu = 14 if high_bandwidth else 12
    else:
        # 4 源及以上：均摊到目标，并设下限
        tpu = max(8, (target + num_urls - 1) // num_urls)
        if high_bandwidth:
            tpu = max(tpu, 10)
    while tpu * num_urls < target:
        tpu += 1
    tpu = min(tpu, 24)
    return tpu, tpu * num_urls


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
    """对 URL 主机做 TCP 建连测延迟，失败返回 UNREACHABLE_MS。

    不使用 socket.setdefaulttimeout：并行探测时多线程会互相踩踏全局超时。
    """
    parsed = urlparse(url)
    host = parsed.hostname
    if not host:
        return UNREACHABLE_MS
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    lats = []
    for _ in range(count):
        try:
            t0 = time.time()
            sock = socket.create_connection((host, port), timeout=timeout)
            lat = (time.time() - t0) * 1000
            sock.close()
            lats.append(lat)
        except Exception:
            pass
    if not lats:
        return UNREACHABLE_MS
    return sum(lats) / len(lats)


def plan_sample_concurrency(num_urls: int) -> Tuple[int, int]:
    """轻量摸底连接数：总连接约 8～12。"""
    if num_urls <= 0:
        return 0, 0
    tpu = max(2, (SAMPLE_TARGET_CONNECTIONS + num_urls - 1) // num_urls)
    total = tpu * num_urls
    while total > 12 and tpu > 2:
        tpu -= 1
        total = tpu * num_urls
    while total < 8:
        tpu += 1
        total = tpu * num_urls
        if tpu >= 8:
            break
    return tpu, total


def parallel_probe(
    sources: Sequence[Source],
    stop_event: Optional[threading.Event] = None,
    ping_one: Optional[Callable[[str], float]] = None,
    min_reachable: int = EARLY_REACHABLE,
    early_wait_s: float = EARLY_WAIT_S,
    on_complete: Optional[Callable[[List[ProbeResult]], None]] = None,
) -> List[ProbeResult]:
    """并行探测；可达≥min_reachable 或已等 early_wait_s 则先返回。

    若提供 on_complete 且仍有未完成任务，后台静默补全后回调完整结果。
    未提供 on_complete 时，会等全部结束再返回（兼容旧调用）。
    """
    ping_one = ping_one or (lambda url: tcp_ping_url(url))
    if not sources:
        if on_complete is not None:
            on_complete([])
        return []

    results_by_url: dict = {}
    map_lock = threading.Lock()
    early = threading.Event()

    def _one(item: Source) -> ProbeResult:
        if stop_event is not None and stop_event.is_set():
            return item[0], item[1], UNREACHABLE_MS
        name, url = item
        lat = float(ping_one(url))
        row = (name, url, lat)
        with map_lock:
            results_by_url[url] = row
            ok = sum(1 for r in results_by_url.values() if r[2] < UNREACHABLE_MS)
            if ok >= min_reachable:
                early.set()
        return row

    workers = min(8, len(sources))
    pool = ThreadPoolExecutor(max_workers=workers)
    futs = {pool.submit(_one, s): s for s in sources}
    t0 = time.time()
    try:
        while True:
            if early.is_set():
                break
            if stop_event is not None and stop_event.is_set():
                break
            with map_lock:
                finished = len(results_by_url)
                ok_n = sum(1 for r in results_by_url.values() if r[2] < UNREACHABLE_MS)
            if finished >= len(sources):
                break
            elapsed = time.time() - t0
            # 时间到且已有可达源 → 返回；若还一个都没有，继续等到硬超时，避免空结果
            if elapsed >= early_wait_s and ok_n >= 1:
                break
            if elapsed >= PROBE_HARD_WAIT_S:
                break
            early.wait(timeout=0.05)
    except Exception:
        pass

    with map_lock:
        partial = list(results_by_url.values())
        ok_n = sum(1 for r in partial if r[2] < UNREACHABLE_MS)
    partial.sort(key=lambda x: x[2])

    def _drain_all() -> List[ProbeResult]:
        try:
            for fut in as_completed(futs, timeout=PROBE_HARD_WAIT_S):
                if stop_event is not None and stop_event.is_set():
                    break
                src = futs[fut]
                name, url = src
                try:
                    fut.result(timeout=0.1)
                except Exception:
                    with map_lock:
                        if url not in results_by_url:
                            results_by_url[url] = (name, url, UNREACHABLE_MS)
        except TimeoutError:
            pass
        with map_lock:
            for fut, (name, url) in futs.items():
                if url not in results_by_url:
                    results_by_url[url] = (name, url, UNREACHABLE_MS)
            full = list(results_by_url.values())
        full.sort(key=lambda x: x[2])
        return full

    unfinished = len(partial) < len(sources)
    # 尚无任何可达源却仍有任务在跑：同步排空，避免「空结果就绪」
    if ok_n == 0 and unfinished:
        partial = _drain_all()
        unfinished = False
        if on_complete is not None:
            on_complete(list(partial))
            pool.shutdown(wait=False)
            return partial

    if on_complete is not None:
        if unfinished:
            def _bg():
                try:
                    full = _drain_all()
                    on_complete(full)
                finally:
                    pool.shutdown(wait=False)

            threading.Thread(target=_bg, daemon=True).start()
        else:
            pool.shutdown(wait=False)
            on_complete(list(partial))
        return partial

    # 无回调：等齐再返回
    full = _drain_all()
    pool.shutdown(wait=False)
    return full


class ProbeSession:
    """会话级探测缓存：同地区复用；换区 / invalidate 才重探。

    延迟够用即就绪 + 轻量摸底定 high_bandwidth；静默补探可更新择优但不轻易重摸。
    """

    def __init__(
        self,
        ping_fn: Optional[Callable[..., List[ProbeResult]]] = None,
        catalog: type = RegionCatalog,
        sample_fn: Optional[Callable[..., float]] = None,
        on_update: Optional[Callable[[], None]] = None,
        use_progressive: bool = True,
    ):
        self._custom_ping = ping_fn
        self._use_progressive = use_progressive and ping_fn is None
        self._ping_fn = ping_fn
        self._sample_fn = sample_fn
        self._on_update = on_update
        self._catalog = catalog
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._region: Optional[str] = None
        self._results: List[ProbeResult] = []
        self._selected: List[ProbeResult] = []
        self._ready = False
        self._busy = False
        self._generation = 0
        self._high_bandwidth = False
        self._sample_mbps = 0.0
        self._sample_ok = False
        self._sample_done = False
        self._sampling = False  # 摸底进行中，禁止嵌套/并发再摸底
        self._silent_updates = True
        self._status_hint = ""

    @property
    def ready(self) -> bool:
        with self._lock:
            return self._ready

    @property
    def region(self) -> Optional[str]:
        with self._lock:
            return self._region

    @property
    def high_bandwidth(self) -> bool:
        with self._lock:
            return self._high_bandwidth

    @property
    def sample_mbps(self) -> float:
        with self._lock:
            return self._sample_mbps

    @property
    def sample_ok(self) -> bool:
        with self._lock:
            return self._sample_ok

    @property
    def sample_done(self) -> bool:
        with self._lock:
            return self._sample_done

    @property
    def sampling(self) -> bool:
        """是否正在跑带宽摸底（正式上下载前应等到 False）。"""
        with self._lock:
            return self._sampling

    @property
    def status_hint(self) -> str:
        with self._lock:
            return self._status_hint

    def set_silent_updates(self, allowed: bool) -> None:
        with self._lock:
            self._silent_updates = bool(allowed)

    def invalidate(self) -> None:
        with self._lock:
            self._ready = False
            self._results = []
            self._selected = []
            self._high_bandwidth = False
            self._sample_mbps = 0.0
            self._sample_ok = False
            self._sample_done = False
            self._sampling = False
            self._status_hint = ""
            self._generation += 1
            self._cond.notify_all()

    def wait_for_sample(
        self,
        timeout: float = 1.0,
        stop_event: Optional[threading.Event] = None,
    ) -> bool:
        """开测前短等摸底结束（含禁止与正式下载叠流的 sampling 段）。"""
        deadline = time.time() + max(0.0, timeout)
        with self._cond:
            while (not self._sample_done) or self._sampling:
                if stop_event is not None and stop_event.is_set():
                    return self._sample_done and (not self._sampling)
                remain = deadline - time.time()
                if remain <= 0:
                    return self._sample_done and (not self._sampling)
                self._cond.wait(timeout=min(0.1, remain))
            return True

    def ensure_probed(
        self,
        region_id: str,
        stop_event: Optional[threading.Event] = None,
    ) -> List[ProbeResult]:
        while True:
            with self._cond:
                if self._ready and self._region == region_id and self._sample_done:
                    return list(self._selected)
                if self._busy:
                    self._cond.wait(timeout=0.2)
                    if stop_event is not None and stop_event.is_set():
                        return list(self._selected)
                    continue
                if self._ready and self._region == region_id and self._sample_done:
                    return list(self._selected)
                gen = self._generation
                self._busy = True
                self._status_hint = "探测节点"

            try:
                sources = self._catalog.sources_for(region_id)
                results = self._run_latency_probe(sources, stop_event, gen)
                selected = pick_optimal_sources(results)
                with self._cond:
                    if gen != self._generation:
                        pass
                    else:
                        self._region = region_id
                        self._results = list(results)
                        self._selected = list(selected)
                        self._ready = True
                        self._status_hint = "带宽摸底"

                if gen == self._generation:
                    self._run_bandwidth_sample(stop_event)
                    with self._cond:
                        if gen == self._generation:
                            self._status_hint = ""
                            return list(self._selected)
            finally:
                with self._cond:
                    self._busy = False
                    self._cond.notify_all()
            if stop_event is not None and stop_event.is_set():
                return []

    def _run_latency_probe(
        self,
        sources: Sequence[Source],
        stop_event: Optional[threading.Event],
        gen: int,
    ) -> List[ProbeResult]:
        if self._custom_ping is not None:
            return self._custom_ping(sources, stop_event=stop_event)

        def _on_complete(full: List[ProbeResult]) -> None:
            self._merge_silent_results(full, gen)

        return parallel_probe(
            sources,
            stop_event=stop_event,
            on_complete=_on_complete,
        )

    def _merge_silent_results(self, full: List[ProbeResult], gen: int) -> None:
        need_resample = False
        with self._cond:
            if gen != self._generation:
                return
            if not self._silent_updates:
                # 测速中：只合并原始结果表，不改择优
                self._results = list(full)
                return
            self._results = list(full)
            new_selected = pick_optimal_sources(full)
            self._selected = list(new_selected)
            # 仅「摸底已结束且不合格」才补摸；摸底进行中绝对不能再开一轮（会和正式下载互抢→0）
            if (
                self._sample_done
                and (not self._sample_ok)
                and (not self._sampling)
                and len(new_selected) >= 3
            ):
                need_resample = True
            self._cond.notify_all()
        if need_resample:
            self._run_bandwidth_sample(None)
        cb = self._on_update
        if cb is not None:
            try:
                cb()
            except Exception:
                pass

    def _run_bandwidth_sample(self, stop_event: Optional[threading.Event]) -> None:
        with self._cond:
            if self._sampling:
                return
            self._sampling = True
            urls = [u for _, u, _ in self._selected]
            sample_fn = self._sample_fn
        mbps = 0.0
        ok = len(urls) >= 3
        try:
            if sample_fn is not None and urls:
                try:
                    mbps = float(sample_fn(urls, stop_event=stop_event))
                except Exception:
                    mbps = 0.0
                    ok = False
            elif not urls:
                ok = False
            else:
                # 无 sample_fn：不实测，仅按源数标记是否具备摸底条件
                ok = len(urls) >= 3
                mbps = 0.0

            with self._cond:
                self._sample_mbps = mbps
                if sample_fn is None:
                    # 未注入摸底：不定高带宽档；sample_ok=False 以便日后注入后可补摸
                    self._sample_ok = False
                    self._high_bandwidth = False
                else:
                    self._sample_ok = bool(ok)
                    self._high_bandwidth = bool(ok and mbps >= HIGH_BW_SAMPLE_MBPS)
                self._sample_done = True
                self._cond.notify_all()
        finally:
            with self._cond:
                self._sampling = False
                self._cond.notify_all()

    def selected_urls(self) -> List[str]:
        with self._lock:
            return [u for _, u, _ in self._selected]

    def selected_results(self) -> List[ProbeResult]:
        with self._lock:
            return list(self._selected)

    def all_results(self) -> List[ProbeResult]:
        with self._lock:
            return list(self._results)

    def summary_text(self, sep: str = " ｜ ") -> str:
        """节点延迟摘要；默认用窄分隔符，便于 UI 换行显示全。"""
        with self._lock:
            if not self._ready or not self._selected:
                return "尚未探测"
            parts = [f"{n} {lat:.0f}ms" for n, _, lat in self._selected]
            return sep.join(parts)


def download_plan_for_session(session: ProbeSession, num_urls: int) -> Tuple[int, int]:
    """开测接线 seam：会话 high_bandwidth 必须进入连接规划。"""
    return plan_download_concurrency(num_urls, high_bandwidth=session.high_bandwidth)


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
