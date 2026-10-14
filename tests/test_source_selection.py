# -*- coding: utf-8 -*-
"""探测选源 seam 测试：只验证公共行为，不测内部实现。"""

import unittest

from source_selection import (
    DEFAULT_REGION,
    HIGH_BW_SAMPLE_MBPS,
    RegionCatalog,
    ProbeSession,
    download_plan_for_session,
    parallel_probe,
    pick_optimal_sources,
    plan_download_concurrency,
    plan_sample_concurrency,
    resolve_region,
)


class TestRegionCatalog(unittest.TestCase):
    def test_south_china_has_at_least_three_sources(self):
        sources = RegionCatalog.sources_for("south_china")
        self.assertGreaterEqual(len(sources), 3)
        for name, url in sources:
            self.assertTrue(name)
            self.assertTrue(url.startswith("https://"))

    def test_south_pool_size_and_host_diversity(self):
        from urllib.parse import urlparse

        sources = RegionCatalog.sources_for("south_china")
        self.assertGreaterEqual(len(sources), 6)
        self.assertLessEqual(len(sources), 7)
        hosts = {urlparse(u).hostname for _, u in sources}
        self.assertGreaterEqual(len(hosts), 5)

    def test_east_pool_size_and_host_diversity(self):
        from urllib.parse import urlparse

        sources = RegionCatalog.sources_for("east_china")
        self.assertGreaterEqual(len(sources), 5)
        self.assertLessEqual(len(sources), 6)
        hosts = {urlparse(u).hostname for _, u in sources}
        self.assertGreaterEqual(len(hosts), 4)

    def test_unknown_region_falls_back_to_south_china(self):
        fallback = RegionCatalog.sources_for("no_such_region")
        south = RegionCatalog.sources_for("south_china")
        self.assertEqual(fallback, south)

    def test_east_china_differs_or_is_nonempty(self):
        east = RegionCatalog.sources_for("east_china")
        self.assertGreaterEqual(len(east), 2)


class TestPickOptimalSources(unittest.TestCase):
    def test_default_aims_top3_for_bandwidth(self):
        # 带宽优先：默认至少 3 个；58ms 相对 31ms 仍应纳入
        results = [
            ("阿里", "https://a.example/x", 31.0),
            ("腾讯", "https://b.example/x", 39.0),
            ("华为", "https://c.example/x", 58.0),
            ("华为2", "https://d.example/x", 70.0),
        ]
        picked = pick_optimal_sources(results)
        names = [n for n, _, _ in picked]
        self.assertGreaterEqual(len(picked), 3)
        self.assertLessEqual(len(picked), 4)
        self.assertIn("阿里", names)
        self.assertIn("腾讯", names)
        self.assertIn("华为", names)

    def test_picks_within_slack_of_best(self):
        # 显式窄余裕：最优 20ms + 25 → 45ms；D=50 排除
        results = [
            ("A", "https://a.example/x", 20.0),
            ("B", "https://b.example/x", 30.0),
            ("C", "https://c.example/x", 40.0),
            ("D", "https://d.example/x", 50.0),
        ]
        picked = pick_optimal_sources(results, min_n=2, max_n=4, slack_ms=25)
        names = [n for n, _, _ in picked]
        self.assertEqual(names, ["A", "B", "C"])

    def test_at_least_min_n_even_if_outside_slack(self):
        results = [
            ("A", "https://a.example/x", 10.0),
            ("B", "https://b.example/x", 100.0),
            ("C", "https://c.example/x", 200.0),
        ]
        picked = pick_optimal_sources(results, min_n=3, max_n=4, slack_ms=25)
        self.assertEqual(len(picked), 3)
        self.assertEqual([n for n, _, _ in picked], ["A", "B", "C"])

    def test_caps_at_max_n(self):
        results = [
            ("A", "https://a.example/x", 10.0),
            ("B", "https://b.example/x", 12.0),
            ("C", "https://c.example/x", 14.0),
            ("D", "https://d.example/x", 16.0),
            ("E", "https://e.example/x", 18.0),
        ]
        picked = pick_optimal_sources(results, min_n=3, max_n=4, slack_ms=80)
        self.assertEqual(len(picked), 4)
        self.assertEqual([n for n, _, _ in picked], ["A", "B", "C", "D"])

    def test_ignores_unreachable(self):
        results = [
            ("A", "https://a.example/x", 15.0),
            ("Dead", "https://dead.example/x", 99999.0),
            ("B", "https://b.example/x", 20.0),
            ("C", "https://c.example/x", 25.0),
        ]
        picked = pick_optimal_sources(results, min_n=3, max_n=4, slack_ms=80)
        names = [n for n, _, _ in picked]
        self.assertEqual(names, ["A", "B", "C"])
        self.assertNotIn("Dead", names)

    def test_empty_returns_empty(self):
        self.assertEqual(pick_optimal_sources([]), [])


class TestPlanDownloadConcurrency(unittest.TestCase):
    def test_two_sources_get_at_least_32_connections(self):
        tpu, total = plan_download_concurrency(2)
        self.assertGreaterEqual(total, 32)
        self.assertEqual(tpu * 2, total)

    def test_three_sources_stay_in_high_parallel_band(self):
        tpu, total = plan_download_concurrency(3)
        self.assertGreaterEqual(total, 30)
        self.assertLessEqual(total, 48)

    def test_four_sources_about_32_to_40(self):
        tpu, total = plan_download_concurrency(4)
        self.assertGreaterEqual(total, 32)
        self.assertLessEqual(total, 48)

    def test_high_bandwidth_boost(self):
        tpu, total = plan_download_concurrency(2, high_bandwidth=True)
        self.assertGreaterEqual(total, 40)


class TestResolveRegion(unittest.TestCase):
    def test_auto_locate_off_uses_settings_region(self):
        settings = {"region": "east_china", "auto_locate": False}
        self.assertEqual(resolve_region(settings, locate_fn=lambda: "nationwide"), "east_china")

    def test_auto_locate_on_uses_locator(self):
        settings = {"region": "east_china", "auto_locate": True}
        self.assertEqual(resolve_region(settings, locate_fn=lambda: "nationwide"), "nationwide")

    def test_auto_locate_failure_falls_back_to_south_china(self):
        settings = {"region": "east_china", "auto_locate": True}

        def boom():
            raise RuntimeError("network down")

        self.assertEqual(resolve_region(settings, locate_fn=boom), DEFAULT_REGION)

    def test_auto_locate_none_falls_back_to_south_china(self):
        settings = {"region": "east_china", "auto_locate": True}
        self.assertEqual(resolve_region(settings, locate_fn=lambda: None), DEFAULT_REGION)


class TestProbeSession(unittest.TestCase):
    def test_reuses_probe_within_session(self):
        calls = {"n": 0}

        def fake_ping(sources, stop_event=None):
            calls["n"] += 1
            return [(n, u, 20.0 + i) for i, (n, u) in enumerate(sources)]

        session = ProbeSession(ping_fn=fake_ping)
        a = session.ensure_probed("south_china")
        b = session.ensure_probed("south_china")
        self.assertEqual(calls["n"], 1)
        self.assertEqual(a, b)

    def test_invalidate_forces_reprobe(self):
        calls = {"n": 0}

        def fake_ping(sources, stop_event=None):
            calls["n"] += 1
            return [(n, u, 10.0) for n, u in sources]

        session = ProbeSession(ping_fn=fake_ping)
        session.ensure_probed("south_china")
        session.invalidate()
        session.ensure_probed("south_china")
        self.assertEqual(calls["n"], 2)

    def test_region_change_forces_reprobe(self):
        calls = {"n": 0}

        def fake_ping(sources, stop_event=None):
            calls["n"] += 1
            return [(n, u, 10.0) for n, u in sources]

        session = ProbeSession(ping_fn=fake_ping)
        session.ensure_probed("south_china")
        session.ensure_probed("east_china")
        self.assertEqual(calls["n"], 2)

    def test_selected_urls_prefer_at_least_three_when_available(self):
        def fake_ping(sources, stop_event=None):
            # 模拟华南实测：两近两稍远，默认应选到至少 3 个
            lats = [31.0, 39.0, 58.0, 70.0]
            out = []
            for i, (n, u) in enumerate(sources):
                out.append((n, u, lats[i] if i < len(lats) else 200.0))
            return out

        session = ProbeSession(ping_fn=fake_ping)
        session.ensure_probed("south_china")
        urls = session.selected_urls()
        self.assertGreaterEqual(len(urls), 3)
        self.assertLessEqual(len(urls), 4)


class TestDownloadWiring(unittest.TestCase):
    """测速接线：下载 URL 列表必须来自择优结果，而非整表。"""

    def test_selected_urls_subset_of_region_pool(self):
        def fake_ping(sources, stop_event=None):
            return [(n, u, 10.0 + i * 3) for i, (n, u) in enumerate(sources)]

        session = ProbeSession(ping_fn=fake_ping)
        session.ensure_probed("south_china")
        pool_urls = {u for _, u in RegionCatalog.sources_for("south_china")}
        selected = session.selected_urls()
        self.assertTrue(selected)
        self.assertTrue(set(selected).issubset(pool_urls))
        self.assertLessEqual(len(selected), 4)


class TestProbeSessionConcurrency(unittest.TestCase):
    """未探测完就开测：两路 ensure 只应真正探测一次，并都能拿到结果。"""

    def test_concurrent_ensure_probes_once(self):
        import threading
        import time

        calls = {"n": 0}
        gate = threading.Event()

        def slow_ping(sources, stop_event=None):
            calls["n"] += 1
            gate.wait(timeout=2.0)
            time.sleep(0.05)
            return [(n, u, 20.0 + i) for i, (n, u) in enumerate(sources)]

        session = ProbeSession(ping_fn=slow_ping)
        out = [None, None]

        def worker(i):
            out[i] = session.ensure_probed("south_china")

        t1 = threading.Thread(target=worker, args=(0,))
        t2 = threading.Thread(target=worker, args=(1,))
        t1.start()
        time.sleep(0.05)  # 让 t1 先进入探测
        t2.start()
        gate.set()
        t1.join(timeout=3)
        t2.join(timeout=3)
        self.assertEqual(calls["n"], 1)
        self.assertTrue(out[0])
        self.assertEqual(out[0], out[1])

    def test_invalidate_during_probe_discards_stale(self):
        import threading

        calls = {"n": 0}
        entered = threading.Event()
        release = threading.Event()

        def slow_ping(sources, stop_event=None):
            calls["n"] += 1
            if calls["n"] == 1:
                entered.set()
                release.wait(timeout=2.0)
                return [(n, u, 15.0) for n, u in sources]
            # 失效后自动重试的第二轮
            return [(n, u, 99.0) for n, u in sources]

        session = ProbeSession(ping_fn=slow_ping)
        result_box = {"r": None}

        def worker():
            result_box["r"] = session.ensure_probed("south_china")

        t = threading.Thread(target=worker)
        t.start()
        self.assertTrue(entered.wait(timeout=2))
        session.invalidate()
        release.set()
        t.join(timeout=3)
        self.assertTrue(session.ready)
        self.assertEqual(calls["n"], 2)
        self.assertEqual(session.selected_results()[0][2], 99.0)
        self.assertEqual(result_box["r"][0][2], 99.0)

    def test_summary_text_keeps_high_latency_values(self):
        session = ProbeSession(
            ping_fn=lambda sources, stop_event=None: [
                ("腾讯-微信", "https://a", 550.0),
                ("阿里云镜像", "https://b", 1551.0),
                ("华为云-Python", "https://c", 3087.0),
            ]
        )
        session.ensure_probed("south_china")
        text = session.summary_text()
        self.assertIn("3087ms", text)
        self.assertIn("1551ms", text)
        self.assertIn("华为云-Python", text)


class TestProgressiveProbe(unittest.TestCase):
    def test_early_return_without_waiting_slowest(self):
        import time

        def ping_one(url: str) -> float:
            if "slow" in url:
                time.sleep(1.2)
                return 40.0
            time.sleep(0.02)
            return 20.0

        sources = [
            ("A", "https://a.example/x"),
            ("B", "https://b.example/x"),
            ("C", "https://c.example/x"),
            ("Slow", "https://slow.example/x"),
        ]
        t0 = time.time()
        partial = parallel_probe(
            sources, ping_one=ping_one, min_reachable=3, early_wait_s=2.5,
            on_complete=lambda _full: None,
        )
        elapsed = time.time() - t0
        self.assertLess(elapsed, 0.8)
        self.assertGreaterEqual(
            sum(1 for *_, lat in partial if lat < 99999), 3,
        )

    def test_on_complete_merges_remaining(self):
        import threading
        import time

        done = threading.Event()
        box = {"full": None}

        def ping_one(url: str) -> float:
            if "slow" in url:
                time.sleep(0.35)
                return 15.0
            return 25.0

        sources = [
            ("A", "https://a.example/x"),
            ("B", "https://b.example/x"),
            ("C", "https://c.example/x"),
            ("Slow", "https://slow.example/x"),
        ]

        def on_complete(full):
            box["full"] = full
            done.set()

        parallel_probe(
            sources, ping_one=ping_one, min_reachable=3, early_wait_s=2.5,
            on_complete=on_complete,
        )
        self.assertTrue(done.wait(timeout=3))
        names = {n for n, _, _ in box["full"]}
        self.assertIn("Slow", names)


class TestBandwidthSample(unittest.TestCase):
    def test_sample_above_threshold_sets_high_bandwidth(self):
        samples = {"n": 0}

        def ping(sources, stop_event=None):
            return [(n, u, 10.0 + i) for i, (n, u) in enumerate(sources)]

        def sample(urls, stop_event=None):
            samples["n"] += 1
            return HIGH_BW_SAMPLE_MBPS + 20

        session = ProbeSession(ping_fn=ping, sample_fn=sample)
        session.ensure_probed("south_china")
        self.assertTrue(session.sample_done)
        self.assertTrue(session.sample_ok)
        self.assertTrue(session.high_bandwidth)
        self.assertEqual(samples["n"], 1)

    def test_sample_below_threshold_stays_normal(self):
        def ping(sources, stop_event=None):
            return [(n, u, 10.0 + i) for i, (n, u) in enumerate(sources)]

        session = ProbeSession(
            ping_fn=ping, sample_fn=lambda urls, stop_event=None: 180.0,
        )
        session.ensure_probed("south_china")
        self.assertTrue(session.sample_ok)
        self.assertFalse(session.high_bandwidth)

    def test_silent_update_keeps_high_bandwidth_without_resample(self):
        samples = {"n": 0}

        def ping(sources, stop_event=None):
            return [(n, u, 10.0 + i) for i, (n, u) in enumerate(sources)]

        def sample(urls, stop_event=None):
            samples["n"] += 1
            return 360.0

        session = ProbeSession(ping_fn=ping, sample_fn=sample)
        session.ensure_probed("south_china")
        self.assertEqual(samples["n"], 1)
        self.assertTrue(session.high_bandwidth)
        # 模拟静默补探换源
        full = session.all_results() + [("新源", "https://new.example/x", 5.0)]
        session._merge_silent_results(full, session._generation)
        self.assertEqual(samples["n"], 1)
        self.assertTrue(session.high_bandwidth)

    def test_resample_only_when_prior_sample_invalid(self):
        samples = {"n": 0}

        def ping(sources, stop_event=None):
            # 只返回 2 个可达 → 摸底不合格
            out = []
            for i, (n, u) in enumerate(sources):
                lat = 10.0 + i if i < 2 else 99999.0
                out.append((n, u, lat))
            return out

        def sample(urls, stop_event=None):
            samples["n"] += 1
            return 100.0 if len(urls) < 3 else 360.0

        session = ProbeSession(ping_fn=ping, sample_fn=sample)
        session.ensure_probed("south_china")
        self.assertFalse(session.sample_ok)
        self.assertEqual(samples["n"], 1)
        # 静默补到足够源
        full = [
            ("A", "https://a", 10.0),
            ("B", "https://b", 12.0),
            ("C", "https://c", 14.0),
            ("D", "https://d", 16.0),
        ]
        session._merge_silent_results(full, session._generation)
        self.assertEqual(samples["n"], 2)
        self.assertTrue(session.high_bandwidth)

    def test_sample_concurrency_in_light_band(self):
        tpu, total = plan_sample_concurrency(3)
        self.assertGreaterEqual(total, 8)
        self.assertLessEqual(total, 12)


class TestDownloadPlanWiring(unittest.TestCase):
    def test_session_high_bandwidth_affects_plan(self):
        def ping(sources, stop_event=None):
            return [(n, u, 10.0 + i) for i, (n, u) in enumerate(sources)]

        low = ProbeSession(
            ping_fn=ping, sample_fn=lambda urls, stop_event=None: 100.0,
        )
        low.ensure_probed("south_china")
        _, total_low = download_plan_for_session(low, 3)

        high = ProbeSession(
            ping_fn=ping, sample_fn=lambda urls, stop_event=None: 400.0,
        )
        high.ensure_probed("south_china")
        _, total_high = download_plan_for_session(high, 3)

        self.assertGreaterEqual(total_low, 30)
        self.assertLess(total_low, 40)
        self.assertGreaterEqual(total_high, 40)
        self.assertGreater(total_high, total_low)


if __name__ == "__main__":
    unittest.main()
