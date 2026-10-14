# -*- coding: utf-8 -*-
"""探测选源 seam 测试：只验证公共行为，不测内部实现。"""

import unittest

from source_selection import (
    DEFAULT_REGION,
    RegionCatalog,
    ProbeSession,
    pick_optimal_sources,
    plan_download_concurrency,
    resolve_region,
)


class TestRegionCatalog(unittest.TestCase):
    def test_south_china_has_at_least_three_sources(self):
        sources = RegionCatalog.sources_for("south_china")
        self.assertGreaterEqual(len(sources), 3)
        for name, url in sources:
            self.assertTrue(name)
            self.assertTrue(url.startswith("https://"))

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


if __name__ == "__main__":
    unittest.main()
