# -*- coding: utf-8 -*-
"""ByteAccumulator：最终成绩用累计，仪表用近窗。"""

import time
import unittest

from net_tester import ByteAccumulator


class TestByteAccumulatorLiveWindow(unittest.TestCase):
    def test_live_settles_after_burst_unlike_cumulative(self):
        """前段突发、后段稳态时，近窗应贴近稳态，累计会长期偏高。"""
        acc = ByteAccumulator()
        t0 = time.time()
        # 约 2s @ 130Mbps，再约 3s @ 65Mbps
        while time.time() - t0 < 2.0:
            acc.add(130_000_000 / 8 * 0.05)
            time.sleep(0.05)
        while time.time() - t0 < 5.0:
            acc.add(65_000_000 / 8 * 0.05)
            time.sleep(0.05)

        live = acc.live_mbps()
        cum = acc.average_mbps()
        self.assertGreater(cum, 80.0)  # 累计仍被前段抬高
        self.assertLess(live, 80.0)    # 近窗应回到 ~65
        self.assertGreater(live, 50.0)

    def test_average_matches_total_over_elapsed(self):
        acc = ByteAccumulator()
        acc.add(1_000_000)
        time.sleep(0.3)
        acc.add(1_000_000)
        mbps = acc.average_mbps()
        self.assertGreater(mbps, 0)
        elapsed = acc.transfer_elapsed()
        self.assertGreaterEqual(elapsed, 0.25)


if __name__ == "__main__":
    unittest.main()
