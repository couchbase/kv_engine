"""
Copyright 2017-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
"""

import unittest
import cbstats

class CBStatsTest(unittest.TestCase):
    def test_time_label(self):
        self.assertEqual(cbstats.time_label(100), ' 100us')
        self.assertEqual(cbstats.time_label(10000), '10000us')
        self.assertEqual(cbstats.time_label(1000*1000), '1000ms')
        self.assertEqual(cbstats.time_label(10*1000*1000), '10000ms')
        self.assertEqual(cbstats.time_label(600*1000*1000), ' 600s')
        self.assertEqual(cbstats.time_label(605000000), '10m:05s')

if __name__ == '__main__':
    unittest.main()