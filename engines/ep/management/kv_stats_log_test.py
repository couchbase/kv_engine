#!/usr/bin/env python3

'''
Copyright 2023-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
'''

import unittest
import tempfile
import kv_stats_log


class KVStatsLogTest(unittest.TestCase):
    def process_input(self, input, **kwargs):
        infile = tempfile.TemporaryFile(mode='w+b')
        infile.write(input)
        infile.seek(0)

        outfile = tempfile.TemporaryFile(mode='w+b')

        defaulted_kwargs = {'greppable': False,
                            'list_only': False,
                            'command_filter': None,
                            'command_args_filter': None,
                            'bucket_filter': None}
        defaulted_kwargs.update(kwargs)

        kv_stats_log.process_input(outfile, infile, **defaulted_kwargs)

        outfile.seek(0)
        output = outfile.read()

        infile.close()
        outfile.close()

        return output

    def test_basic(self):
        input = (
            b'==============================================================================\n'
            b'memcached stats all\n'
            b'cbstats -a 127.0.0.1:11209 all -u @ns_server\n'
            b'==============================================================================\n'
            b'******************************************************************************\n'
            b'bucketa\n'
            b'  cmd_get:                                                   0\n'
            b'  cmd_set:                                                   0\n'
            b'******************************************************************************\n'
            b'bucketb\n'
            b'  cmd_get:                                                   0\n'
            b'  cmd_set:                                                   0\n'
            b'==============================================================================\n'
            b'memcached stats uuid\n'
            b'cbstats -a 127.0.0.1:11209 uuid -u @ns_server\n'
            b'==============================================================================\n'
            b'******************************************************************************\n'
            b'bucketa\n'
            b' uuid: 7c959220f697cbeaa94cb6df1a5ef86c\n'
            b'******************************************************************************\n'
            b'bucketb\n'
            b' uuid: 41a30377630a0fefbd7175a31db0c89b\n')
        expected_output = (
            b'cbstats all:bucketa:  cmd_get:                                                   0\n'
            b'cbstats all:bucketa:  cmd_set:                                                   0\n'
            b'cbstats all:bucketb:  cmd_get:                                                   0\n'
            b'cbstats all:bucketb:  cmd_set:                                                   0\n'
            b'cbstats uuid:bucketa: uuid: 7c959220f697cbeaa94cb6df1a5ef86c\n'
            b'cbstats uuid:bucketb: uuid: 41a30377630a0fefbd7175a31db0c89b\n')
        output = self.process_input(input, greppable=True)
        self.assertEqual(
            expected_output, output,
            f'\nExpected:\n{expected_output.decode()}\nActual:\n{output.decode()}')


if __name__ == '__main__':
    unittest.main()
