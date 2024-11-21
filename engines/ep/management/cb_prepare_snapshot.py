#!/usr/bin/env python3

"""
Prepare a snapshot of the specified vBucket.

  Copyright 2024-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.

"""

import mc_bin_client
import sys

HOST = '127.0.0.1'
PORT = 12000

if len(sys.argv) < 4:
    msg = ('Usage: {} <user> <password> <bucket> <vbid>'.format(
        sys.argv[0]))
    print(msg, file=sys.stderr)
    sys.exit(1)

client = mc_bin_client.MemcachedClient(host=HOST, port=PORT)
client.sasl_auth_plain(user=sys.argv[1], password=sys.argv[2])
client.bucket_select(sys.argv[3])

client.enable_xerror()
client.enable_collections()

client.hello("cb_prepare_snapshot.py")
client.vbucketId = int(sys.argv[4])

# Returns JSON manifest
print(client.prepare_snapshot())
