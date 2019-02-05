#!/usr/bin/env python3

"""
Evict the specified key from memory.

Copyright (c) 2017 Couchbase, Inc
"""

import mc_bin_client
import sys

HOST = '127.0.0.1'
PORT = 12000

if len(sys.argv) < 5:
    msg = ('Usage: {} <user> <password> <bucket> <vbid> <key> <optional: "scopename.collectionname" or collection-ID>'.format(
        sys.argv[0]))
    print(msg, file=sys.stderr)
    sys.exit(1)

client = mc_bin_client.MemcachedClient(host=HOST, port=PORT)
client.sasl_auth_plain(user=sys.argv[1], password=sys.argv[2])
client.bucket_select(sys.argv[3])

collection=None
client.enable_xerror()
if len(sys.argv) == 7:
    client.enable_collections()

client.hello("cb_evict_key.py")

if len(sys.argv) == 7:
    try:
        collection=int(sys.argv[6])
    except ValueError:
        collection=sys.argv[6]

key = sys.argv[5]
client.vbucketId = int(sys.argv[4])
print(client.evict_key(key, collection=collection))
