#!/usr/bin/env python3

"""
Simple CLI for basic SyncWrite operations.

  Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.

"""

import mc_bin_client
import memcacheConstants
import sys

if len(sys.argv) < 8:
    print("Usage: {} <host[:port]> <user> <password> <bucket> <op> <key> "
          "<collection> [value] [args]".format(sys.argv[0]), file = sys.stderr)
    print("Note collection is the complete scope.collection name")
    sys.exit(1)

(host, port) = sys.argv[1].split(":")
if not port:
    port = 11210

client = mc_bin_client.MemcachedClient(host=host, port=port)
client.enable_xerror()
client.enable_mutation_seqno()
client.enable_tracing()
client.enable_collections()
client.hello("set_durable")
client.sasl_auth_plain(user=sys.argv[2], password=sys.argv[3])
client.bucket_select(sys.argv[4])

op = sys.argv[5]
key = sys.argv[6]
collection = sys.argv[7]
if len(sys.argv) > 8:
    value = sys.argv[8]
level = memcacheConstants.DURABILITY_LEVEL_MAJORITY
timeout = 1000

if op == "get":
    print (client.get(key, collection=collection))
elif op == "set":
    print (client.set(key, 0, 0, value, collection=collection))
elif op == "bulk_set":
    count = int(sys.argv[9])
    for i in range(count):
        client.set(key + "_" + str(i),
                   0,
                   0,
                   value,
                   collection=collection)
elif op == "loop_set":
    count = int(sys.argv[9])
    for i in range(count):
        print (client.set(key,
                          0,
                          0,
                          value + "_" + str(i),
                          collection=collection))
elif op == "setD":
    if len(sys.argv) > 9:
        level = int(sys.argv[9])
    if len(sys.argv) > 10:
        timeout = int(sys.argv[10])
    print (client.setDurable(key, 0, 0, value, level=level, timeout=timeout,
                             collection=collection))
elif op == "bulk_setD":
    count = int(sys.argv[9])
    if len(sys.argv) > 10:
        level = int(sys.argv[10])

    for i in range(count):
        client.setDurable(key + "_" + str(i),
                          0,
                          0,
                          value,
                          collection=collection)
elif op == "loop_setD":
    count = int(sys.argv[9])
    if len(sys.argv) > 10:
        level = int(sys.argv[10])

    for i in range(count):
        client.setDurable(key,
                          0,
                          0,
                          value + "_" + str(i),
                          level=level,
                          collection=collection)
elif op == "add":
    print (client.add(key, 0, 0, value, collection=collection))
elif op == "addD":
    print (client.addDurable(key, 0, 0, value, collection=collection))
elif op == "replace":
    print (client.replace(key, 0, 0, value, collection=collection))
elif op == "replaceD":
    print (client.replaceDurable(key, 0, 0, value, collection=collection))
elif op == "delete":
    print (client.delete(key, collection=collection))
elif op == "deleteD":
    print (client.deleteDurable(key, collection=collection))
else:
    print("Unknown op '" + op + "'", file=sys.stderr)
