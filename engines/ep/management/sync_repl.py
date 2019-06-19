#!/usr/bin/env python3

""" Simple CLI for basic SyncWrite operations."""

import mc_bin_client
import memcacheConstants
import sys

if len(sys.argv) < 7:
    print("Usage: {} <host[:port]> <user> <password> <bucket> <op> <key> [value] [args]".format(sys.argv[0]), file = sys.stderr)
    sys.exit(1)

(host, port) = sys.argv[1].split(":")
if not port:
    port = 11210

client = mc_bin_client.MemcachedClient(host=host, port=port)
client.enable_xerror()
client.enable_mutation_seqno()
client.hello("set_durable")
client.sasl_auth_plain(user=sys.argv[2], password=sys.argv[3])
client.bucket_select(sys.argv[4])

op = sys.argv[5]
key = sys.argv[6]
if len(sys.argv) > 7:
    value = sys.argv[7]
level = memcacheConstants.DURABILITY_LEVEL_MAJORITY

if op == "get":
    print (client.get(key))
elif op == "set":
    print (client.set(key, 0, 0, value))
elif op == "loop_set":
    count = int(sys.argv[8])
    for i in range(count):
        print (client.set(key, 0, 0, value + "_" + str(i)))
elif op == "setD":
    if len(sys.argv) > 8:
        level = int(sys.argv[8])
    if len(sys.argv) > 9:
        timeout = int(sys.argv[9])
    print (client.setDurable(key, 0, 0, value, level=level, timeout=timeout))
elif op == "bulk_setD":
    count = int(sys.argv[8])
    if len(sys.argv) > 9:
        level = int(sys.argv[9])

    for i in range(count):
        client.setDurable(key + "_" + str(i), 0, 0, value)
elif op == "loop_setD":
    count = int(sys.argv[8])
    for i in range(count):
        print (client.setDurable(key, 0, 0, value + "_" + str(i)))
elif op == "add":
    print (client.add(key, 0, 0, value))
elif op == "addD":
    print (client.addDurable(key, 0, 0, value))
elif op == "replace":
    print (client.replace(key, 0, 0, value))
elif op == "replaceD":
    print (client.replaceDurable(key, 0, 0, value))
elif op == "delete":
    print (client.delete(key))
elif op == "deleteD":
    print (client.deleteDurable(key))
else:
    print("Unknown op '" + op + "'", file=sys.stderr)
