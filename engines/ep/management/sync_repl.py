#!/usr/bin/env python

""" Simple CLI for basic SyncWrite operations."""

from __future__ import print_function
from collections import defaultdict
import mc_bin_client
import sys

if len(sys.argv) < 7:
    print("Usage: {} <host[:port]> <user> <password> <bucket> <op> <key> [value]".format(sys.argv[0]), file = sys.stderr)
    sys.exit(1)

(host, port) = sys.argv[1].split(":")
if not port:
    port = 11210

client = mc_bin_client.MemcachedClient(host=host, port=port)
client.enable_xerror()
client.hello("set_durable")
client.sasl_auth_plain(user=sys.argv[2], password=sys.argv[3])
client.bucket_select(sys.argv[4])

op = sys.argv[5]
key = sys.argv[6]
if len(sys.argv) > 7:
    value = sys.argv[7]

if op == "get":
    print (client.get(key))
elif op == "set":
    print (client.set(key, 0, 0, value))
elif op == "setD":
    print (client.setDurable(key, 0, 0, value))
elif op == "add":
    print (client.add(key, 0, 0, value))
elif op == "addD":
    print (client.addDurable(key, 0, 0, value))
elif op == "replace":
    print (client.replace(key, 0, 0, value))
elif op == "replaceD":
    print (client.replaceDurable(key, 0, 0, value))
elif op == "delete":
    print (client.delete(key, 0, 0))
elif op == "deleteD":
    print (client.deleteDurable(key, 0, 0))
else:
    print("Unknown op '" + op + "'", file=sys.stderr)
