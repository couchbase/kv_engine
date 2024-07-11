#!/usr/bin/env python3

"""
This script's purpose is to look for documents stored in a couchbase vbucket
(when the storage engine is couchstore) that have a CAS above a threshold.
The script can be executed in a way so that those keys which are above
threshold are "mutated" using the touch command so that they can be given a
new CAS. The script will only really have an affect if the vbucket max_cas
has already been "repaired" using cbepctl

To ship this as a standalone requires a mc_bin_client with this patch

* https://review.couchbase.org/c/kv_engine/+/191118

"""

import argparse
import json

# Import a mc_bin_client that doesn't exist, i.e. require that a new file is
# created using https://review.couchbase.org/c/kv_engine/+/191118
import mc_bin_client_ns
import os
import re
import shutil
import subprocess
import time

argParser = argparse.ArgumentParser()
argParser.add_argument(
    "-d",
    "--datafile",
    help="Required: Path to the bucket database files",
    required=True)
argParser.add_argument(
    "-u",
    "--username",
    help="Required: Username to authenticate with",
    required=True)
argParser.add_argument(
    "-p",
    "--password",
    help="Required: Password to authenticate with",
    required=True)
argParser.add_argument(
    "-b",
    "--bucket",
    help="Required: The name of the bucket to repair",
    required=True)
argParser.add_argument(
    "-v",
    "--vbucket",
    help="Required: The numerical number of the vbucket to repair, accepts 0 "
         "to 1023",
    required=True,
    type=int)
argParser.add_argument(
    "-c",
    "--caslimit",
    help="Optional: Repair documents when CAS exceeds this value. When omitted "
         "documents with CAS greater then now+50 days are will be repaired",
    type=int)
argParser.add_argument(
    "-f",
    "--fix",
    help="Optional: Actually make changes to the target bucket.vbucket, "
         "otherwise just inspect and print affected documents",
    action='store_true')
argParser.add_argument(
    "--verbose",
    help="Optional: Print information about all affected keys",
    action='store_true')
argParser.add_argument(
    "--debug",
    help="Optional: Print information debug information as the script runs",
    action='store_true')

args = argParser.parse_args()

if args.vbucket > 1023 or args.vbucket < 0:
    raise Exception(
        "Requested vbucket is out of range 0 to 1023, value given is {}".format(
            args.vbucket))

for tool in ["couch_dbdump"]:
    if shutil.which(tool) is None:
        raise Exception(
            "cannot find {}. Check $PATH for required tools".format(tool))

if args.caslimit is None:
    # When not specified, create a reasonable bad future value, here it is 50
    # days into the future. The script will be able to fix anything with CAS
    # that exceeds this value.
    args.caslimit = int(time.time() + (60 * 60 * 24 * 50))
    # cas is nanosecond, time.time is seconds
    args.caslimit = args.caslimit * 1000000000


# Find the couchstore file for the vbucket
regex = re.compile("(" + str(args.vbucket) + "\\.couch.[0-9]*)")
datafile = None
for root, dirs, files in os.walk(args.datafile):
    for file in files:
        if regex.match(file):
            datafile = args.datafile + "/" + file

if datafile is None:
    raise Exception(
        "No database file was found for vbucket {} in {}".format(
            args.vbucket, args.datafile))
else:
    print("Processing with {}".format(datafile))

# Script only runs against local memcached (it is using this nodes data file)
HOST = "localhost"
PORT = "11210"

if args.fix:
    # Only need to connect/auth when fixing
    memcache = mc_bin_client_ns.MemcachedClient(host=HOST, port=PORT)
    memcache.sasl_auth_plain(user=args.username, password=args.password)
    memcache.bucket_select(args.bucket)
    memcache.enable_collections()
    memcache.hello("Couchbase CAS repair python script")
    memcache.vbucketId = args.vbucket

# Using couch_dbdump, obtain the metadata of every key. For each key...
# 1) Check if the document CAS is greater than our threshold value
cmd = ["couch_dbdump", "--no-body", "--json", datafile]
count = 0
mcClientErrors = 0

proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
for line in proc.stdout:
    # Running with --json so we can easily parse the metadata into the required
    # components
    if args.debug:
        print(line)

    try:
        json_entry = json.loads(line)
    except Exception as e:
        print("Warning: Caught an exception {} whilst processing "
              "line {}".format(e, line))
        sys.exit(1)

    cas = json_entry['cas']

    # Skip deleted documents
    # if "deleted" in json_entry:
    #    continue

    # Is CAS above our limit?
    if int(cas) > args.caslimit:
        try:
            doc_id = json_entry['id']
            # doc_id needs further processing to split the key/collection
            # Input is "(type:0xf2)key_name" and we want to obtain the type, but
            # also get the logical_key
            collection_info, logical_key = doc_id.split(')', 1)

            if "system" in collection_info:
                # System events have a CAS, but they do not replicate like
                # regular documents, these will get fixed by a regular rebalance
                # and do not pose a threat to the vbucket CAS. Just warn that
                # one was found
                if args.verbose:
                    print(
                        "A system event {} has a future CAS {}, this isn't a "
                        "problem as the CAS will be reset if "
                        "replicated".format(
                            doc_id, cas))
            else:
                count = count + 1

                # Now we will need to separate out the collection ID, a number
                # which uniquely identifies the collection for this document.
                collection_id = collection_info.split(':')[1]

                if args.fix:
                    # With the -f option the script writes back to the database
                    # to change the expiry and generate a new CAS, all using
                    # touch.

                    # Convert the collection to an int, mc_bin_client will skip
                    # trying (and failing) to map to an ID when the input is an
                    # int.
                    collection = int(collection_id, 16)
                    # Get the expiry
                    expiry = json_entry['expiry']

                    # If the expiry is 0, a touch of 0 has no affect. In this
                    # case set the expiry to some future time (30 days from now)
                    # then back to 0
                    if expiry == 0:
                        if args.verbose:
                            print(
                                "Fixing key {} in collection {} which has "
                                "cas of {}, setting expiry to +30days and "
                                "back to 0".format(
                                    logical_key, collection_id, cas))

                        memcache.touch(logical_key, (60 * 60 * 24 * 30),
                                       collection=collection)
                        memcache.touch(logical_key, 0, collection=collection)
                    else:
                        # Touch requires that the expiry is mutated, else it
                        # won't update and regeneate the CAS. Here we adjust by
                        # 1 second
                        if expiry == 0xffffffff:
                            expiry = expiry - 1
                        else:
                            new_expiry = expiry + 1
                        if args.verbose:
                            print(
                                "Fixing touch of key {} in collection {} which "
                                "has cas of {}, changing expiry "
                                "from {} to {}".format(
                                    logical_key, collection_id, cas, expiry, new_expiry))
                        memcache.touch(
                            logical_key, new_expiry, collection=collection)
                elif args.verbose:
                    print(
                        "Warning key {} in collection {} has a cas of {} which "
                        "is above the threshold of {}".format(
                            logical_key, collection_id, cas, args.caslimit))
        except mc_bin_client_ns.MemcachedError as e:
            print(
                "Warning: Caught a MemcachedError {} whilst processing key {}".format(
                    e, logical_key))
            mcClientErrors = mcClientErrors + 1


if count:
    if args.fix:
        print("Complete with {} documents now fixed".format(count))
    else:
        print("Complete with {} documents found above threshold".format(count))
else:
    print("No documents were found with a CAS above threshold")

if mcClientErrors:
    print("Warning the script caught {} mc_bin_client_ns.MemcachedError which "
          "are logged".format(
              mcClientErrors))
