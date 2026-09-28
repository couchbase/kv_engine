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
import sys
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
    "--persist-timeout",
    help="Optional: Seconds to wait for the touched documents to be "
         "persisted when using --fix, defaults to 300",
    default=300,
    type=int)
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
nonUTFSkipped = 0
deletedSkipped = 0
touchAttempted = False

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

                # Deleted documents are counted but cannot be touched, so
                # skip them
                if "deleted" in json_entry:
                    deletedSkipped += 1
                    if args.verbose:
                        print(
                            "Warning deleted key {} has a cas of {} which is "
                            "above the threshold of {}, skipping".format(
                                logical_key, cas, args.caslimit))
                    continue

                # Now we will need to separate out the collection ID, a number
                # which uniquely identifies the collection for this document.
                collection_id = collection_info.split(':')[1]

                if args.fix:
                    # This is the symbol that couch_dbump replaces in its output
                    # when it spots a non UTF-8 character in the document's key.
                    # We cannot fix these documents because we are unable to get
                    # the true key for the document.
                    if "�" in logical_key:
                        nonUTFSkipped += 1
                        print(
                            "Warning: skipping key {} in collection {} "
                            "(cas {}) because it contains non-UTF-8 bytes "
                            "and cannot be repaired via this script".format(
                                logical_key, collection_info, cas
                            )
                        )
                        continue
                    # With the -f option the script writes back to the database
                    # to change the expiry and generate a new CAS, all using
                    # touch.

                    touchAttempted = True

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
                            new_expiry = expiry - 1
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

proc.wait()
if proc.returncode != 0:
    print("Error: {} exited with status {}, results are incomplete".format(
        " ".join(cmd), proc.returncode))
    sys.exit(1)

# Get the high seqno so we can wait for it to be persisted, preventing a race
# with backfill that receives the poisoned document from disk and the fixed
# version from memory. This will cause cas poisoning to re-occur.
highSeqno = None
if touchAttempted:
    stat_key = "vb_{}:high_seqno".format(args.vbucket)
    try:
        seqno_stats = memcache.stats("vbucket-seqno {}".format(args.vbucket))
        highSeqno = int(seqno_stats[stat_key])
    except (mc_bin_client_ns.MemcachedError, KeyError, ValueError) as e:
        print("Warning: Unable to read {} after touching documents: "
              "{}".format(stat_key, e))

# Wait for the high seqno to be persisted, so that all touched documents (and
# their new CAS) are on disk.
highSeqnoPersisted = False
if highSeqno is not None:
    persisted_key = "vb_{}:last_persisted_seqno".format(args.vbucket)
    deadline = time.monotonic() + args.persist_timeout
    persistedSeqno = None
    while True:
        try:
            seqno_stats = memcache.stats(
                "vbucket-seqno {}".format(args.vbucket))
            persistedSeqno = int(seqno_stats[persisted_key])
        except (mc_bin_client_ns.MemcachedError, KeyError, ValueError) as e:
            print("Warning: Unable to read {}: {}".format(persisted_key, e))
            break
        if persistedSeqno >= highSeqno:
            highSeqnoPersisted = True
            break
        if time.monotonic() >= deadline:
            print("Warning: Timed out after {} seconds waiting for seqno {} to "
                  "be persisted, {} is {}".format(
                      args.persist_timeout, highSeqno, persisted_key,
                      persistedSeqno))
            break
        if args.verbose:
            print("Waiting for seqno {} to be persisted, {} is {}".format(
                highSeqno, persisted_key, persistedSeqno))
        time.sleep(1)

if count:
    if args.fix:
        fixed = count - deletedSkipped - nonUTFSkipped - mcClientErrors
        print("Complete with {} of {} documents above threshold now fixed, "
              "Skipped {} deleted documents and "
              "{} documents with non UTF-8 keys".format(
                  fixed, count, deletedSkipped, nonUTFSkipped))
        if highSeqno is not None:
            print("vbucket {} high seqno after all touches is {}, {}".format(
                args.vbucket, highSeqno,
                "persisted" if highSeqnoPersisted else "NOT confirmed as "
                "persisted"))
    else:
        print("Complete with {} documents found above threshold".format(count))
else:
    print("No documents were found with a CAS above threshold")

if mcClientErrors:
    print("Warning the script caught {} mc_bin_client_ns.MemcachedError which "
          "are logged".format(
              mcClientErrors))

if touchAttempted and not highSeqnoPersisted:
    print("Error: Unable to confirm the touched documents were persisted")
    sys.exit(1)
