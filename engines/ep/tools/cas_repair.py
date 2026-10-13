#!/usr/bin/env python3

"""
This script's purpose is to look for documents stored in a couchbase vbucket
(when the storage engine is couchstore) that have a CAS above a threshold.
The script can be executed in a way so that those keys which are above
threshold are "mutated" using the touch command so that they can be given a
new CAS. Deleted documents without a value are deleted again using
DeleteWithMeta so that the server generates a new CAS for them. The script
will only really have an affect if the vbucket max_cas has already been
"repaired" using cbepctl

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

# Script only runs against local memcached (it is using this nodes data file)
HOST = "localhost"
PORT = "11210"


class RepairStats:
    """Counters and state gathered whilst processing the couch_dbdump output"""

    def __init__(self):
        # Documents (excluding system events) with a CAS above the threshold
        self.above_threshold = 0
        # Documents above the threshold which are deleted but still have a
        # value (e.g. system xattrs, transactions), so cannot be repaired
        self.deleted_skipped = 0
        # Documents above the threshold which are deleted, but could not be
        # checked for a value (key contains a NUL byte, or the couch_dbdump
        # lookup failed), so were not repaired
        self.deleted_unchecked = 0
        # Documents above the threshold with a non UTF-8 key, so cannot be
        # repaired
        self.non_utf_skipped = 0
        # Documents where a repair failed with a MemcachedError
        self.mc_client_errors = 0
        # Set once the first repair (touch or DeleteWithMeta) has been attempted
        self.repair_attempted = False


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d",
        "--datafile",
        help="Required: Path to the bucket database files",
        required=True)
    parser.add_argument(
        "-u",
        "--username",
        help="Required: Username to authenticate with",
        required=True)
    parser.add_argument(
        "-p",
        "--password",
        help="Required: Password to authenticate with",
        required=True)
    parser.add_argument(
        "-b",
        "--bucket",
        help="Required: The name of the bucket to repair",
        required=True)
    parser.add_argument(
        "-v",
        "--vbucket",
        help="Required: The numerical number of the vbucket to repair, accepts "
             "0 to 1023",
        required=True,
        type=int)
    parser.add_argument(
        "-c",
        "--caslimit",
        help="Optional: Repair documents when CAS exceeds this value. When "
             "omitted documents with CAS greater then now+50 days are will be "
             "repaired",
        type=int)
    parser.add_argument(
        "-f",
        "--fix",
        help="Optional: Actually make changes to the target bucket.vbucket, "
             "otherwise just inspect and print affected documents",
        action='store_true')
    parser.add_argument(
        "--repair-deleted-with-value",
        help="Optional: When using --fix, also repair deleted documents which "
             "still have a value, instead of skipping them. The server keeps "
             "their system xattrs, but any user xattrs and body are removed",
        action='store_true')
    parser.add_argument(
        "--persist-timeout",
        help="Optional: Seconds to wait for the repaired documents to be "
             "persisted when using --fix, defaults to 300",
        default=300,
        type=int)
    parser.add_argument(
        "--verbose",
        help="Optional: Print information about all affected keys",
        action='store_true')
    parser.add_argument(
        "--debug",
        help="Optional: Print information debug information as the script "
             "runs",
        action='store_true')

    args = parser.parse_args()

    if args.vbucket > 1023 or args.vbucket < 0:
        raise Exception(
            "Requested vbucket is out of range 0 to 1023, value given is "
            "{}".format(args.vbucket))

    if args.caslimit is None:
        # When not specified, create a reasonable bad future value, here it is
        # 50 days into the future. The script will be able to fix anything with
        # CAS that exceeds this value.
        args.caslimit = int(time.time() + (60 * 60 * 24 * 50))
        # cas is nanosecond, time.time is seconds
        args.caslimit = args.caslimit * 1000000000

    return args


def check_required_tools():
    for tool in ["couch_dbdump"]:
        if shutil.which(tool) is None:
            raise Exception(
                "cannot find {}. Check $PATH for required tools".format(tool))


def find_datafile(data_dir, vbucket):
    """Find the couchstore file for the vbucket"""
    regex = re.compile("(" + str(vbucket) + "\\.couch.[0-9]*)")
    datafile = None
    for root, dirs, files in os.walk(data_dir):
        for file in files:
            if regex.match(file):
                datafile = data_dir + "/" + file

    if datafile is None:
        raise Exception(
            "No database file was found for vbucket {} in {}".format(
                vbucket, data_dir))
    return datafile


def connect(args):
    """Connect and authenticate to the local memcached"""
    memcache = mc_bin_client_ns.MemcachedClient(host=HOST, port=PORT)
    memcache.sasl_auth_plain(user=args.username, password=args.password)
    memcache.bucket_select(args.bucket)
    memcache.enable_collections()
    memcache.hello("Couchbase CAS repair python script")
    memcache.vbucketId = args.vbucket
    return memcache


def touch_document(memcache, json_entry, logical_key, collection_id, verbose):
    """
    Touch the document so that it is given a new CAS. Touch requires that the
    expiry is mutated, else it won't update and regenerate the CAS.
    """
    cas = json_entry['cas']

    # Convert the collection to an int, mc_bin_client will skip trying (and
    # failing) to map to an ID when the input is an int.
    collection = int(collection_id, 16)
    # Get the expiry
    expiry = json_entry['expiry']

    # If the expiry is 0, a touch of 0 has no affect. In this case set the
    # expiry to some future time (30 days from now) then back to 0
    if expiry == 0:
        if verbose:
            print(
                "Fixing key {} in collection {} which has cas of {}, setting "
                "expiry to +30days and back to 0".format(
                    logical_key, collection_id, cas))

        memcache.touch(logical_key, (60 * 60 * 24 * 30), collection=collection)
        memcache.touch(logical_key, 0, collection=collection)
        return

    # Adjust the expiry by 1 second
    if expiry == 0xffffffff:
        new_expiry = expiry - 1
    else:
        new_expiry = expiry + 1
    if verbose:
        print(
            "Fixing touch of key {} in collection {} which has cas of {}, "
            "changing expiry from {} to {}".format(
                logical_key, collection_id, cas, expiry, new_expiry))
    memcache.touch(logical_key, new_expiry, collection=collection)


def delete_document(memcache, json_entry, logical_key, collection_id, verbose):
    """
    Delete the (already deleted) document again using DeleteWithMeta, so that
    the server generates a new CAS for the tombstone. No value is sent. If the
    tombstone has a value (only with --repair-deleted-with-value), the server
    keeps its system xattrs but removes any user xattrs and body.

    The CAS from couch_dbdump is sent as the request CAS, so the server
    rejects the delete with KEY_EEXISTS if the document has changed since it
    was scanned (e.g. it has been recreated).
    """
    # couch_dbdump outputs the CAS as a string, it must be an int to be sent
    cas = int(json_entry['cas'])

    # Convert the collection to an int, mc_bin_client will skip trying (and
    # failing) to map to an ID when the input is an int.
    collection = int(collection_id, 16)

    # Keep the existing flags and delete time, and increment the rev seqno as
    # touch does. Regenerating the CAS requires skipping conflict resolution.
    rev_seqno = json_entry['rev'] + 1
    # The server rejects a meta CAS of 0 (or one too far in the future), even
    # with REGENERATE_CAS, so send the current time (cas is nanoseconds,
    # time.time is seconds). With REGENERATE_CAS the server replaces it with a
    # generated CAS, and it is not used to advance the vbucket max_cas.
    meta_cas = int(time.time() * 1000000000)
    options = (mc_bin_client_ns.memcacheConstants.REGENERATE_CAS |
               mc_bin_client_ns.memcacheConstants.SKIP_CONFLICT_RESOLUTION_FLAG)
    # Keep the delete source, couch_dbdump reports it as "TTL" or "explicit"
    if json_entry['deleted'] == "TTL":
        options |= mc_bin_client_ns.memcacheConstants.IS_EXPIRATION
    if verbose:
        print(
            "Fixing deleted key {} in collection {} which has cas of {}, "
            "deleting again with a server generated cas".format(
                logical_key, collection_id, cas))
    memcache.delWithMeta(key=logical_key,
                         value='',
                         exp=json_entry['delete_time'],
                         flags=json_entry['flags'],
                         seqno=rev_seqno,
                         remote_cas=meta_cas,
                         collection=collection,
                         options=options,
                         cas=cas)


def deleted_document_has_value(datafile, logical_key, collection_id):
    """
    The main couch_dbdump uses --no-body, so look up the deleted document on
    its own to check if it has a value. A deleted document without a value has
    a null body. Raises subprocess.CalledProcessError if the lookup fails (e.g.
    the file was replaced by compaction or the tombstone was purged), or
    ValueError / KeyError if the output cannot be parsed.
    """
    # couch_dbdump expects the collection ID in decimal
    cmd = ["couch_dbdump", "--json", "--key", logical_key, "--collection",
           str(int(collection_id, 16)), datafile]
    result = subprocess.run(cmd, stdout=subprocess.PIPE, check=True)
    return json.loads(result.stdout)['body'] is not None


def process_document(json_entry, args, memcache, stats, datafile):
    """
    Check the CAS of a document from couch_dbdump. If it is above the threshold
    either report it, or with --fix repair it so that it is given a new CAS.
    """
    cas = json_entry['cas']

    # Only documents with a CAS above our limit need repairing
    if int(cas) <= args.caslimit:
        return

    doc_id = json_entry['id']
    # doc_id needs further processing to split the key/collection
    # Input is "(type:0xf2)key_name" and we want to obtain the type, but
    # also get the logical_key
    collection_info, logical_key = doc_id.split(')', 1)

    if "system" in collection_info:
        # System events have a CAS, but they do not replicate like regular
        # documents, these will get fixed by a regular rebalance and do not
        # pose a threat to the vbucket CAS. Just warn that one was found
        if args.verbose:
            print(
                "A system event {} has a future CAS {}, this isn't a problem "
                "as the CAS will be reset if replicated".format(doc_id, cas))
        return

    stats.above_threshold += 1

    deleted = "deleted" in json_entry

    # Now we will need to separate out the collection ID, a number which
    # uniquely identifies the collection for this document.
    collection_id = collection_info.split(':')[1]

    if not args.fix:
        if args.verbose:
            print(
                "Warning {}key {} in collection {} has a cas of {} which is "
                "above the threshold of {}".format(
                    "deleted " if deleted else "", logical_key,
                    collection_id, cas, args.caslimit))
        return

    # This is the symbol that couch_dbump replaces in its output
    # when it spots a non UTF-8 character in the document's key.
    # We cannot fix these documents because we are unable to get
    # the true key for the document.
    if "�" in logical_key:
        stats.non_utf_skipped += 1
        print(
            "Warning: skipping key {} in collection {} (cas {}) because it "
            "contains non-UTF-8 bytes and cannot be repaired via this "
            "script".format(logical_key, collection_info, cas))
        return

    # Deleted documents without a value are repaired using DeleteWithMeta.
    # Deleted documents which still have a value (e.g. system xattrs) are
    # counted but skipped, unless --repair-deleted-with-value is used, in which
    # case they are repaired without checking for a value
    if deleted and not args.repair_deleted_with_value:
        if "\x00" in logical_key:
            # A NUL byte cannot be passed as an argument to couch_dbdump
            stats.deleted_unchecked += 1
            print(
                "Warning: skipping deleted key {} in collection {} (cas {}) "
                "because it contains a NUL byte so cannot be checked for a "
                "value".format(logical_key, collection_id, cas))
            return
        try:
            has_value = deleted_document_has_value(datafile, logical_key,
                                                   collection_id)
        except (subprocess.CalledProcessError, ValueError, KeyError) as e:
            stats.deleted_unchecked += 1
            print(
                "Warning: skipping deleted key {} in collection {} (cas {}) "
                "because it could not be checked for a value: {}".format(
                    logical_key, collection_id, cas, e))
            return
        if has_value:
            stats.deleted_skipped += 1
            if args.verbose:
                print(
                    "Warning deleted key {} has a cas of {} which is above "
                    "the threshold of {} and has a value, skipping".format(
                        logical_key, cas, args.caslimit))
            return

    # With the -f option the script writes back to the database to generate a
    # new CAS, using DeleteWithMeta for deleted documents and otherwise touch
    # to change the expiry.
    stats.repair_attempted = True
    try:
        if deleted:
            delete_document(memcache, json_entry, logical_key, collection_id,
                            args.verbose)
        else:
            touch_document(memcache, json_entry, logical_key, collection_id,
                           args.verbose)
    except mc_bin_client_ns.MemcachedError as e:
        print(
            "Warning: Caught a MemcachedError {} whilst processing key "
            "{}".format(e, logical_key))
        stats.mc_client_errors += 1


def process_datafile(args, datafile, memcache):
    """
    Using couch_dbdump, obtain the metadata of every key and process each one.
    Exits if the output of couch_dbdump cannot be parsed, or if it fails.
    """
    cmd = ["couch_dbdump", "--no-body", "--json", datafile]
    stats = RepairStats()

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    for line in proc.stdout:
        # Running with --json so we can easily parse the metadata into the
        # required components
        if args.debug:
            print(line)

        try:
            json_entry = json.loads(line)
        except Exception as e:
            print("Warning: Caught an exception {} whilst processing "
                  "line {}".format(e, line))
            sys.exit(1)

        process_document(json_entry, args, memcache, stats, datafile)

    proc.wait()
    if proc.returncode != 0:
        print("Error: {} exited with status {}, results are incomplete".format(
            " ".join(cmd), proc.returncode))
        sys.exit(1)

    return stats


def get_high_seqno(memcache, vbucket):
    """
    Touch does not return the seqno of the mutation, so read the vbucket high
    seqno once all repairs are done instead. Returns None if the high seqno
    cannot be read.
    """
    stat_key = "vb_{}:high_seqno".format(vbucket)
    try:
        seqno_stats = memcache.stats("vbucket-seqno {}".format(vbucket))
        return int(seqno_stats[stat_key])
    except (mc_bin_client_ns.MemcachedError, KeyError, ValueError) as e:
        print("Warning: Unable to read {} after repairing documents: "
              "{}".format(stat_key, e))
        return None


def wait_for_persistence(memcache, vbucket, seqno, timeout, verbose):
    """
    Wait for the seqno to be persisted, so that all repaired documents (and
    their new CAS) are on disk. Returns True if seqno was persisted within
    timeout seconds.
    """
    persisted_key = "vb_{}:last_persisted_seqno".format(vbucket)
    deadline = time.monotonic() + timeout
    while True:
        try:
            seqno_stats = memcache.stats("vbucket-seqno {}".format(vbucket))
            persisted_seqno = int(seqno_stats[persisted_key])
        except (mc_bin_client_ns.MemcachedError, KeyError, ValueError) as e:
            print("Warning: Unable to read {}: {}".format(persisted_key, e))
            return False
        if persisted_seqno >= seqno:
            return True
        if time.monotonic() >= deadline:
            print("Warning: Timed out after {} seconds waiting for seqno {} to "
                  "be persisted, {} is {}".format(
                      timeout, seqno, persisted_key, persisted_seqno))
            return False
        if verbose:
            print("Waiting for seqno {} to be persisted, {} is {}".format(
                seqno, persisted_key, persisted_seqno))
        time.sleep(1)


def print_summary(args, stats, high_seqno, persisted):
    if stats.above_threshold:
        if args.fix:
            fixed = (stats.above_threshold - stats.deleted_skipped -
                     stats.deleted_unchecked - stats.non_utf_skipped -
                     stats.mc_client_errors)
            print("Complete with {} of {} documents above threshold now "
                  "fixed, Skipped {} deleted documents with a value, {} "
                  "deleted documents which could not be checked and {} "
                  "documents with non UTF-8 keys".format(
                      fixed, stats.above_threshold, stats.deleted_skipped,
                      stats.deleted_unchecked, stats.non_utf_skipped))
            if high_seqno is not None:
                print("vbucket {} high seqno after all repairs is {}, "
                      "{}".format(
                          args.vbucket, high_seqno,
                          "persisted" if persisted else
                          "NOT confirmed as persisted"))
        else:
            print("Complete with {} documents found above threshold".format(
                stats.above_threshold))
    else:
        print("No documents were found with a CAS above threshold")

    if stats.mc_client_errors:
        print("Warning the script caught {} mc_bin_client_ns.MemcachedError "
              "which are logged".format(stats.mc_client_errors))


def main():
    args = parse_args()
    check_required_tools()

    datafile = find_datafile(args.datafile, args.vbucket)
    print("Processing with {}".format(datafile))

    memcache = None
    if args.fix:
        # Only need to connect/auth when fixing
        memcache = connect(args)

    stats = process_datafile(args, datafile, memcache)

    # Get the high seqno so we can wait for it to be persisted, preventing a
    # race with backfill that receives the poisoned document from disk and the
    # fixed version from memory. This will cause cas poisoning to re-occur.
    high_seqno = None
    persisted = False
    if stats.repair_attempted:
        high_seqno = get_high_seqno(memcache, args.vbucket)
        if high_seqno is not None:
            persisted = wait_for_persistence(memcache, args.vbucket,
                                             high_seqno, args.persist_timeout,
                                             args.verbose)

    print_summary(args, stats, high_seqno, persisted)

    if stats.repair_attempted and not persisted:
        print("Error: Unable to confirm the repaired documents were "
              "persisted")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
