#!/usr/bin/env python2.7

"""
Copyright 2017-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
"""

# Script to migrate a Gerrit Change from memcached or ep-engine repos
# to the new kv_enigne repo.

from __future__ import print_function
import json
import subprocess
import sys
import urllib2


class bcolors:
    HEADER = '\033[35m'
    OKBLUE = '\033[34m'
    OKGREEN = '\033[32m'
    WARNING = '\033[33m'
    FAIL = '\033[31m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


def header(*args):
    print(bcolors.OKBLUE, end='')
    print(*args, end='')
    print(bcolors.ENDC)


def success(*args):
    print(bcolors.OKGREEN, end='')
    print(*args, end='')
    print(bcolors.ENDC)


if len(sys.argv) != 3:
    print("""Migrates existing Gerrit changes from
memcached/ep-engine repo to kv_engine repo.
Checks out the specifed change, rebases to kv_engine and re-uploads to
gerrit as a kv_engine change.

Usage: gerrit_move_to_kv_engine.py <username> <change>

    username   Username for Gerrit - see
               http://review.couchbase.org/#/settings/ under 'Profile'
    change     Change number

Example: gerrit_move_to_kv_engine.py drigby 78322
    """, file=sys.stderr)
    exit(1)

user = sys.argv[1]
change = sys.argv[2]

info = dict()
# Check this patch exists and determine which project it is from.
try:
    url = "http://review.couchbase.org/changes/{}".format(change)
    f = urllib2.urlopen(url + "?o=CURRENT_REVISION&o=DOWNLOAD_COMMANDS")
    data = f.read()[5:]  # The first 4 chars of response are ')]}'
    info = json.loads(data)
except urllib2.HTTPError as e:
    print ("Error: failed to lookup change {} from {}: {}".format(change,
                                                                  url, e),
           file=sys.stderr)
    print ("Check your Change is valid", file=sys.stderr)
    exit(1)

header("** Migrating change {} - '{}' to kv_engine".format(change,
                                                           info['subject']))

cur_rev_info = info['revisions'][info['current_revision']]
fetch_info = cur_rev_info['fetch']['anonymous http']

header(">> Fetching", fetch_info['ref'])
subprocess.check_output(["git", "fetch", fetch_info['url'],
                         fetch_info['ref']])

header("-- Checking out change", bcolors.ENDC)
subprocess.check_output(["git", "checkout", "FETCH_HEAD"])

download_URL = "git@github.com:couchbase/kv_engine.git"
header("-- Checking out latest kv_engine/master")
subprocess.check_output(["git", "fetch", download_URL, "master"])

header("-- Rebasing change onto kv_engine/master")
if info['project'] == 'ep-engine':
    subprocess.check_output(["git", "rebase",
                             "--strategy=subtree",
                             "--strategy-option=subtree=engines/ep",
                             "FETCH_HEAD"])
else:
    subprocess.check_output(["git", "rebase", "FETCH_HEAD"])

upload_URL = "ssh://{}@review.couchbase.org:29418/kv_engine".format(user)
header("<< Uploading to Gerrit under kv_engine")
subprocess.check_output(["git", "push", upload_URL,
                         "HEAD:refs/for/master"])
success("** Successfully moved change to kv_engine")
