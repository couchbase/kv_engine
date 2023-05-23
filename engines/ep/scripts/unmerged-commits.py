#!/usr/bin/env python3

"""
Copyright 2017-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
"""

# Script to show which commit(s) are not yet merged between our release branches.


import os
import subprocess
import sys


class bcolors:
    """Define ANSI color codes, if we're running under a TTY."""
    if sys.stdout.isatty():
        HEADER = '\033[36m'
        WARNING = '\033[33m'
        ENDC = '\033[0m'
    else:
        HEADER = ''
        WARNING = ''
        ENDC = ''


# Sequences of branches to check for unmerged patches. Each toplevel
# element is a series of branches (ordered by ancestry) which should
# be merged into each other.  i.e. the oldest supported branch to the
# newest, which is the order patches should be merged.
#
# There is a single sequence of branches for branches representing
# whole release "trains" for a given project - for example
# kv_engine/7.1.x is a train of (7.1.0, 7.1.1, 7.1.2, ...) and should
# be kept merged into neo (7.2.0, ...), as new maintenance releases
# come along.
#
# However, we sometimes have specific branches for a single release
# (e.g. 7.1.4) to support maintenance patches (MPs) which occur
# concurrently alongside the next GA release - 7.1.4-MP1, 7.1.4-MP2,
# ...
#
# As such, there are multiple sequence of branches -
# one for the main release train and one for each set of branches for MPs.
sequences = {
    'couchstore': [
         [('couchbase/neo', 'couchbase/master')]
    ],

    'kv_engine': [
        # main kv_engine release train sequence
        [('couchbase/neo',
          'couchbase/master')],
        # kv_engine 7.1.x release train; one branch for each
        # maintenance release which required subsequent maintenance
        # patches, finishing in neo branch.
        [('couchbase/7.1.3',
          'couchbase/7.1.4'),
         ('couchbase/7.1.4',
          'couchbase/7.1.x'),
         ('couchbase/7.1.x',
          'couchbase/neo')]
    ],

    'platform': [
        # main platform release train
        [('couchbase/neo', 'couchbase/master')],

        # platform 7.1.x maintenance train
        [('couchbase/7.1.4', 'couchbase/neo')],
    ]
}


project = os.path.basename(
    subprocess.check_output(['git', 'rev-parse', '--show-toplevel'],
                            text=True).strip())

total_unmerged = 0
for sequence in sequences[project]:
    for (downstream, upstream) in sequence:
        commits = subprocess.check_output(['git', 'cherry', '-v',
                                           upstream, downstream],
                                          text=True)
        count = len(commits.splitlines())
        total_unmerged += count
        if count > 0:
            print((bcolors.HEADER +
                   "{} commits in '{}' not present in '{}':" +
                   bcolors.ENDC).format(count, downstream, upstream))
            print(commits)

if total_unmerged:
    print((bcolors.WARNING + "Total of {} commits outstanding" +
           bcolors.ENDC).format(total_unmerged))

sys.exit(total_unmerged)
