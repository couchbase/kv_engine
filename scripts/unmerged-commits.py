#!/usr/bin/env python3

"""
Copyright 2017-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
"""

# Script to show which commit(s) are not yet merged between our release
# branches.


import os
import subprocess
import sys


class bcolors:
    """Define ANSI color codes, if we're running under a TTY."""
    if sys.stdout.isatty():
        HEADER = '\033[36m'
        WARNING = '\033[33m'
        INFO = '\033[0;34m'
        FAINT = '\033[2m'
        ENDC = '\033[0m'
    else:
        HEADER = ''
        WARNING = ''
        INFO = ''
        FAINT = ''
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
        [('couchbase/trinity', 'couchbase/master')],
        [('couchbase/neo', 'couchbase/trinity')]
    ],

    'kv_engine': [
        # main kv_engine release train sequence
        [('couchbase/cypher', 'couchbase/master')],
        [('couchbase/trinity', 'couchbase/cypher')],
        [('couchbase/neo', 'couchbase/trinity')],
        [('couchbase/7.2.4', 'couchbase/neo')],

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
        [('couchbase/trinity', 'couchbase/master')],

        [('couchbase/neo', 'couchbase/trinity')],

        # platform 7.1.x maintenance train
        [('couchbase/7.1.4', 'couchbase/neo')],
    ],

    'sigar': [
        # main sigar release train
        [('couchbase/neo', 'couchbase/master')]
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
        commits = commits.splitlines()
        count = len(commits)
        total_unmerged += count
        if count > 0:
            print(
                f"{bcolors.HEADER}{count} commits in '{downstream}' not present in '{upstream}':{bcolors.ENDC}")
            for commit in commits:
                if commit[0] == '-':
                    # Equivalent in upstream, de-emphasise
                    color = bcolors.FAINT
                else:
                    color = bcolors.ENDC
                print(f"  {color}{commit}{bcolors.ENDC}")
            print()

if total_unmerged:
    print(f"{bcolors.INFO}Commits listed in oldest to newest order.{bcolors.ENDC}")
    print(f"{bcolors.FAINT}  '-' for commits that have an equivalent in <upstream>.{bcolors.ENDC}")
    print("  '+' for commits that do not.")
    print()
    print(f"{bcolors.WARNING}Total of {total_unmerged} commits outstanding{bcolors.ENDC}")

sys.exit(total_unmerged)
