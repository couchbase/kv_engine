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
# concurrently alongside the next GA release - 7.1.4-MP1, 7.1.4-MP2.
# Those should be merged back into the GA release branch.
#
# As such, there are multiple sequence of branches -
# one for the main release train and one for each MP branch.
sequences = {
    'couchstore': [
        [('couchbase/morpheus', 'couchbase/master')],
        [('couchbase/trinity', 'couchbase/morpheus')],
        [('couchbase/neo', 'couchbase/trinity')]
    ],

    'kv_engine': [
        # main kv_engine release train sequence
        [('couchbase/morpheus', 'couchbase/master')],
        [('couchbase/trinity', 'couchbase/morpheus')],
        [('couchbase/neo', 'couchbase/trinity')],

        # 7.6 release train (trinity)
        [('couchbase/7.6.2', 'couchbase/trinity'),
         ('couchbase/7.6.3', 'couchbase/trinity'),
         ('couchbase/7.6.4', 'couchbase/trinity'),
         ('couchbase/7.6.5', 'couchbase/trinity')],

        # 7.2 release train (neo) (neo is the confusing release name
        # used both for 7.1 and 7.2...). Ideally we should have
        # 7.2.4 -> 7.2.5, but the script reports unmerged changes
        # and it would be surprising if we suddenly started to
        # create new 7.2.5 builds with the forward merge at this point
        [('couchbase/7.2.5', 'couchbase/neo'),
         ('couchbase/7.2.6', 'couchbase/neo')],

        # kv_engine 7.1.x release train; one branch for each
        # maintenance release which required subsequent maintenance
        # patches, finishing in neo branch.
        [('couchbase/7.1.3', 'couchbase/7.1.x'),
         ('couchbase/7.1.4', 'couchbase/7.1.x'),
         ('couchbase/7.1.x', 'couchbase/neo')]
    ],

    'phosphor': [
        [('couchbase/morpheus', 'couchbase/master')],
        [('couchbase/trinity', 'couchbase/morpheus')],
        [('couchbase/neo', 'couchbase/trinity')]
    ],

    'platform': [
        # main platform release train
        [('couchbase/morpheus', 'couchbase/master')],
        [('couchbase/trinity', 'couchbase/morpheus')],
        [('couchbase/neo', 'couchbase/trinity')],

        # platform 7.1.x maintenance train
        [('couchbase/7.1.4', 'couchbase/7.1.x'),
         ('couchbase/7.1.4', 'couchbase/neo')]
    ],

    'sigar': [
        # main sigar release train
        [('couchbase/morpheus', 'couchbase/master')],
        [('couchbase/trinity', 'couchbase/morpheus')],
        [('couchbase/neo', 'couchbase/trinity')]
    ]
}


def format_commit(sha):
    """Pretty prints the commit."""
    # Short hash, message, committer name
    return subprocess.check_output(
        ['git', 'log', '-1', '--pretty=format:%h %s (%cn)', sha],
        text=True)

project = os.path.basename(
    subprocess.check_output(['git', 'rev-parse', '--show-toplevel'],
                            text=True).strip())

total_unmerged = 0
for sequence in sequences[project]:
    for (downstream, upstream) in sequence:
        commits = subprocess.check_output(['git', 'cherry',
                                           upstream, downstream],
                                          text=True)
        commits = commits.splitlines()
        count = len(commits)
        total_unmerged += count
        if count > 0:
            print(
                f"{bcolors.HEADER}{count} commits in '{downstream}' not present in '{upstream}':{bcolors.ENDC}")
            for commit in commits:
                status, sha = commit.split(' ', maxsplit=1)
                commit_text = f'{status} {format_commit(sha)}'
                if status == '-':
                    # Equivalent in upstream, de-emphasise
                    color = bcolors.FAINT
                else:
                    color = bcolors.ENDC
                print(f"  {color}{commit_text}{bcolors.ENDC}")
            print()

if total_unmerged:
    print(f"{bcolors.INFO}Commits listed in oldest to newest order.{bcolors.ENDC}")
    print(f"{bcolors.FAINT}  '-' for commits that have an equivalent in <upstream>.{bcolors.ENDC}")
    print("  '+' for commits that do not.")
    print()
    print(f"{bcolors.WARNING}Total of {total_unmerged} commits outstanding{bcolors.ENDC}")

sys.exit(total_unmerged)
