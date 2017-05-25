#!/usr/bin/env python2.7

# Script to show which commit(s) are not yet merged between our release branches.

from __future__ import print_function
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

# Set of branches to check for unmerged patches. Ordered by ancestory;
# i.e. the oldest supported branch to the newest, which is the order
# patches should be merged.
branches = ['couchbase/3.0.x',
            'couchbase/sherlock',
            'couchbase/watson',
            'couchbase/master']

total_unmerged = 0
for downstream, upstream in zip(branches, branches[1:]):
    commits = subprocess.check_output(['git', 'cherry', '-v',
                                       upstream, downstream])
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
