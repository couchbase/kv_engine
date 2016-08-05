#!/usr/bin/env python2.7

# Script to show which commit(s) are not yet merged between our release branches.

from __future__ import print_function
import subprocess

class bcolors:
    HEADER = '\033[36m'
    OKBLUE = '\033[34m'
    OKGREEN = '\033[32m'
    WARNING = '\033[33m'
    FAIL = '\033[31m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

# Set of branches to check for unmerged patches. Ordered by ancestory;
# i.e. the oldest supported branch to the newest, which is the order
# patches should be merged.
branches = ['couchbase/3.0.x',
            'couchbase/sherlock',
            'couchbase/watson',
            'couchbase/master']

for downstream, upstream in zip(branches, branches[1:]):
    print((bcolors.HEADER +
           "Commits in '{}' not present in '{}':" +
           bcolors.ENDC).format(downstream, upstream))
    commits = subprocess.check_output(['git', 'cherry', '-v',
                                       upstream, downstream])
    print(commits)

    count = len(commits.splitlines())
    if count > 0:
        print ((bcolors.WARNING +
                "{} commits outstanding" + bcolors.ENDC).format(count))
