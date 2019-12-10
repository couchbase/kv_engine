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

# Branches to check for unmerged patches. Each toplevel element is a series
# of branches (ordered by ancestory) which should be merged into each other.
# i.e. the oldest supported branch to the newest, which is the order
# patches should be merged.
branches = (('couchbase/watson_ep',
             'couchbase/spock'),
            ('couchbase/watson_mc',
             'couchbase/spock'),
            ('couchbase/spock',
             'couchbase/vulcan'),
            ('couchbase/vulcan',
             'couchbase/alice'),
            ('couchbase/alice',
             'couchbase/mad-hatter'),
            ('couchbase/mad-hatter',
             'couchbase/master'))

total_unmerged = 0
for series in branches:
    for downstream, upstream in zip(series, series[1:]):
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
