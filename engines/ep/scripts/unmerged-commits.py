#!/usr/bin/env python2.7

"""
Copyright 2017-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
"""

# Script to show which commit(s) are not yet merged between our release branches.

from __future__ import print_function
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
# whole release "trains" for a given project - for example kv_engine/vulcan is
# a train of (5.5.0, 5.5.1, 5.5.2) and should be kept merged into alice
# (6.0.0, 6.0.1, 6.0.2, ...), as new maintenance releases come along.
#
# However, we also have specific branches for a single release
# (e.g. 6.5.0, 7.0.1) which are of limited lifespan - once 6.5.0 has shipped the
# branch will not change and future fixes from say alice which need to
# be included in 6.5.x should be merged into the release train branch
# (e.g. mad-hatter).
#
# As such, there are multiple (currently two) sequence of branches -
# one for the main release trains and (currently) one for 6.5.0 in
# particular.
sequences = {
    'couchstore': [
        [('couchbase/spock', 'couchbase/vulcan'),
         ('couchbase/vulcan', 'couchbase/alice'),
         ('couchbase/alice', 'couchbase/mad-hatter'),
         ('couchbase/mad-hatter', 'couchbase/cheshire-cat'),
         ('couchbase/cheshire-cat', 'couchbase/master')], ],

    'kv_engine': [
        # main kv_engine release train sequence
        [('couchbase/watson_ep',
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
          'couchbase/cheshire-cat'),
         ('couchbase/cheshire-cat',
          'couchbase/master')],
        # kv_engine 6.5.x release train; merging into 6.6.x ('mad-hatter') branch
        [('couchbase/6.5.0',
          'couchbase/6.5.1'),
         ('couchbase/6.5.1',
          'couchbase/6.5.2'),
         ('couchbase/6.5.2',
          'couchbase/mad-hatter')],
        # kv_engine 6.6.x release train.
        [('couchbase/6.6.0',
          'couchbase/6.6.3'),
         ('couchbase/6.6.3',
          'couchbase/mad-hatter')],
        # kv_engine 7.0.x release train; merging into cheshire-cat branch.
        [('couchbase/7.0.0',
          'couchbase/7.0.1'),
         ('couchbase/7.0.1',
          'couchbase/cheshire-cat')]
    ],

    'platform': [
        [('couchbase/spock', 'couchbase/vulcan'),
         ('couchbase/vulcan', 'couchbase/alice'),
         ('couchbase/alice', 'couchbase/mad-hatter'),
         ('couchbase/mad-hatter', 'couchbase/cheshire-cat'),
         ('couchbase/cheshire-cat', 'couchbase/master')], ]
}


project = os.path.basename(
    subprocess.check_output(['git', 'rev-parse', '--show-toplevel']).strip())

total_unmerged = 0
for sequence in sequences[project]:
    for (downstream, upstream) in sequence:
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
