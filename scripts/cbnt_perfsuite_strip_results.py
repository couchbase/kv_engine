#!/usr/bin/env python2.7

"""
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.

This script removes lines from the perfsuite test files which contain
a match to the input strings

Usage: python cbnt_perfsuite_strip_results.py -d . -p output -i '.pct99' -i '.pct95'
"""

import optparse
from os import listdir
from os.path import isfile, join
import sys


def main():
    parser = optparse.OptionParser()
    required_args = optparse.OptionGroup(parser, 'Required Arguments')
    required_args.add_option('-d', '--dir', action='store', type='string',
                             dest='input_dir', help='The directory where the '
                                                    'test files are located')
    required_args.add_option('-p', '--prefix', action='store', type='string',
                             dest='prefix', help='The prefix each of the test '
                                                 'files start with')
    required_args.add_option('-i', '--ignore', dest='ignore', action='append',
                             help='Section of string used to ignore results, '
                                  'eg \'.pct96\'')
    parser.add_option_group(required_args)
    (options, args) = parser.parse_args()

    # Check no arguments were passed to the script,
    # everything is done through options parsing
    if len(args) != 0:
        print('cbnt_perfsuite_strip_results does not take any direct arguments')
        parser.print_help()
        sys.exit(-2)

    # Check that all options have a setting, even the
    # optional ones. Optional args should all have default values.
    for option in options.__dict__:
        if options.__dict__[option] is None:
            print('Some required arguments were not set')
            parser.print_help()
            sys.exit(-2)

    # Get a list of all files in the directory with the matching prefix
    input_files = [f for f in listdir(options.input_dir.strip()) if
                   (isfile(join(options.input_dir.strip(), f)) and
                    f.startswith(options.prefix))]

    # Strip out the results from the files matching the input ignores
    for i in input_files:
        f = open(i, 'r')
        lines = f.readlines()
        f.close()

        f = open(i, 'w')
        for l in lines:
            if any(x in l for x in options.ignore):
                continue
            f.write(l)
        f.close()


if __name__ == '__main__':
    main()
