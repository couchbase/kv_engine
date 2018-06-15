#!/usr/bin/env python2.7

"""
Copyright 2018 Couchbase, Inc

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

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
