#!/usr/bin/env python2.7

"""
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.

This script takes the JSON output from a Google Benchmark testsuite and
produces an XML version of the same report, which we can use for the CBNT
performance test system.

Usage: python benchmark2xml.py -f some_test.json -o test-detail.xml -t ms -c
"""


import optparse
import sys
import os
import json
import collections


def main():
    parser = optparse.OptionParser()
    required_args = optparse.OptionGroup(parser, 'Required Arguments')
    required_args.add_option('-b', '--benchmark_file', action='store',
                             type='string', dest='input_file',
                             help='The input file from Google Benchmark')
    required_args.add_option('-o', '--output_file', action='store',
                             type='string', dest='output_file',
                             help='The file to output the generated XML to')
    required_args.add_option('-c', '--cbnt_metrics', action='store',
                             dest='cbnt_metrics', type='string',
                             help='String that specifies the list of metrics '
                                  'to be tracked, comma separated. That can '
                                  'be any of the metrics measured in a bench '
                                  '(eg, GBench real_time\'/\'cpu_time\', or '
                                  'any user metric.)')
    parser.add_option_group(required_args)

    optional_args = optparse.OptionGroup(parser, 'Optional Arguments')
    optional_args.add_option('-s', '--separator', action='store',
                             dest='separator', type='string', default='/',
                             help='The separator character used in the test '
                                  'name')
    optional_args.add_option('-i', '--in_place', action='store_true',
                             dest='consume', default=False,
                             help='Edit the input file in place. '
                                  '(Note, destroys the original file)')
    optional_args.add_option('-n', '--name', action='store', dest='suite_name',
                             type='string', default="",
                             help='An optional string which gets added to the '
                                  'start of each test suite name. For example'
                                  '"Logger/".')
    parser.add_option_group(optional_args)
    (options, args) = parser.parse_args()

    # Check no arguments were passed to the script,
    # everything is done through options parsing
    if len(args) != 0:
        print('benchmark2xml does not take any direct arguments')
        parser.print_help()
        sys.exit(-2)

    # Check that all options have a setting, even the
    # optional ones. Optional args should all have default values.
    for option in options.__dict__:
        if options.__dict__[option] is None:
            print('Some required arguments were not set')
            parser.print_help()
            sys.exit(-2)

    # Open the specified input file
    try:
        input_file = open(options.input_file.strip(), 'r')
    except IOError as e:
        print('Input file does not exist or cannot be opened:\n\t {}'.
              format(e))
        sys.exit(-1)

    # Load the json data from the file so we can use it.
    # If we encounter an error, then exit.
    try:
        json_data = json.load(input_file)
    except Exception as e:
        print('Failed to load JSON data from input file:\n\t {}'.format(e))
        sys.exit(-1)
    input_file.close()

    timestamp = json_data['context']['date'].replace(' ', 'T')

    # Get the base names of the test suite
    test_suites = collections.defaultdict(list)
    for suite in json_data['benchmarks']:
        name = suite['name'].split(options.separator.strip())[0]
        test_suites[name].append(suite)

    # If we are consuming the input file, delete it
    if options.consume:
        try:
            os.remove(options.input_file.strip())
        except Exception as e:
            print('Failed to remove the input file:\n\t {}'.format(e))
            sys.exit(-1)

    # Create the output file, if we encounter an error then exit
    try:
        output_file = open(options.output_file.strip(), 'w')
    except IOError as e:
        print('Output file could not be created:\n\t{}'.format(e))
        sys.exit(-1)

    # Write the XML data to the output file in the format used within CBNT.
    testcase_string = '    <testcase name="{}" time="%f" classname="{}{}"/>\n'
    output_file.write('<testsuites timestamp="{}">\n'.format(timestamp))

    metrics = options.cbnt_metrics.split(',')

    for test_suite in test_suites:
        output_file.write('  <testsuite name="{}{}">\n'.
                          format(options.suite_name, test_suite))

        for test in test_suites[test_suite]:
            base_name = options.separator.join(
                test['name'].split(options.separator.strip())[1:])
            if not base_name:
                base_name = test['name']

            for metric in metrics:
                if metric not in test:
                    print('Metric \'{}\' not found for test {}'.format(
                        metric, test))
                    sys.exit(-1)

                test_name = options.separator.join([base_name,
                                                    metric])
                # Note: The sample goes in a field called 'time' in the xml
                #  only because that is the CBNT way of naming things.
                sample = test[metric]
                output_file.write(
                    testcase_string.format(test_name, options.suite_name,
                                           test_suite) % sample)

        output_file.write('  </testsuite>\n')
    output_file.write('</testsuites>\n')

    output_file.close()


if __name__ == '__main__':
    main()
