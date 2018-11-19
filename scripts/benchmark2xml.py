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


def convert_time(input_time, input_format, desired_format):
    units = {"s": 1, "ms": 1000, "us": 1000000, "ns": 1000000000}
    if input_format == desired_format:
        return float(input_time)
    if units[input_format] > units[desired_format]:
        factor = float(units[desired_format]) / float(units[input_format])
        return float(input_time) * float(factor)
    else:
        factor = float(units[input_format]) / float(units[desired_format])
        return float(input_time) / float(factor)


def main():
    parser = optparse.OptionParser()
    required_args = optparse.OptionGroup(parser, 'Required Arguments')
    required_args.add_option('-b', '--benchmark_file', action='store',
                             type='string', dest='input_file',
                             help='The input file from Google Benchmark')
    required_args.add_option('-o', '--output_file', action='store',
                             type='string', dest='output_file',
                             help='The file to output the generated XML to')
    required_args.add_option('-t', '--time_format', action='store',
                             dest='time_format', type='choice',
                             choices=('s', 'ms', 'us', 'ns'),
                             help='Coverts all time based results into this '
                                  'format. [s, ms, us, ns]')
    parser.add_option_group(required_args)

    optional_args = optparse.OptionGroup(parser, 'Optional Arguments')
    optional_args.add_option('-c', '--cpu_time', action='store_true',
                             dest='cpu_time', default=False,
                             help='Create test results for CPU time stats')
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
    optional_args.add_option('--cbnt_metric', action='store',
                             dest='cbnt_metric', type='string', default='',
                             help='An optional string marking which named '
                                  'value should be taken as the result. '
                                  'If specified, this will override the time '
                                  'results, however if it is specified and it '
                                  'does not exist in the test results, '
                                  'then the \'real_time\' value will be used.')
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

    test_suites = collections.defaultdict(list)

    # Dictionary containing the names of the stats we want to include in the
    # final result. The dictionary is in the form { Name->string : Flag->bool }
    # The flag represents whether we need to append the stat name to the result
    # in order to differentiate them.
    test_cases = {'real_time': False}

    if options.cpu_time:
        test_cases['cpu_time'] = True

    # Get the base names of the test suite
    for test in json_data['benchmarks']:
        name = test['name'].split(options.separator.strip())[0]
        test_suites[name].append(test)

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
    for test_suite in test_suites:
        output_file.write('  <testsuite name="{}{}">\n'.
                          format(options.suite_name, test_suite))
        for test in test_suites[test_suite]:
            name = options.separator.join(
                test['name'].split(options.separator.strip())[1:])
            if not name:
                name = test['name']
            if options.cbnt_metric:
                if options.cbnt_metric in test:
                    # Use cbnt_metric result
                    time = test[options.cbnt_metric]
                else:
                    # Use real time by default
                    time = float(convert_time(test['real_time'],
                                              test['time_unit'],
                                              options.time_format.strip()))
                output_file.write(
                    testcase_string.format(name, options.suite_name,
                                           test_suite) % time)
            else:
                for stat in test_cases:
                    if stat not in test:
                        continue
                    if test_cases[stat]:
                        name = options.separator.join([name, stat])
                    time = float(convert_time(test[stat], test['time_unit'],
                                              options.time_format.strip()))
                    output_file.write(
                        testcase_string.format(name, options.suite_name,
                                               test_suite) % time)
        output_file.write('  </testsuite>\n')
    output_file.write('</testsuites>\n')

    output_file.close()


if __name__ == '__main__':
    main()
