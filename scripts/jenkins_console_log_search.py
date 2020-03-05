#!/usr/bin/env python3

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

This short script uses curl requests to search the last 100 builds of
a jenkins job to find recurring errors, written in Python3.
It results in printing a list of links to builds that match the search
As the requests package is not included within kv, you will need to either
download this package yourself or reference the one included inside
couchbase-cli.
This is currently limited to searching for log patterns contained within
one line of the logs, as the search checks line-by-line.

Usage: python jenkins_console_log_search.py -j <job-name> -s <RegEx Search term>
"""

import argparse
import re
import requests
import sys
import time

class ASCIIFormat:
    BOLD = '\033[1m'
    END = '\033[0m'


# Search for searchParameter in logText, handling either a string or a RegEx inside
# searchPattern depending on whether the regex flag is True, and assuming that logText
# is line separated by \n's
def search(logText, searchPattern, isRegex):
    output = []

    if isRegex:
        # Check regex against whole text
        for find in re.finditer(pattern, logText):
            group_list = []
            if find.groups():
                group_list.extend(find.groups())
            else:
                group_list.append(find.group(0))
            for term in group_list:
                output.append(term)

    else:  # Not a RegEx
        lines = []
        for line in logText.split('\n'):
            result = line.find(searchPattern)
            if result != -1:
                # Wrap the search term in ASCII formatting to make it bold
                lines.append(line.replace(searchPattern, ASCIIFormat.BOLD
                                          + searchPattern + ASCIIFormat.END))
        output.extend(lines)

    return output


# --- Start Main Script ---
# Create argparser so the user can specify which job to search
argParser = argparse.ArgumentParser()
argParser.add_argument('--job', '-j', type=str,
                       help='The cv job to query. '
                            "Example jobs are: 'kv_engine.ASan-UBSan/job/master', "
                            "'kv_engine-clang_analyzer-master', "
                            "'kv_engine.linux/job/master', "
                            "'kv_engine.linux/job/mad-hatter', "
                            "'kv_engine.threadsanitizer/job/master', "
                            "'kv_engine-windows-master', "
                            "'kv_engine-clang_format', "
                            "'kv-engine-cv-perf'", required=True)
argParser.add_argument('--search', '-s', type=str, required=True,
                       help='The string to search the logs for in a RegEx format')
argParser.add_argument('--build-no', '-b', type=int,
                       help='The build number of cv job to check backwards from. '
                            '0 (default) fetches latest build number', default=0)
argParser.add_argument('--no-of-builds', '-n', type=int,
                       help='The number of builds to check back', default=100)
argParser.add_argument('--format', '-f', default="plain", type=str,
                       help="Select the format to print results. "
                            "Available formats are: "
                            "plain (default), log-line, jira")
argParser.add_argument('--url-prefix', '-u', type=str, default='cv',
                       help='Determine the endpoint of logs to check, '
                            'http://<url-prefix>.jenkins.couchbase.com')

args = argParser.parse_args()
job = 'job/' + args.job + '/'

serverURL = 'http://' + str(args.url_prefix) + '.jenkins.couchbase.com/'

# Control the eventual output format of the findings
availableFormats = ["plain", "log-line", "jira"]
outputFormat = args.format.lower()
assert outputFormat in availableFormats, "%r format is not supported" % outputFormat

consoleText = '/consoleText/'
resultURLs = {}
failedBuildNums = []

if args.build_no == 0:
    # Need to fetch the latest build number
    r = requests.get(serverURL + job + 'lastBuild/api/json')
    j = r.json()
    args.build_no = j['number']

# Determine whether the inputted search parameter is a regex
isRegex = True
try:
    pattern = re.compile(args.search)
    searchingFor = 'RegEx "' + args.search + '"'
except re.error:
    isRegex = False
    pattern = args.search
    searchingFor = '"' + args.search + '"'

print("Searching for", searchingFor, "in console logs of job:",
      args.job, "between build", args.build_no - (args.no_of_builds - 1),
      "and", args.build_no, file=sys.stderr)

# Trigger timing check start
start_time = time.time()

for i in range(0, args.no_of_builds):
    print('\r >>> Current progress: {}   '.format(str(i)), end='',
    flush=True, file=sys.stderr)

    # Get the console log text from the jenkins job
    r = requests.get(serverURL + job + str(args.build_no-i) + consoleText)
    if r.status_code != 200:
        failedBuildNums.append(args.build_no-i)

    # Perform Search
    output = []
    output.extend(search(r.text, pattern, isRegex))

    if output:
        resultURLs[serverURL + job + str(args.build_no-i) + '/console/'] = output

# Finish timing
print('\r Completed search in', (time.time() - start_time), 's', file=sys.stderr)
if failedBuildNums:
    print("Failed log request on build(s) no:", failedBuildNums, file=sys.stderr)

# Ensure above prints actually print before results (and not mangled inside results)
sys.stderr.flush()

# Result output
if not resultURLs:
    # Empty results, did not find any matches
    print("No matches found")
elif outputFormat == 'jira':
    # Print in a JIRA format
    print("{panel:title=Search for", searchingFor,
          "in console logs of job", args.job, "between build no",
          args.build_no - (args.no_of_builds - 1), "and", args.build_no, '}')
    for url in resultURLs:
        print('[', url, ']', sep="")
        print('{noformat}')
        for line in resultURLs[url]:
            print(line.replace(ASCIIFormat.BOLD, '').replace(ASCIIFormat.END, ''))
        print('{noformat}')
    print("{panel}")
elif outputFormat == "log-line":
    # Print findings with log line attached
    for url in resultURLs:
        print(url, ':')
        for line in resultURLs[url]:
            print('\t', line)
else:  # outputFormat == "plain"
    # Print findings normally
    for url in resultURLs:
        print(url)
