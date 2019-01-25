#!/usr/bin/env python3

"""
This short script uses curl requests to search the last 100 builds of
a jenkins job to find recurring errors, written in Python3.
It results in printing a list of links to builds that match the search
As the requests package is not included within kv, you will need to either
download this package yourself or reference the one included inside
couchbase-cli.
This is currently limited to searching for log patterns contained within
one line of the logs, as the search checks line-by-line.
"""

import argparse
import requests
import sys
import time

# Create argparser so the user can specify which job to search
argParser = argparse.ArgumentParser()
argParser.add_argument('--job', '-j', type=str,
                       help='The cv job to query. '
                            "Common jobs are: 'kv_engine-ASan-UBSan-master', "
                            "'kv_engine-clang_analyzer-master', "
                            "'kv_engine-linux-master', "
                            "'kv_engine-threadsanitizer-master', "
                            "'kv_engine-windows-master', "
                            "'kv_engine-clang_format', "
                            "'kv-engine-cv-perf'", required=True)
argParser.add_argument('--search', '-s', type=str,
                       help='The string to search the logs for', required=True)
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
                       help='Determine the endpoint of logs to check, ' \
                            'http://<url-prefix>.jenkins.couchbase.com')

args = argParser.parse_args()
job = 'job/' + args.job + '/'

serverURL = 'http://' + str(args.url_prefix) + '.jenkins.couchbase.com/'

availableFormats = ["plain", "log-line", "jira"]
outputFormat = args.format.lower()
assert outputFormat in availableFormats, "%r format is not supported" % outputFormat

consoleText = '/consoleText/'
resultURLs = {}
failedBuildNums = []

if args.build_no == 0:
    # need to fetch the latest build number
    r = requests.get(serverURL + job + 'lastBuild/api/json')
    j = r.json()
    args.build_no = j['number']

print("Searching for:", ('"' + args.search + '"'), "in console logs of job:",
      args.job, "between build", args.build_no - (args.no_of_builds - 1),
      "and", args.build_no, file=sys.stderr)

start_time = time.time()
for i in range(0, args.no_of_builds):
    lines = []
    print('\r >>> Current progress: {}   '.format(str(i)), end='',
    flush=True, file=sys.stderr)
    r = requests.get(serverURL + job + str(args.build_no-i) + consoleText)
    if r.status_code != 200:
        failedBuildNums.append(args.build_no-i)
    for line in r.text.split('\n'):
        result = line.find(args.search)
        if result != -1:
            lines.append(line)
    if lines:
        resultURLs[serverURL + job + str(args.build_no-i) + '/console/'] = lines

print('\r Completed search in', (time.time() - start_time), 's', file=sys.stderr)
if failedBuildNums:
    print("Failed log request on build(s) no:", failedBuildNums, file=sys.stderr)

# Ensure above prints actually print before results (and not mangled inside results)
sys.stderr.flush()

if not resultURLs:
    # Empty results, did not find any matches
    print("No matches found")
elif outputFormat == 'jira':
    # Print in a JIRA format
    print("{panel:title=Search for", ('"' + args.search + '"'),
          "in console logs of job", args.job, "between build no",
          args.build_no - (args.no_of_builds - 1), "and", args.build_no, '}')
    for url in resultURLs:
        print('[', url, ']', sep="")
        print('{noformat}')
        for line in resultURLs[url]:
            print(line)
        print('{noformat}')
    print("{panel}")
elif outputFormat == "log-line":
    # Print findings with log line attached
    for url in resultURLs:
        print(url, end=" : ")
        for line in resultURLs[url]:
            print(line, end=", ")
        print('\n', end="")
else:  # outputFormat == "plain"
    # Print findings normally
    for url in resultURLs:
        print(url)
