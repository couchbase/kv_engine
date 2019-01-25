#!/usr/bin/env python3

"""
This short script uses curl requests to search the last 100 builds of
a jenkins job to find recurring errors, written in Python3.
It results in printing a list of links to builds that match the search
As the requests package is not included within kv, you will need to either
download this package yourself or reference the one included inside
couchbase-cli.
"""

import argparse
import json
import requests
import sys
import time

serverURL = 'http://cv.jenkins.couchbase.com/'

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
                       help='The build number of cv job to check backwards '
                            'from. 0 (default) fetches latest build number',
                       default=0)
argParser.add_argument('--no-of-builds', '-n', type=int,
                       help='The number of builds to check back', default=100)

args = argParser.parse_args()
job = 'job/' + args.job + '/'

consoleText = '/consoleText/'
resultURLs = []

if args.build_no == 0:
    # need to fetch the latest build number
    r = requests.get(serverURL + job + 'lastBuild/api/json')
    j = r.json()
    args.build_no = j['number']

print("Searching for:", ('"' + args.search + '"'), "in console logs of job:",
      args.job, "between build", args.build_no - (args.no_of_builds - 1), "and",
      args.build_no, file=sys.stderr)

start_time = time.time()
for i in range(0, args.no_of_builds):
    print('\r >>> Current progress: {}   '.format(str(i)), end='',
    flush=True, file=sys.stderr)
    r = requests.get(serverURL + job + str(args.build_no-i) + consoleText)
    result = r.text.find(args.search)
    if result != -1:
        resultURLs.append(serverURL + job + str(args.build_no-i) + '/console/')

print('\r Completed search in', (time.time() - start_time), 's',
      file=sys.stderr)

for url in resultURLs:
    print(url)
