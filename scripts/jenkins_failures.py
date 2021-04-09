#!/usr/bin/env python3

"""
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
"""

"""Script to summarise the details of intermittent CV test failures from
Jenkins.

It attempts to group logically identical failure reasons together, and then
outputs a list of observed failure reasons, ordered by frequency.

Note: This is _very_ rough-and-ready - it "works" in that it extracts useful
information from our CV jobs, but it's likely very specialised to the currently
observed test failures - i.e. the filtering in filter_failed_builds() will
likely need enhancing as and when tests change.

Usage: ./jenkins_failures.py <USERNAME> <JENKINS_API_TOKEN> [--mode=MODE]

The Jenkins API Token can be setup by logging into cv.jenkins.couchbase.com
and clicking "Add New Token" from your user page
 (e.g. http://cv.jenkins.couchbase.com/user/daverigby/configure)
"""

import argparse
import collections
import datetime
import json
import logging
import multiprocessing
import os
import re
import sys

try:
    import jenkins
except ModuleNotFoundError as e:
    print(
        "{}.\n\nInstall using `python3 -m pip install python-jenkins` or similar.".format(
            e), file=sys.stderr);
    sys.exit(1)

logging.basicConfig(level=logging.DEBUG)
logging.getLogger('urllib3').setLevel(logging.ERROR)


def init_worker(function, url, username, password):
    """Initialise a multiprocessing worker by establishing a connection to
    the Jenkins server at url/username/password"""
    function.server = jenkins.Jenkins(url, username=username, password=password)


def get_build_info(build):
    """For the given build job and number, download the information for
    that job from Jenkins.
    """
    job = build['job']
    number = build['number']
    depth = 5  # Minimum depth needed to fetch all the details we need.
    info = get_build_info.server.get_build_info(job, number, depth)
    result = info['result']
    logging.debug("Build: {}-{}: {}".format(job, number, result))
    if not result:
        # Job still running - skip
        return
    if result in ('SUCCESS', 'ABORTED'):
        # Job succeeded or explicitly aborted - skip
        return
    key = job + "-" + str(number)
    return (key, info)


def fetch_failed_builds(server_url, username, password, build_limit):
    """For the given Jenkins server URL & credentials, fetch details of all
    failed builds.
    Returns a dictionary of job+build_number to build details dictionary."""
    server = jenkins.Jenkins(server_url, username=username, password=password)
    user = server.get_whoami()
    version = server.get_version()
    logging.info('Hello {} from Jenkins {}'.format(user['fullName'], version))

    details = dict()

    jobs = ('kv_engine.linux/master', 'kv_engine.linux-CE/master',
            'kv_engine.macos/master', 'kv_engine.ASan-UBSan/master',
            'kv_engine.threadsanitizer/master', 'kv_engine-windows-master')

    with multiprocessing.Pool(os.cpu_count() * 4,
                              initializer=init_worker,
                              initargs=(get_build_info,
                                        server_url,
                                        username,
                                        password)) as pool:
        for job in jobs:
            builds = server.get_job_info(job, depth=1, fetch_all_builds=True)[
                'builds']
            for b in builds:
                b['job'] = job
            logging.debug(
                "get_job_info({}) returned {} builds".format(job, len(builds)))

            # Constrain to last N builds of each job.
            builds = builds[:build_limit]
            results = pool.map(get_build_info, builds)
            for r in results:
                if r:
                    details[r[0]] = r[1]
    return details


def get_gerrit_patch_from_parameters_action(action):
    """For the given action dictionary, return the Gerrit patch URL if present,
    else return None."""
    for param in action['parameters']:
        if param['name'] == 'GERRIT_CHANGE_URL':
            return param['value']
    return None


def extract_failed_builds(details):
    failures = dict()
    for number, info in details.items():
        actions = info['actions']
        description = None
        gerrit_patch = None
        for a in actions:
            if '_class' in a and a['_class'] == 'hudson.model.ParametersAction':
                gerrit_patch = get_gerrit_patch_from_parameters_action(a)
            if '_class' in a and a['_class'] == 'com.sonyericsson.jenkins.plugins.bfa.model.FailureCauseBuildAction':
                for cause in a['foundFailureCauses']:
                    description = cause['description'].strip()
                    timestamp = datetime.datetime.utcfromtimestamp(
                        info['timestamp'] / 1000.0)
                    if description not in failures:
                        failures[description] = list()
                    assert(gerrit_patch)
                    failures[description].append({'description': description,
                                                  'gerrit_patch': gerrit_patch,
                                                  'timestamp': timestamp,
                                                  'url': info['url']})
        if not description:
            logging.warning(
                "extract_failed_builds: Did not find failure cause for " +
                info['url'] + " Result:" + info['result'])
        if not gerrit_patch:
            logging.warning(
                "extract_failed_builds: Did not find Gerrit patch for " +
                info['url'] + " Result:" + info['result'])
    return failures


def filter_failed_builds(details):
    """Discard any failures which are not of interest, and collapse effectively
    identical issues:
    - compile errors which are not transient and hence are not of interest when
      looking for intermittent issues.
    - Errors which mention an address which differs but is otherwise identical :)
    """

    def include(elem):
        for ignored in (r'Unexpected stat:', r'Compile error at',
                        r'^The warnings threshold for this job was exceeded'):
            if re.search(ignored, elem[0]):
                return False
        return True

    filtered = dict(filter(include, details.items()))

    # Replace all hex addresses with a single sanitized value.
    def mask(matchobj):
        return '0x' + ('X' * len(matchobj.group(1)))

    merged = collections.defaultdict(list)
    for key, value in filtered.items():
        key = re.sub(r'0x([0-9a-fA-F]+)', mask, key)
        # Strip out pipeline build  timestamps of the form
        # '[2020-11-26T15:30:05.571Z] ' - non-pipeline builds don't include
        # them.
        key = re.sub(r'\[\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{3}Z\] ', '',
                     key)
        # Filter timings of the form '2 ms'
        key = re.sub(r'\d+ ms', 'X ms', key)
        # Filter exact test order / test number - e.g.
        #    13/398 Test #181: ep-engine_ep_unit_tests.
        key = re.sub(r'\s+\d+/\d+ Test\s+#\d+:', ' N/M Test X:', key)
        # Filter engine_testapp exact order / test number - e.g.
        #     Running [0007/0047]: multi se
        key = re.sub(r'Running \[\d+/\d+]:', 'Running [X/Y]:', key)
        # Replace long '....' strings used for padding / alignment
        key = re.sub(r'\.\.\.+', '...', key)
        # Filter expected / actual values
        key = re.sub(r'last value:\d+', 'last value:XXX', key)
        key = re.sub(r"Expected `(\d+)', got `\d+'",
                     r"Expected `\1', got `YYY'", key)
        key = re.sub(r"Expected `\d+' to be greater than or equal to `(\d+)'",
                     r"Expected `XXX' to be greater than or equal to `\1'", key)
        # Normalise file paths across Windows & *ix.
        key = key.replace('\\', '/')
        # Merge value-only and full-eviction failures
        key = key.replace('value_only', '<EVICTION_MODE>')
        key = key.replace('full_eviction', '<EVICTION_MODE>')
        # Merge couchstore filenames
        key = re.sub(r'\d+\.couch\.\d+', 'VBID.couch.REV', key)
        # Filter local ephemeral port numbers
        key = re.sub(r'127\.0\.0\.1(.)\d{5}', r'127.0.0.1\1<PORT>', key)
        key = re.sub(r'localhost(.)\d{5}', r'localhost\1<PORT>', key)

        merged[key] += value

    # For each failure, determine the time range it has occurred over, and
    # for which patches.
    # If it only occurred:
    #   a) Over a small time (6 hours), or
    #   b) For a single patch
    # then that implies it's not a repeated, intermittent issue - e.g. just an
    # issue with a single patch / a transient issue on some machine - hence
    # discard it.
    merged2 = dict()
    for key, details in merged.items():
        timestamps = list()
        patches = dict()
        for d in details:
            timestamps.append(d['timestamp'])
            patches[d['gerrit_patch']] = True
        range = max(timestamps) - min(timestamps)

        if range > datetime.timedelta(hours=6) and len(patches) > 1:
            merged2[key] = details
    return merged2


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('username', help='Username for Jenkins REST API')
    parser.add_argument('password', help='Password/token for Jenkins REST API')
    parser.add_argument('--mode',
                        choices=['download', 'parse', 'download_and_parse'],
                        default='download_and_parse',
                        help=('Analysis mode.\n'
                              'download: Download build details, writing to stdout as JSON.\n'
                              'parse: Parse previously-downloaded build details read from stdin.\n'
                              'download_and_parse: Download and parse build details.\n'))
    parser.add_argument('--build-limit',
                        type=int,
                        default=200,
                        help=('Maximum number of builds to download & scan '
                              'for each job'))
    args = parser.parse_args()

    if args.mode != 'parse':
        raw_builds = fetch_failed_builds('http://cv.jenkins.couchbase.com',
                                         args.username, args.password, args.build_limit)
    if args.mode == 'download':
        print(json.dumps(raw_builds))
        sys.exit(0)
    if args.mode =='parse':
        raw_builds = json.load(sys.stdin)
    logging.info("Number of builds downloaded: {}.".format(len(raw_builds)))
    failures = extract_failed_builds(raw_builds)
    logging.info("Number of builds with at least one failure: {}.".format(len(failures)))
    failures = filter_failed_builds(failures)
    logging.info("Number of unique failures: {}".format(len(failures)))

    sorted_failures = sorted(failures.items(), key=lambda i: len(i[1]),
                             reverse=True)

    # Count total number of failures
    total_failures = 0
    for (_, details) in sorted_failures:
        total_failures += len(details)

    for (summary, details) in sorted_failures:
        num_failures = len(details)
        print('+Error signature+')
        print('{code}')
        print(summary)
        print('{code}\n')
        print('+Details+')
        print(
            '{} instances of this failure ({:.1f}% of sampled failures):'.format(
                num_failures,
                (num_failures * 100.0) / total_failures))
        for d in details[:10]:
            human_time = d['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
            print("* Time: {}, Jenkins job: {}, patch: {}".format(human_time,
                                                                  d['url'],
                                                                  d[
                                                                      'gerrit_patch']))
        if len(details) > 10:
            print(
                "<cut> - only showing details of first 10/{} instances.".format(
                    len(details)))
        print('\n===========\n')
