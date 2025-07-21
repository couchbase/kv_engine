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

The script also extracts NODE_NAME from Jenkins console output to help identify
which Jenkins nodes experienced failures. If no name is found, it will be set
to 'Unknown'.

Note: This is _very_ rough-and-ready - it "works" in that it extracts useful
information from our CV jobs, but it's likely very specialised to the currently
observed test failures - i.e. the filtering in filter_failed_builds() will
likely need enhancing as and when tests change.

Usage: ./jenkins_failures.py [-u <USERNAME>] [-p <JENKINS_API_TOKEN>] [--mode=MODE]

The Jenkins API Token can be setup by logging into cv.jenkins.couchbase.com
and clicking "Add New Token" from your user page
 (e.g. http://cv.jenkins.couchbase.com/user/daverigby/configure)
"""

from urllib.parse import urlparse
import argparse
import collections
import datetime
import json
import logging
import multiprocessing
import os
import re
import sys
import requests

try:
    import jenkins
except ModuleNotFoundError as e:
    print(
        "{}.\n\nInstall using `python3 -m pip install python-jenkins` or similar.".format(e),
        file=sys.stderr)
    sys.exit(1)

logging.basicConfig(level=logging.DEBUG)
logging.getLogger('urllib3').setLevel(logging.ERROR)


class JenkinsWithTreeFilter(jenkins.Jenkins):
    """
    Injects a tree filter in the REST API requests.
    The JSON endpoint supports a tree parameter which contains an encoding of
    an object tree, and whatever matches is returned. It can significantly
    reduce the response size. The Jenkins documentation is **brief** and
    merely mentions it's existense. The Python library does not expose it, so
    we have to append it to the request URL.

    Set JenkinsWithTreeFilter.tree_filter to the desired tree parameter value.

    Useful links:
     - https://www.cloudbees.com/blog/taming-jenkins-json-api-depth-and-tree
     - https://wiki.jenkins-ci.org/display/JENKINS/Remote+access+API
    """

    def jenkins_request(
            self, req, add_crumb=True, resolve_auth=True, stream=None):

        # When we've got a JSON API request, simply append the specified
        # tree_filter to the URL.
        url = urlparse(req.url)
        if re.match(r'^/job/.*/api/json$', url.path) \
           and getattr(self, 'tree_filter', '') != '':
            req.url += '?' if url.query == '' else '&'
            req.url += 'tree=' + self.tree_filter
        return super().jenkins_request(req, add_crumb, resolve_auth, stream)

class SessionWithAuth(requests.Session):
    """
    A session that persists the authentication credentials.
    """
    def __init__(self, username, password):
        super().__init__()
        self.auth = (username, password)


def init_worker(function, url, username, password):
    """Initialise a multiprocessing worker by establishing a connection to
    the Jenkins server at url/username/password"""
    function.server = JenkinsWithTreeFilter(
        url, username=username, password=password)
    # We only ever access these fields from these code paths.
    function.server.tree_filter = 'url,result,timestamp,actions[parameters[*],foundFailureCauses[*]]'

    # Initialize for HTTP requests
    function.request_session = SessionWithAuth(username, password)

def get_node_name(server, job, number, session):
    console_url = None

    if job.startswith('kv_engine-windows-'):
        # Sample console URL: /job/kv_engine-windows-master/63025/consoleText
        console_url = f"{server.server}job/{job}/{number}/consoleText"
    else:
        # We need the branch separate from the job name to construct the console URL.
        # Sample console URL: /job/kv_engine.ASan-UBSan/job/master/43301/consoleText
        (job_name, branch) = job.split('/')
        console_url = f"{server.server}job/{job_name}/job/{branch}/{number}/consoleText"

    try:
        response = session.get(console_url, stream=True, timeout=30)
        response.raise_for_status()

        # Node name and VM name in the first ~5000 bytes, however load 10000 bytes to be safe.
        for chunk in response.iter_content(decode_unicode=True, chunk_size=10000):
            node_match = re.search(r'NODE_NAME=([^\s]+)', chunk)
            labels_match = re.search(r'NODE_LABELS=([^\r\n]+)', chunk)
            node_name = node_match.group(1) if node_match else "Unknown"
            labels = labels_match.group(1) if labels_match else None

            if node_name:
                return {'node_name': node_name, 'labels': labels}
            break

        logging.warning(f"Failed to find NODE_NAME for {job}-{number}")

    except requests.exceptions.RequestException as e:
        logging.debug(f"get_node_name: Failed to fetch from {console_url}: {e}")
        return None
    except Exception as e:
        logging.warning(f"get_node_name: Failed to fetch console output for {job}-{number}: {e}")
        return None

    return None

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

    session = get_build_info.request_session
    node_info = get_node_name(get_build_info.server, job, number, session)
    if node_info:
        info['node_name'] = node_info['node_name']
        info['labels'] = node_info['labels']
        logging.debug("Build: {}-{}: Node: {}, Labels: {}"
                      .format(job, number, node_info['node_name'], node_info['labels']))
    else:
        info['node_name'] = 'Unknown'
        info['labels'] = None

    key = job + "-" + str(number)
    return (key, info)


def fetch_failed_builds(server_url, username, password, build_limit, branch):
    """For the given Jenkins server URL & credentials, fetch details of all
    failed builds.
    Returns a dictionary of job+build_number to build details dictionary."""
    server = JenkinsWithTreeFilter(
        server_url, username=username, password=password)
    user = server.get_whoami()
    version = server.get_version()
    logging.info('Hello {} from Jenkins {}'.format(user['fullName'], version))

    details = dict()

    jobs = (
        'kv_engine-windows-',
        'kv_engine.aarch64-linux/',
        'kv_engine.ASan-UBSan/',
        'kv_engine.linux-CE/',
        'kv_engine.linux-debug/',
        'kv_engine.linux/',
        'kv_engine.macos/',
        'kv_engine.threadsanitizer/',
    )

    with multiprocessing.Pool(os.cpu_count() * 4,
                              initializer=init_worker,
                              initargs=(get_build_info,
                                        server_url,
                                        username,
                                        password)) as pool:
        for job in jobs:
            # We only want the list of builds. The Python API requires the
            # firstBuild and fullName to be able to fetch all builds, and
            # underneath is does some pagination on the results.
            server.tree_filter = 'firstBuild[number],fullName,builds[number,result]'
            builds = server.get_job_info(
                job + branch, depth=1, fetch_all_builds=True)['builds']
            for b in builds:
                b['job'] = job + branch
            logging.debug(
                "get_job_info({}) returned {} builds".format(
                    job + branch, len(builds)))

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
                    assert (gerrit_patch)
                    failures[description].append({'description': description,
                                                  'gerrit_patch': gerrit_patch,
                                                  'timestamp': timestamp,
                                                  'url': info['url'],
                                                  'node_name': info['node_name'],
                                                  'labels': info['labels']})
        if not description:
            logging.warning(
                "extract_failed_builds: Did not find failure cause for " +
                info['url'] + " Result:" + info['result'])
        if not gerrit_patch:
            logging.warning(
                "extract_failed_builds: Did not find Gerrit patch for " +
                info['url'] + " Result:" + info['result'])
    return failures


class Template:
    def __init__(self, template, variables):
        self.template = template
        self.variables = variables


def make_template(strings):
    """
    Create a template from the provided strings by replacing non-matching
    spans with variables.
    """

    variables = collections.defaultdict(list)

    strings_to_matches = [list(re.finditer(r'\d+', s)) for s in strings]
    # All strings should have the same number of matches
    assert (len(set(len(matches) for matches in strings_to_matches)) == 1)
    matches_per_string = len(strings_to_matches[0])

    # Match index to matches in the strings
    all_matches = list(zip(*strings_to_matches))

    variable_matches = collections.defaultdict(list)

    for cur_match_in_strings in all_matches:
        cur_group_in_strings = (match.group()
                                for match in cur_match_in_strings)

        if len(set(cur_group_in_strings)) == 1:
            # The i-th number is the same in all strings; we don't need to
            # turn it into a variable
            continue
        else:
            variable_name = f'{{Var{len(variables) + 1}}}'
            for s_idx, match in enumerate(cur_match_in_strings):
                # Replace that match with a variable name
                variables[variable_name].append(match.group())
                variable_matches[s_idx].append((match, variable_name))

    # Replace all matched spans in all strings with the assigned variables
    for s_idx, s in enumerate(strings):
        new_s = ''
        last_idx = 0
        for match, variable_name in variable_matches[s_idx]:
            begin, end = match.span()
            new_s += s[last_idx:begin]
            new_s += '{' + variable_name + '}'
            last_idx = end
        new_s += s[last_idx:]
        strings[s_idx] = new_s

    # Make sure we've done the job of turning all strings into a template
    # without losing any info
    assert (len(set(strings)) == 1)
    return Template(strings[0], variables)


def erase_possible_variables(s):
    """
    Erases all spans which would be considered for replacement with a variable.
    This allows us to identify groups of strings which can be turned into a
    template.
    """
    return re.sub('\\d+', '\0', s)


def preprocess_output(key):
    # Replace all hex addresses with a single sanitized value.
    def mask(matchobj):
        return '0x' + ('X' * len(matchobj.group(1)))

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
    # Normalise file paths across Windows & *ix.
    key = key.replace('\\', '/')
    # Normalise GTest failure messages across Windows & *ix
    # (don't know why GTest TestPartResultTypeToString() prints differently:
    """
        case TestPartResult::kNonFatalFailure:
        case TestPartResult::kFatalFailure:
    #ifdef _MSC_VER
                return "error: ";
    #else
                return "Failure\n";
    #endif
    """
    key = re.sub(r"\berror: ", r"Failure\n", key)
    # Normalise source file/line number printing across Windows & *ix.
    key = re.sub(r'(\w+\.cc)\((\d+)\)', r'\1:\2', key)
    # Merge value-only and full-eviction failures
    key = key.replace('value_only', '<EVICTION_MODE>')
    key = key.replace('full_eviction', '<EVICTION_MODE>')
    # Merge passive and active compression modes.
    key = key.replace('comp_active', '<COMPRESSION_MODE>')
    key = key.replace('comp_passive', '<COMPRESSION_MODE>')
    # Merge couchstore filenames
    key = re.sub(r'\d+\.couch\.\d+', 'VBID.couch.REV', key)
    # Filter local ephemeral port numbers
    key = re.sub(r'127\.0\.0\.1(.)\d{5}', r'127.0.0.1\1<PORT>', key)
    key = re.sub(r'localhost(.)\d{5}', r'localhost\1<PORT>', key)

    return key


def filter_failed_builds(details):
    """Discard any failures which are not of interest, and collapse effectively
    identical issues:
    - compile errors which are not transient and hence are not of interest when
      looking for intermittent issues.
    - Errors which mention an address which differs but is otherwise identical :)
    """

    def include(elem):
        for ignored in (
            r'Unexpected stat:',
            r'Compile error at',
            r'^One of more core files found at the end of the build',
            r'^Test exceeded the timeout:',
                r'^The warnings threshold for this job was exceeded'):
            if re.search(ignored, elem[0]):
                return False
        return True

    included = filter(include, details.items())

    filtered = [
        (preprocess_output(key), value) for key, value in included
    ]

    # Groups of outputs which can be turned into templates. Items are indexes
    # into the filtered list.
    generic_strings = [
        erase_possible_variables(s) for s in [
            s for s, _ in filtered]]
    groups = collections.defaultdict(list)
    for i, s in enumerate(generic_strings):
        groups[s].append(i)

    merged = {}
    # Each group becomes one template
    for group in groups.values():
        template = make_template([filtered[i][0] for i in group])
        details = [
            dict(
                filtered[i][1][0],
                variables=template.variables) for i in group]
        merged[template.template] = details

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
    parser.add_argument(
        '-u', '--username', help='Username for Jenkins REST API',
        required=False)
    parser.add_argument('-p', '--password',
                        help='Password/token for Jenkins REST API',
                        required=False)
    parser.add_argument(
        '--mode',
        choices=[
            'download',
            'parse',
            'download_and_parse'],
        default='download_and_parse',
        help=(
            'Analysis mode.\n'
            'download: Download build details, writing to stdout as JSON.\n'
            'parse: Parse previously-downloaded build details read from stdin.\n'
            'download_and_parse: Download and parse build details.\n'))
    parser.add_argument('--branch', type=str, default='master',
                        help='Branch to scan for')
    parser.add_argument('--build-limit',
                        type=int,
                        default=200,
                        help=('Maximum number of builds to download & scan '
                              'for each job'))
    args = parser.parse_args()

    if args.mode != 'parse':
        if args.username is None or args.password is None:
            print('Mode requires username and password', file=sys.stderr)
            parser.print_usage()
            sys.exit(1)
        raw_builds = fetch_failed_builds(
            'http://cv.jenkins.couchbase.com',
            args.username,
            args.password,
            args.build_limit,
            args.branch)
    if args.mode == 'download':
        print(json.dumps(raw_builds))
        sys.exit(0)
    if args.mode == 'parse':
        raw_builds = json.load(sys.stdin)
    logging.info("Number of builds downloaded: {}.".format(len(raw_builds)))
    failures = extract_failed_builds(raw_builds)
    logging.info(
        "Number of builds with at least one failure: {}.".format(
            len(failures)))
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
        for d_idx, d in enumerate(details[:100]):
            human_time = d['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
            node_name = d['node_name']
            labels = d['labels']
            print("* Time: {}, Jenkins job: {}, patch: {}, node: {}, labels: [{}]".format(human_time,
                  d['url'], d['gerrit_patch'], node_name, labels))
            if len(d['variables']) > 0:
                print(' `- where ', end='')
                for name, value in d['variables'].items():
                    print(f'{name} = `{value[d_idx]}`;', end='')
                print()
        if len(details) > 100:
            print(
                "<cut> - only showing details of first 100/{} instances.".format(
                    len(details)))
        print('\n===========\n')
