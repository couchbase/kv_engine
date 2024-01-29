#!/usr/bin/env python3

"""
Copyright 2024-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
"""

"""
Generates a Dockerfile with matching OS and C/C++ runtime packages as on
the host from which the couchbase.log was taken.

Also uses a matching CPU architecture. Non-matching containers can be
run via emulation, such that x86 environments can be reproduced on M1
for example.

The container will have GDB installed, which should mean that one can
just run this script, build and run a container and attach GDB with a
core dump from the host.

The Dockerfile will fetch and install CB Server packages from
latestbuilds.service.couchbase.com, although it could probably use the
release mirror for GA builds, as it is faster.
"""


import argparse
import itertools
import re
import urllib.request
import sys
parser = argparse.ArgumentParser(
    prog='docker_repro_env.py',
    description='Builds a Docker image with GDB matching the environment described in a couchbase.log.')
parser.add_argument('logfile', nargs='?',
                    type=argparse.FileType('r'), default=sys.stdin)
parser.parse_args()

SEPARATOR = '=' * 78 + '\n'
RELEASES_INDEX = 'https://releases.service.couchbase.com/builds/releases/'


def dbg(*args, **kwargs):
    """
    Prints to stderr.
    """
    print('\033[93m', file=sys.stderr, end='', flush=False)
    print(*args, **kwargs, file=sys.stderr, flush=False)
    print('\033[0m', file=sys.stderr, end='', flush=True)


def iterate_blocks(lines):
    """
    Iterator over blocks of output from couchbase.log.
    Each returned value is a list of the lines in the blocks.
    """
    body = []
    for line in lines:
        if line == SEPARATOR:
            yield body
            body = []
        else:
            body.append(line)


def pairwise(iterable):
    "s -> (s0, s1), (s1, s2), (s2, s3), ..."
    a, b = itertools.tee(iterable)
    next(b, None)
    return zip(a, b)


def match_outputs(outputs, keys_and_substrings):
    """
    Given a list of outputs [(header, body), and a mapping {key -> substring},
    returns {key, body} for the outputs where header contains the given substring.

    Example:
    >>> outputs = [('cat /etc/os-release', 'OS_VERSION=Ubuntu...')]
    >>> keys_and_substrings = {'os': 'cat /etc/os-release'}
    >>> match_outputs(outputs, keys_and_substrings)
    Returns:
    >>> [('os', 'OS_VERSION=Ubuntu...')]
    """
    for header, body in outputs:
        for key, substring in keys_and_substrings.items():
            if any(substring in l for l in header[:3]):
                yield key, body


def detect_package_manager(cblog_data):
    """
    Determines which package manager is in use, based on scaped the cblog_data.
    Expects: cblog_data contains keys 'dpkg', 'rpm' with the relevant output
    (error or package info).
    """
    interesting_keys = dict((k, cblog_data[k]) for k in ('dpkg', 'rpm'))
    # Longer output wins!
    return max(interesting_keys.items(), key=lambda item: len(item[1][0]))[0]


def select_docker_image(os_info):
    """
    Selects the appropriate Docker image to use for supported distros based on
    the contents of /etc/os-release.

    Returns: (image, tag)
    """
    def get_version_id(os_info):
        try:
            return re.search(r'VERSION_ID="([\d\.]+)"', os_info).groups()[0]
        except:
            raise ValueError(
                f'Expected to find VERSION_ID in the OS version info:\n{os_info}')

    if 'Amazon Linux' in os_info:
        version_id = get_version_id(os_info)
        return 'amazonlinux', version_id
    if 'Debian' in os_info:
        version_id = get_version_id(os_info)
        return 'debian', version_id
    if 'Ubuntu' in os_info:
        version_id = get_version_id(os_info)
        return 'ubuntu', version_id

    dbg(f'Unknown OS\n++++{os_info}++++')
    dbg('Docker image to use [e.g. ubuntu]: ', end='')
    image = input().strip()
    dbg(f'Tag of {image} to use [e.g. 20.04]: ', end='')
    version = input().strip()
    return image, version


def detect_platform(uname):
    """
    Detects the CPU architecture (platform in Docker parlance) from the output
    of uname -a.

    Returns: 'amd64' or 'arm64'
    """
    if 'amd64' in uname or 'x86_64' in uname:
        return 'amd64'
    elif 'arm64' in uname or 'aarch64' in uname:
        return 'arm64'

    raise ValueError(f'Unknown platform! uname -a\n{uname}')


def get_platform_alternative_names(platform):
    """
    Returns the list of alternative names for a given platform.
    """
    return {
        'amd64': ('amd64', 'x86_64'),
        'arm64': ('arm64', 'aarch64'),
    }[platform]


def get_pkg_manager_ext(package_manager):
    """
    Returns the file extension used for packages.
    """
    return {
        'dpkg': '.deb',
        'rpm': '.rpm',
    }[package_manager]


def iterate_dpkg(output):
    """
    Iterates over the output of dpkg -l.
    Returns: (package, version)
    """
    line_skip = 0
    for line in output:
        line_skip += 1
        if '+++-====' in line:
            break

    for line in output[line_skip:]:
        pkg, version = re.search(
            r'[a-z]{2}\s+([\w\-\.:\+]+)\s+([\w\-\.:~\+]+)', line).groups()
        yield pkg, version


def iterate_rpm(output):
    """
    Iterates over the output of rpm -qa.
    Returns: (package, version)
    """
    for line in output:
        try:
            noarch = line.rsplit('.', 1)[0]  # Strip arch
            nobuild, build_number = noarch.rsplit('-', 1)
            pkg, version = nobuild.rsplit('-', 1)
            yield pkg, f'{version}-{build_number}'
        except Exception:
            dbg(f'Skipping unexpected rpm output: {line}')


def preserve_packages(preserve, packages):
    """
    Given a list of package names to preserve, and a dictionary of
    {package -> version}, retains the packages listed in preserve.
    Returns: (package, version)
    """
    preserve = set([f'{pkg}{suffix}' for suffix in (
        '', ':amd64', ':arm64') for pkg in preserve])
    for pkg, version in packages.items():
        if pkg in preserve:
            yield pkg, version


def fetch(url):
    """
    Fetches the given URL and decodes the output as a str.
    Returns: Body of the response as str.
    """
    try:
        with urllib.request.urlopen(url, timeout=10) as req:
            return req.read().decode("utf8")
    except Exception as e:
        dbg(f'Fetching {url}')
        raise


def fetch_releases_index():
    """
    Fetches the list of released version of Couchbase Server.
    """
    html = fetch(RELEASES_INDEX)
    matches = re.findall(r'<a href="(\d.\d.\d(?:-\w+)?)/', html)
    return matches


def fetch_package_lists(release_version):
    """
    Fetches the list of packages for a released version of Couchbase Server.
    """
    index_url = f'{RELEASES_INDEX}{release_version}/'
    index_html = fetch(index_url)
    try:
        manifest_name = re.findall(
            r'"(couchbase-server-[\w\.\-]+-manifest\.xml)"', index_html)[0]
    except Exception as e:
        raise ValueError(f'Failed to find manifest.xml!\n{e}')

    packages = re.findall(
        r'("couchbase-server-[\w\.\-_]*\.(?:deb|rpm)?")', index_html)
    manifest = fetch(f'{index_url}{manifest_name}')
    try:
        build_number = re.findall(
            r'<annotation name="BLD_NUM" value="(\d+)" />', manifest)[0]
    except Exception as e:
        raise ValueError(f'Failed to find build number in manifest.xml!\n{e}')

    return set(pkg[1:-1] for pkg in packages), build_number


def main(logfile):
    # Search the log file for matching blocks.
    interesting_blocks = {
        'os_info': 'cat /etc/os-release',
        'uname': 'uname -a',
        'dpkg': 'dpkg -l',
        'rpm': 'rpm -qa',
        'manifest': 'Manifest file',
    }

    dbg(f'Searching through {logfile}')
    cblog_data = {}
    for key, lines in match_outputs(
            pairwise(iterate_blocks(logfile)), interesting_blocks):
        cblog_data[key] = lines

    logfile.close()

    # Extract Couchbase Server version and build
    manifest = ''.join(cblog_data['manifest'])
    cb_version = re.findall(
        r'<annotation name="VERSION" value="([\d\.]+)" />', manifest)[0]
    cb_build_number = re.findall(
        r'<annotation name="BLD_NUM" value="(\d+)" />', manifest)[0]

    dbg(f'Detected Couchbase Server: {cb_version}-{cb_build_number}')

    # We need the releases index to find the URL for downloading the packages
    dbg(f'Fetching releases index from {RELEASES_INDEX}')
    releases_index = fetch_releases_index()

    # Choose the docker image to match the OS
    image, tag = select_docker_image(''.join(cblog_data['os_info']))
    dbg(f'Using Docker image {image}:{tag}')

    # Choose the same CPU arch
    platform = detect_platform(''.join(cblog_data['uname']))
    dbg(f'Detected platform: {platform}')

    # Parse the list of installed packages
    pkg_manager = detect_package_manager(cblog_data)
    dbg(f'Detected package manager: {pkg_manager}')

    candidate_versions = [
        release for release in releases_index if cb_version in release]
    dbg(f'Version {cb_version} matches releases: {", ".join(candidate_versions)}')
    dbg(f'Attempting to match package build number to Couchbase Server releases. This might take a while...')

    package_base_url = None
    for candidate in candidate_versions:
        available_pkgs, candidate_build_number = fetch_package_lists(
            candidate)
        if candidate_build_number in cb_build_number:
            package_base_url = f'{RELEASES_INDEX}{candidate}'
            break

    dbg(f'Build number resolves to {package_base_url}')

    platform_alts = get_platform_alternative_names(platform)
    # Filter packages to ones matching the target platform
    available_pkgs = list(pkg for pkg in available_pkgs
                          if any((plt in pkg for plt in platform_alts)))
    available_pkgs = list(pkg for pkg in available_pkgs
                          if pkg.endswith(get_pkg_manager_ext(pkg_manager)))
    available_pkgs = list(pkg for pkg in available_pkgs
                          if '-linux' in pkg and '-enterprise' in pkg)

    server_pkg_name = next(pkg for pkg in available_pkgs
                           if 'dbg' not in pkg and 'debug' not in pkg)
    symbols_pkg_name = next(pkg for pkg in available_pkgs
                            if 'dbg' in pkg or 'debug' in pkg)

    dbg(f'Using packages {server_pkg_name} and {symbols_pkg_name}')

    # wget doesn't like the self-signed TLS certificate we use
    server_pkg = f'{package_base_url}/{server_pkg_name}'.replace(
        'https', 'http')
    symbols_pkg = f'{package_base_url}/{symbols_pkg_name}'.replace(
        'https', 'http')

    if pkg_manager == 'dpkg':
        packages = dict(iterate_dpkg(cblog_data['dpkg']))
    elif pkg_manager == 'rpm':
        packages = dict(iterate_rpm(cblog_data['rpm']))
    else:
        raise NotImplementedError(f'Support for {pkg_manager} not implemented')

    # First line in Dockerfile
    print(f'FROM --platform=linux/{platform} {image}:{tag}')

    important_packages = [
        'libc6',
        'libgcc1',
    ]
    additional_packages = [
        ('binutils', None),
        ('gdb', None),
        ('wget', None),
        ('ca-certificates', None)]
    # Docker image will have matching important_packages as well as the
    # additional_packages we want
    packages_to_install = list(preserve_packages(
        important_packages, packages)) + additional_packages

    dbg(f'Packages to install: {packages_to_install}')

    # Reuse newline + &&
    command_separator = '\\\n  && '

    # Install dependencies
    if pkg_manager == 'dpkg':
        print(
            'RUN apt-get update', command_separator,
            end='')
        for pkg, version in packages_to_install:
            if version:
                print(
                    f'(apt-get -y -f install --no-install-recommends \'{pkg}={version}\' || apt-get -y install --no-install-recommends \'{pkg}\')',
                    command_separator,
                    end='')
            else:
                print(
                    f'(apt-get -y -f install --no-install-recommends \'{pkg}\')',
                    command_separator,
                    end='')
        print('true')
    elif pkg_manager == 'rpm':
        # check-update retuns 100 on success, so use an OR.
        print(
            'RUN yum check-update || ',
            end='')
        for pkg, version in packages_to_install:
            if version:
                print(
                    f'(yum install -y \'{pkg}-{version}\' || yum install -y \'{pkg}\')',
                    command_separator,
                    end='')
            else:
                print(
                    f'(yum install -y \'{pkg}\')',
                    command_separator,
                    end='')
        print('true')

    # Fetch and install Couchbase Server
    if pkg_manager == 'dpkg':
        # Fetch packages (in parallel), install and cleanup
        print(
            f'RUN wget \'{server_pkg}\' & wget \'{symbols_pkg}\' && wait ',
            command_separator, f'dpkg -i \'{server_pkg_name}\'',
            command_separator, f'dpkg -i \'{symbols_pkg_name}\'',
            command_separator,
            f'rm \'{server_pkg_name}\' \'{symbols_pkg_name}\'')
    elif pkg_manager == 'rpm':
        # Fetch packages (in parallel), install and cleanup
        print(
            f'RUN wget \'{server_pkg}\' & wget \'{symbols_pkg}\' && wait ',
            command_separator, f'rpm -i --nodeps \'{server_pkg_name}\'',
            command_separator, f'rpm -i --nodeps \'{symbols_pkg_name}\'',
            command_separator,
            f'rm \'{server_pkg_name}\' \'{symbols_pkg_name}\'')

    dbg('\nDone! Run:\n  docker build -t docker_repro_env - < Dockerfile && docker run --rm -it -v $PWD:/media docker_repro_env')
    dbg('If the image architecture does not match your host, you may need to install '
        'emulation support:\n  docker run --privileged --rm tonistiigi/binfmt --install arm64,amd64')


if __name__ == '__main__':
    main(**vars(parser.parse_args()))
