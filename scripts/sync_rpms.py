#!/usr/bin/env python2.7

from __future__ import print_function

"""
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
"""

"""Script to synchronise the current system with the given list of
packages, for example to replicate the environment of a given Couchbase
installation.

Takes a list of package names on stdin (as printed by `rpm -qa` and
listed in couchbase.log) and for each one:

- Downloads it from the correct repo
- Installs it

Example usage:

    grep '^\(glibc\|libstdc++\)' couchbase.log | ./sync_rpms.py
"""

import fileinput
import logging
import os
import requests
import shutil
import subprocess
import sys

logging.basicConfig(level=logging.INFO)
logging.getLogger('urllib3').setLevel(logging.ERROR)

def is_package_installed(package):
    with open(os.devnull, 'w') as devnull:
        return subprocess.call(["rpm", "-q", package], stdout=devnull) == 0


def download_package(package):
    """Given a package name+version string, attempt to locate the file
    from the appropriate package repository; downloads it and returns
    the name of the local file it was downloaded to.  Returns None if
    package could not be located.

    """
    if '.el' in package:
        return download_centos_package(package)
    if package.startswith('couchbase-server'):
        return download_couchbase_package(package)


def try_download_from_url(base_url, package):
    filename = package + ".rpm"
    if os.path.isfile(filename):
        print(("Skipping download of {} as file already " +
               "exists.").format(filename), file=sys.stderr)
        return filename
    url = base_url + "/" + filename
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        logging.info("Found at {}".format(url))
        with open(filename, 'wb') as rpm_file:
            response.raw.decode_content = True
            shutil.copyfileobj(response.raw, rpm_file)
            return filename
    return None


def download_centos_package(package):
    logging.info("Searching for CentOS package '%s'", package)
    # "Enterprise Linux' aka CentOS.
    # Current releases are available from mirror, historic from vault,
    # debuginfo from debuginfo.
    if 'debuginfo' in package:
        return try_download_from_url('http://debuginfo.centos.org/7/x86_64',
                                     package)

    repos = {'http://mirror.centos.org/centos': ('7',),
             'http://vault.centos.org':
                 ('7.8.2003', '7.7.1908', '7.6.1810', '7.5.1804', '7.4.1708',
                  '7.3.1611', '7.2.1511', '7.1.1503', '7.0.1406')
             }
    subdirs = ('os/x86_64/Packages', 'updates/x86_64/Packages')
    for host, versions in repos.iteritems():
        for version in versions:
            for subdir in subdirs:
                base_url = host + '/' + version + '/' + subdir
                filename = try_download_from_url(base_url, package)
                if filename:
                    return filename
    return None


def download_couchbase_package(package):
    logging.info("Searching for Couchbase package '%s'", package)
    # GA'd versions available from packages.couchbase.com, pre-release from
    # latestbuilds (requires VPN).
    (name, version, build_arch) = package.rsplit('-', 2)
    (build, arch) = build_arch.split('.')
    version_to_dir = {'7.0.0': 'http://latestbuilds.service.couchbase.com/builds/latestbuilds/couchbase-server/cheshire-cat'}
    if version in version_to_dir:
        base_url = version_to_dir[version] + '/' + build
        # Map package name to filename - the EE package is named
        # 'couchbase-server', the CE one 'couchbase-server-community'.
        pkg_to_file = {'couchbase-server': 'couchbase-server-enterprise',
                       'couchbase-server-debuginfo': 'couchbase-server-enterprise-debuginfo'}
        if name in pkg_to_file:
            name = pkg_to_file[name]

        # Insert the distribution name
        # TODO: derive this from current system
        filename = (name + '-' + version + '-' + build + '-centos7.' + arch)

        return try_download_from_url(base_url, filename)
    return None


def install_packages(packages):
    subprocess.check_call(["rpm", "-Uvh", "--oldpackage"] + packages)


packages = []
for line in fileinput.input():
    package = line.strip()
    if is_package_installed(package):
        continue
    f = download_package(package)
    if f:
        packages.append(f)
    else:
        logging.error("Failed to locate package for '{}'".format(package))

if packages:
    logging.info("Installing packages:" + ' '.join(packages))
    install_packages(packages)
else:
    logging.info("All packages already installed.")
