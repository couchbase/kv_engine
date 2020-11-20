#!/usr/bin/env python2.7

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

from __future__ import print_function
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
    if '.el' in package:
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


def try_download_from_url(host, version, subdir, package):
    filename = package + ".rpm"
    if os.path.isfile(filename):
        print(("Skipping download of {} as file already " +
               "exists.").format(filename), file=sys.stderr)
        return filename
    url = host + "/" + version + "/" + subdir + "/" + filename
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
        return try_download_from_url('http://debuginfo.centos.org', '7',
                                     'x86_64', package)

    repos = {'http://mirror.centos.org/centos': ('7',),
             'http://vault.centos.org':
                 ('7.0.1406', '7.1.1503', '7.2.1511', '7.3.1611', '7.4.1708',
                  '7.5.1804', '7.6.1810', '7.7.1908', '7.8.2003')
             }
    subdirs = ('os/x86_64/Packages', 'updates/x86_64/Packages')
    for host, versions in repos.iteritems():
        for version in versions:
            for subdir in subdirs:
                filename = try_download_from_url(host, version, subdir, package)
                if filename:
                    return filename
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
