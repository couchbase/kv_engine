#!/usr/bin/env python3

#     Copyright 2018-Present Couchbase, Inc.
#
#   Use of this software is governed by the Business Source License included
#   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
#   in that file, in accordance with the Business Source License, use of this
#   software will be governed by the Apache License, Version 2.0, included in
#   the file licenses/APL2.txt.

#   Test to check that KV-engine tests have a define only-once guard, e.g.
#   they contain a #pragma once directive or contain a #ifndef, #define
#   and #endif wrapper

from __future__ import print_function
import os
import sys
import argparse


def check_for_only_def_once(file):
    """
    Function to check for the presence of #pragma once or a #ifndef guard in a
    file
    :param file: file to search for #pragma once or (#ifndef X, #define X and
    #endif)
    :return: returns true if the correct macros are found or false if not
    """
    header_file = open(file, "r")
    # re-set the seek location to the beginning of the file as read()
    # will have made it point to the end
    header_file.seek(0)
    for line in header_file:
        # if there is a #pragma once then were good
        if line == str("#pragma once\n"):
            return True
    return False


def find_and_check_headers_in_dir(top_level_dir, exclusions):
    """
    Function to iterate though all the header files in a directory and its
    sub-directories
    """
    # if check_for_only_def_once() ever returns false then this will be set to
    # false to indicate a failure
    test_pass = True

    for root, dirs, files in os.walk(top_level_dir):
        for current_file in files:
            full_path = os.path.join(root, current_file)
            if current_file.endswith(".h") and not (full_path in exclusions):
                if not check_for_only_def_once(full_path):
                    print("TEST FAIL - Header file found without "
                          "#pragma once\" or \"#ifndef\" wrapper: " +
                          full_path, file=sys.stderr)
                    test_pass = False
    return test_pass


# create argparser so the user can specify which files to exclude if needed
argParser = argparse.ArgumentParser()
argParser.add_argument('--rootdir',
                       metavar='/Users/user/source/couchbase/kv_engine',
                       type=str, nargs=1,
                       help='Directory to check header files in, defaults to '
                            'the current working directory.',
                       default=[os.getcwd()])
argParser.add_argument('--exclude',
                       metavar='engines/ep/src/tasks.def.h',
                       type=str, nargs='+',
                       help='List of files to exclude from checking, ' +
                            'defined by their path within --rootdir or ' +
                            'if --rootdir is not specified their path within' +
                            ' the working directory.',
                       default=[])

args = argParser.parse_args()

# get the grand-parent dir of the file which should be the kv_engine directory
dirToSearchForHeaders = args.rootdir[0]

if not os.path.isabs(dirToSearchForHeaders):
    dirToSearchForHeaders = os.path.abspath(dirToSearchForHeaders)
dirToSearchForHeaders = os.path.normpath(dirToSearchForHeaders)

listOfExclusions = args.exclude
# fully expand exclusion file paths
listOfExclusions = [
    os.path.normpath(
        os.path.abspath(os.path.join(dirToSearchForHeaders, path)))
    for path in listOfExclusions]

if find_and_check_headers_in_dir(dirToSearchForHeaders, listOfExclusions):
    exit(0)  # exit with 0 for pass
else:
    exit(1)  # exit with a general error code if there was a test failure
