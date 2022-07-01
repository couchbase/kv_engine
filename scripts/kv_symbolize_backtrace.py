#!/usr/bin/env python3

import argparse
import fileinput
import re
import subprocess

"""
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
"""

"""Script to convert addresses from backtraces printed via print_backtrace()
to symbol names.
Accepts input from stdin or files provided on the command line; for each line
checks to see if it matches the backtrace format and if so invokes gdb to
symbolise the address.

If the line doesn't match then simply passes that line through - as such this
can be used as a filter on a complete log.

Example usage:

    cat memcached.log | grep CRITICAL | ./kv_symbolize_backtrace.py

Note: Spawns GDB once for each matched line (as it must read the correct
binary) so can be slow with large numbers of lines. In such as case just pass
the script the specific lines to symbolify.
"""

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--source-line', action='store_true',
                        help='Include source file + line details')
    parser.add_argument('files', metavar='FILE', nargs='*',
                        help='files to read, if empty, stdin is used')
    args = parser.parse_args()

    # Match a stack frame of the form, extracting the module and address;
    # also capturing text before and after match
    #     /lib64/libc.so.6(clone+0x6d) [0x7f77c18db000+0xfe8dd]
    pattern = re.compile(
        '(.*?\s+)([\w/\.\+]+)\([^\)]*\) \[(0x[0-9a-f]+)\+(0x[0-9a-f]+)\](.*?)')

    for line in fileinput.input(
            files=args.files if len(args.files) > 0 else ('-',)):
        match = pattern.search(line)
        if match:
            (pre, binary, base_addr, offset, post) = match.groups()
            # For relocatable binaries we should use the offset address, but
            # for the main executable binary we should use base+offset.
            # As a heuristic, Assume base > 0x400000 is relocable (0x400000 is
            # normal base address for memcached on CentOS7).
            if int(base_addr, 0) > 0x400000:
                addr = int(offset, 0)
            else:
                addr = int(base_addr, 0) + int(offset, 0)
            gdb_args = ['gdb', binary, '--batch', '-ex',
                        'info symbol ' + hex(addr)]
            if args.source_line:
                gdb_args += ['-ex', 'printf "\t"', '-ex',
                             'info line *' + hex(addr)]
            out = subprocess.check_output(gdb_args, universal_newlines=True)
            print(pre, out.strip(), post)
        else:
            print(line, end='')
