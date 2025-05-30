#!/usr/bin/env python3

"""
Copyright 2023-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.

This script allows stats.log to be searched by grep and specific outputs to be
extracted from stats.log.

Example: kv_stats_log.py stats.log -c cbstats -a all -b default
Output:
  <`cbstats all` output for bucket `default`>
  ...
  auth_cmds:    0
  auth_errors:	0
  ...

Example: kv_stats_log.py stats.log -g
Output:
  <entire stats.log with lines prefixed by group and bucket>
  ...
  cbstats all:default: auth_cmds:   0
  cbstats all:default: auth_errors: 0
  ...
"""

import argparse
from collections import namedtuple
from enum import Enum, auto
import re
import time
import sys
import os

parser = argparse.ArgumentParser(
    prog=os.path.basename(__file__),
    description='Parses stats.log')
parser.add_argument(
    'infile', nargs='?', type=argparse.FileType(
        'rb', bufsize=8 * 1024 * 1024),
    default=sys.stdin)
parser.add_argument(
    '-g', '--greppable', action='store_true',
    help='Enables greppable output (output is prefixed by command and bucket)')
parser.add_argument(
    '-c', '--command', choices=('cbstats', 'mcstat', 'mctimings'),
    help='Filter by command')
parser.add_argument('-a', '--command-args',
                    help="Filter by command arguments")
parser.add_argument('-b', '--bucket', help='Filter by bucket')
parser.add_argument('-s', '--stat', help='Filter by stat or histogram')
parser.add_argument('-l', '--list', action='store_true',
                    help='Print available options (filters are applied)')
parser.add_argument('-v', '--verbose', action='store_true',
                    help='Print timing information')

args = parser.parse_args()

command_filter = args.command.encode() if args.command else None
command_args_filter = args.command_args.encode() if args.command_args else None
bucket_filter = args.bucket.encode() if args.bucket else None
stat_filter = args.stat.encode() if args.stat else None

SEP_CMD = (b'=' * 78) + b'\n'
SEP_BUCKET = (b'*' * 78) + b'\n'


# Contains a command (cbstats all), bucket name and associated output
CBStatOutput = namedtuple("CBStatOutput", ('command', 'bucket', 'output'))


class ParserState(Enum):
    """
    State machine for parsing the input
    """
    CMD = auto()  # Lines specifying the command
    BUCKET = auto()  # Lines specifying the bucket
    BLOCK = auto()  # Block of stats


def read_stats_log(file):
    """
    Generator which reads the input file and returned the parsed CBStatOutput
    elements.
    """
    state = ParserState.BLOCK
    # Current command
    cmd = None
    # Current block
    block = None
    # Current output object
    output_object = None

    for (lineno, line) in enumerate(file):
        if state == ParserState.CMD:
            # End of command lines
            if line == SEP_CMD:
                block = None
                state = ParserState.BLOCK
                continue
            cmd.append(line)
            continue
        elif state == ParserState.BUCKET:
            # Line specifies the bucket name and starts a new block
            # Yield the output reads so far
            if output_object is not None:
                yield output_object
            # Initialise new ouput block for this bucket
            block = []
            output_object = CBStatOutput(cmd, line, block)
            state = ParserState.BLOCK
            continue

        assert state == ParserState.BLOCK

        if line == SEP_BUCKET:
            state = ParserState.BUCKET
            continue
        elif line == SEP_CMD:
            state = ParserState.CMD
            cmd = []
            continue

        if not line.isspace():
            if block is None:
                # We have lines to process but no block to assign them to.
                # By proxy, this means we've not seen a bucket separator, so
                # the block we're now reading is not associated with a bucket.
                # Output what we've got so far.
                if output_object is not None:
                    yield output_object
                # And initialise a new output block without a bucket.
                block = []
                output_object = CBStatOutput(cmd, b'@no bucket@', block)
            block.append(line)

    if output_object is not None:
        yield output_object


def clean_cmd(cmd_lines):
    """
    Parse the command name from the raw input. The command block may be
    multiple lines long.
    """
    name = cmd_lines[0].decode().strip()
    if name == 'mctimings []':
        return b'mctimings'
    name = re.sub('^memcached stats', 'cbstats', name)
    name = re.sub('^memcached mcstat', 'mcstat', name)
    return name.encode()


def clean_bucket(bucket):
    """
    Parse the bucket name from the raw input. Some commands may prefix it with
    "Bucket:".
    """
    name = bucket.decode().strip()
    name = re.sub(r'^Bucket:\'(.*)\'$', r'\1', name)
    return name.encode()

def make_stat_regex(stat_filter):
    if not stat_filter:
        return None
    return re.compile(rb'[: ]' + re.escape(stat_filter) + rb'[: ]')

def process_input(outfile, infile, greppable, list_only, command_filter,
                  command_args_filter, bucket_filter, stat_filter):
    stat_regex = make_stat_regex(stat_filter)
    current_cmd_line = []
    current_bucket = b''
    for (cmd_line, bucket, output) in read_stats_log(infile):
        cmd_line = clean_cmd(cmd_line)
        cmd_array = cmd_line.split(b' ', maxsplit=1)
        cmd_args = cmd_array[1] if len(cmd_array) > 1 else None
        cmd = cmd_array[0]

        bucket = clean_bucket(bucket)

        prev_cmd_line = current_cmd_line
        prev_bucket = current_bucket
        current_cmd_line = cmd_line
        current_bucket = bucket

        # Apply filters.
        if bucket_filter is not None and bucket_filter != bucket:
            continue

        if command_filter is not None and command_filter != cmd:
            continue

        if command_args_filter is not None and command_args_filter != cmd_args:
            continue

        if list_only:
            if tuple(prev_cmd_line) == tuple(current_cmd_line) and \
                    prev_bucket == current_bucket:
                # Do not print duplicates.
                continue
            if stat_regex is not None:
                # Check if output contains the stat
                if not any((stat_regex.search(line) for line in output)):
                    continue
            # Just list options and exit.
            outfile.write(b'%s:%s:\n' % (cmd_line, bucket))
            continue

        if not args.bucket and not greppable:
            outfile.write(b'%s%s\n' % (SEP_BUCKET, bucket))

        # Greppable adds a prefix for each line.
        if greppable:
            grep_prefix = b'%s:%s:' % (cmd_line, bucket)

        # If we end up printing output, we need separators
        printed_output = False

        if stat_regex is None:
            printed_output = True
            for line in output:
                if greppable:
                    outfile.write(grep_prefix)
                outfile.write(line)
        else:
            # Print only matching stat or histogram.
            # For histograms, print between the first and last line.
            is_histogram = False
            for line in output:
                if not is_histogram and not stat_regex.search(line):
                    continue
                if b' total)\n' in line:
                    is_histogram = True
                if b' Avg ' in line:
                    is_histogram = False
                if greppable:
                    outfile.write(grep_prefix)
                outfile.write(line)
                printed_output = True

        # If not greppable, separate visually as in the original input.
        if not greppable and printed_output:
            outfile.write(b'\n')

    outfile.flush()


if __name__ == '__main__':
    start_time = time.perf_counter_ns()

    output_file = os.fdopen(sys.stdout.fileno(), 'wb', closefd=False)

    try:
        process_input(
            output_file, infile=args.infile, greppable=args.greppable,
            list_only=args.list,
            command_filter=command_filter,
            command_args_filter=command_args_filter,
            bucket_filter=bucket_filter,
            stat_filter=stat_filter)
    except BrokenPipeError:
        # Ignore stdout being closed. This will happen if the output is piped
        # into e.g. head.
        pass

    duration_ns = time.perf_counter_ns() - start_time
    nbytes = args.infile.tell()

    mib = 1 / (1024 * 1024)
    second = 1 / 1e9

    # Print some stats
    if args.verbose:
        print(
            (f'\n{parser.prog} took: {duration_ns * second:.3f}s, '
             f'Read: {nbytes * mib:.2f} MiB, '
             f'Rate: {nbytes / duration_ns * mib / second:.2f} MiB/s'),
            file=sys.stderr)
