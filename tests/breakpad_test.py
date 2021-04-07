#!/usr/bin/python

#     Copyright 2015 Couchbase, Inc
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

"""Tests the operation of Breakpad as a crash reporter for memcached.

Starts up memcached (with breakpad enabled where supported), terminates it
and verifies:
  * Message is written to stderr reporting the crash.
  * Backtrace for crash location is reported.
  * Backtrace where exception was thrown (if via throwWithTrace) is reported.
If breakpad is enabled:
  * A minidump file is successfully created.
If necessary support binaries are present (gdb, minidump2core):
  * The minidump can be converted to a core file.
  * The core file can be loaded into GDB.
  * GDB can read various useful information from the core dump.
"""

from __future__ import print_function
import argparse
import json
import logging
import os
import re
import signal
import shutil
import subprocess
import sys
import tempfile
import time
import threading

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                    datefmt='%Y-%m-%dT%H:%M:%S',
                    level=logging.INFO)

minidump_dir=None

def cleanup_and_exit(status_code):
    """Remove any temporary files etc and exit with the given status code."""
    if minidump_dir:
        shutil.rmtree(minidump_dir)
    exit(status_code);

def print_stderrdata(stderrdata):
    print("=== stderr begin ===")
    for line in stderrdata.splitlines():
        print(line)
    print("=== stderr end ===")

def invoke_gdb(gdb_exe, program, core_file, commands=[]):
    """Invoke GDB on the specified program and core file, running the given
    commands. Returns a string of GDB's output.
    """
    args = [gdb_exe, program,
            '--core=' + os.path.abspath(core_file.name),
            '--batch']
    for c in commands:
        args += ['--eval-command='+c]

    logging.debug("GDB args:" + ', '.join(args))
    gdb = subprocess.Popen(args, stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE)
    gdb.wait()
    return gdb.stdout.read()


def check_gdb(memcached_exe, gdb_exe, md2core_exe, minidump):
    """Performs various checks on the generated minidump file with GDB.
    """
    logging.info("Analysing minidump file '" + minidump + "'")
    with tempfile.NamedTemporaryFile() as core_file:
        # Convert minidump to core file.
        try:
            subprocess.check_call([md2core_exe, minidump],
                                  stdout=core_file)
        except subprocess.CalledProcessError as e:
            logging.error(
                "FAIL - minidump-2-core failed with return code {0}".format(
                    e.returncode))
            os.remove(minidump)
            cleanup_and_exit(7)

        # To avoid invoking gdb multiple times, lets collect all of
        # the information in a single pass. To make it easy to figure
        # out the output from each command, echo a fixed delimiter to
        # split the data.
        delimiter = '-' * 80
        logging.info('GDB: Collect information from the minidump file')
        gdb_output = invoke_gdb(gdb_exe, memcached_exe, core_file,
                                ['set print pretty off',
                                 'echo ' + delimiter + '\\n',
                                 'info shared',
                                 'echo ' + delimiter + '\\n',
                                 'backtrace',
                                 'echo ' + delimiter + '\\n',
                                 'x/x $sp'])

        logging.debug("=== gdb output begin ===")
        logging.debug(gdb_output)
        logging.debug("=== gdb output end ===")

        if delimiter not in gdb_output:
            logging.error(
                "UNTESTABLE - GDB output missing the expected delimiter ('-' * 80).")
            logging.error("GDB output:")
            logging.error(gdb_output)
            cleanup_and_exit(8)

        gdb_output = gdb_output.split(delimiter)

        # Check for shared library information, and symbols successfully ead
        # (needed for any useful backtraces).
        # Shared libraries may change over time, but we explicitly asked for
        # crash_engine.so in the config so we should have that.
        logging.info('GDB: Checking for shared library information')
        m = re.search(
            "^0x[0-9a-f]+\s+0x[0-9a-f]+\s+(\w+)\s+([^\s]+crash_engine\.so$)",
            gdb_output[1], re.MULTILINE)
        if not m:
            logging.error("FAIL - GDB unable to show information for " +
                          "crash_engine.so shared library.")
            cleanup_and_exit(9)

        if not m.group(1).startswith('Yes'):
            logging.error(
                "FAIL - GDB unable to read symbols for crash_engine.so " +
                "(tried path: '{0}').".format(m.group(2)))
            cleanup_and_exit(10)

        # Check for sensible backtrace. This is again tricky as we could have
        # received the SIGABRT pretty much anywhere so we can't check for a
        # particular trace. Instead we check that all frames but the first have
        # a useful-looking symbol (and not just a random address.)
        logging.info('GDB: Checking for sensible backtrace')
        lines = gdb_output[2].splitlines()

        # Discard all those lines before the stacktrace
        backtrace = [i for i in lines if re.match('#\d+', i)]

        # Discard the innermost frame, as it could be anywhere (and hence may
        # not have a valid symbol).
        backtrace.pop(-1)

        for i, frame in enumerate(backtrace):
            # GDB prints stack frames in a variety of formats, however for
            # these purposes we just care that we *don't* have with an unknown
            # symbol, i.e. of the format:
            #     #0 <hex-address> in ??
            m = re.match('#\d+\s+0x[0-9a-f]+ in \?\? \(\) from ([^\s]+)', frame)
            if m:
                # However allow unknown symbols if they are in system libraries.
                so_name = m.group(1)
                if not any(so_name.startswith(d) for d in ['/lib',
                                                           '/usr/lib',
                                                           '/usr/local/lib']):
                    logging.error(
                        ("FAIL - GDB unable to identify the symbol of " +
                         "frame {0} - found '{1}'.").format(i, frame))
                    print("=== GDB begin ===")
                    for line in lines:
                        print(line)
                    print("=== GDB end ===")
                    cleanup_and_exit(11)

        # Check we can read stack memory. Another tricky one as again we have
        # no idea where we crashed. Just ensure that we get *something* back
        logging.info('GDB: Checking for readable stack memory')
        m = re.search('(0x[0-9a-f]+):\s(0x[0-9a-f]+)?', gdb_output[3])
        if not m:
            logging.error(
                "FAIL - GDB failed to output memory disassembly when " +
                "attempting to examine stack.")
            cleanup_and_exit(12)
        if not m.group(2):
            logging.error("FAIL - GDB unable to examine stack memory " +
                          "(tried address {0}).".format(m.group(1)))
            cleanup_and_exit(13)


class Subprocess(object):
    """Simple wrapper around subprocess to add a timeout & core file limit
    to the child process.
    """

    def __init__(self, args):
        self.args = args
        self.process = None

    def run(self, timeout):
        def set_core_file_ulimit():
            try:
                import resource
                resource.setrlimit(resource.RLIMIT_CORE, (0,0))
            except ImportError:
                if os.name == 'nt':
                    # Not possible to set core on Windows.
                    pass
                else:
                    raise

        def target():
            self.process = subprocess.Popen(self.args, stderr=subprocess.PIPE,
                                            env = os.environ,
                                            preexec_fn=set_core_file_ulimit)
            (_, self.stderrdata) = self.process.communicate()

        thread = threading.Thread(target=target)
        thread.start()

        thread.join(timeout)
        if thread.is_alive():
            logging.error("Timeout - terminating process " + self.args[0])
            self.process.terminate()
            thread.join()
        return (self.process.returncode, self.stderrdata)


parser = argparse.ArgumentParser()
parser.add_argument('memcached_exe', help='Path to memcached executable')
parser.add_argument('crash_mode', help='Type of crash to perform')
parser.add_argument('--breakpad', action='store_true', help='Is Breakpad enabled or not?')
parser.add_argument('--gdb_exe',
                    help='Path to GDB executable, enables GDB checks. Requires that m2core_exe has been specified.')
parser.add_argument('--md2core_exe',
                    help='Path to minidump2core executable, enables minidump checks checks')
args = parser.parse_args()

# Given there are multiple breakpad tests which can run in parallel, give
# each one it's own minidump directory.
minidump_dir = tempfile.mkdtemp(prefix='breakpad_test_tmp.')
logging.debug("Using minidump_dir=" + minidump_dir)

rbac_data = {}
rbac_file = tempfile.NamedTemporaryFile(delete=False)
rbac_file.write(json.dumps(rbac_data))
rbac_file.close()

# 'verbosity' isn't functionally needed, but helpful to debug test issues.
config = {"interfaces": [{"tag":"plain",
                          "port": 0,
                          "host": "*"}],
          "breakpad": { "enabled": True,
                        "minidump_dir" : minidump_dir
                      },
          "verbosity" : 2,
          "rbac_file" : os.path.abspath(rbac_file.name)}
config_json = json.dumps(config)

# Need a temporary file which can be opened (a second time) by memcached,
# therefore use NamedTemporaryFile(delete=False) and manually unlink
# when no longer needed.
config_file = tempfile.NamedTemporaryFile(delete=False)
config_file.write(config_json)
config_file.close()

os.environ['MEMCACHED_UNIT_TESTS'] = "true"
os.environ['MEMCACHED_CRASH_TEST'] = args.crash_mode

mc_args = [args.memcached_exe, "-C", os.path.abspath(config_file.name)]

# Spawn memcached from a child thread.
logging.info('Spawning memcached')
logging.debug('"MEMCACHED_UNIT_TESTS=' +
              os.environ['MEMCACHED_UNIT_TESTS'] +
              ' MEMCACHED_CRASH_TEST=' +
              os.environ['MEMCACHED_CRASH_TEST'] + ' ' +
              (' '.join(mc_args) + '"'))
memcached = Subprocess(mc_args)

# Wait for memcached to initialise (and consequently crash due to loading
# crash_engine).
(status, stderrdata) = memcached.run(timeout=30)
logging.info('Process exited with status ' + str(status))
logging.debug("=== memcached stderr begin ===")
logging.debug(stderrdata)
logging.debug("=== memcached stderr end ===")

# Cleanup config_file (no longer needed).
os.remove(config_file.name)
os.remove(rbac_file.name)

# Check a message was written to stderr
if args.breakpad and 'Breakpad caught a crash' not in stderrdata:
    logging.error("FAIL - No message written to stderr on crash.")
    print_stderrdata(stderrdata)
    cleanup_and_exit(3)

# Check the message includes the exception what() message (std::exception crash)
if args.crash_mode.startswith('std_exception') and 'what():' not in stderrdata:
    logging.error("FAIL - No exception what() message written to stderr on crash.")
    print_stderrdata(stderrdata)
    cleanup_and_exit(3)

# Check the message include the location the exception was thrown from (throwWithTrace)
if args.crash_mode == 'std_exception_with_trace' and 'Exception thrown from:' not in stderrdata:
    logging.error("FAIL - No exception thrown backtrace message was written to stderr on crash.")
    print_stderrdata(stderrdata)
    cleanup_and_exit(3)

# Check the message also included a stack backtrace - we just check
# for one known function.
if args.breakpad and 'recursive_crash_function' not in stderrdata:
    logging.error("FAIL - No stack backtrace written to stderr on crash.")
    print_stderrdata(stderrdata)
    cleanup_and_exit(3)

if args.breakpad:
    # Check there is a minidump path in the output.
    m = re.search('Writing crash dump to ([\w\\\/\:\-.]+)', stderrdata)
    if not m:
        logging.error("FAIL - Unable to find crash filename in stderr.")
        print_stderrdata(stderrdata)
        cleanup_and_exit(4)

    # Check the minidump file exists on disk.
    minidump = m.group(1)
    if not os.path.exists(minidump):
        logging.error(
            "FAIL - Minidump file '{0}' does not exist.".format(minidump))
        print_stderrdata(stderrdata)
        cleanup_and_exit(5)

    # On Windows we don't have md2core or gdb; so skip these tests.
    if args.md2core_exe and args.gdb_exe:
        check_gdb(args.memcached_exe, args.gdb_exe, args.md2core_exe, minidump)
    else:
        # Check the minidump file is non-zero in size.
        statinfo = os.stat(minidump)
        if statinfo.st_size == 0:
            logging.error(
                "FAIL - minidump file '{0}' is zero bytes in size.".format(
                    minidump))
            cleanup_and_exit(14)

        # Done with the minidump
        os.remove(minidump)

# Got to the end - that's a pass
logging.info("Pass")
cleanup_and_exit(0)
