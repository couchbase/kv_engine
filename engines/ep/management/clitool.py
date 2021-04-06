"""

  Copyright 2010-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included
in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
in that file, in accordance with the Business Source License, use of this
software will be governed by the Apache License, Version 2.0, included in
the file licenses/APL2.txt.

"""

import optparse
import socket
import sys

import mc_bin_client


class CliTool(object):

    def __init__(self, extraUsage=""):
        self.cmds = {}
        self.flags = {}
        self.extraUsage = extraUsage.strip()
        self.parser = optparse.OptionParser(
            usage="%prog host[:dataport] command [options]\n\n"
                  "dataport [default:11210]")

    def addCommand(self, name, f, help=None, hidden=False):
        if not help:
            help = name
        self.cmds[name] = (f, help, hidden)

    def addFlag(self, flag, key, description):
        self.flags[flag] = description
        self.parser.add_option(flag, dest=key, action='store_true',
                               help=description)

    def addHiddenFlag(self, flag, key):
        self.parser.add_option(flag, dest=key, action='store_true',
                               help=optparse.SUPPRESS_HELP)

    def addOption(self, flag, key, description):
        self.flags[flag] = description
        self.parser.add_option(flag, dest=key, action='store',
                               help=description)

    def execute(self):
        shortUsage = self.parser.usage
        self.parser.usage += "\n" + self.format_command_list()

        opts, args = self.parser.parse_args()  # -h handled here

        self.parser.usage = shortUsage  # set usage back to short

        if len(args) < 2:
            print(self.parser.error("Too few arguments"), file=sys.stderr)
            sys.exit(2)

        hp, self.cmd = args[:2]
        try:
            (host, port, family) = mc_bin_client.parse_address(hp)
        except Exception as e:
            print(e)
            sys.exit(1)

        try:
            mc = mc_bin_client.MemcachedClient(host, port, family)
        except OSError as e:
            print(
                "Failed to connect to host {} on port {}: {}".format(host, port,
                                                                     e))
            sys.exit(1)

        f = self.cmds.get(self.cmd)

        if not f:
             print(self.parser.error("Unknown command"))

        try:
            if callable(f[0]):
                f[0](mc, *args[2:], **opts.__dict__)
            else:
                getattr(mc, f[0])(*args[2:])
        except socket.error as e:
            # "Broken pipe" is confusing, so print "Connection refused" instead.
            if type(e) is tuple and e[0] == 32 or \
                    isinstance(e, socket.error) and e.errno == 32:
                print("Could not connect to %s:%d: "
                    "Connection refused" % (host, port), file=sys.stderr)
            else:
                raise

    def format_command_list(self):
        output = ""

        # Create list and ignore any hidden=True commands
        cmds = sorted(c[1] for c in (y for y in self.cmds.values() if not y[2]))
        output += "\nCommands:\n"

        for c in cmds:
            output += "    %s\n" % c

        output = output[:-1]

        output += self.extraUsage

        return output
