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

    def addCommand(self, name, f, help=None):
        if not help:
            help = name
        self.cmds[name] = (f, help)

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
        self.parser.usage +=  "\n" + self.format_command_list()

        opts, args = self.parser.parse_args()

        if len(args) < 2:
            print >> sys.stderr, self.parser.error("Too few arguments")
            sys.exit(2)


        hp, self.cmd = args[:2]
        if ':' in hp:
            host, port = hp.split(':', 1)
            try:
                port = int(port)
            except ValueError:
                print >> sys.stderr, self.parser.error(
                                                 "invalid host[:dataport]")
                sys.exit(2)
        else:
            host = hp
            port = 11210

        try:
            mc = mc_bin_client.MemcachedClient(host, port)
        except socket.gaierror, e:
            print 'Connection error: %s' % e
            sys.exit(1)

        f = self.cmds.get(self.cmd)

        if not f:
             print self.parser.error("Unknown command")

        try:
            if callable(f[0]):
                f[0](mc, *args[2:], **opts.__dict__)
            else:
                getattr(mc, f[0])(*args[2:])
        except socket.error, e:
            # "Broken pipe" is confusing, so print "Connection refused" instead.
            if type(e) is tuple and e[0] == 32 or \
                    isinstance(e, socket.error) and e.errno == 32:
                print >> sys.stderr, "Could not connect to %s:%d: " \
                    "Connection refused" % (host, port)
            else:
                raise

    def format_command_list(self):
        output = ""

        cmds = sorted(c[1] for c in self.cmds.values())
        output += "\nCommands:\n"

        for c in cmds:
            output += "    %s\n" % c

        output = output[:-1]

        output += self.extraUsage

        return output
