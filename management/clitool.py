import optparse
import socket
import sys

import mc_bin_client

class CliTool(object):

    def __init__(self, extraUsage=""):
        self.cmds = {}
        self.extraUsage = extraUsage.strip()
        self.parser = optparse.OptionParser()

    def addCommand(self, name, f, help=None):
        if not help:
            help = name
        self.cmds[name] = (f, help)

    def addFlag(self, flag, key, description):
        self.parser.add_option(flag, dest=key, action='store_true',
                               help=description)

    def execute(self):
        opts, args = self.parser.parse_args()

        try:
            hp, self.cmd = args[:2]
            host, port = hp.split(':')
            port = int(port)
        except ValueError:
            self.usage()

        mc = mc_bin_client.MemcachedClient(host, port)

        f = self.cmds.get(self.cmd)

        if not f:
            self.usage()

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

    def usage(self):
        cmds = sorted(c[1] for c in self.cmds.values())
        print >>sys.stderr, "Usage: %s host:port %s" % (sys.argv[0], cmds[0])
        for c in cmds[1:]:
            print >>sys.stderr, "  or   %s host:port %s" % (sys.argv[0], c)
        if self.extraUsage:
            print >>sys.stderr, "\n" + self.extraUsage
        sys.exit(1)
