import socket
import sys

import mc_bin_client

class CliTool(object):

    def __init__(self, extraUsage=None):
        self.cmds = {}
        self.extraUsage = extraUsage

    def addCommand(self, name, f, help=None):
        if not help:
            help = name
        self.cmds[name] = (f, help)

    def execute(self):

        try:
            hp, self.cmd = sys.argv[1:3]
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
                f[0](mc, *sys.argv[3:])
            else:
                getattr(mc, f[0])(*sys.argv[3:])
        except socket.error, e:
            # "Broken pipe" is confusing, so print "Connection refused" instead.
            if e.errno == 32:
                print >> sys.stderr, "Could not connect to {0}:{1}: " \
                    "Connection refused".format(host, port)
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
