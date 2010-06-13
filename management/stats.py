#!/usr/bin/env python

import clitool
import sys

def stats_formatter(stats, prefix=" "):
    longest = max((len(x) + 2) for x in stats.keys())
    for stat, val in sorted(stats.items()):
        s = stat + ":"
        print "%s%s%s" % (prefix, s.ljust(longest), val)

def stats_vkey(mc, key):
    cmd = 'vkey ' + key
    vbs = mc.stats(cmd)
    print "verification for key", key
    stats_formatter(vbs)

if __name__ == '__main__':

    c = clitool.CliTool()

    c.addCommand('vkey', stats_vkey, 'vkey keyname')

    c.execute()
