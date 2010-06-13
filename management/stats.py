#!/usr/bin/env python

import clitool
import sys

def stats_vkey(mc, key):
    cmd = 'vkey ' + key
    vbs = mc.stats(cmd)
    for stat in vbs:
        print "stat: %s value: %s" % (stat, vbs[stat])

if __name__ == '__main__':

    c = clitool.CliTool()

    c.addCommand('vkey', stats_vkey, 'vkey keyname')

    c.execute()
