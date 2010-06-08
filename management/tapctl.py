#!/usr/bin/env python
"""
Tap control for ep-engine.

"""

import clitool

def status(mc):
    stats = mc.stats("tap")

    print "Current replication stats: "
    print "ep_replication_peer  : %s" % stats['ep_replication_peer']
    print "ep_replication_state : %s" % stats['ep_replication_state']
    print "ep_replication_status: %s" % stats['ep_replication_status']

    if int(stats['ep_tap_count']) > 0:
        print "Replication clients:"
        for t in [ t for t in stats if ':qlen' in t ]:
            k = t.split(':')
            print "%s:%s - backlog: %s" %( k[1], k[2], stats[t])
        print

if __name__ == '__main__':

    c = clitool.CliTool("""Available params:
    tap_peer - the replication master (host:port)""")

    c.addCommand('stop', 'stop_replication')
    c.addCommand('start', 'start_replication')
    c.addCommand('set', 'set_tap_param', 'set param value')
    c.addCommand('status', status)

    c.execute()
