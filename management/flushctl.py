#!/usr/bin/env python
"""
Flush control for ep-engine.

Copyright (c) 2010  Dustin Sallings <dustin@spy.net>
"""
import time

import clitool

def stop(mc):
    mc.stop_persistence()
    stopped = False
    while not stopped:
        time.sleep(0.5)
        stats = mc.stats()
        if stats['ep_flusher_state'] == 'paused':
            stopped = True

if __name__ == '__main__':

    c = clitool.CliTool("""Available params:
    min_data_age  - minimum data age before flushing data"
    queue_age_cap - maximum queue age before flushing data"
    max_txn_size - maximum number of items in a flusher transaction""")

    c.addCommand('stop', stop)
    c.addCommand('start', 'start_persistence')
    c.addCommand('set', 'set_flush_param', 'set param value')

    c.execute()
