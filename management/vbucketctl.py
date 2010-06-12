#!/usr/bin/env python

import clitool

def listvb(mc):
    vbs = mc.stats('vbucket')
    for (vb, state) in sorted(list(vbs.items())):
        print "vbucket", vb[3:], state

if __name__ == '__main__':

    c = clitool.CliTool()

    c.addCommand('list', listvb)
    c.addCommand('set', 'set_vbucket_state', 'set [vbid] [vbstate]')
    c.addCommand('rm', 'delete_vbucket', 'rm [vbid]')

    c.execute()
