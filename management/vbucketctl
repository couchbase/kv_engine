#!/usr/bin/env python

import clitool

def listvb(mc, auth="none", username="", password=""):
    if auth == "sasl":
        mc.sasl_auth_plain(username, password)
    vbs = mc.stats('vbucket')
    for (vb, state) in sorted(list(vbs.items())):
        print "vbucket", vb[3:], state

def setvb(mc, vbid, vbstate, auth="none", username="", password=""):
    if auth == "sasl":
        mc.sasl_auth_plain(username, password)
    mc.set_vbucket_state(vbid, vbstate)

def rmvb(mc, vbid, auth="none", username="", password=""):
    if auth == "sasl":
        mc.sasl_auth_plain(username, password)
    mc.delete_vbucket(vbid)

if __name__ == '__main__':

    c = clitool.CliTool()

    c.addCommand('list', listvb, 'list sasl [username] [password]')
    c.addCommand('set', setvb, 'set [vbid] [vbstate] sasl [username] [password]')
    c.addCommand('rm', rmvb, 'rm [vbid] sasl [username] [password]')

    c.execute()
