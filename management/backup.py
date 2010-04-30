#!/usr/bin/env python
import sys
import os
import glob
import shutil
import mc_bin_client

def usage():
    print >> sys.stderr, """
Usage: %s <dest_dir>
""" % os.path.basename(sys.argv[0])
    sys.exit(1)

def main():
    if len(sys.argv) != 2:
        usage()

    cmd_dir = os.path.dirname(sys.argv[0])
    dest_dir = sys.argv[1]
    flushctl = os.path.join(cmd_dir, 'flushctl.py')

    mc = mc_bin_client.MemcachedClient('127.0.0.1')
    db_path = mc.stats()['ep_dbname']
    db_files = glob.glob('%s*' % db_path)

    print 'Pausing persistence... ',
    os.system('"%s" 127.0.0.1:11211 stop' % flushctl)
    print 'paused.'
    try:
        for fn in db_files:
            dest_fn = os.path.join(dest_dir, os.path.basename(fn))
            print 'Copying %s to %s' % (fn, dest_fn)
            shutil.copyfile(fn, dest_fn)
    finally:
        print 'Unpausing persistence.'
        os.system('"%s" 127.0.0.1:11211 start' % flushctl)


if __name__ == '__main__':
    main()
