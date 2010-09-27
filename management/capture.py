#!/usr/bin/env python

import sys
import time

from sqlite3 import dbapi2 as sqlite

import mc_bin_client

CREATE_TABLE = """
create table if not exists samples (
    host     varchar(64) not null,
    uptime   integer not null,
    statname varchar(64) not null,
    value    varchar(64) not null
)
"""

INS = """insert into samples (host, uptime, statname, value) values (?, ?, ?, ?)"""

dbpath = None

def wants_db(orig):
    def f(*args):
        global dbpath
        db = sqlite.connect(dbpath)
        try:
            return orig(*args + (db,))
        finally:
            db.commit()
            db.close()
    return f

@wants_db
def setup(db):
    db.execute(CREATE_TABLE)

@wants_db
def sample(hp, db):
    cur = db.cursor()

    try:
        h, p = hp.split(':')
        p = int(p)
    except:
        h = hp
        port = 11211

    mc = mc_bin_client.MemcachedClient(h, p)
    stats = mc.stats()
    uptime = int(stats['uptime'])
    print "Sampling", h, "at uptime =", uptime
    for k,v in stats.iteritems():
        cur.execute(INS, (h, uptime, k, v))

if __name__ == '__main__':
    dbpath = sys.argv[1]
    hosts = sys.argv[2:]
    assert hosts

    setup()
    while True:
        try:
            for h in hosts:
                sample(h)
        except:
            import traceback
            traceback.print_exc()
        time.sleep(60)
