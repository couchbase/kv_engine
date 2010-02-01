# Master Slave Replication HOWTO

Master/slave replication is a simple fault-tolerance configuration
that allows a slave to fully sync up and maintain sync'd up with a
master.

## Configuring the Master

No special configuration is required on the master, but here's an
example startup command that is known to work:

    MCDIR=/path/to/memcached/
    EPDIR=/path/to/ep-engine/
    DBDIR=/path/to/write/database
    $MCDIR/memcached -u nobody -v -E $EPDIR/.libs/ep.so -e dbname=$DBDIR/mem.db

## Configuring the slave

The slave configuration is almost identical to the master, but
includes a `-O` parameter pointing to the location of the master.  For
example:

    MASTER=w.x.y.z:11211
    $MCDIR/memcached -u nobody -v -O $MASTER \
        -E $EPDIR/.libs/ep.so -e dbname=$DBDIR/mem.db
