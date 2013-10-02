# The Couchbase engine

This is the Couchbase engine, previously known as the membase engine.
Initially, somewhat jokingly, it was called the "eventually
persistent" engine.  The name stuck, we think mostly because it's easy
to say.

## Building

You will need a storage-engine capable memcached and its included
headers.

The easiest way to do this if you don't want to install memcached from
source would be to just create a source tree and reference it.

### Building Memcached

For example, assume you keep all of your projects in `~/prog/`, you
can do this:

    cd ~/prog
    git clone -b engine git://github.com/membase/memcached.git
    cd memcached
    git checkout engine
    ./config/autorun.sh
    ./configure
    make

### Building the Eventually Persistent Engine

    cd ~/prog
    git clone git@github.com:membase/ep-engine.git
    cd ep-engine
    ./config/autorun.sh
    ./configure --with-memcached=$HOME/prog/memcached
    make

## Running

An example invocation using the ep engine from your dev tree and
keeping the database in `/tmp/ep.db` looks like this:

    ~/prog/memcached/memcached -v -E ~/prog/ep-engine/.libs/ep.so \
        -e dbname=/tmp/ep.db

## Versioning

While historically ep-engine had used tags and branches to describe
it's versions, it no longer does so.  The versions of ep-engine are
now defined by the Couchbase repo manifest which may be found at
https://github.com/couchbase/manifest

You will no longer find a tag for various releases of Couchbase.
