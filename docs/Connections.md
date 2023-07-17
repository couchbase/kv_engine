# Connection management

This document describes the various mechanisms memcached uses for
connection management.

From memcached perspective a connection is a single socket established
from a client to the server. The SDKs may or may not utilize multiple
sockets to the server, and from memcached's perspective this is multiple
connections.

The server will automatically shut down a connection if:

1. There is an error on the socket (read/write errors)
2. Protocol error in the MCBP format
3. The connected engine encountered a hard error it can't resume from

## TCP Keepalive

If nothing happens on the connection (no commands being executed) the
TCP connection is completely idle, and connections may be "stuck" on
the server if there is an issue on the client machine / network
and no "reset" was received on the server. To automatically recover
from these issues the server configures 
[TCP Keepalive](https://en.wikipedia.org/wiki/Keepalive) on the sockets.

The default values:

    tcp_keepalive_idle=360     The number of seconds before keepalive kicks in
    tcp_keepalive_interval=10  The number of seconds between each probe sent in keepalive
    tcp_keepalive_probes=3     The number of missing probes before the connection is considered dead

These settings may be tuned through ns_server (their default value may be found
in `ns_server/src/ns_config_default.erl`).

## Connection policy

ns_server configures memcached to allow up to 60k client connections, and
one may choose between two different modes of operation when all the
connections are in use.

(ns_server set aside a pool of 5k connections reserved for its own use
in addition to the 60k client connections. The system connections aren't
affected by a connection policy).

(The default values may be found in `ns_server/src/ns_config_default.erl`)

### Disconnect

The next client connecting to the system gets accepted and immediately
disconnected.

#### WARNING
One problem with the disconnect mode is that you're vulnerable to a "DoS"
attach by someone just creating sockets to the server and consuming all
the available sockets. One doesn't need to authenticate, and the server
will not reclaim the sockets.

The server should be deployed behind a firewall which only allows
connections from known hosts.

### Recycle

1% of the max number of clients is being reserved as a "free pool"
(by default 600). If the newly connected client needs to be
allocated from the free pool, one of the least recently used connections
gets disconnected the entry put back into the free pool.

If the system cannot disconnect the least recently used connections
fast enough so that the free pool becomes empty, it falls back to
the "disconnect" mode until there are available entries in the free
pool.

#### WARNING
One problem with the recycle mode is that you're vulnerable to a "DoS"
attach by someone just creating sockets (no need to authenticate) to the
server and kick out the sockets used by the application unless the server
is protected behind a firewall. (Note: the same would happen if the
applications are using all the available connections causing the
application to DoS itself)

Given that the desired behavior _is_ to recycle the connection(s)
it is hard (nearly impossible) to protect against this _AND_ also
implement the recycling of connections. The primary motivation for
adding the mode was to not become unavailable when all sockets were
"lost" due to network issues in a more timely fashion (Earlier the
TCP keepalive feature would use whatever was configured at the OS
level; which was in "hours"). With the new TCP keepalive tunables,
the system should recover from these issues a lot quicker and one
might not need the recycle mode.

## Client info

It is possible to get information about the clients connected to the
system. By default, the server does _not_ collect this information,
and it needs to be enabled with:

    curl -u <username>:<password> \
      http://<servername>:8091/pools/default/settings/memcached/global \
      -d max_client_connection_details=<number>

ex:

    curl -u Administrator:password \
         http://localhost:8091/pools/default/settings/memcached/global \
         -d max_client_connection_details=20

The information may then be fetched by invoking the following command:


    $ echo password | ./mcstat --user Administrator \
                               --password - \
                               --tls \
                               --json=pretty \
                               client_connection_details

Which would produce an output looking something like this:

    {
        "127.0.0.1": {
            "current": 19,
            "disconnect": 0,
            "last_used": "16 s",
            "total": 37
        },
        "172.17.0.1": {
            "current": 0,
            "disconnect": 0,
            "last_used": "100 s",
            "total": 3
        },
        "192.168.86.218": {
            "current": 1,
            "disconnect": 0,
            "last_used": "148 ms",
            "total": 8
        }
    }

## Connection information

The server provides a privileged statistic named `connections` which
returns a JSON representation of each connection currently to the node.
The format of the JSON is _NOT_ a committed interface which means that
it may change without notice. A lot of the information would only be
useful for developers, but there are a few fields one may want to
look to get useful information.

    bucket_index - This is the bucket the client is connected to.
                   One may map this to a name by looking at the
                   index field returned in "bucket_details" stat

    "last_used"  - This is a text field with the duration since
                   the last time a command was executed on the
                   connection.

    "user"       - This is a JSON object containing the user the
                   connection was authenticated as. A username
                   starting with '@' represents an internal user.

By combining these fields, you may easily build up a map like (mine isn't very
interesting as I used the output from an idle cluster):

    Bucket: @no bucket@ 9 connections and the oldest has been idle for 16m:23s
        {"domain":"local","name":"<ud>Administrator</ud>"}: 1 connections. longest idle: 11 ms
        {"domain":"local","name":"@projector"}: 5 connections. longest idle: 15m:58s
        {"domain":"local","name":"@ns_server"}: 3 connections. longest idle: 16m:23s
    Bucket: travel-sample 5 connections and the oldest has been idle for 15m:41s
        {"domain":"local","name":"@ns_server"}: 5 connections. longest idle: 15m:41s
    Bucket: beer-sample 6 connections and the oldest has been idle for 15m:32s
        {"domain":"local","name":"@ns_server"}: 6 connections. longest idle: 15m:32s
    Bucket: gamesim-sample 6 connections and the oldest has been idle for 15m:27s
        {"domain":"local","name":"@ns_server"}: 6 connections. longest idle: 15m:27s
