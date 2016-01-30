# Overview

TAP provides a mechanism to observe from the outside data changes
going on within a memcached server. TAP is the predecessor of DCP
which was introduced in Couchbase Server 3.0.

## Caution

The TAP API is an internal Couchbase Server API, which was used
by much of the core functionality of Couchbase Server up to 3.x.
TAP was never supported as an API for external use, since usage
of TAP without a complete understanding of the
interface and its server side implementation would most likely have a
severe impact on memory and IO on cluster nodes.

## Please Note

TAP is only implemented for Couchbase bucket types.

# Use Cases

TAP was designed to be a building block for new types of things that
would like to react to changes within a memcached server without
having to actually modify memcached itself.

## Replication

One simple use case is replication.

Upon initial connect, a client can ask for all existing data within a
server as well as to be notified as values change.

We receive all data related to each item that's being set, so just
replaying that data on another node makes replication an easy
exercise.

## Observation

Requesting a TAP stream of only future changes makes it very easy to
see the types of things that are changing within your memcached
instance.

## Secondary Layer Cache Invalidation

If you have frontends that are performing their own cache, requesting
a TAP stream of future changes is useful for invalidating items stored
within this cache.

## External Indexing

A TAP stream pointed at an index server (e.g. sphinx or solr) will
send all data changes to the index allowing for an always-up-to-date
full-text search index of your data.

## vbucket transition

For the purposes of vbucket transfer between nodes, a new type of TAP
request can be created that is every item stored in a vbucket (or set
of vbuckets) that is both existing and changing, but with the ability
to terminate the stream and cut-over ownership of the vbucket once the
last item is enqueued.

# Protocol

A TAP session begins by initiating a command from the client which
tells the server what we're interested in receiving and then the
server begins sending /client/ commands back across the connection
until the connection is terminated.

## Initial Base Message

A TAP stream begins with a binary protocol message with the ID of `0x40` .

The packet's key may specify a unique client identifier that can be
used to allow reconnects (resumable at the server's discretion).

A simple base message from a client referring to itself as "node1"
would appear as follows.

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0|       80      |    40 ('@')   |       00      |       05      |
        +---------------+---------------+---------------+---------------+
       4|       04      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
       8|       00      |       00      |       00      |       09      |
        +---------------+---------------+---------------+---------------+
      12|       00      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      16|       00      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      20|       00      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      24|       00      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      28|    6e ('n')   |    6f ('o')   |    64 ('d')   |    65 ('e')   |
        +---------------+---------------+---------------+---------------+
      32|    31 ('1')   |

    Header breakdown
    tap connect command
    Field        (offset) (value)
    Magic        (0)    : 0x80 (PROTOCOL_BINARY_REQ)
    Opcode       (1)    : 0x40
    Key length   (2,3)  : 0x0005 (5)
    Extra length (4)    : 0x04
    Data type    (5)    : 0x00
    vbucket      (6,7)  : 0x0000 (0)
    Total body   (8-11) : 0x00000009 (9)
    Opaque       (12-15): 0x00000000
    CAS          (16-23): 0x0000000000000000
    Flags        (24-27): 0x00000000
    Name         (28-32): [node1]

## Options

Additional TAP options may be specified as a 32-bit flags specifying
options. The flags will appear in the "extras" section of the request
packet. If omitted, it is assumed that all flags are 0.

Options may or may not have values. For options that do, the values
will appear in the body in the order they're defined (LSB \-> MSB).

### Backfill

`BACKFILL` (`0x01`) contains a single 64-bit body that represents
the oldest entry (from epoch) you're interested in. Specifying a time
in the future (for the server you are connecting to), will cause it to
start streaming only current changes.

An example TAP stream request that specifies a backfill of -1
(meaning future only) would look like this:

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0|       80      |    40 ('@')   |       00      |       05      |
        +---------------+---------------+---------------+---------------+
       4|       04      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
       8|       00      |       00      |       00      |       11      |
        +---------------+---------------+---------------+---------------+
      12|       00      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      16|       00      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      20|       00      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      24|       00      |       00      |       00      |       01      |
        +---------------+---------------+---------------+---------------+
      28|    6e ('n')   |    6f ('o')   |    64 ('d')   |    65 ('e')   |
        +---------------+---------------+---------------+---------------+
      32|    31 ('1')   |       ff      |       ff      |       ff      |
        +---------------+---------------+---------------+---------------+
      36|       ff      |       ff      |       ff      |       ff      |
        +---------------+---------------+---------------+---------------+
      40|       ff      |

    Header breakdown
    tap connect command
    Field        (offset) (value)
    Magic        (0)    : 0x80 (PROTOCOL_BINARY_REQ)
    Opcode       (1)    : 0x40
    Key length   (2,3)  : 0x0005 (5)
    Extra length (4)    : 0x04
    Data type    (5)    : 0x00
    vbucket      (6,7)  : 0x0000 (0)
    Total body   (8-11) : 0x00000011 (17)
    Opaque       (12-15): 0x00000000
    CAS          (16-23): 0x0000000000000000
    Flags        (24-27): 0x00000001
      backfill
    Name         (28-32): [node1]
    Backfill date(33-40): 0xffffffffffffffff (-1)

#### Dump

`DUMP` (`0x02`) contains no extra body and will cause the server to
transmit only existing items and disconnect after all of the items
have been transmitted.

An example TAP stream request that specifies only dumping existing
records would look like this:

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0|       80      |    40 ('@')   |       00      |       05      |
        +---------------+---------------+---------------+---------------+
       4|       04      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
       8|       00      |       00      |       00      |       09      |
        +---------------+---------------+---------------+---------------+
      12|       00      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      16|       00      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      20|       00      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      24|       00      |       00      |       00      |       02      |
        +---------------+---------------+---------------+---------------+
      28|    6e ('n')   |    6f ('o')   |    64 ('d')   |    65 ('e')   |
        +---------------+---------------+---------------+---------------+
      32|    31 ('1')   |

    Header breakdown
    tap connect command
    Field        (offset) (value)
    Magic        (0)    : 0x80 (PROTOCOL_BINARY_REQ)
    Opcode       (1)    : 0x40
    Key length   (2,3)  : 0x0005 (5)
    Extra length (4)    : 0x04
    Data type    (5)    : 0x00
    vbucket      (6,7)  : 0x0000 (0)
    Total body   (8-11) : 0x00000009 (9)
    Opaque       (12-15): 0x00000000
    CAS          (16-23): 0x0000000000000000
    Flags        (24-27): 0x00000002
      dump
    Name         (28-32): [node1]

#### Specify vbuckets

`LIST_VBUCKETS` (`0x04`) contains a list of vbuckets and will cause
the server to transmit only notifications about changes in the
specified vbuckets.

An example TAP stream request that specifies 3 vbuckets (vbucket 0, 1
and 2) would look like this:

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0|       80      |    40 ('@')   |       00      |       05      |
        +---------------+---------------+---------------+---------------+
       4|       04      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
       8|       00      |       00      |       00      |       11      |
        +---------------+---------------+---------------+---------------+
      12|       00      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      16|       00      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      20|       00      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      24|       00      |       00      |       00      |       04      |
        +---------------+---------------+---------------+---------------+
      28|    6e ('n')   |    6f ('o')   |    64 ('d')   |    65 ('e')   |
        +---------------+---------------+---------------+---------------+
      32|    31 ('1')   |       00      |       03      |       00      |
        +---------------+---------------+---------------+---------------+
      36|       00      |       00      |       01      |       00      |
        +---------------+---------------+---------------+---------------+
      40|       02      |

    Header breakdown
    tap connect command
    Field        (offset) (value)
    Magic        (0)    : 0x80 (PROTOCOL_BINARY_REQ)
    Opcode       (1)    : 0x40
    Key length   (2,3)  : 0x0005 (5)
    Extra length (4)    : 0x04
    Data type    (5)    : 0x00
    vbucket      (6,7)  : 0x0000 (0)
    Total body   (8-11) : 0x00000011 (17)
    Opaque       (12-15): 0x00000000
    CAS          (16-23): 0x0000000000000000
    Flags        (24-27): 0x00000004
      list vbuckets
    Name         (28-32): [node1]
    VBucket list (33-40):
        # listed (33-34): 0x0003 (3)
        vbucket  (35-36): 0x0000 (0)
        vbucket  (37-38): 0x0001 (1)
        vbucket  (39-40): 0x0002 (2)

#### Transfer vbucket ownership

`TAKEOVER_VBUCKETS` (`0x08`) contains no extra data and is used
together with `LIST_VBUCKETS` to transfer the ownership from the
server at the end of the dump of the server.

An example TAP stream request that specifies 3 vbuckets (vbucket 0, 1
and 2) would look like this:

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0|       80      |    40 ('@')   |       00      |       05      |
        +---------------+---------------+---------------+---------------+
       4|       04      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
       8|       00      |       00      |       00      |       11      |
        +---------------+---------------+---------------+---------------+
      12|       00      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      16|       00      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      20|       00      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      24|       00      |       00      |       00      |       0c      |
        +---------------+---------------+---------------+---------------+
      28|    6e ('n')   |    6f ('o')   |    64 ('d')   |    65 ('e')   |
        +---------------+---------------+---------------+---------------+
      32|    31 ('1')   |       00      |       03      |       00      |
        +---------------+---------------+---------------+---------------+
      36|       00      |       00      |       01      |       00      |
        +---------------+---------------+---------------+---------------+
      40|       02      |

    Header breakdown
    tap connect command
    Field        (offset) (value)
    Magic        (0)    : 0x80 (PROTOCOL_BINARY_REQ)
    Opcode       (1)    : 0x40
    Key length   (2,3)  : 0x0005 (5)
    Extra length (4)    : 0x04
    Data type    (5)    : 0x00
    vbucket      (6,7)  : 0x0000 (0)
    Total body   (8-11) : 0x00000011 (17)
    Opaque       (12-15): 0x00000000
    CAS          (16-23): 0x0000000000000000
    Flags        (24-27): 0x0000000c
      list vbuckets, takeover vbuckets
    Name         (28-32): [node1]
    VBucket list (33-40):
        # listed (33-34): 0x0003 (3)
        vbucket  (35-36): 0x0000 (0)
        vbucket  (37-38): 0x0001 (1)
        vbucket  (39-40): 0x0002 (2)

#### ACK

`SUPPORT_ACK` (`0x10`) contains no extra data and is to notify the
TAP server that we (the consumer) support sending an ack message back
to the TAP server whenever the TAP server asks for an ack.

An example TAP stream request that specifies that we support acks
would look like this:

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0|       80      |    40 ('@')   |       00      |       05      |
        +---------------+---------------+---------------+---------------+
       4|       04      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
       8|       00      |       00      |       00      |       09      |
        +---------------+---------------+---------------+---------------+
      12|       00      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      16|       00      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      20|       00      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      24|       00      |       00      |       00      |       10      |
        +---------------+---------------+---------------+---------------+
      28|    6e ('n')   |    6f ('o')   |    64 ('d')   |    65 ('e')   |
        +---------------+---------------+---------------+---------------+
      32|    31 ('1')   |

    Header breakdown
    tap connect command
    Field        (offset) (value)
    Magic        (0)    : 0x80 (PROTOCOL_BINARY_REQ)
    Opcode       (1)    : 0x40
    Key length   (2,3)  : 0x0005 (5)
    Extra length (4)    : 0x04
    Data type    (5)    : 0x00
    vbucket      (6,7)  : 0x0000 (0)
    Total body   (8-11) : 0x00000009 (9)
    Opaque       (12-15): 0x00000000
    CAS          (16-23): 0x0000000000000000
    Flags        (24-27): 0x00000010
      support ack
    Name         (28-32): [node1]


#### Keys only

`KEYS_ONLY` (`0x20`) contains no extra data and is to notify the
TAP server that we (the consumer) don't care about the values so we
would _prefer_ that the TAP server didn't send them. The server _may_
however decide to ignore your request.

An example TAP stream request that specifies that we would prefer the
just the keys would look like this:

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0|       80      |    40 ('@')   |       00      |       05      |
        +---------------+---------------+---------------+---------------+
       4|       04      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
       8|       00      |       00      |       00      |       09      |
        +---------------+---------------+---------------+---------------+
      12|       00      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      16|       00      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      20|       00      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      24|       00      |       00      |       00      |       20      |
        +---------------+---------------+---------------+---------------+
      28|    6e ('n')   |    6f ('o')   |    64 ('d')   |    65 ('e')   |
        +---------------+---------------+---------------+---------------+
      32|    31 ('1')   |

    Header breakdown
    tap connect command
    Field        (offset) (value)
    Magic        (0)    : 0x80 (PROTOCOL_BINARY_REQ)
    Opcode       (1)    : 0x40
    Key length   (2,3)  : 0x0005 (5)
    Extra length (4)    : 0x04
    Data type    (5)    : 0x00
    vbucket      (6,7)  : 0x0000 (0)
    Total body   (8-11) : 0x00000009 (9)
    Opaque       (12-15): 0x00000000
    CAS          (16-23): 0x0000000000000000
    Flags        (24-27): 0x00000020
      request keys only
    Name         (28-32): [node1]

#### Registered Client

`REGISTERED_CLIENT` (`0x80`) contains no extra data and is to
notify the TAP server that we (the consumer) would like to create a
TAP connection where the TAP connection will exist until we deregister
the TAP client. Deregistering a TAP client can only be done through
the deregister TAP client command through the memcached front end.

*Note: A registered TAP connection that is not being read from will
not allow closed checkpoints in the memcached engine to be purged from
memory causing out of memory errors. Use of a registered TAP
connection is only recommended for expert Couchbase users.*

An example TAP stream request that specifies that we would like a
registered TAP client would look like this:

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0|       80      |    40 ('@')   |       00      |       05      |
        +---------------+---------------+---------------+---------------+
       4|       04      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
       8|       00      |       00      |       00      |       09      |
        +---------------+---------------+---------------+---------------+
      12|       00      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      16|       00      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      20|       00      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      24|       00      |       00      |       00      |       80      |
        +---------------+---------------+---------------+---------------+
      28|    6e ('n')   |    6f ('o')   |    64 ('d')   |    65 ('e')   |
        +---------------+---------------+---------------+---------------+
      32|    31 ('1')   |

    Header breakdown
    tap connect command
    Field        (offset) (value)
    Magic        (0)    : 0x80 (PROTOCOL_BINARY_REQ)
    Opcode       (1)    : 0x40
    Key length   (2,3)  : 0x0005 (5)
    Extra length (4)    : 0x04
    Data type    (5)    : 0x00
    vbucket      (6,7)  : 0x0000 (0)
    Total body   (8-11) : 0x00000009 (9)
    Opaque       (12-15): 0x00000000
    CAS          (16-23): 0x0000000000000000
    Flags        (24-27): 0x00000080
      request keys only
    Name         (28-32): [node1]

#### Complex example

You may of course mix all of the fields:

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0|       80      |    40 ('@')   |       00      |       05      |
        +---------------+---------------+---------------+---------------+
       4|       04      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
       8|       00      |       00      |       00      |       1d      |
        +---------------+---------------+---------------+---------------+
      12|       00      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      16|       00      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      20|       00      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      24|       00      |       00      |       00      |    35 ('5')   |
        +---------------+---------------+---------------+---------------+
      28|    6e ('n')   |    6f ('o')   |    64 ('d')   |    65 ('e')   |
        +---------------+---------------+---------------+---------------+
      32|    31 ('1')   |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      36|       00      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      40|       05      |       00      |       05      |       00      |
        +---------------+---------------+---------------+---------------+
      44|       00      |       00      |       01      |       00      |
        +---------------+---------------+---------------+---------------+
      48|       02      |       00      |       03      |       00      |
        +---------------+---------------+---------------+---------------+
      52|       04      |

    Header breakdown
    tap connect command
    Field        (offset) (value)
    Magic        (0)    : 0x80 (PROTOCOL_BINARY_REQ)
    Opcode       (1)    : 0x40
    Key length   (2,3)  : 0x0005 (5)
    Extra length (4)    : 0x04
    Data type    (5)    : 0x00
    vbucket      (6,7)  : 0x0000 (0)
    Total body   (8-11) : 0x0000001d (29)
    Opaque       (12-15): 0x00000000
    CAS          (16-23): 0x0000000000000000
    Flags        (24-27): 0x00000035
      backfill, list vbuckets, support ack, request keys only
    Name         (28-32): [node1]
    Backfill date(33-40): 0x0000000000000005 (5)
    VBucket list (41-52):
        # listed (41-42): 0x0005 (5)
        vbucket  (43-44): 0x0000 (0)
        vbucket  (45-46): 0x0001 (1)
        vbucket  (47-48): 0x0002 (2)
        vbucket  (49-50): 0x0003 (3)
        vbucket  (51-52): 0x0004 (4)

### Response Commands

After initiating TAP, a series of responses will begin streaming
commands back to the caller. These commands are similar to, but not
necessarily the same as existing commands.

In particular, each command includes a section of engine-specific data
as well as a TTL to avoid replication loops.

*todo: describe extended formats*

#### Mutation

All mutation events arrive as `TAP_MUTATION` (`0x41`) events. These
are conceptualy similar to set commands. A mutation event for key
"mykey" with a value of "value" (no flags or expiry) and a TTL of 255
would look like:

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0|       80      |    41 ('A')   |       00      |       05      |
        +---------------+---------------+---------------+---------------+
       4|       10      |       00      |       00      |    66 ('f')   |
        +---------------+---------------+---------------+---------------+
       8|       00      |       00      |       00      |       1a      |
        +---------------+---------------+---------------+---------------+
      12|       00      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      16|       00      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      20|       00      |       00      |       00      |       03      |
        +---------------+---------------+---------------+---------------+
      24|       00      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      28|       ff      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      32|       00      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      36|       00      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      40|    6d ('m')   |    79 ('y')   |    6b ('k')   |    65 ('e')   |
        +---------------+---------------+---------------+---------------+
      44|    79 ('y')   |    76 ('v')   |    61 ('a')   |    6c ('l')   |
        +---------------+---------------+---------------+---------------+
      48|    75 ('u')   |    65 ('e')   |

    Header breakdown
    Field        (offset) (value)
    Magic        (0)    : 0x80 (PROTOCOL_BINARY_REQ)
    Opcode       (1)    : 0x41 (tap mutation)
    Key length   (2,3)  : 0x0005 (5)
    Extra length (4)    : 0x10
    Data type    (5)    : 0x00
    vbucket      (6,7)  : 0x0066 (102)
    Total body   (8-11) : 0x0000001a (26)
    Opaque       (12-15): 0x00000000 (0)
    CAS          (16-23): 0x0000000000000003 (3)
    Engine priv. (24-25): 0x0000 (0)
    Flags        (26-27): 0x0000
    TTL             (28): ff
    Reserved        (29): 00
    Reserved        (30): 00
    Reserved        (31): 00
    Item Flags   (32-35): 0x00000000
    Item Expiry  (36-39): 0x00000000
    Key          (40-44): [mykey]
    Value        (45-49): [value]

*In Couchbase 2.0 the engine private field is used to specify the
length of any meta data sent along with the key and value. This
meta-data will be placed between the Item Expiry field and the Key
field.*

#### Delete

`TAP_DELETE` (`0x42`) may contain an engine specific section. A
typical packet for deletion of "mykey" with a TTL of 255 without any
engine specific data would look like this:

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0|       80      |    42 ('B')   |       00      |       05      |
        +---------------+---------------+---------------+---------------+
       4|       08      |       00      |       00      |    66 ('f')   |
        +---------------+---------------+---------------+---------------+
       8|       00      |       00      |       00      |       0d      |
        +---------------+---------------+---------------+---------------+
      12|       00      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      16|       00      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      20|       00      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      24|       00      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      28|       ff      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      32|    6d ('m')   |    79 ('y')   |    6b ('k')   |    65 ('e')   |
        +---------------+---------------+---------------+---------------+
      36|    79 ('y')   |

    Header breakdown
    Field        (offset) (value)
    Magic        (0)    : 0x80 (PROTOCOL_BINARY_REQ)
    Opcode       (1)    : 0x42 (tap delete)
    Key length   (2,3)  : 0x0005 (5)
    Extra length (4)    : 0x08
    Data type    (5)    : 0x00
    vbucket      (6,7)  : 0x0066 (102)
    Total body   (8-11) : 0x0000000d (13)
    Opaque       (12-15): 0x00000000 (0)
    CAS          (16-23): 0x0000000000000000 (0)
    Engine priv. (24-25): 0x0000 (0)
    Flags        (26-27): 0x0000
    TTL             (28): ff
    Reserved        (29): 00
    Reserved        (30): 00
    Reserved        (31): 00
    Key          (32-36): [mykey]

*In Couchbase 2.0 the engine private field is used to specify the
length of any meta data sent along with the key and value. This
meta-data will be placed between the Item Expiry field and the Key
field.*

#### Flush

`TAP_FLUSH` (`0x43`) may contain an engine specific section. This
message is used to replicate a flush_all command. A typical flush
packet with a TTL of 255 without any engine specific data would look
like this:

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0|       80      |    43 ('C')   |       00      |       00      |
        +---------------+---------------+---------------+---------------+
       4|       08      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
       8|       00      |       00      |       00      |       08      |
        +---------------+---------------+---------------+---------------+
      12|       00      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      16|       00      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      20|       00      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      24|       00      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      28|       ff      |       00      |       00      |       00      |

    Header breakdown
    Field        (offset) (value)
    Magic        (0)    : 0x80 (PROTOCOL_BINARY_REQ)
    Opcode       (1)    : 0x43 (tap flush)
    Key length   (2,3)  : 0x0000 (0)
    Extra length (4)    : 0x08
    Data type    (5)    : 0x00
    vbucket      (6,7)  : 0x0000 (0)
    Total body   (8-11) : 0x00000008 (8)
    Opaque       (12-15): 0x00000000 (0)
    CAS          (16-23): 0x0000000000000000 (0)
    Engine priv. (24-25): 0x0000 (0)
    Flags        (26-27): 0x0000
    TTL             (28): ff
    Reserved        (29): 00
    Reserved        (30): 00
    Reserved        (31): 00


#### Opaque

The purpose of the `TAP_OPAQUE` (`0x44`) packet is for engine
writers to be able to send control data to the consumer.

A TAP opaque packet with 4 bytes of engine specific data (`0xff 0xff
0xff 0xff` for vbucket 1023 and a TTL of 255 would look like this:

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0|       80      |    44 ('D')   |       00      |       00      |
        +---------------+---------------+---------------+---------------+
       4|       08      |       00      |       03      |       ff      |
        +---------------+---------------+---------------+---------------+
       8|       00      |       00      |       00      |       0c      |
        +---------------+---------------+---------------+---------------+
      12|       00      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      16|       00      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      20|       00      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      24|       00      |       04      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      28|       ff      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      32|       ff      |       ff      |       ff      |       ff      |

    Header breakdown
    Field        (offset) (value)
    Magic        (0)    : 0x80 (PROTOCOL_BINARY_REQ)
    Opcode       (1)    : 0x44 (tap opaque)
    Key length   (2,3)  : 0x0000 (0)
    Extra length (4)    : 0x08
    Data type    (5)    : 0x00
    vbucket      (6,7)  : 0x03ff (1023)
    Total body   (8-11) : 0x0000000c (12)
    Opaque       (12-15): 0x00000000 (0)
    CAS          (16-23): 0x0000000000000000 (0)
    Engine priv. (24-25): 0x0004 (4)
    Flags        (26-27): 0x0000
    TTL               (28): ff
    Reserved          (29): 00
    Reserved          (30): 00
    Reserved          (31): 00

In an opaque message the engine private field contains control message
that are used to signal to the receiving engine that the state of the
TAP stream has changed. Below is a list of engine private control
values:

`TAP_OPAQUE_ENABLE_AUTO_NACK` (0) - When the receiving node initiates
a TAP stream with a sending node the receiver may specify that it
wants to receive acknowledgment messages from the sender. The receiver
does this by specifying an ACK flag when creating the connection. If
the sending node receives this flag it sends the
TAP_OPAQUE_ENABLE_AUTO_NACK in order to signal that the sender
supports acknowledgement messages.

`TAP_OPAQUE_INITIAL_VBUCKET_STREAM` (1) - Signals the beginning of the
backfill operation. Backfill takes place when the newest checkpoint in
the receiving node is behind the oldest checkpoint in the sending
node. Backfill takes a snapshot of both disk and memory on the sending
node and streams it to the receiving node in order to get the
receiving node up to date.

`TAP_OPAQUE_ENABLE_CHECKPOINT_SYNC` (2) - Signals to the receiving
node that the sending node supports checkpoints.

`TAP_OPAQUE_OPEN_CHECKPOINT (3)` - Signals that we are at the
beginning of an open checkpoint. This message is only sent if the TAP
stream is a registered TAP stream.

`TAP_OPAQUE_START_ONLINEUPDATE` (4) - This signal can be sent by
either the sender or receiver. It tells whatever engine that receives
it to stop persisting data and only keep things in memory.

`TAP_OPAQUE_STOP_ONLINEUPDATE` (5) -
This message can also be sent by the sender or receiver. It signals to
the receiving node that the persistence should be resumed.

`TAP_OPAQUE_REVERT_ONLINEUPDATE` (6) - This message can also be sent
by the sender or receiver. It signals to the receiving node that
everything received since the TAP_OPAQUE_START_ONLINEUPDATE should be
deleted from the engine. Note that this will remove everything
received by the engine after TAP_OPAQUE_START_ONLINEUPDATE is received
including message not send through the memcached front end as well as
other TAP streams.

`TAP_OPAQUE_CLOSE_TAP_STREAM` (7) - Sent by the
sending node to signal that the sending node is finished sending data
and is closing it's connection to the receiver.

`TAP_OPAQUE_CLOSE_BACKFILL` (8) - Signals the end of the backfill.

*Note: Online update is a feature that can be used to test cluster
usage with the addition of new data. It is not recommended for use in
normal production setting.*

#### Set vbucket

The purpose of the `TAP_VBUCKET` (`0x45`) packet is to set the
state of a virtual bucket in the consumer (this is part of vbucket
takeover)

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0|       80      |    45 ('E')   |       00      |       00      |
        +---------------+---------------+---------------+---------------+
       4|       08      |       00      |       00      |    38 ('8')   |
        +---------------+---------------+---------------+---------------+
       8|       00      |       00      |       00      |       0c      |
        +---------------+---------------+---------------+---------------+
      12|       00      |       00      |       00      |    39 ('9')   |
        +---------------+---------------+---------------+---------------+
      16|       00      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      20|       00      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      24|       00      |       04      |       00      |       01      |
        +---------------+---------------+---------------+---------------+
      28|       ff      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      32|       00      |       00      |       00      |       03      |

    Header breakdown
    Field        (offset) (value)
    Magic        (0)    : 0x80 (PROTOCOL_BINARY_REQ)
    Opcode       (1)    : 0x45 (tap vbucket set)
    Key length   (2,3)  : 0x0000 (0)
    Extra length (4)    : 0x08
    Data type    (5)    : 0x00
    vbucket      (6,7)  : 0x0038 (56)
    Total body   (8-11) : 0x0000000c (12)
    Opaque       (12-15): 0x00000039 (57)
    CAS          (16-23): 0x0000000000000000 (0)
    Engine priv. (24-25): 0x0000 (0)
    Flags        (26-27): 0x0001
      ack
    TTL             (28): ff
    Reserved        (29): 00
    Reserved        (30): 00
    Reserved        (31): 00
    VB State     (32-35): 0x00000003 (pending)

#### Checkpoint Start

The purpose of the `TAP_CHECKPOINT_START` (`0x46`) packet is to
tell the TAP consumer that we are starting a new checkpoint.

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0|       80      |    46 ('F')   |       00      |       00      |
        +---------------+---------------+---------------+---------------+
       4|       08      |       00      |       00      |    38 ('8')   |
        +---------------+---------------+---------------+---------------+
       8|       00      |       00      |       00      |       0c      |
        +---------------+---------------+---------------+---------------+
      12|       00      |       00      |       00      |    39 ('9')   |
        +---------------+---------------+---------------+---------------+
      16|       00      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      20|       00      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      24|       00      |       04      |       00      |       01      |
        +---------------+---------------+---------------+---------------+
      28|       ff      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      32|       00      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      36|       00      |       00      |       00      |       ff      |

    Header breakdown
    Field        (offset) (value)
    Magic        (0)    : 0x80 (PROTOCOL_BINARY_REQ)
    Opcode       (1)    : 0x46 (tap checkpoint start)
    Key length   (2,3)  : 0x0000 (0)
    Extra length (4)    : 0x08
    Data type    (5)    : 0x00
    vbucket      (6,7)  : 0x0038 (56)
    Total body   (8-11) : 0x0000000c (12)
    Opaque       (12-15): 0x00000039 (57)
    CAS          (16-23): 0x0000000000000000 (0)
    Engine priv. (24-25): 0x0000 (0)
    Flags        (26-27): 0x0001
      ack
    TTL             (28): ff
    Reserved        (29): 00
    Reserved        (30): 00
    Reserved        (31): 00
    VB State     (32-39): 0x000000000000000ff (checkpoint 255)

#### Checkpoint End

The purpose of the `TAP_CHECKPOINT_END` (`0x47`) packet is to tell
the TAP consumer that we are ending a checkpoint.

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0|       80      |    47 ('G')   |       00      |       00      |
        +---------------+---------------+---------------+---------------+
       4|       08      |       00      |       00      |    38 ('8')   |
        +---------------+---------------+---------------+---------------+
       8|       00      |       00      |       00      |       0c      |
        +---------------+---------------+---------------+---------------+
      12|       00      |       00      |       00      |    39 ('9')   |
        +---------------+---------------+---------------+---------------+
      16|       00      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      20|       00      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      24|       00      |       04      |       00      |       01      |
        +---------------+---------------+---------------+---------------+
      28|       ff      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      32|       00      |       00      |       00      |       00      |
        +---------------+---------------+---------------+---------------+
      36|       00      |       00      |       00      |       fa      |

    Header breakdown
    Field        (offset) (value)
    Magic        (0)    : 0x80 (PROTOCOL_BINARY_REQ)
    Opcode       (1)    : 0x46 (tap checkpoint start)
    Key length   (2,3)  : 0x0000 (0)
    Extra length (4)    : 0x08
    Data type    (5)    : 0x00
    vbucket      (6,7)  : 0x0038 (56)
    Total body   (8-11) : 0x0000000c (12)
    Opaque       (12-15): 0x00000039 (57)
    CAS          (16-23): 0x0000000000000000 (0)
    Engine priv. (24-25): 0x0000 (0)
    Flags        (26-27): 0x0001
      ack
    TTL             (28): ff
    Reserved        (29): 00
    Reserved        (30): 00
    Reserved        (31): 00
    VB State     (32-39): 0x000000000000000fa (checkpoint 250)
