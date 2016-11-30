
##Delete With Meta (delwithmeta) v4.6

The delete with meta command is used to delete data with metadata for a key. The
minimum meta data passed is CAS, revision sequence number, flags and expiration.
The command can also include an extended meta data section which allows for many
more fields to be specified.

The request:

* Must have extras
* Must have key

### Conflict Resolution

A delete with meta command performs conflict resolution based upon the bucket
configuration flag, `conflict_resolution_mode`. When conflict resolution is
performed the command's CAS or revision sequence number is compared against any
existing document's CAS or revision sequence number.

#### conflict_resolution_mode=lww (Last Write Wins)

Last write wins compares the incoming CAS (from Extras) with any existing
document's CAS, if the CAS doesn't determine a winner, RevSeqno is compared.

```
if (command.CAS > document.CAS)
  command succeeds
else if (command.CAS == document.CAS)
  // Check the RevSeqno
  if (command.RevSeqno > document.RevSeqno)
    command succeeds

command fails
```

#### conflict_resolution_mode=seqno (Revision Seqno or "Most Writes Wins")

Revision seqno compares the incoming RevSeqno with any existing document's
RevSeqno. If the RevSeqno doesn't determine a winner, CAS is compared.

```
if (command.RevSeqno > document.RevSeqno)
  command succeeds
else if (command.RevSeqno == document.RevSeqno)
  // Check the CAS
  if (command.CAS > document.CAS)
    command succeeds

command fails
```

If the compared values are equal


####Binary Implementation

    Delete With Meta Binary Request

    Byte/     0       |       1       |       2       |       3       |
       /              |               |               |               |
      |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
      +---------------+---------------+---------------+---------------+
     0|       80      |       A8      |       00      |       05      |
      +---------------+---------------+---------------+---------------+
     4|       1A      |       00      |       00      |       03      |
      +---------------+---------------+---------------+---------------+
     8|       00      |       00      |       00      |       1F      |
      +---------------+---------------+---------------+---------------+
    12|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
    16|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
    20|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
    24|       00      |       00      |       00      |       02      |
      +---------------+---------------+---------------+---------------+
    28|       00      |       00      |       00      |       07      |
      +---------------+---------------+---------------+---------------+
    32|       00      |       00      |       00      |       0A      |
      +---------------+---------------+---------------+---------------+
    36|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
    40|       00      |       00      |       00      |       14      |
      +---------------+---------------+---------------+---------------+
    44|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
    48|       00      |       00      |       00      |       1E      |
      +---------------+---------------+---------------+---------------+
    52|       00      |       00      |       6D      |       79      |
      +---------------+---------------+---------------+---------------+
    56|       6B      |       65      |       79      |
      +---------------+---------------+---------------+

    DEL_WITH_META command
    Field         (offset) (value)
    Magic         (0)    : 0x80 (Request)
    Opcode        (1)    : 0xA8 (Delete With Meta)
    Key length    (2,3)  : 0x0005 (5)
    Extra length  (4)    : 0x1A (26)
    Data type     (5)    : 0x00
    VBucket       (6,7)  : 0x0003 (3)
    Total body    (8-11) : 0x0000001F (31)
    Opaque        (12-15): 0x00000000
    CAS           (16-23): 0x0000000000000000
    Extras               :
      Options     (24-27): 0x00000002 (FORCE_ACCEPT_WITH_META_OPS is set)
      Flags       (28-31): 0x00000007 (7)
      Expiration  (32-35): 0x0000000A (10)
      RevSeqno    (36-43): 0x0000000000000014 (20)
      Cas         (44-51): 0x000000000000001E (30)
      Meta length (52,53): 0x0000 (0)
    Key           (54-58): mykey

    Delete With Meta Binary Response

    Byte/     0       |       1       |       2       |       3       |
       /              |               |               |               |
      |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
      +---------------+---------------+---------------+---------------+
     0|       81      |       A8      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
     4|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
     8|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
    12|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
    16|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
    20|       00      |       00      |       00      |       01      |
      +---------------+---------------+---------------+---------------+

    DEL_WITH_META command
    Field        (offset) (value)
    Magic        (0)    : 0x81 (Response)
    Opcode       (1)    : 0xA8 (Delete With Meta)
    Key length   (2,3)  : 0x0000
    Extra length (4)    : 0x00
    Data type    (5)    : 0x00
    Status       (6,7)  : 0x0000 (0)
    Total body   (8-11) : 0x00000000
    Opaque       (12-15): 0x00000000
    CAS          (16-23): 0x0000000000000001 (1)

###Extras
The following fields are required by the command:
* Flags
* Expiration
* RevSeqno
* Cas

The following are optional fields:
* Options
* Meta length

Thus valid values for "Extra length" are:

* 24 - The required fields
* 26 - The required fields and the "Meta length" field
* 28 - The required fields and the "Options" field
* 30 - The required fields, "Options" and the "Meta length" field

Any other "Extra length" values will result in `PROTOCOL_BINARY_RESPONSE_EINVAL`

###Options

Set with meta supports a number of options which change the behaviour of the
command.

* `SKIP_CONFLICT_RESOLUTION_FLAG 0x01`
* `FORCE_ACCEPT_WITH_META_OPS 0x02`
* `REGENERATE_CAS 0x04`

The options are encoded in the extras as a 4-byte value. Having no options
encoded is valid, but certain configurations, like last-write-wins require
options.

#### SKIP_CONFLICT_RESOLUTION_FLAG

The command will perform no conflict resolution - the incoming delete will
always occur.

#### FORCE_ACCEPT_WITH_META_OPS

All Last Write Wins buckets require that the client sets the force flag.
Revision seqno buckets will reject the command if this flag is set.

#### REGENERATE_CAS

This flag must be specified in conjunction with `SKIP_CONFLICT_RESOLUTION_FLAG`.
When this flag is set the CAS of the document is regenerated by the server.

###Extended Meta Data Section

The extended meta data section is used to send extra meta data for a particular
mutation. This section should come at the very end, after the key. Its length
should be set in the "Meta length" field. A length of 0 or the omission of the
"Meta length" field means that there is no extended meta data section.

####Verison 1 (0x01)

In this version the extended meta data section has the following format:

    | version | id_1 | len_1 | field_1 | ... | id_n | len_n | field_n |

Here,
* version: 1B
* id_n: 1B
* len_n: 2B
* field_n: "len_n"B

**Meta Data IDs:**

* 0x01 - adjusted time. (4.6 ignores this).
* 0x02 - conflict resolution mode. (4.6 ignores this).

###Errors

**PROTOCOL_BINARY_RESPONSE_KEY_ENOENT (0x01)**

If a the key does not exist.

**PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS (0x02)**

Failed conflict resolution.

**PROTOCOL_BINARY_RESPONSE_EINVAL (0x04)**

If data in this packet is malformed or incomplete then this error is returned.

**PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET (0x07)**

If the vbucket does not exist.

**PROTOCOL_BINARY_RESPONSE_ENOMEM (0x82)**

If the server is permanently out of memory

**PROTOCOL_BINARY_RESPONSE_ETMPFAIL (0x86)**

If the server is currently warming up or we are temporarily out of memory.
