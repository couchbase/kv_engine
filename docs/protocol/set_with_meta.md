
##Set With Meta (setwithmeta) v4.6

The set with meta command is used to set data and metadata for a key. The
minimum meta data passed is CAS, revision sequence number, flags and expiration.
The command can also include an extended meta data section which allows for many
more fields to be specified.

### Conflict Resolution

A set with meta command performs conflict resolution based upon the bucket
configuration flag, `conflict_resolution_mode`. When conflict resolution is
performed the command's CAS or revision sequence number is compared against any
existing document's CAS or revision sequence number.

#### conflict_resolution_mode=lww (Last Write Wins)

Last write wins compares the incoming CAS (from Extras) with any existing
document's CAS, if the CAS doesn't determine a winner, other metadata is
compared.

```
if (command.CAS > document.CAS)
  command succeeds
else if (command.CAS == document.CAS)
  // Check the RevSeqno
  if (command.RevSeqno > document.RevSeqno)
    command succeeds
  else if (command.RevSeqno == document.RevSeqno)
    // Check the expiry time
    if (command.Expiry > document.Expiry)
      command succeeds
    else if (command.Expiry == document.Expiry)
      // Finally check flags
      if (command.Flags < document.Flags)
        command succeeds

command fails
```

#### conflict_resolution_mode=seqno (Revision Seqno or "Most Writes Wins")

Revision seqno compares the incoming RevSeqno with any existing document's
RevSeqno. If the RevSeqno doesn't determine a winner, other metadata is
compared.

```
if (command.RevSeqno > document.RevSeqno)
  command succeeds
else if (command.RevSeqno == document.RevSeqno)
  // Check the CAS
  if (command.CAS > document.CAS)
    command succeeds
  else if (command.CAS == document.CAS)
    // Check the expiry time
    if (command.Expiry > document.Expiry)
      command succeeds
    else if (command.Expiry == document.Expiry)
      // Finally check flags
      if (command.Flags < document.Flags)
        command succeeds

command fails
```

All set_with_meta requests:

* Must have extras
* Must have a key
* Must have a value

####Binary Implementation

    Set With Meta Binary Request

    Byte/     0       |       1       |       2       |       3       |
       /              |               |               |               |
      |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
      +---------------+---------------+---------------+---------------+
     0|       80      |       A2      |       00      |       05      |
      +---------------+---------------+---------------+---------------+
     4|       1A      |       00      |       00      |       03      |
      +---------------+---------------+---------------+---------------+
     8|       00      |       00      |       00      |       26      |
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
    56|       6B      |       65      |       79      |       6D      |
      +---------------+---------------+---------------+---------------+
    50|       79      |       76      |       61      |       6C      |
      +---------------+---------------+---------------+---------------+
    54|       75      |       65      |
      +---------------+---------------+

    SET_WITH_META command
    Field         (offset) (value)
    Magic         (0)    : 0x80 (Request)
    Opcode        (1)    : 0xA2 (setwithmeta)
    Key length    (2,3)  : 0x0005 (5)
    Extra length  (4)    : 0x1E (30)
    Data type     (5)    : 0x00
    VBucket       (6,7)  : 0x0003 (3)
    Total body    (8-11) : 0x0000002A (3)
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
    Value         (59-65): myvalue
    Note: Extended Meta would appear after the value.

    Set With Meta Binary Response

    Byte/     0       |       1       |       2       |       3       |
       /              |               |               |               |
      |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
      +---------------+---------------+---------------+---------------+
     0|       81      |       A2      |       00      |       00      |
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

    SET_WITH_META command
    Field        (offset) (value)
    Magic        (0)    : 0x81 (Response)
    Opcode       (1)    : 0xA2 (setwithmeta)
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

The command will perform no conflict resolution - the incoming set will always
occur.

#### FORCE_ACCEPT_WITH_META_OPS

All Last Write Wins buckets require that the client sets the force flag.
Revision seqno buckets will reject the command if this flag is set.

#### REGENERATE_CAS

This flag must be specified in conjunction with `SKIP_CONFLICT_RESOLUTION_FLAG`.
When this flag is set the CAS of the document is regenerated by the server.

###Extended Meta Data Section

The extended meta data section is used to send extra meta data for a particular
mutation. This section should come at the very end, after the value. Its length
should be set in the "Meta length" field. A length of 0 or the omission of the
field means that there is no extended meta data section.

####Verison 1 (0x01)

In this version the extended meta data section has the following format:

    | version | id_1 | len_1 | field_1 | ... | id_n | len_n | field_n |

Here,
* version: 1B
* id_n: 1B
* len_n: 2B
* field_n: "len_n"B

**Meta Data IDs:**

* 0x01 - adjusted time (from 4.6 the server ignores this).
* 0x02 - conflict resolution mode (from 4.6 the server ignores this).

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

If the server is permenently out of memory

**PROTOCOL_BINARY_RESPONSE_ETMPFAIL (0x86)**

If the server is currently warming up or we are temporarily out of memory.
