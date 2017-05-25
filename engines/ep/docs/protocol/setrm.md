
##Set Returning Meta (setrm)

The setrm command is used to set data for a given key and returns back all meta data information associated with the mutation. Meta data is defined as the cas, sequence number, flags, and expiration for a given mutation.

####Binary Implementation

    Setrm Binary Request

    Byte/     0       |       1       |       2       |       3       |
       /              |               |               |               |
      |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
      +---------------+---------------+---------------+---------------+
     0|       80      |       B2      |       00      |       05      |
      +---------------+---------------+---------------+---------------+
     4|       0C      |       00      |       00      |       03      |
      +---------------+---------------+---------------+---------------+
     8|       00      |       00      |       00      |       14      |
      +---------------+---------------+---------------+---------------+
    12|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
    16|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
    20|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
    24|       00      |       00      |       00      |       01      |
      +---------------+---------------+---------------+---------------+
    28|       00      |       00      |       00      |       07      |
      +---------------+---------------+---------------+---------------+
    32|       00      |       00      |       00      |       0A      |
      +---------------+---------------+---------------+---------------+
    36|       6D      |       79      |       6B      |       65      |
      +---------------+---------------+---------------+---------------+
    40|       79      |       6D      |       79      |       76      |
      +---------------+---------------+---------------+---------------+
    44|       61      |       6C      |       75      |       65      |
      +---------------+---------------+---------------+---------------+

    Header breakdown
    Setrm command
    Field        (offset) (value)
    Magic        (0)    : 0x80 (Request)
    Opcode       (1)    : 0xB2 (retmeta)
    Key length   (2,3)  : 0x0005 (5)
    Extra length (4)    : 0x0C
    Data type    (5)    : 0x00                (field not used)
    VBucket      (6,7)  : 0x0003 (3)
    Total body   (8-11) : 0x00000014 (20)
    Opaque       (12-15): 0x00000000
    CAS          (16-23): 0x0000000000000000
    Extras              :
      Op Type    (24-27): 0x00000001 (setrm)
      Flags      (28-31): 0x00000007 (7)
      Expiration (32-35): 0x0000000A (10)
    Key          (36-40): mykey
    Value        (41-47): myvalue


    Setrm Binary Response

    Byte/     0       |       1       |       2       |       3       |
       /              |               |               |               |
      |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
      +---------------+---------------+---------------+---------------+
     0|       81      |       B2      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
     4|       10      |       00      |       00      |       03      |
      +---------------+---------------+---------------+---------------+
     8|       00      |       00      |       00      |       10      |
      +---------------+---------------+---------------+---------------+
    12|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
    16|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
    20|       00      |       00      |       00      |       A7      |
      +---------------+---------------+---------------+---------------+
    24|       00      |       00      |       00      |       07      |
      +---------------+---------------+---------------+---------------+
    28|       4E      |       82      |       A9      |       7A      |
      +---------------+---------------+---------------+---------------+
    32|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
    36|       00      |       00      |       00      |       0E      |
      +---------------+---------------+---------------+---------------+

    Header breakdown
    Setrm command
    Field        (offset) (value)
    Magic        (0)    : 0x81 (Response)
    Opcode       (1)    : 0xB2 (retmeta)
    Key length   (2,3)  : 0x0000              (field not used)
    Extra length (4)    : 0x10                (field not used)
    Data type    (5)    : 0x00                (field not used)
    VBucket      (6,7)  : 0x0003 (3)
    Total body   (8-11) : 0x00000010 (16)
    Opaque       (12-15): 0x00000000
    CAS          (16-23): 0x00000000000000A7 (167)
    Extras              :
      Flags      (24-27): 0x00000007 (7)
      Expiration (28-31): 0x4E82A97A (1317185914)
      Seq Num    (32-39): 0x000000000000000E (14)

#####Extra Fields

**Flags**

The flags field is for use by clients. This field can be used to signal that data for a specific key has certain properties. One example use for the flags field is to allow clients to serialize data.

**Expiration**

The expiration field specifies the amount of time the lock will be valid for. Specifying a time less than 2592000 seconds (30 days) will be interpreted as relative time (now + expiration). Specifying a expiration greater than 30 days will be interpreted as UNIX time (eg. time from the epoch).

The server by default also specifes a default lock time and a maximum lock time which are set at 15 and 30 seconds respectively. Any GetL command not specifying an expiration (setting expiration to 0) will obtain the default lock time. Any expiration set greater than the maximum lock time will be set to the maximum lock time.

**Sequence Number**

The sequence number is used for conflict resolution of keys that are updated concurrently in different clusters. This conflict resolution takes place when using Couchbases cross data center replication feature. The sequence number keeps track of how many times a document is mutated.

#####Returns

The setrm operation returns meta data for a given mutation that takes place in a given cluster. The information returned is the cas, sequence number, flags, and expiration. The extra fields section contains more information on these fields.

#####Errors

**PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS (0x02)**

This error is returned if the cas value specified in the request packet does not match the cas value on the server. If the cas value is specified as 0 then this error will never be returned.

**PROTOCOL_BINARY_RESPONSE_EINVAL (0x04)**

If data in this packet is malformed or incomplete then this error is returned and means that their is likely a bug in the client that sent the request.

**PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET (0x07)**

If a server recieves this command for a given vbucket and that vbucket is not contained on this server a not my vbucket error is returned.

**PROTOCOL_BINARY_RESPONSE_ENOMEM (0x82)**

This error is returned if the request cannot be completed because the server is permanently out of memory.

**PROTOCOL_BINARY_RESPONSE_ETMPFAIL (0x86)**

This error is returned if the server is overloaded and cannot complete the request. If this error is returned the client should retry the operation later.

#####Use Cases

**Client-Side Cross Datacenter Replication**

This feature can be used to allow a client to send data to another datacenter and still be able deal with potential conflicts from updates to the same key in the other datacenter.


