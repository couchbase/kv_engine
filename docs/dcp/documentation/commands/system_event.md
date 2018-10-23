### System Event (0x5f) - Draft - subject to change

Tells the consumer that the message contains a system event. The system event message
encodes event information that relates to the users data, but is not necessarily something
they can control.

The primary use-case for system events is collections, and thus the following system events can occur:

* Collection creation.
* Collection deletion.
* Collection separator has been changed.

The extra data encodes an event identifier to indicate which event has occurred.

* 0 - A collection has been created
* 1 - A collection has been deleted
* 2 - The collections separator has changed

The request:
* Must have extras
* Must have key
* May have value

Extra looks like:

     Byte/     0       |       1       |       2       |       3       |
        /              |               |               |               |
       |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
       +---------------+---------------+---------------+---------------+
      0| by_seqno ...                                                  |
       +---------------+---------------+---------------+---------------+
      4| ... by_seqno                                                  |
       +---------------+---------------+---------------+---------------+
      8| event                                                         |
       +---------------+---------------+---------------+---------------+
       Total 12 bytes

The consumer should not send a reply to this command.

The following example shows the breakdown of the message, the example message is showing
a system event for the creation of "mycollection" with a revision of 2. This would occur
because the node has been told to create "mycollection" from a JSON manifest such as:

```
{"revision":2, "separator":"::", "collections":["mycollection"]}
```

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x80          | 0x5f          | 0x00          | 0x05          |
        +---------------+---------------+---------------+---------------+
       4| 0x0c          | 0x00          | 0x02          | 0x10          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x29          |
        +---------------+---------------+---------------+---------------+
      12| 0x00          | 0x00          | 0x12          | 0x10          |
        +---------------+---------------+---------------+---------------+
      16| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      20| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      24| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      28| 0x00          | 0x00          | 0x00          | 0x04          |
        +---------------+---------------+---------------+---------------+
      32| 0x00          | 0x00          | 0x00          | 0x01          |
        +---------------+---------------+---------------+---------------+
      36| 0x00 ('m')    | 0x00 ('y')    | 0x00 ('c')    | 0x00 ('o')    |
        +---------------+---------------+---------------+---------------+
      40| 0x00 ('l')    | 0x00 ('l')    | 0x00 ('e')    | 0x00 ('c')    |
        +---------------+---------------+---------------+---------------+
      44| 0x00 ('t')    | 0x00 ('i')    | 0x00 ('o')    | 0x00 ('n')    |
        +---------------+---------------+---------------+---------------+
      48| 0x00          | 0x00          | 0x00          | 0x02          |
        +---------------+---------------+---------------+---------------+
          DCP_SYSTEM_EVENT command
    Field        (offset) (value)
    Magic        (0)    : 0x80
    Opcode       (1)    : 0x57
    Key length   (2,3)  : 0x0005
    Extra length (4)    : 0x0c
    Data type    (5)    : 0x00
    Vbucket      (6,7)  : 0x0210
    Total body   (8-11) : 0x00000029
    Opaque       (12-15): 0x00001210
    CAS          (16-23): 0x0000000000000000
      by seqno   (24-31): 0x0000000000000004
      event      (32-35): 0x00000001
    Key          (36-47): mycollection
    Value        (48-51): 0x00000002

### Returns

This message will not return a response unless an error occurs.

### Errors

**PROTOCOL_BINARY_RESPONSE_KEY_ENOENT (0x01)**

If a stream does not exist for the vbucket specfied on this connection.

**PROTOCOL_BINARY_RESPONSE_EINVAL (0x04)**

If data in this packet is malformed or incomplete then this error is returned.

**PROTOCOL_BINARY_RESPONSE_ERANGE (0x22)**

If the by sequence number is lower than the current by sequence number contained on the consumer. The by sequence number should always be greater than the one the consumer currently has so if the by sequence number in the message is less than or equal to the by sequence number on the consumer this error is returned.

**(Disconnect)**

If this message is sent to a connection that is not a consumer.
