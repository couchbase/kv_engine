# No-Op (opcode 0x5c)

A No-Op message is sent by the Producer to the Consumer if the Producer has not sent any messages for a given interval of time. This interval is referred to as the *"noop interval"*. Upon receiving this message the Consumer is expected to respond with a No-Op response. When the Producer sends the No-Op it starts a timer and expects to see a No-Op response from the Consumer in a period equal to the *"noop interval"*. If no response is seen during this period then the Producer will disconnect. The Consumer should similarly expect to see some sort of message or a No-Op message in a period equal to twice the *"noop interval"* otherwise it should close its connection. See the page on [Dead Connection Detection](../dead-connections.md) for more details.

#### Binary Implementation

    Dcp No-Op Binary Request

    Byte/     0       |       1       |       2       |       3       |
       /              |               |               |               |
      |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
      +---------------+---------------+---------------+---------------+
     0|       80      |       5C      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
     4|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
     8|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
    12|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
    16|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
    20|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
    24|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+

    Header breakdown
    Dcp No-Op command
    Field        (offset) (value)
    Magic        (0)    : 0x80                (Request)
    Opcode       (1)    : 0x5C                (Dcp No-Op)
    Key length   (2,3)  : 0x0000              (0)
    Extra length (4)    : 0x00                (0)
    Data type    (5)    : 0x00                (Field not used)
    VBucket      (6,7)  : 0x0000              (Field not used)
    Total body   (8-11) : 0x00000000          (0)
    Opaque       (12-15): 0x00000005          (0)
    CAS          (16-23): 0x0000000000000000  (Field not used)

    Dcp No-Op Binary Response

    Byte/     0       |       1       |       2       |       3       |
       /              |               |               |               |
      |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
      +---------------+---------------+---------------+---------------+
     0|       81      |       5C      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
     4|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
     8|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
    12|       00      |       00      |       00      |       05      |
      +---------------+---------------+---------------+---------------+
    16|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
    20|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+

    Header breakdown
    Dcp No-Op command
    Field        (offset) (value)
    Magic        (0)    : 0x81 	              (Response)
    Opcode       (1)    : 0x5C                (Dcp No-Op)
    Key length   (2,3)  : 0x0000              (filed not used)
    Extra length (4)    : 0x00                (0)
    Data type    (5)    : 0x00                (field not used)
    Status       (6,7)  : 0x0000              (Success)
    Total body   (8-11) : 0x00000000          (0)
    Opaque       (12-15): 0x00000005          (5)
    CAS          (16-23): 0x0000000000000000  (field not used)

### Returns

Whether or not the operation has succeeded

### Errors

**(Disconnect)**

A disconnect may happen for one of two reasons:

* If the connection state no longer exists on the server. The most likely reason this will happen is if another connection is made to the server from a different client with the same name as the current connection.
* If a No-Op reequest is sent to a Producer.
