# Buffer Acknowledgement (opcode 0x5d)

Sent to by the Consumer to the Producer in order to inform the Producer that the Consumer has consumed some or all of the data the the Producer has sent and that the Consumer is ready for more data.
Note: In the acknowledgement we account for both data bytes and message header bytes.

The request:

* Must have a 4 byte extras section

Extra looks like:

     Byte/     0       |       1       |       2       |       3       |
        /              |               |               |               |
       |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
       +---------------+---------------+---------------+---------------+
      0| bytes to ack                                                  |
       +---------------+---------------+---------------+---------------+

The extras section will contain a 32-bit value which denotes the amount of bytes that the Consumer has processed and indicates to the Producer that the Consumer is capable of receiving more data.

#### Binary Implementation

    Buffer Acknowledgement Binary Request

    Byte/     0       |       1       |       2       |       3       |
       /              |               |               |               |
      |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
      +---------------+---------------+---------------+---------------+
     0|       80      |       5D      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
     4|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
     8|       00      |       00      |       00      |       04      |
      +---------------+---------------+---------------+---------------+
    12|       00      |       00      |       00      |       05      |
      +---------------+---------------+---------------+---------------+
    16|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
    20|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
    24|       00      |       00      |       10      |       00      |
      +---------------+---------------+---------------+---------------+

    Header breakdown
    Buffer Acknowledgement command
    Field        (offset) (value)
    Magic        (0)    : 0x80                (Request)
    Opcode       (1)    : 0x5D                (Buffer Acknowledgement)
    Key length   (2,3)  : 0x0000              (0)
    Extra length (4)    : 0x04                (0)
    Data type    (5)    : 0x00                (Field not used)
    VBucket      (6,7)  : 0x0000              (Field not used)
    Total body   (8-11) : 0x00000004          (4)
    Opaque       (12-15): 0x00000005          (5)
    CAS          (16-23): 0x0000000000000000  (Field not used)
	Buffer Bytes (24-27): 0x00001000          (4096)

    Buffer Acknowledgement Binary Response (Currently unused, DCP consumers should not expect this)

    Byte/     0       |       1       |       2       |       3       |
       /              |               |               |               |
      |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
      +---------------+---------------+---------------+---------------+
     0|       81      |       5D      |       00      |       00      |
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
    Buffer Acknowledgement command
    Field        (offset) (value)
    Magic        (0)    : 0x81 	              (Response)
    Opcode       (1)    : 0x5D                (Buffer Acknowledgement)
    Key length   (2,3)  : 0x0000              (filed not used)
    Extra length (4)    : 0x00                (0)
    Data type    (5)    : 0x00                (field not used)
    Status       (6,7)  : 0x0000              (Success)
    Total body   (8-11) : 0x00000000          (0)
    Opaque       (12-15): 0x00000005          (5)
    CAS          (16-23): 0x0000000000000000  (field not used)

##### Returns

Whether or not the operation has succeeded

##### Errors

**PROTOCOL_BINARY_RESPONSE_KEY_ENOENT (0x01)**

The server no longer knows about this buffer

**PROTOCOL_BINARY_RESPONSE_EINVAL (0x04)**

If data in this packet is malformed or incomplete then this error is returned. This error is also returned if the stream name specified in the packet does not exist.
