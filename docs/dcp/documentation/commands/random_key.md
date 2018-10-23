
## Random Key

The random key command is used to get a single random key from the cache. It is an internal API and is used by the cluster manager in order to help users visualize what there keys look like when writing view code.

#### Binary Implementation

    Random Key Binary Request

    Byte/     0       |       1       |       2       |       3       |
       /              |               |               |               |
      |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
      +---------------+---------------+---------------+---------------+
     0|       80      |       B6      |       00      |       00      |
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

    Header breakdown
    Random Key command
    Field        (offset) (value)
    Magic        (0)    : 0x80                (Request)
    Opcode       (1)    : 0xB6                (Random Key)
    Key length   (2,3)  : 0x0000              (0)
    Extra length (4)    : 0x00                (0)
    Data type    (5)    : 0x00                (Field not used)
    VBucket      (6,7)  : 0x0000              (Field not used)
    Total body   (8-11) : 0x00000000          (0)
    Opaque       (12-15): 0x00000000          (Field not used)
    CAS          (16-23): 0x0000000000000000  (Field not used)

    Random Key Binary Response

    Byte/     0       |       1       |       2       |       3       |
       /              |               |               |               |
      |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
      +---------------+---------------+---------------+---------------+
     0|       81      |       B6      |       00      |       08      |
      +---------------+---------------+---------------+---------------+
     4|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
     8|       00      |       00      |       00      |       10      |
      +---------------+---------------+---------------+---------------+
    12|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
    16|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
    20|       00      |       00      |       00      |       00      |
      +---------------+---------------+---------------+---------------+
    24|       72      |       61      |       6E      |       64      |
      +---------------+---------------+---------------+---------------+
    28|       5F      |       6B      |       65      |       79      |
      +---------------+---------------+---------------+---------------+

    Header breakdown
    Random Key command
    Field        (offset) (value)
    Magic        (0)    : 0x81 	              (Response)
    Opcode       (1)    : 0xB6                (Random Key)
    Key length   (2,3)  : 0x0008              (8)
    Extra length (4)    : 0x00                (0)
    Data type    (5)    : 0x00                (field not used)
    Status       (6,7)  : 0x0000              (Success)
    Total body   (8-11) : 0x00000008          (8)
    Opaque       (12-15): 0x00000000
    CAS          (16-23): 0x0000000000000000  (field not used)
	Key          (24-31): "rand_key"

##### Returns

A random key and value

##### Errors

**PROTOCOL_BINARY_RESPONSE_KEY_ENOENT (0x01)**

If the server doesn't contain any keys.

**PROTOCOL_BINARY_RESPONSE_EINVAL (0x04)**

If data in this packet is malformed or incomplete then this error is returned. This error is also returned if the stream name specified in the packet does not exist.

##### Use Cases

Used to get a random key and value in order to help inspect the data format contained in the server.


