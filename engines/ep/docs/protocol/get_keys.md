##Get Keys (getkeys) v2.0

The get keys command is used to fetch all keys from a vbucket.

The request:
* Can have a key, which will be treated as the `start_key`.
    * If the key is not specified `GET_KEYS` will default to returning keys from
    the default collection
    * If the op is sent on a collection enabled connection, `GET_KEYS` will work
    on a per collection basics. This means that if your call `GET_KEYS` then you
    may only get keys from one collection at a time. To specify which collection
    to get keys from, encode the `start_key` with the collectionID e.g.
    `\bstart_key` where the collection id here is `0xb`.
* Can have 4 byte extras (`uint32_t`), which is treated as `max count`. The
maximum number of keys to return. If this is not specified ep-engine will
default to 1000.

The response:
* Keys are placed in the value section of the response. This is returned in the
format of `uint32_t` which specifies the length of the key in bytes. After this
their may be another `uint32_t` for the next key. This should be repeated to get
all the keys returned till the number of bytes observed is equal to total body
bytes.

####Binary Implementation

    Get Meta Binary Request

    Byte/     0       |       1       |       2       |       3       |
       /              |               |               |               |
      |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
      +---------------+---------------+---------------+---------------+
     0|    0x80       |     0xB8      |     0x00      |     0x09      |
      +---------------+---------------+---------------+---------------+
     4|    0x04       |     0x00      |     0x00      |     0x03      |
      +---------------+---------------+---------------+---------------+
     8|    0x00       |     0x00      |     0x00      |     0x00      |
      +---------------+---------------+---------------+---------------+
    12|    0x00       |     0x00      |     0x00      |     0x00      |
      +---------------+---------------+---------------+---------------+
    16|    0x00       |     0x00      |     0x00      |     0x00      |
      +---------------+---------------+---------------+---------------+
    20|    0x00       |     0x00      |     0x00      |     0x00      |
      +---------------+---------------+---------------+---------------+
    24|    0x00       |     0x00      |     0x00      |     0x00      |
      +---------------+---------------+---------------+---------------+
    28|    0x0A('s')  |     0x73('t') |     0x74('a') |     0x61('r') |
      +---------------+---------------+---------------+---------------+
    32|    0x72('t')  |     0x74('_') |     0x5F('k') |     0x6B('e') |
      +---------------+---------------+---------------+---------------+
    36|    0x65('y')  |
      +---------------+

    GET_KEYS command
    Field        (offset) (value)
    Magic        (0)    : 0x80                (Request)
    Opcode       (1)    : 0xB8                (Get Keys)
    Key length   (2,3)  : 0x0009              (9)
    Extra length (4)    : 0x04                (4)
    Data type    (5)    : 0x00                (Field not used)
    VBucket      (6,7)  : 0x0003              (3)
    Total body   (8-11) : 0x00000000          (0)
    Opaque       (12-15): 0x00000000          (Field not used)
    CAS          (16-23): 0x0000000000000000  (Field not used)
    Extras              :
      Count      (24-28): 0x00000002          (2)
    Key          (29-38): start_key

    Get Keys Binary Response

    Byte/     0       |       1       |       2       |       3       |
       /              |               |               |               |
      |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
      +---------------+---------------+---------------+---------------+
     0|     0x81      |     0xB8      |     0x00      |     0x00      |
      +---------------+---------------+---------------+---------------+
     4|     0x15      |     0x00      |     0x00      |     0x00      |
      +---------------+---------------+---------------+---------------+
     8|     0x00      |     0x00      |     0x00      |     0x1E      |
      +---------------+---------------+---------------+---------------+
    12|     0x00      |     0x00      |     0x00      |     0x00      |
      +---------------+---------------+---------------+---------------+
    16|     0x00      |     0x00      |     0x00      |     0x00      |
      +---------------+---------------+---------------+---------------+
    20|     0x00      |     0x00      |     0x00      |     0x00      |
      +---------------+---------------+---------------+---------------+
    24|     0x00      |     0x00      |     0x00      |     0x0A      |
      +---------------+---------------+---------------+---------------+
    28|     0x00      |    0x0A('s')  |     0x73('t') |     0x74('a') |
      +---------------+---------------+---------------+---------------+
    32|     0x61('r') |    0x72('t')  |     0x74('_') |     0x5F('k') |
      +---------------+---------------+---------------+---------------+
    36|     0x6B('e') |     0x65('y') |     0x31('1') |     0x00      |
      +---------------+---------------+---------------+---------------+
    40|     0x00      |     0x00      |     0x0A      |     0x00      |
      +---------------+---------------+---------------+---------------+
    44|    0x0A('s')  |     0x73('t') |     0x74('a') |     0x61('r') |
      +---------------+---------------+---------------+---------------+
    48|    0x72('t')  |     0x74('_') |     0x5F('k') |     0x6B('e') |
      +---------------+---------------+---------------+---------------+
    52|    0x65('y')  |     0x32('2') |
      +---------------+---------------+

    GET_KEYS command
    Field        (offset) (value)
    Magic        (0)    : 0x81 (Response)
    Opcode       (1)    : 0xB8 (Get Keys)
    Key length   (2,3)  : 0x0000
    Extra length (4)    : 0x00 (0)
    Data type    (5)    : 0x00
    Status       (6,7)  : 0x0000 (0)
    Total body   (8-11) : 0x0000001E (30)
    Opaque       (12-15): 0x00000000
    CAS          (16-23): 0x0000000000000000 (0) (Field not used)
    Extras              : (Field not used)
    Key                 : (Field not used)
    Value        (24-34): "0x0000000A\0start_string10x0000000A\0start_string2"

###Status

**PROTOCOL_BINARY_RESPONSE_SUCCESS (0x00)**

The operation succeeded and the result is in the value of the response

**PROTOCOL_BINARY_RESPONSE_EINVAL (0x04)**

The data in this packet is malformed or incomplete then this error is returned.

**PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET (0x07)**

The vbucket does not exist.

**PROTOCOL_BINARY_RESPONSE_EACCESS (0x24)**

The caller lacks the correct privilege to read documents

**PROTOCOL_BINARY_RESPONSE_UNKNOWN_COLLECTION (0x88)**

The collection of the `start_key` does not exist or the client does not have
access to the collection. Should only be returned on an collection enabled
connection.