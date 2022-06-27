# Range Scan Continue (0xDB)

Requests that the server continues an existing range scan, returning to the 
client a sequence of keys or documents. The client can limit how many documents
are returned and/or how much time the continue request can run for.

The request:
* Must contain extras
* No key
* No value

Each continue request will return at least 1 key/document unless:

* The scan reaches the end
* An error occurs

The client is responsible for progressing the scan by issuing new continue
requests until the server indicates the scan has reached the end.

The command uses an extras section to describe the input parameters.

## Extra definition

The extras for a continue range scan encodes:

* 128-bit uuid identifying the scan (obtained from range-scan-create)
* Maximum key/document count to return (when 0 there is no limit)
* Maximum time (ms) for the scan to keep returning key/documents (when 0 there
  is no time limit)
* Bytes to return (when 0 there is no limit). Note the response will always include
  whole keys or documents, thus even with a byte limit in place the client
  must be prepared to receive at least one document or key that could exceed the
  value of this field. E.g. A value of 1 can result in a payload of 20MB
  (assuming 20MB is the maximum document size).

```
     Byte/     0       |       1       |       2       |       3       |
        /              |               |               |               |
       |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
       +---------------+---------------+---------------+---------------+
      0| scan identifier                                               |
       +                                                               +
      4|                                                               |
       +                                                               +
      8|                                                               |
       +                                                               +
     12|                                                               |
       +                                                               +
       +---------------+---------------+---------------+---------------+
     16| item limit (network byte order)                               |
       +---------------+---------------+---------------+---------------+
     20| time limit (network byte order)                               |
       +---------------+---------------+---------------+---------------+
     24| byte limit (network byte order)                               |
       +---------------+---------------+---------------+---------------+
       Total 28bytes
```

## Response format

A successful continue will trigger the server to iterate forwards through the
range of keys, return keys/documents using a bulk framing.

The many keys/documents of a single continue maybe sent (framed) in multiple
responses, where a final response (with bespoke status) indicates the end of the
individual continue or the end of the scan itself. Each response of the continue
may encode multiple keys or documents, the formatting of which is described
in the following sections.


A successful continue will return key/documents using the following framing. A
series of key/documents always begins with the standard mcbp 24-byte response
header which will include a success status and a value, the value encodes many
keys/documents.

The following examples highlight how the responses to range-scan-continue works.

For example a range may cover an unknown number of keys and the client only
wants to handle up to 500 keys per range-scan-continue. Thus the client would
first issue a range-scan-continue (0xDB) with the uuid from range-scan-create
(0xDA), item limit set to 500 and the time limit set to 0.

The server will respond with a series of responses, there is no definition of
how many keys each response may encode.

Example 1, range scan progresses, yet more continues will be required.

* response status=success, bodylen=261
* response status=success, bodylen=259
* response status=success, bodylen=260
* response status=range-scan-more (0xA6), bodylen=241

In this sequence 500 keys are returned (distributed over 4 different responses).
The final response has the status of range-scan-more informing the client that
the scan has more data.

Example 2, range scan progress to the end

* response status=success, bodylen=262
* response status=range-scan-complete (0xA7), bodylen=120

In this sequence <= 500 keys have been returned (distributed over 2 different
responses). The final response as the status of range-scan-complete, informing
the client that the scan has now completed (server has closed the scan).

Example 3, range scan progresses, but vbucket state change interrupts. This
example demonstrates that the client must be aware that the sequence of continue
responses could be bookmarked with a status indicating some other issue. In this
case the range-scan has also been cancelled due to the issue.

* response status=success, bodylen=261
* response status=success, bodylen=259
* response status=not-my-vbucket (0xa6), bodylen=80

In this example < 500 keys have been spread over three responses, the final
response indicates that the target vbucket has changed state.

### Response Value Encoding `"key_only":true`

The mcbp header defines how much data follows, in this command only a value is
transmitted (key and extras fields are both 0). The format of the value is a
sequence of length/key pairs. Each length is a varint (leb128) encoding of the
key length.

For example if 3 keys are returned in one response the value encodes the
following sequence (note the final key is 128 bytes long to demonstrate leb128
encoding, but not all 128 bytes of the key are shown.)

* length=4, "key0"
* length=5, "key11"
* length=128, "key2222...3"


```
     Byte/     0       |       1       |       2       |       3       |
        /              |               |               |               |
       |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
       +---------------+---------------+---------------+---------------+
      0| 0x04          | 'k' 0x6B      | 'e' 0x65      | 'y' 0x79      |
       +---------------+---------------+---------------+---------------+
      4| '0' 0x30      | 0x05          | 'k' 0x6B      | 'e' 0x65      |
       +---------------+---------------+---------------+---------------+
      8| 'y' 0x79      | '1' 0x31      | '1' 0x31      | 0x80          |
       +---------------+---------------+---------------+---------------+
     12| 0x01          | 'k' 0x6B      | 'e' 0x65      | 'y' 0x79      |
       +---------------+---------------+---------------+---------------+
     16| '2' 0x32      | '2' 0x32      | '2' 0x32      | '2' 0x32      |
       +---------------+---------------+-------------------------------+
       ...
       +---------------+
    140| '3' 0x33      |
       +---------------+
       Total 140bytes
```

### Response Value Encoding `"key_only":false`

The mcbp header defines how much data follows, in this command only a value is
transmitted (key and extras fields are both 0). The format of the value is a
sequence of documents. Each document has the following 3 parts.

* Fixed length metadata
* variable length key
* variable length value

The document encoding will prefix the variable length key and value each with
a varint (leb128).

For example if 2 documents are returned in one response.

* document1 {fixed meta}{varint, key}{varint, value}
* document2 {fixed meta}{varint, key}{varint, value}

The format of a single document is now described in more detail.

Fixed metadata (25 bytes)
* 32-bit flags
* 32-bit expiry
* 64-bit seqno
* 64-bit cas
* 8-bit datatype - Note the datatype is critical to interpreting the value,
  e.g. if datatype has the snappy bit set, the value is compressed.

E.g. a document with key="key0" and value="value0"

```
     Byte/     0       |       1       |       2       |       3       |
        /              |               |               |               |
       |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
       +---------------+---------------+---------------+---------------+
      0| flags (network order)                                         |
       +---------------+---------------+---------------+---------------+
      4| expiry (network order)                                        |
       +---------------+---------------+---------------+---------------+
      8| sequence number  (network order)                              |
       +                                                               +
     12|                                                               |
       +---------------+---------------+---------------+---------------+
     16| cas (network order)                                           |
       +                                                               +
     20|                                                               |
       +---------------+---------------+---------------+---------------+
     24| datatype      | 0x04          | 'k' 0x6B      | 'e' 0x65      |
       +---------------+---------------+---------------+---------------+
     28| 'y' 0x79      | '0' 0x30      | 0x06          | 'v' 0xaa      |
       +---------------+---------------+---------------+---------------+
     32| 'a' 0x79      | 'l' 0x30      | 'u' 0xaa      | 'e' 0xaa      |
       +---------------+---------------+---------------+---------------+
     36| '0' 0x30      | <next document>
       +---------------+---------------...
       Total 36bytes
```

In the above layout, a second document would begin at offset 37, no padding or
alignment.

### Success

A range-scan-continue response sequence indicates success using status codes

* Status::Success 0x00: Used for intermediate responses making up a larger response.
* Status::RangeScanMore 0xA6: Scan has not reached the end key, more data maybe available, client should issue another continue.
* Status::RangeScanComplete 0xA7: Scan has reached the end of the range.

### Errors

**Status::KeyEexists (0x01)**

No scan with the given uuid could be found.

**Status::Einval (0x04)**

The request failed an input validation check, details of which should be returned
in the response.

**Status::NotMyVbucket (0x07)**

The requested vbucket no longer exists on this node.

**Status::Eaccess (0x24)**

The user no longer has the required privilege for range-scans.

**Status::EBusy (0x85)**

The scan with the given uuid cannot be continued because it is already continuing.

**Status::UnknownCollection (0x88)**

The collection was dropped.

**Status::RangeScanCancelled (0xA5)**

The scan was cancelled whilst returning data. This could be the only status if
the cancel was noticed before a key/value was loaded.
