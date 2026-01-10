# Mutate with Meta

## Introduction

Mutate with Meta is a command designed to perform atomic mutations
(Add, Set, or Delete) with explicit metadata, while optionally
supporting on-the-fly patching of CAS and sequence numbers within the
document's extended attributes (XATTRs).

This command is particularly useful when the client needs to maintain
consistency between the document content and its metadata (like CAS)
stored within the document itself.

One may think of the command as a combination of Add/Set/Del with meta
and the macro expansion functionality offered by Subdoc. If this
duplicates logic by Subdoc, why not use Subdoc you may ask? Well the
answer is pretty simple. Subdoc have a (configurable) max limit of
paths it may operate on, and there is no max limit on the number of
xattrs one may have. If we "remove" the limitation it still won't fly
as subdoc would rebuild the entire document every time it applies
*one* path (it simply won't scale on documents with a lot of xattr
paths). Why didn't we extend the current Add/Set/Del with meta
commands to include these new features? The simple answer for that is
that the encoding of those commands was already pretty "complex" as it
used the size of the extrata section to "guess" which options the user
provided. Now that we needed two new arrays of offsets we would get
into an even more complex way of decoding (and would also need to
steal from the body field as these arrays of offsets would easily grow
the extrata field beyond its 255 bytes.

## 0x4b MutateWithMeta

Request:

* MUST have extras.
* MUST have key.
* MUST have value.

### Request Extras

The extras section contains a 4-byte unsigned integer (network byte
order) specifying the size of the metadata segment located at the end
of the value field.

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| Meta Size                                                     |
        +---------------+---------------+---------------+---------------+
        Total 4 bytes

### Request Value

The value field is divided into two parts:
1. **Document Data**: The actual content to be stored.
2. **Metadata Segment**: A JSON-encoded object containing operation options
   and metadata.

The size of the Metadata Segment is defined by the `Meta Size` field
in the extras.

#### Metadata Segment JSON Format

The metadata segment is a JSON object that can contain the following fields:

| Field | Type | Description                                                                                                                                      |
|-------|------|--------------------------------------------------------------------------------------------------------------------------------------------------|
| `command` | string | The mutation operation: `"add"`, `"set"`, or `"delete"`.                                                                                         |
| `cas` | string/number | The CAS value to associate with the item. This field should *NOT* be present if options contains `REGENERATE_CAS` (as that don't make any sense) |
| `expiration` | string/number | The expiration time (TTL) for the item                                                                                                           |
| `flags` | string/number | The item flags                                                                                                                                   |
| `rev_seqno` | string/number | The revision sequence number                                                                                                                     |
| `options` | string/number | Bitmask of operation options (see below)                                                                                                         |
| `cas_offsets` | array | (Optional) Array of byte offsets within the document where the server-generated CAS should be patched.                                           |
| `seqno_offsets` | array | (Optional) Array of byte offsets within the document where the server-generated sequence number should be patched.                               |

*Note: 64-bit values (CAS, rev_seqno) SHOULD be provided as hexadecimal strings (e.g., `"0xdeadbeef"`) to avoid precision loss in some JSON parsers.*

#### Options Bitmask

The `options` field uses the following flags:

| Value | Name | Description |
|-------|------|-------------|
| 0x04 | `REGENERATE_CAS` | Server should generate a new CAS instead of using the one provided. Required if `cas_offsets` is used. |
| 0x08 | `SKIP_CONFLICT_RESOLUTION_FLAG` | Skip conflict resolution for this operation. |
| 0x10 | `IS_EXPIRATION` | Only valid for `delete` command. Indicates the deletion is due to expiration. |
| 0x20 | `FORCE_ACCEPT_WITH_META_OPS` | Force acceptance of the operation even if it would normally be rejected (e.g., by conflict resolution). |

### Patching CAS and Sequence Numbers

If `cas_offsets` or `seqno_offsets` are provided, the server will:
1. Generate the actual CAS and/or sequence number for the mutation.
2. Write these values at the specified offsets within the **XATTR** section of the document before storing it.
3. The values are written as 18-byte hexadecimal strings (including the `0x` prefix) using digits and lowercase letters.

**Constraints**:
* Patching is only supported if the document contains an XATTR section.
* Offsets must point within the XATTR section.
* The document must not be compressed (or must be decompressed by the server if SNAPPY is used).
* If `cas_offsets` is used, `REGENERATE_CAS` (0x04) MUST be set.

### Response

Response header contains the standard mutation information.

* MUST have CAS.
* MAY have extras (Mutation Seqno if enabled).
* MUST NOT have key.
* MUST NOT have value.

If the Mutation Seqno feature is enabled, the extras will contain 16 bytes:
* 8 bytes: vBucket UUID (network byte order).
* 8 bytes: Sequence Number (network byte order).

## Usage Examples

### Basic Set with Meta

Store a document with specific flags and expiration using an
unconditional "set".

**Request Value**:
```json
{
  "body": "Hello World"
}
{"command": "set", "flags": "0xdeadbeef", "expiration": 3600}
```
*(Where the second line is the metadata segment at the end of the value)*

### Add with CAS Patching

Add a document and have the server patch the generated CAS into
multiple locations in the metadata stored within the document's
XATTRs.

**Document Content (XATTRs + Body)**:
```
{
  "_sys": {"cas": "0x0000000000000000"},
  "user": {"updated_cas": "0x0000000000000000"}
}
{"actual": "content"}
```

**Metadata Segment**:
```json
{
  "command": "add",
  "options": "0x04",
  "cas_offsets": [15, 46]
}
```

### Delete with Meta

Perform a deletion that is marked as an expiration.

**Metadata Segment**:
```json
{
  "command": "delete",
  "options": "0x10"
}
```
