# DCP Expiry Opcode Output

## Introduction to Expiration
Although expiry times (TTL - Time To Live) for documents have been a feature
for a while, by default DCP does not distinguish if a document was explicitly
deleted or whether it was expired through the use of its TTL. This control
message allows a DCP consumer to notify a producer that it can handle this
difference, allowing the client to identify for certain if the removal of a
document is by explicit deletion or expiration.

## Usage
This feature is available in Couchbase 6.5.0 upwards.

To utilise this feature as a DCP consumer, you will need to trigger a control
message to notify the producer that you are able to receive expirations.

First, [open](commands/open-connection.md) your DCP stream as you normally
would. Then, trigger a [DCP control](commands/control.md) message with the key
"enable_expiry_opcode" and the value "true".

From there, start a [stream request](commands/stream-request.md) for all
vbuckets of interest. Whenever you expire a document, you can expect to receive
an [expiration packet](commands/expiration.md) with opcode 0x59 instead of a
[deletion packet](commands/deletion.md) (opcode 0x58) from this stream. The
packet information is listed in the links provided.
