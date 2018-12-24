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


### Why am I getting mutations instead of deletions/expirations on my expired documents?

If you are receiving mutations over DCP for documents that have expired, and
that you expect to see a DCP expiration or DCP deletion, this is expected
behaviour in some situations.

If an item with an expiry time is written to memory and logically expires after
the item is persisted to disk, or does not get persisted at all (i.e. the
flusher does not run on the item), when a DCP stream request is received this
item will be sent as a mutation. However, if the item expires and is then
persisted, the flusher expires the document and a DCP stream will either send a
deletion (or an expiration, if that has been enabled). This difference can be
more evident when using ephemeral buckets. This is because ephemeral does not
persist to disk, so all the logically expired items will appear as mutations if
you DCP stream them without performing any gets or expiry pager runs prior.

Although this may seem like abnormal behaviour, the expiry time is consistent
over the mutations so once the item has an attempted use at the end location of
a DCP stream, it should be denied access at that point. The expectation is that
if a consumer is utilising documents with an expiration time, they should also
be able to handle this ‘is expired’ check.

It is important to note that if you call a get on the item, or the expiry pager
runs when item is logically expired, this will cause the item to be
deleted/expired as you would expect and the DCP output corresponds as such.
For consistency over your system, this behaviour should be replicated by the
recipient of the DCP stream.
