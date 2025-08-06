# Document size limits in kv

Couchbase is based upon memcached which have a maximum document size of 1MB, but
when we created the Coucbase style buckets we set this limit to 20MB which
seemed very high at that time.

The maximum document size is specified on a "per bucket" basis, so you can have
different limits for different buckets. This is useful if you have some
buckets that store small documents and others that store large documents. It
is passed as a parameter when creating the bucket via the `max_item_size`
parameter (default is 20MB, and ns_server does not provide this parameter
as of today).

We do however have some guardrails in place to protect the system:

1. The server will disconnect a client trying to send a MCBP frame larger than a
   configured maximum size. This is set to 30MB by default, but can be
   changed via the`max_packet_size` configuration parameter in `memcached.json`.

2. When we receive a frame on the wire which claims to be compressed we try to
   inflate the object to verify that is in fact compressed. If the inflated
   object is larger than a maximum size, we will reject the frame. This is
   currently hard coded to 30MB (see
   [MB-67957](https://jira.issues.couchbase.com/browse/MB-67957))

There is also the `dcp_backfill_byte_limit` configuration parameter which is
used to limit the size of the backfill that DCP will send to a client. I
don't *think* one needs to increase this limit; it just means that things
will run slower (It'll hit the max limit after inserting 1 item which
exceeds the threshold).
