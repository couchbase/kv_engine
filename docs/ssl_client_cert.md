# Client certificate verification

It is possible to have the Couchbase cluster verify the client by asking
the client to provide a certificate during the SSL handshake. If the
certificate provided by the client can't be verified, the server refuses
the client to connect. Couchbase supports 3 different modes:

* Disabled - Don't ask the client for a certificate during the SSL handshake
* Enabled - Ask the client to provide a certificate during the SSL handshake,
  but it is OK if it doesn't provide one. If one is provided the server must
  be able to verify it.
* Mandatory - Ask the client to provide a certificate during the SSL handshake
  and fail if the client doesn't provide one (or if the verification fails).

All connections are put in the "default" bucket (if it exists).

# Client certificate authentication

All operations in memcached are subject to [RBAC](rbac.md). Given that it is
possible to ask the client to provide a client certificate as part of the
SSL handshake, information in the client may be used to authenticate the
client in the server.

Iff such mapping is defined, the provided client certificates *must*
contain the fields which is used for the mapping, *and* the user *must*
be defined in Couchbase for the connection to be established.

A connection authenticated by using the Client certificate cannot perform
SASL authentication to change its user identity (Some time in the future
we might implement feature similar to su/sudo as part of the system wide
[RBAC](rbac.md)).

@todo expand this section with the mapping rules
