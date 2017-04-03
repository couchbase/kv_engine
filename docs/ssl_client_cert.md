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

Couchbase may use information from the following fields:

 * Common Name (CN) in the subject
 * X509v3 Subject Alternative Name

The mapping is configured through [memcached.json](../man/man4/memcached.json.4.txt)
and allows for specifying:

 * path - Specifies which field in the certificate to use.
          (Currently only subject.cn, san.uri, san.email and san.dnsname are
           allowed)
 * prefix - The prefix to search for in the value for the field. The prefix
            is stripped from the value.
 * delimiter - This is a list of optionally characters to use stop the parsing
               of the value.

If multiple fields map the path (e.g. multiple san.uri fields exists), the
first value which match the prefix will be used.

Given the following fields specified in the SAN:

    URI:urn:li:testurl_1
    URI:email:testapp@example.com
    URI:couchbase://myuser@mycluster/mybucket
    DNS:MyServerName

If the path is set to `san.uri`, prefix set to `couchbase://` and delimiter
set to `@` the resulting username would be `myuser`
