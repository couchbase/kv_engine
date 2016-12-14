# Error Map and Extended Errors

An _Error Map_ is a mapping of error to their attributes and properties.
It is used by connected clients to handle error codes which they may
otherwise not be aware of.

The error map solves the problem where clients would incorrectly handle
newer error codes, and/or where the server would disconnect the client
because it needed to send it a new error code.

The solution via _Error Map_ is to establish a contract between client and
server about certain *attributes* which each error code may have. These
attributes indicate whether an error may be passed through, whether the command
is retriable and so on. When a client receives an error code it does not know
about it, it can look up its attributes and then determine the next course of
action.

## Error Map Format

The error map is a JSON formatted file containing a mapping of error codes
to their extended information. The top-level format of the file is:

```
{
    "version": 1,
    "revision": 1
    "errors": {
        "1f": {
            "name": "AUTH_STALE",
            "desc": "Reauthentication Required",
            "attrs": ["conn-state-invalidated"]
        },
        "20": {
            "name": "AUTH_ERROR",
            "desc": "Authentication failed",
            "attrs": ["conn-state-invalidated", "auth"]
        }
    }
}
```

* `version` is the current error map format. The error map is versioned
  in case the file format changes in the future, so that clients can request
  a version they can understand. There is currently only one version, which is
  `1`
* `revision` is the revision of the error map. The revision is incremented
  whenever a new error code is added. Clients can use this to compare error
  maps from different servers and select the map with the highest revision
  as the one to use.
* `errors` is the actual mapping of error codes to their attributes. This is
  a JSON object. Each key represents the error code in hexadecimal format.
  Each value contains the following fields:
  * `name`: The short name of the error code, e.g. `KEY_ENOENT`.
    The client may use this in logs.
  * `desc`: An expanded description of the error code. e.g. `Key not found`.
    The client may use this as part of an error message or string.
  * `attrs`: One or more _attributes_ of the error. The client will scan the
    attributes and determine the course of action.


### Error Attributes

_Attributes_ are the most important part of the error map as they convey _how_
an error code should be handled. An error code may have more than a single
attribute.

* `item-only`: This attribute means that the error is related to a constraint
  failure regarding the item itself, i.e. the item does not exist, already exists,
  or its current value makes the current operation impossible. Retrying the
  operation when the item's value or status has changed may succeed.

* `invalid-input`: This attribute means that a user's input was invalid because
  it violates the semantics of the operation, or exceeds some predefined limit.
  
* `fetch-config`: The client's cluster map may be outdated and requires updating.
  The client should obtain a newer configuration.
  
* `conn-state-invalidated`: The current connection is no longer valid. The client
  must reconnect to the server. Note that the presence of other attributes may
  indicate an alternate remedy to fixing the connection without a disconnect, but
  without special remedial action a disconnect is needed.
  
* `auth`: The operation failed because the client failed to authenticate or is not
  authorized to perform this operation. Note that this error in itself does not
  mean the connection is invalid, unless `conn-state-invalidated` is also present.
  
* `special-handling`: This error code must be handled specially. If it is not handled,
  the connection must be dropped.
  
* `support`: The operation is not supported, possibly because the of server version,
  bucket type, or current user.
  
* `temp`: This error is transient. Note that this does not mean the error is retriable.

* `internal`: This is an internal error in the server.

* `retry-now`: The operation may be retried immediately.

* `retry-later`: The operation may be retried after some time.

* `subdoc`: The error is related to the subdocument subsystem.

* `dcp`: The error is related to the DCP subsystem.


## Protocol

A client which is able to make use of the Error Map is able to handle error codes
that it does not explicitly understand. This also allows the server to provide
more information (in the form of the `desc` and `name` of the error) for a given
failure.

To make full use of the error map facility, and to ensure backwards compatibility
with older versions of the server (which do not support the `GET_ERROR_MAP` command)
and/or older versions of the client (which are not equipped to handle unknown error
codes), the following protocol should be employed:

1. In the initial negotiation in the `HELLO` command, the client should indicate that
   it supports the `XERROR` (`mcbp::Feature::XERROR`, 0x07) feature. The `XERROR` feature
   tells the server that it may send error codes beyond the "basic" scope of legacy
   Memcached/Couchbase clients, which are the classic Memcached error codes, and
   `NOT_MY_VBUCKET`.
2. If the server responds that it also supports the `XERROR` feature, the client
   should request the error map using the `GET_ERROR_MAP` command. The command takes
   a 16-bit integer indicating the maximum error map version the client supports (though
   at the time of writing, only a single version is defined).
3. The server may respond with an error map version equal to, or lower than the version
   the client requested (the lowest version is 1). The _version_ is increased whenever
   a new attribute is added, or when the format of the error map changes.
4. The client should compare its current global error map with the one received from the
   server. It should compare the _revision_ of both error maps, and use the error map
   with the highest revision. The revision is increased whenever a new error is added, or
   when a specific code's attributes have been changed. Note that an error map with a
   higher revision but a lower version is better than an error map with a higher version
   but a lower revision.
5. When a client receives an error it does not understand, it should consult its current
   error map and determine the course of action. Errors with an attribute of `item-only`
   may be passed to the user, for example. Errors with `conn-state-invalidated` should
   close the current connection and retry the command (if the `retry` attribute is present)
   and so on.
