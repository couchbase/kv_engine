# External AUTH provider

Memcached may be configured to use an external process as an RBAC provider.
The configuration parameter `external_auth_service` must be set to `true`,
and the provider needs to register itself by sending `AuthProvider` on a
connection holding the `SecurityManagement` privilege and with `Duplex`
enabled. Once successfully registered memcached will start using the
connection. It is not defined how memcached will utilize multiple
registered connections (that's an internal implementation detail
within memcached). It may use one, it may round robin etc.

Memcached continues to try it's internal username/password database
first, and will *only* try to use the external authentication provider
if the user *isn't* found in the local username database.

It is recommended that multiple connections is registered as authentication
providers to avoid the situation that all external users is locked out
if there is problems with the connection and it is closed for some reason.

The authentication protocol between memcached and external RBAC providers
proxies the SASL protocol between the external client and memcached.

As of now only PLAIN authentication mechanism is implemented.

## Authentication request

memcached will send `Authenticate` with the following payload

    {
      "step" : false,
      "context" : "",
      "mechanism" : "",
      "challenge" : "base64encoded challenge sent from client",
      "authentication-only" : true
    }

`step` should be set to true if this is a continuation of an ongoing
authentication. If not present this is assumed to be set to false.

`context` is an opaque context string returned from the external
provider _iff_ the authentication process needs multiple iterations.

`mechanism` is the authentication mechanism to use (this field may
not be present if this is a continuation of an authentication).

`challenge` is base64 encoding of the challenge sent from the client
to memcached.

`authentication-only` should be set to true if an RBAC entry already
exists locally (in memcached). If this is set to true a potential
success response won't include the RBAC entry.

### Packet dump example

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x82          | 0x02          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       4| 0x00          | 0x01          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x3c          |
        +---------------+---------------+---------------+---------------+
      12| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      16| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      20| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      24| 0x7b ('{')    | 0x22 ('"')    | 0x63 ('c')    | 0x68 ('h')    |
        +---------------+---------------+---------------+---------------+
      28| 0x61 ('a')    | 0x6c ('l')    | 0x6c ('l')    | 0x65 ('e')    |
        +---------------+---------------+---------------+---------------+
      32| 0x6e ('n')    | 0x67 ('g')    | 0x65 ('e')    | 0x22 ('"')    |
        +---------------+---------------+---------------+---------------+
      36| 0x3a (':')    | 0x22 ('"')    | 0x41 ('A')    | 0x47 ('G')    |
        +---------------+---------------+---------------+---------------+
      40| 0x39 ('9')    | 0x7a ('z')    | 0x59 ('Y')    | 0x6d ('m')    |
        +---------------+---------------+---------------+---------------+
      44| 0x39 ('9')    | 0x31 ('1')    | 0x63 ('c')    | 0x6d ('m')    |
        +---------------+---------------+---------------+---------------+
      48| 0x35 ('5')    | 0x6c ('l')    | 0x41 ('A')    | 0x48 ('H')    |
        +---------------+---------------+---------------+---------------+
      52| 0x42 ('B')    | 0x68 ('h')    | 0x63 ('c')    | 0x33 ('3')    |
        +---------------+---------------+---------------+---------------+
      56| 0x4e ('N')    | 0x33 ('3')    | 0x62 ('b')    | 0x33 ('3')    |
        +---------------+---------------+---------------+---------------+
      60| 0x4a ('J')    | 0x6b ('k')    | 0x22 ('"')    | 0x2c (',')    |
        +---------------+---------------+---------------+---------------+
      64| 0x22 ('"')    | 0x6d ('m')    | 0x65 ('e')    | 0x63 ('c')    |
        +---------------+---------------+---------------+---------------+
      68| 0x68 ('h')    | 0x61 ('a')    | 0x6e ('n')    | 0x69 ('i')    |
        +---------------+---------------+---------------+---------------+
      72| 0x73 ('s')    | 0x6d ('m')    | 0x22 ('"')    | 0x3a (':')    |
        +---------------+---------------+---------------+---------------+
      76| 0x22 ('"')    | 0x50 ('P')    | 0x4c ('L')    | 0x41 ('A')    |
        +---------------+---------------+---------------+---------------+
      80| 0x49 ('I')    | 0x4e ('N')    | 0x22 ('"')    | 0x7d ('}')    |
        +---------------+---------------+---------------+---------------+
        Total 84 bytes (24 bytes header and 60 value)

    Field        (offset) (value)
    Magic        (0)    : 0x82 (ServerRequest)
    Opcode       (1)    : 0x02 (Authenticate)
    Key length   (2,3)  : 0x0000
    Extra length (4)    : 0x00
    Data type    (5)    : 0x01
    Vbucket      (6,7)  : 0x0000
    Total body   (8-11) : 0x0000003c
    Opaque       (12-15): 0x00000000
    CAS          (16-23): 0x0000000000000000
    Data         (24-83): JSON payload containing:
       {
         "challenge":"AG9zYm91cm5lAHBhc3N3b3Jk",
         "mechanism":"PLAIN"
       }

## Successful Authentication response

The provider should reply with status code Success or AuthContinue.

    {
      "response": "base64 encoded response to send to the client",
      "rbac": {
        "Users RBAC entry": ""
      }
    }

`response` is an optional field which contains the base64 encoded
data which should be returned to the client as part of the SASL command.

`rbac` contains the RBAC entry in the same format as in
[rbac.md](rbac.md#File-Format). This field is mandatory unless the request
contained `"authentication-only" : true`. In that case it may or may not be
present.

If nothing is to be sent back to the client and `"authentication-only" : true`
was set to true, the response looks like:

    {}

### Packet dump example

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x83          | 0x02          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       4| 0x00          | 0x01          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x83          |
        +---------------+---------------+---------------+---------------+
      12| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      16| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      20| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      24| 0x7b ('{')    | 0x22 ('"')    | 0x72 ('r')    | 0x62 ('b')    |
        +---------------+---------------+---------------+---------------+
      28| 0x61 ('a')    | 0x63 ('c')    | 0x22 ('"')    | 0x3a (':')    |
        +---------------+---------------+---------------+---------------+
      32| 0x7b ('{')    | 0x22 ('"')    | 0x6f ('o')    | 0x73 ('s')    |
        +---------------+---------------+---------------+---------------+
      36| 0x62 ('b')    | 0x6f ('o')    | 0x75 ('u')    | 0x72 ('r')    |
        +---------------+---------------+---------------+---------------+
      40| 0x6e ('n')    | 0x65 ('e')    | 0x22 ('"')    | 0x3a (':')    |
        +---------------+---------------+---------------+---------------+
      44| 0x7b ('{')    | 0x22 ('"')    | 0x62 ('b')    | 0x75 ('u')    |
        +---------------+---------------+---------------+---------------+
      48| 0x63 ('c')    | 0x6b ('k')    | 0x65 ('e')    | 0x74 ('t')    |
        +---------------+---------------+---------------+---------------+
      52| 0x73 ('s')    | 0x22 ('"')    | 0x3a (':')    | 0x7b ('{')    |
        +---------------+---------------+---------------+---------------+
      56| 0x22 ('"')    | 0x64 ('d')    | 0x65 ('e')    | 0x66 ('f')    |
        +---------------+---------------+---------------+---------------+
      60| 0x61 ('a')    | 0x75 ('u')    | 0x6c ('l')    | 0x74 ('t')    |
        +---------------+---------------+---------------+---------------+
      64| 0x22 ('"')    | 0x3a (':')    | 0x5b ('[')    | 0x22 ('"')    |
        +---------------+---------------+---------------+---------------+
      68| 0x52 ('R')    | 0x65 ('e')    | 0x61 ('a')    | 0x64 ('d')    |
        +---------------+---------------+---------------+---------------+
      72| 0x22 ('"')    | 0x2c (',')    | 0x22 ('"')    | 0x53 ('S')    |
        +---------------+---------------+---------------+---------------+
      76| 0x69 ('i')    | 0x6d ('m')    | 0x70 ('p')    | 0x6c ('l')    |
        +---------------+---------------+---------------+---------------+
      80| 0x65 ('e')    | 0x53 ('S')    | 0x74 ('t')    | 0x61 ('a')    |
        +---------------+---------------+---------------+---------------+
      84| 0x74 ('t')    | 0x73 ('s')    | 0x22 ('"')    | 0x2c (',')    |
        +---------------+---------------+---------------+---------------+
      88| 0x22 ('"')    | 0x49 ('I')    | 0x6e ('n')    | 0x73 ('s')    |
        +---------------+---------------+---------------+---------------+
      92| 0x65 ('e')    | 0x72 ('r')    | 0x74 ('t')    | 0x22 ('"')    |
        +---------------+---------------+---------------+---------------+
      96| 0x2c (',')    | 0x22 ('"')    | 0x44 ('D')    | 0x65 ('e')    |
        +---------------+---------------+---------------+---------------+
     100| 0x6c ('l')    | 0x65 ('e')    | 0x74 ('t')    | 0x65 ('e')    |
        +---------------+---------------+---------------+---------------+
     104| 0x22 ('"')    | 0x2c (',')    | 0x22 ('"')    | 0x55 ('U')    |
        +---------------+---------------+---------------+---------------+
     108| 0x70 ('p')    | 0x73 ('s')    | 0x65 ('e')    | 0x72 ('r')    |
        +---------------+---------------+---------------+---------------+
     112| 0x74 ('t')    | 0x22 ('"')    | 0x5d (']')    | 0x7d ('}')    |
        +---------------+---------------+---------------+---------------+
     116| 0x2c (',')    | 0x22 ('"')    | 0x64 ('d')    | 0x6f ('o')    |
        +---------------+---------------+---------------+---------------+
     120| 0x6d ('m')    | 0x61 ('a')    | 0x69 ('i')    | 0x6e ('n')    |
        +---------------+---------------+---------------+---------------+
     124| 0x22 ('"')    | 0x3a (':')    | 0x22 ('"')    | 0x65 ('e')    |
        +---------------+---------------+---------------+---------------+
     128| 0x78 ('x')    | 0x74 ('t')    | 0x65 ('e')    | 0x72 ('r')    |
        +---------------+---------------+---------------+---------------+
     132| 0x6e ('n')    | 0x61 ('a')    | 0x6c ('l')    | 0x22 ('"')    |
        +---------------+---------------+---------------+---------------+
     136| 0x2c (',')    | 0x22 ('"')    | 0x70 ('p')    | 0x72 ('r')    |
        +---------------+---------------+---------------+---------------+
     140| 0x69 ('i')    | 0x76 ('v')    | 0x69 ('i')    | 0x6c ('l')    |
        +---------------+---------------+---------------+---------------+
     144| 0x65 ('e')    | 0x67 ('g')    | 0x65 ('e')    | 0x73 ('s')    |
        +---------------+---------------+---------------+---------------+
     148| 0x22 ('"')    | 0x3a (':')    | 0x5b ('[')    | 0x5d (']')    |
        +---------------+---------------+---------------+---------------+
     152| 0x7d ('}')    | 0x7d ('}')    | 0x7d ('}')    |
        +---------------+---------------+---------------+
        Total 155 bytes (24 bytes header and 131 value)

    Field        (offset) (value)
    Magic        (0)    : 0x83 (ServerResponse)
    Opcode       (1)    : 0x02 (Authenticate)
    Key length   (2,3)  : 0x0000
    Extra length (4)    : 0x00
    Data type    (5)    : 0x01
    Status       (6,7)  : 0x0000 (Success)
    Total body   (8-11) : 0x00000083
    Opaque       (12-15): 0x00000000
    CAS          (16-23): 0x0000000000000000
    Data        (24-154): JSON payload containing:
        {
          "rbac":{
            "osbourne":{
              "buckets":{
                "default":["Read","SimpleStats","Insert","Delete","Upsert"]
              },
              "domain":"external",
              "privileges":[]
            }
          }
        }

## Authentication continuation response

SASL may require multiple roundtrips between the client and the server.
If continuation is required, the provider should reply with status
code set to AuthContinue and the following payload:

    {
      "response": "base64 encoded response to send to the client",
      "context" : "provider-generated token",
    }

`response` contains the base64 encoded data which should be returned
to the client as part of the SASL response.

`context` is a token that the provider wants back as part of the
next Authenticate (with `step` set to true)

## Unsuccessfull Authentication response

The provider should use the correct error code, and use the following
payload:

    {
      "error": {
        "context": "textual error context",
        "ref": "UUID client may search for"
      }
    }

In order to have memcached generate the correct audit events, the
following reasons codes should be used:

    KeyEnoent    If the user doesn't exist
    KeyEExists   If the user exists, but the password is incorrect
    AuthError    If the username and password combination is correct
                 but the user doesn't have access to the system

(These response codes don't need the above payload)

### Packet example for unknown user

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x83          | 0x02          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       4| 0x00          | 0x01          | 0x00          | 0x01          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      12| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      16| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      20| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
        Total 24 bytes

    Field        (offset) (value)
    Magic        (0)    : 0x83 (ServerResponse)
    Opcode       (1)    : 0x02 (Authenticate)
    Key length   (2,3)  : 0x0000
    Extra length (4)    : 0x00
    Data type    (5)    : 0x01
    Status       (6,7)  : 0x0001 (Not found)
    Total body   (8-11) : 0x00000000
    Opaque       (12-15): 0x00000000
    CAS          (16-23): 0x0000000000000000


### Packet example of incorrect password

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x83          | 0x02          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       4| 0x00          | 0x01          | 0x00          | 0x02          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      12| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      16| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      20| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
        Total 24 bytes

    Field        (offset) (value)
    Magic        (0)    : 0x83 (ServerResponse)
    Opcode       (1)    : 0x02 (Authenticate)
    Key length   (2,3)  : 0x0000
    Extra length (4)    : 0x00
    Data type    (5)    : 0x01
    Status       (6,7)  : 0x0002 (Data exists for key)
    Total body   (8-11) : 0x00000000
    Opaque       (12-15): 0x00000000
    CAS          (16-23): 0x0000000000000000


### Packet example of missing RBAC entry

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x83          | 0x02          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       4| 0x00          | 0x01          | 0x00          | 0x20          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      12| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      16| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      20| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
        Total 24 bytes

    Field        (offset) (value)
    Magic        (0)    : 0x83 (ServerResponse)
    Opcode       (1)    : 0x02 (Authenticate)
    Key length   (2,3)  : 0x0000
    Extra length (4)    : 0x00
    Data type    (5)    : 0x01
    Status       (6,7)  : 0x0020 (Auth failure)
    Total body   (8-11) : 0x00000000
    Opaque       (12-15): 0x00000000
    CAS          (16-23): 0x0000000000000000

## ActiveExternalUsers request

memcached will send `ActiveExternalUsers` command to to the external
auth provider at a configurable interval (configured through the
`active_external_users_push_interval` attribute in the configuration
file specified via the -C command line option) to notify them about
the external users which are currently active on the server.

The payload contains an array of the user currently logged into the
server. Ex: `["Foo","Bar"]`

The server ignores the reply to this message (and won't complain if
the authentication provider don't send any).

### Example

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x82          | 0x03          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       4| 0x00          | 0x01          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x0d          |
        +---------------+---------------+---------------+---------------+
      12| 0xde          | 0xad          | 0xca          | 0xfe          |
        +---------------+---------------+---------------+---------------+
      16| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      20| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      24| 0x5b ('[')    | 0x22 ('"')    | 0x46 ('F')    | 0x6f ('o')    |
        +---------------+---------------+---------------+---------------+
      28| 0x6f ('o')    | 0x22 ('"')    | 0x2c (',')    | 0x22 ('"')    |
        +---------------+---------------+---------------+---------------+
      32| 0x42 ('B')    | 0x61 ('a')    | 0x72 ('r')    | 0x22 ('"')    |
        +---------------+---------------+---------------+---------------+
      36| 0x5d (']')    |
        +---------------+
        Total 37 bytes (24 bytes header and 13 value)

    Field        (offset) (value)
    Magic        (0)    : 0x82 (ServerRequest)
    Opcode       (1)    : 0x03 (ActiveExternalUsers)
    Key length   (2,3)  : 0x0000
    Extra length (4)    : 0x00
    Data type    (5)    : 0x01
    Vbucket      (6,7)  : 0x0000
    Total body   (8-11) : 0x0000000d
    Opaque       (12-15): 0xfecaadde
    CAS          (16-23): 0x0000000000000000
    Value        (24-36): The textual string '["Foo","Bar"]'

## UpdateExternalUserPermission

The external auth providers may update the RBAC entry for a user by using
the `UpdateExternalUserPermission` command. The payload contains the full
updated RBAC definition for the user:

    {
        "user1": {
           "buckets": [
              "bucket1": ["Read", "Write", "SimpleStats"],
              "bucket2": ["Read", "SimpleStats"]
           ],
           "privileges": ["BucketManagement"],
           "domain": "external"
        }
    }

## GetAuthorization request

memcached will send `Authorize` to request the authorization data for
the user provided in the key. (no extras, no value).

## Successful GetAuthorization response

The provider should reply with status code Success.

    {
      "rbac": {
        "Users RBAC entry": ""
      }
    }

`rbac` contains the RBAC entry in the same format as in
[rbac.md](rbac.md#File-Format).

## Unsuccessfull GetAuthorization response

The provider should use the correct error code, and use the following
payload:

    {
      "error": {
        "context": "textual error context",
        "ref": "UUID client may search for"
      }
    }

### Packet example of missing RBAC entry

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| 0x83          | 0x02          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
       4| 0x00          | 0x01          | 0x00          | 0x20          |
        +---------------+---------------+---------------+---------------+
       8| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      12| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      16| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
      20| 0x00          | 0x00          | 0x00          | 0x00          |
        +---------------+---------------+---------------+---------------+
        Total 24 bytes

    Field        (offset) (value)
    Magic        (0)    : 0x83 (ServerResponse)
    Opcode       (1)    : 0x04 (GetAuthorization)
    Key length   (2,3)  : 0x0000
    Extra length (4)    : 0x00
    Data type    (5)    : 0x01
    Status       (6,7)  : 0x0020 (Auth failure)
    Total body   (8-11) : 0x00000000
    Opaque       (12-15): 0x00000000
    CAS          (16-23): 0x0000000000000000
