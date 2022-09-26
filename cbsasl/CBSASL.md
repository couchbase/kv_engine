# CBSASL - Couchbase SASL implementation

cbsasl provides a minimalistic client and server implementation of SASL
authentication. It tries to be "source compatible" with the SASL
implementation provided on MacOSX and Solaris (except that we've
prefixed our methods and constants with "cb")

## Supported authentication mechanisms

### SCRAM-SHA512, SCRAM-SHA256 and SCRAM-SHA1

The SCRAM-SHA implementation is a minimum implementation and is not fully
compliant with the RFC, but "good enough" for our usage. The current
implementation does not perform the SASLPrep on the username as specified
in https://www.ietf.org/rfc/rfc4013.txt, but we don't really need to
given the username/password restrictions in Couchbase.

### PLAIN

The PLAIN authentication allows users for authenticating by providing
the username and password in plain text. The server does however not
store the plain text password, so we'll calculate the appropriate hash
of the provided password and compare with the hashed version on the server.

## Server

The server should be initialized by calling `cbsasl_server_init` and shut
down by calling `cbsasl_server_term`.

## Internal User database

The user database is stored in JSON format with the following syntax:

    {
        "@@version@@": 2
         "username" : {
             "hash": {
                 "algorithm": "[argon2id/SHA-1/pbkdf2-hmac-sha512]",
                 "hash": "base64 encoded salted hash of the password",
                 "hashes": [ "base64 encoded salted hash of the password 1",
                             "base64 encoded salted hash of the password 2",
                             ...],
                 "memory": memory-cost,
                 "parallelism": parallel-cost,
                 "salt": "base64 encoded salt"
                 "time": time-cost,
                 "iterations": iteration-count
             },
             "scram-sha-512" : {
                 "hashes" : [ {"server_key": "b64 enc", "stored_key" : "b64 enc" },
                              {"server_key": "b64 enc", "stored_key" : "b64 enc" },
                              ... ],
                 "salt": "base64 encoded salt"
                 "iterations" : iteration-count
             },
             "scram-sha-256" : {
                 "hashes" : [ {"server_key": "b64 enc", "stored_key" : "b64 enc" },
                              {"server_key": "b64 enc", "stored_key" : "b64 enc" },
                              ... ],
                 "salt": "base64 encoded salt"
                 "iterations" : iteration-count
             },
             "scram-sha-1" : {
                 "hashes" : [ {"server_key": "b64 enc", "stored_key" : "b64 enc" },
                              {"server_key": "b64 enc", "stored_key" : "b64 enc" },
                              ... ],
                 "salt": "base64 encoded salt"
                 "iterations" : iteration-count
             }
         }
     }

To allow for password rotation without being unavailable for some services
while getting the new password the `hashes` array contains the old and new
password during the migration window for password rotation.

The top level `hash` entry controls the legal attributes:

*Argon2id*:

     "hash": {
         "algorithm": "argon2id",
         "hashes": [ "base64 encoded salted hash of the password 1",
                     "base64 encoded salted hash of the password 2",
                     ...],
         "memory": memory-cost,
         "parallelism": parallel-cost,
         "salt": "base64 encoded salt"
         "time": time-cost,
     }

`parallelism` *must* be set to 1

*SHA-1*:

     "hash": {
         "algorithm": "SHA-1",
         "hashes": [ "base64 encoded salted hash of the password 1",
                     "base64 encoded salted hash of the password 2",
                     ...],
         "salt": "base64 encoded salt"
     }

*pbkdf2-hmac-sha512*:

     "hash": {
         "algorithm": "pbkdf2-hmac-sha512",
         "hashes": [ "base64 encoded salted hash of the password 1",
                     "base64 encoded salted hash of the password 2",
                     ...],
         "iterations": iteration-count,
         "salt": "base64 encoded salt"
     }
