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

SCRAM-SHA512 and SCRAM-SHA256 is not supported on all platforms.

### PLAIN

The PLAIN authentication allows users for authenticating by providing
the username and password in plain text. The server does however not
store the plain text password, so we'll calculate a salted HMAC hash
of the password and compare with what we've got stored internally.

## Server

The server should be initialized by calling `cbsasl_server_init` and shut
down by calling `cbsasl_server_term`.

## Internal User database

The user database is stored in JSON format with the following syntax:

    {
        "@@version@@": 2
         "username" : {
             "hash": {
                 "algorithm": "[argon2id/SHA-1]",
                 "hash": "base64 encoded salted hash of the password",
                 "memory": memory-cost,
                 "parallelism": parallel-cost,
                 "salt": "base64 encoded salt"
                 "time": time-cost,
             },
             "scram-sha-512" : {
                 "server_key" : "base64 encoded ",
                 "stored_key" : "base64 encoded",
                 "salt": "base64 encoded salt"
                 "iterations" : iteration-count
             },
             "scram-sha-256" : {
                 "server_key" : "base64 encoded ",
                 "stored_key" : "base64 encoded",
                 "salt": "base64 encoded salt"
                 "iterations" : iteration-count
             },
             "scram-sha-1" : {
                 "server_key" : "base64 encoded ",
                 "stored_key" : "base64 encoded",
                 "salt": "base64 encoded salt"
                 "iterations" : iteration-count
             }
         }
     }

The `hash` entry may currently only use two different algorithm
(and the allowed attributes depend on the algorithm)

*Argon2id*:

     "hash": {
         "algorithm": "argon2id",
         "hash": "base64 encoded salted hash of the password",
         "memory": memory-cost,
         "parallelism": parallel-cost,
         "salt": "base64 encoded salt"
         "time": time-cost,
     }

`parallelism` *must* be set to 1

*SHA-1*:

     "hash": {
         "algorithm": "SHA-1",
         "hash": "base64 encoded salted hash of the password",
         "salt": "base64 encoded salt"
     }
