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

## Authentication backends

### External authentication

External authentication is provided through the use of `saslauthd`.
In order to enable authentication through `saslauthd` the environment
variable `CBAUTH_SOCKPATH` must be set to the location of the unix
domain socket used to communicate with `saslauthd`.

`PLAIN` authentication is the only supported authentication mechanism
for external users (a user is considered as an external user if there
is no entry for the user in the internal user database (see below))

### Internal User database

The user database is stored in JSON format with the following syntax:

    {
         "users" : [
             {
                 "n" : "username",
                 "sha512" : {
                     "h" : "base64 encoded sha512 hash of the password",
                     "s" : "base64 encoded salt",
                     "i" : iteration-count
                 },
                 "sha256" : {
                     "h" : "base64 encoded sha256 hash of the password",
                     "s" : "base64 encoded salt",
                     "i" : iteration-count
                 },
                 "sha1" : {
                     "h" : "base64 encoded sha1 hash of the password",
                     "s" : "base64 encoded salt",
                     "i" : iteration-count
                 },
                 "plain" : "base64 encoded hex version of salt + HMAC
                            of plain text password"
             }
         ]
     }

The `plain` entry consists of a salt and the salted hash of the users
password. The first 16 bytes contains the salt followed by 20 bytes
of the SHA1 HMAC of the password with the salt.
