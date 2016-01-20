# CBSASL - Couchbase SASL implementation

cbsasl provides a minimalistic client and server implementation of SASL
authentication. It tries to be "source compatible" with the SASL implementation
provided on MacOSX and Solaris (except that we've prefixed our methods and
constants with "cb")

## Supported authentication mechanisms

### SCRAM-SHA512, SCRAM-SHA256 and SCRAM-SHA1

The SCRAM-SHA implementation is a minimum implementation and is not fully
compliant with the RFC, but "good enough" for our usage. The current
implementation does not perform the SASLPrep on the username as specified
in https://www.ietf.org/rfc/rfc4013.txt, but we don't really need to
given the username/password restrictions in Couchbase.

SCRAM-SHA512 and SCRAM-SHA256 is not supported on all platforms.

### CRAM-MD5 (Deprecated in Watson)

CRAM-MD5 authentication is deprecated as of Watson and will most likely be
removed in Spock

### PLAIN (Deprecated in Watson)

PLAIN authentication is deprecated as of Watson and will most likely be
removed/disabled in Spock

## Server

The server should be initialized by calling `cbsasl_server_init` and shut
down by calling `cbsasl_server_term`.

### User database

The current implementation of the user database is stored in a file passed
to the system through the environment variable named `ISASL_PWFILE`. This is
a plain text file with the following syntax

    # lines starting with a hash is ignored
    username plain-text-password

Given that we don't store the SHA of the password the salt and the SHA's for
the password is generated when the file is loaded (with a fixed iteration
count set to 4k).
