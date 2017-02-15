# Environment variables

The memcached project use environment variables to tune the behaviour
of the system (both for test programs and in production).

This document tries to list all the environment variables in use, and what
they're used for.

## `BADMALLOC_FAILURE_RATIO`
This is a floating point and may be used to configure how often
`malloc()` / `realloc()` should return NULL.

This setting is only used if the `BadMalloc` allocator hook is enabled.

## `BADMALLOC_GRACE_PERIOD`

This is an unsigned integer used to allow the first N calls to
`malloc()` / `realloc()` to succeed.

This setting is only used if the `BadMalloc` allocator hook is enabled.

## `BADMALLOC_SEED`

This is an unsigned integer used to specify a fixed seed for the
random number generator which controls when to fail.

This setting is only used if the `BadMalloc` allocator hook is enabled.

## `CBAUTH_SOCKPATH`

The `CBAUTH_SOCKPATH` is used to tell `cbsasl` the location of `saslauthd`
to allow external authentication.

## `CBSASL_PWFILE`

`CBSASL_PWFILE` is used to tell `cbsasl` the location of the password
database in JSON format.

## `CONFIG_TEST_MOCK_MALLOC_FAILURE`

This variable is used by a unit test to have the config library
simulate a memory allocation failure.

## `CONFIG_TEST_MOCK_SPOOL_FAILURE`

This variable is used by a unit test to have the config library
simulate a failure reading stuff from disk.

## `COUCHBASE_CBSASL_SECRETS`

This variable should contain a JSON description of some secrets
which may be used in order to decrypt the CBSASL files. The
JSON looks like:

    {
        "cipher" : "the cipher to use",
        "key" : "base64 encoded key",
        "iv" : "base64 encoded iv"
    }

## `COUCHBASE_FORCE_ENABLE_XATTR`

May be used to enable support for extended attributes even if
the configuration file provided to memcached lists it as disabled.

## `COUCHBASE_ENABLE_PRIVILEGE_DEBUG`

By setting this environment variable all privilege checks returns
success. This mode should _NEVER EVER_ be used in production, and
is only intended as a tool to be used during development of the
correct roles. By enabling this you may start an application
without any privileges, and then look in the logs to see which
privileges it would need in order to perform the action successfully.

## `COUCHBASE_PACKET_DUMP`

Some of the command line tools will generate a packet dump if this
variable is set.

## `COUCHBASE_SSL_CIPHER_LIST`

Specify the list of ciphers allowed.

## `COUCHBASE_I_DONT_TRUST_SSL`

If set to a non-null value we don't limit the SASL mechanisms
to use over SSL to PLAIN

## `ISASL_PWFILE`

`ISASL_PWFILE` is used to tell `cbsasl` the location of the password
database in the old (deprecated) format.

## `MEMCACHED_CRASH_TEST`

Variable set to identify that we're running the crash test (and allows)
loading of the crash engine.

## `MEMCACHED_NUM_CPUS`

The number of CPU's to use for frontend threads.

## `MEMCACHED_NUMA_MEM_POLICY`

The NUMA memory to use.

## `MEMCACHED_PARENT_MONITOR`

If set it should contain the process identifier of a process to monitor
the existence of. If that process dies, `memcached` should die.

## `MEMCACHED_PORT_FILENAME`

If set, memcached writes the port number to all of the ports it binds
to during startup. This allows for memcached to start without a single
known port.

## `MEMCACHED_TOP_KEYS`

Should be set to the number of "top keys" memcached should collect
information about

## `MEMCACHED_UNIT_TESTS`

Set to indicate that we're running unit tests

## `RUN_UNDER_VALGRIND`

Set to indicate that we're running under valgrind

## `T_MEMD_INITIAL_MALLOC`

Used to fake a memory allocation in "default_bucket" (should not be used)

## `T_MEMD_SLABS_ALLOC`

Used to tell "default_bucket" to preallocate memory (should not be used)

## `TESTAPP_ATTEMPTS`

May be used to tell `engine_testapp` to retry failed tests.

## `TESTAPP_ENABLE_COLOR`

May be set to let `engine_testapp` use colors in its output.
