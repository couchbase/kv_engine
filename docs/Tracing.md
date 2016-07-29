# Event Tracing in KV-Engine

Memcached utilises [Phosphor](http://github.com/couchbase/phosphor) to achieve
high performance event tracing.

Memcached is explicitly tooled using trace macros which log timestamps and
some associated metadata (categories, names, arguments). An example of such a
trace macro is given below:

    TRACE_EVENT("category", "name", 12345, 67.89);

The full selection of trace macros is documented in the
[Phosphor header file](https://github.com/couchbase/phosphor/blob/master/include/phosphor/phosphor.h).

## Enabling Tracing

Tracing can be enabled in a basic mode through the use of the
`PHOSPHOR_TRACING_START` environment variable. An example in conjunction with
the memcached testapp is shown here:

     PHOSPHOR_TRACING_START="save-on-stop:testapp.%p.json" ./memcached_testapp --gtest_filter="*GetSetTest*"

This will run the memcached Get/Set tests and dump a file in the form
'testapp.<pid>.json' into the current directory each time the memcached daemon
exits. This json file can then be loaded into the Google chrome trace viewer
(chrome://tracing).

There are several semi-colon (';') separated options that the environment
variable accepts:

- save-on-stop: Save to a file when tracing stops, accepts %p (pid) and %d
(timestamp) placeholders.
- buffer-mode: Accepts one of 'ring' or 'fixed' for a buffer which either
overwrites itself when full or stops tracing when full.
- buffer-size: The size of the trace buffer in bytes to be created
- enabled-categories: A comma-separated list of categories to be enabled. This
supports basic globbing '*' and '?'.
- disabled-categories: A comma-separated list of categories to be explicitly
disabled (This mask is applied after the enabled-categories mask).

## Tracing Categories

The convention is followed that memcached categories are prefixed with
'memcached/' and ep-engine categories are prefixed with 'ep-engine/'. This
ensures no collisions and allows all categories in a component to be enabled
with a wild card (e.g. 'memcached/*').

- memcached/state_machine: Events related to the running of MCBP connection
state machinery including the current state and the execution of a specific
connection.
