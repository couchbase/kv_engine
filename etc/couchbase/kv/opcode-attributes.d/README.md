# Opcode attributes

It is possible to set attributes for the various commands used by
Couchbase. During startup `etc/couchbase/kv/opcode-attributes.json` is
read and applied, before all files in `etc/couchbase/kv/opcode-attributes.d`
is read and applied in alphabetical order. Finally the (optional) setting
provided by ns_server through `memcached.json` is applied.

Version 1 of the command attributes allows for specifying a threshold
for when memcached will report an operation as slow in the memcached
log files.

The format of these files is JSON and looks like:

    {
      "version": 1,
      "comment": "User supplied comment for the file (optional)"
      "default": {
        "slow": 500
      },
      "command-name": {
        "slow": 100
      },
    }

The version field is mandatory and dictates the format of the fields
the parser should expect (to allow us to modify the content and still
be able to read the files in an upgrade scenario).

The comment is optional and ignored by the parser.

The field "default" is optional and contains the entry when there
isn't specified a value.

The rest of the file should contain entries per command. Each of
these entries contains a single filed "slow" which contains the
threshold for that command (we might want to add other fields
later on).

If the value for slow is specified as a number, it is specified
in milliseconds. If the value is specified as a string it may
also contain one of the following specifiers:

    ns / nanoseconds
    us / microseconds
    ms / milliseconds
    s / seconds
    m / minutes
    h / hours

    ex:

     "compact_db": {
        "slow": "30 m"
     }
