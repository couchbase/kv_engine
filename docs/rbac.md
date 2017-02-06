# RBAC

memcached offers fine grained access control based upon a privilege system
which may be used to build up different roles. Unlike systems like Trusted
Solaris we're not using the least privilege principle, so the full privilege
set is calculated for the user when they log in. The concept of "roles" is
not used, so a connection always holds the full privilege set the connection
may use (unless the connection performs an authentication step to another
user with more/less privileges).

## Connection lifecycle (with respect to privileges)

When a connection object is created it is associated with a "default"
privilege context object without any privileges. At this time the client
may only perform operations which doesn't require any privileges. At this
time it may authenticate to the system, and after a successful
authentication a new privilege context is created which reflects the current
username. The client may at this time select the bucket to use, and when
that happens the privilege context is recreated to reflect the access
defined for the current user in that bucket.

## Privilege checks

As part of the execution of a command the front-end checks if the
connections privilege context contains the privileges required to perform
the operation.

Some operations may be executed with and without privileges (and the
privileged version may perform more). In these cases the implementation
of the command itself performs the privilege check by using the following
part of the server API:

    if (!sapi->cookie.check_privilege(cookie, Privilege::MetaRead)) {
        return ENGINE_EACCESS;
    }

## Updating the privilege database

Updates to the privilege database is signalled to memcached by a
message sent to memcached to tell it to reload the privilege database.
Note: The file _must_ not be modified in the time window from asking
memcached to reload the file and until it answers back (the behavior
is undefined if you modify the file while it is being read).

Memcached associates an internal version number every time it reads
the file, and if the privilege context being used differs from the
current version of the database the privilege context is defined
to be stale and must be recreated. This check happens for every
operation, so the new privilege database takes effect for the next
operation being performed.

## File Format

The RBAC database is specified in JSON with the following syntax.

    {
        "user1": {
           "buckets": [
              "bucket1": ["Read", "Write", "SimpleStats"],
              "bucket2": ["Read", "SimpleStats"]
           ],
           "privileges": ["BucketManagement"],
           "type": "builtin"
        }
    }

In the example above a user named `user1` is defined with access the buckets
named `bucket1` and `bucket2`. For each of those two buckets there is an
array of privileges allowed in that bucket. In addition to that the user
have access to the `BucketManagement` privilege. The user is a `builtin`
user.
