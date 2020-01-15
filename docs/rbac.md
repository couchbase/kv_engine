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

The RBAC database is specified in a JSON object where the keys
is the name of the user, and the value is the users RBAC entry.

### User object

The user value is an object containing the following keys:

* `buckets` An object containing all of the buckets the user have
          access to. The keys in the object is the name of the bucket
          and the depending on the value type it may be an array
          of privileges for the bucket _or_ a bucket object (see
          below)
* `domain` The domain the user is defined within. May be "external"
           "local"
* `privileges` an array containing all of the privileges not related
               to any bucket operations (management etc).

It is possible to specify "*" for the bucket name indicating all buckets.
The system will first search for an exact match for a bucket and only
use the wildcard entry if there isn't an exact match.

### Bucket object

The bucket object is an object containing one of the following
keys:

* `scopes` An object containing all of the scopes the user have
           access to. The key is the scope id, and the value is
           a scope object (see below).

* `privileges` The privileges the user have to this bucket

The reason why only one of these should be set is because if a
scope is defined the privilege check happens within the given
scope.

### Scope object

The scope object is an object containing one of the following
keys:

* `collections` An object containing all of the collections the
                user have access to. The key is the collection id,
                and the value is a collection object (see below).

* `privileges` The privileges the user have to this scope

The reason why only one of these should be set is because if a
collection is defined the privilege check happens within the given
collection.

### Collection object

The collection object is an object containing the following
keys:

* `privileges` The privileges the user have to this collection


### Example containing collections

    {
      "user1": {
        "buckets": {
          "bucket1": {
            "privileges": ["Read"]
          },
          "bucket2": {
            "scopes": {
              "1": {
                "privileges": ["Read"]
              }
            }
          },
          "bucket3": {
            "scopes": {
              "1": {
                "collections" : {
                  "1" : {
                    "privileges": ["Read"]
                  }
                }
              }
            }
          }
        },
        "privileges": ["BucketManagement"],
        "domain": "local"
      }
    }

In the example above a user named `user` is defined with:

* read access to all scopes and collection in `bucket1`
* read access to all collections within scope `1` in `bucket2`
* read access to collection `1` in scope `1` in `bucket3`
* `BucketManagement` on the node

The user is a locally defined user.

### Example without scopes

    {
        "user1": {
           "buckets": {
              "bucket1": ["Read", "Write", "SimpleStats"],
              "bucket2": ["Read", "SimpleStats"]
           },
           "privileges": ["BucketManagement"],
           "domain": "local"
        }
    }

In the example above a user named `user1` is defined with access the buckets
named `bucket1` and `bucket2`. For each of those two buckets there is an
array of privileges allowed in that bucket. In addition to that the user
have access to the `BucketManagement` privilege. The user is a locally
defined user.

## Privilege debug

It is possible to enter a mode called privilege debug which would cause
the system to let all privilege checks succeed even if the user lacks
the requested privilege. This mode is for *DEVELOPMENT ONLY* where one
wants to figure out the minimal set of privileges an application would
need. One could start the application with an empty set of privileges,
and the system would log the missing privileges. These may so be assigned
to a profile and assigned to the user.

Privilege debug mode may be enabled in two different ways; via an
environment variable, or through the configuration file.

In the configuration file one needs to set `privilege_debug: true`,
which is a dynamic configuration value which may be toggled on/off
while the system is running (and takes effect immediately).

In order to enable it through an environment variable it has to be
done _before_ starting memcached:

    $ export COUCHBASE_ENABLE_PRIVILEGE_DEBUG=1
    $ ./cluster_run -n 1
