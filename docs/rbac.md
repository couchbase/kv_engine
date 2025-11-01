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
the operation. The requested privilege is checked in the following
order:

0) "dropped privileges", if set return no access
1) "global" privilege mask
2) bucket privilege mask (for all bucket privileges)
3) scope privilege mask (for all collection aware privileges)
4) collection privilege mask

Some operations may be executed with and without privileges (and the
privileged version may perform more). In these cases the implementation
of the command itself performs the privilege check by using the following
part of the server API:

    if (!sapi->cookie.check_privilege(cookie, Privilege::MetaRead, sid, cid)) {
        return ENGINE_EACCESS;
    }

### PrivilegeAccess::Status

The privilege database provides a privilege check method that returns a status
for the requested privilege, in general a check is either successful or a
failure. However the PrivilegeAccess::Status is not a boolean type, it has
3 states, of which only one means success.

* Status::Ok - returned for a successful check
* Status::Fail
* Status::FailNoPrivileges

Fail - This status is returned for a failure, the user does not have the
requested privilege.

FailNoPrivileges - This status is returned for a failure, the user does not have
the requested privilege, they also have no privileges for the search "path".

This failure distinction is important for scope/collection privilege checks
where the user effectively provides a path to search, "bucket.scope.collection".
The caller of the privilege check can use the different failure status to fail
the request with the correct protocol error.

Fail maps to "no access". The user does not have the requested privilege but
does have some other privilege for the scope/collection - i.e. you can read but
not write, so the scope/collection is not invisible.

FailNoPrivileges maps to an error making the scope/collection invisible,
"unknown collection/scope". The user does not have the requested privilege and
has 0 privileges for the scope/collection - i.e. you can do nothing against the
scope/collection, so the scope/collection is invisible.

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
           access to. The key is the scope id written in hex
           preferred with a 0x prefix), and the value is
           a scope object (see below).

* `privileges` The privileges the user have to this bucket

The reason why only one of these should be set is because if a
scope is defined the privilege check happens within the given
scope.

### Scope object

The scope object is an object containing one of the following
keys:

* `collections` An object containing all of the collections the
                user have access to. The key is the collection id
                written in hex (preferred with a 0x prefix),
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

