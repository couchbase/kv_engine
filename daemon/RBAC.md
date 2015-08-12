# Role Based Access Control in Memcached

`memcached` implements the principle of least privilege by
adding support for Role Based Access Control (RBAC).

## High level description

The RBAC in memcached is built-up by using "privilege sets". The
set is initialized when the user logs in, and is used until the user
logs out, or explicitly asks it to be changed (by assuming/dropping a
role).

Each operation requires one or more privileges in order to be
successfully executed. The individual privileges are grouped together
in a profile, and these profiles are then assigned to the users.

To make life easier for administrators they may create
roles. For each role they can assign profiles and grant privileges
to other resources, (such as other roles and buckets). An individual
user can then be granted access to the role. This allows administrators
to update just the role rather than updating a high number of users
(with the high risk of forgetting one, or doing something wrong).

## File format

The RBAC configuration is specified through a single configuration file
containing 3 different sections: `profiles`, `roles` and `users`. Each
section contains an array specifying a number of entries:

    {
        "profiles": [
           ... profile entries ...
        ],
        "roles": [
           ... role entries ...
        ],
        "users": [
           ... user entries ...
        ]
    }

### Profile entry

The profile entry is built-up with the following content:

    {
        "name" : "text",
        "description" : "text",
        "memcached" : {
            "opcode" : [
               ... list of commands included in the profile ...
            ]
        }
    }

The `name` attribute is the name of the profile and must be unique
among all of the profile names. It is referenced from the `role` and
`user` entries.

The `description` attribute is a textual description describing the
purpose of the profile. The system does not use this field for anything;
it is solely used for the end user.

The `memcached` attribute specifies the various attributes specific
to memcached. This allow for cross-component profiles. The `memcached`
attribute consists of the following attribute: `opcode`.
`opcode` contains a list of command opcodes from the Memcached Binary Protocol
included in the profile. The opcode may either be specified with its numeric
value, or with the textual name. In addition you may use the special keywords:
`all` and `none`.

### Role entry

The role entry is built up with the following content:

    {
        "name" : "text",
        "buckets" : [ ... ],
        "profiles" : [ ... ],
        "roles" : [ ... ]
    }

The `name` attribute is the name of the role and is referenced
through the `roles` array for users and roles. This is the name
clients will assume in order to operate as this role.

The `buckets` attribute is an array of strings containing the
name of buckets the role has access to. This field is currently
ignored.

The `profiles` attribute is an array of strings containing the
names of the profiles being used by this role.

The `roles` attribute is an array of strings containing the
names of roles available for this role. Each of the roles
must be assumed in order to gain the execution privileges
available in the role.

### User entry

The user entry is built up with the following content:

    {
        "login" : "text",
        "buckets" : [ ... ],
        "profiles" : [ ... ],
        "roles" : [ ... ]
    }

The `login` attribute specifies the login name for the specific user.

The `buckets` attribute is an array of strings containing the
name of buckets the user has access to. This field is currently
ignored.

The `profiles` attribute is an array of strings containing the
names of the profiles being used by the user.

The `roles` attribute is an array of strings containing the
names of roles available for the user. Each of the roles
must be assumed in order to gain the execution privileges
available in the role.

## Todo's

* Should we allow for passing on a `security/rbac/` directory containing
a directory for `profiles.d`, `roles.d` and `users.d` where individual
new entries may be inserted?
