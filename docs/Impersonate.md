# Impersonate (aka "sudo")

Impersonate allows a user to execute a command as another user without
authenticating as that user (similar to `sudo` in a Unix system). The biggest
difference between `impersonate` and the `sudo` command is that the latter
allows for privilege escalation. Impersonate will run the command _as_ the
requested user, but the effective privilege set during the command execution
is the intersection of the authenticated users effective privilege set and
the impersonated users available privilege set.

## Requirements

* The connection must possess the `Impersonate` privilege
* The impersonated user must have access to Couchbase

### Limitations

* The impersonated user must be defined as a local user in Couchbase
* The users with the `impersonate` privilege may execute commands as
  _any_ user defined in Couchbase (you can't specify that "Bob" can
  impersonate "Alice" but not "Joan")
