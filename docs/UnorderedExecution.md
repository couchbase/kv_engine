# Unordered Execution Mode

Initially all connections to memcached is in an ordered execution
mode. That means that the server completes one command _before_ it
starts executing the next command. This mode has a number of pros and
a number of cons, but the biggest problem is that one slow operation
blocks the entire pipeline of commands.

To work around that problem the client may toggle the connection into
unordered execution mode by using [HELO](BinaryProtocol.md#0x1f-helo)
command. When enabled the client tells the server that it is free
to optimize the execution order of all commands unless the
[barrier](BinaryProtocol.md#request-header-with-flexible-framing-extras")
 is specified for the command.

If the client send the following pipeline:

    cmd1
    cmd2
    cmd3 [barrier]
    cmd4
    
The server may start executing `cmd1` and `cmd2`, but has to wait
for _both_ commands to complete before it may start executing `cmd3`.
It *must* wait for `cmd3` to complete before it may start executing `cmd4`.

The server gives the client full freedom to do stupid things like:

    SET foo {somevalue}
    SET foo {someothervalue}
    APPEND foo {somethirdvalue}
    GET foo

(one may argue if this makes sense from an application perspective, and
depending on NMVB handling the order could have been changed before it
was sent on the wire (one foo could have been sent to the old node, and
we saw the new vbucket map before dispatching the second command etc. In
addition to that other clients could also operate on the same documents.
If the execution order matter the client may force the order by using
barriers).

NOTE: Unordered Execution Mode is mutually exclusive with DCP. You
can't enable unordered execution mode on a connection configured for
DCP, and you cannot start DCP on a connection set in unordered execution
mode.

The client may use the opaque field in the request to identify the
which request the response belongs to.

## Commands to be supported in the server (initially)

The following is a list of commands the client may expect that
the server may reorder (NOTE: the fact that a command isn't on
the list doesn't mean that it won't be reordered! All commands
needs to be whitelisted on the server to ensure that it is
safe (no side effects, shared state etc) to execute in
parallel. Once that is performed we _might_ start reordering
the command).

* Get (including quiet versions with and without key)
* Get Replica
* Get locked
* Get and touch
* Touch
* Unlock
* Incr / decr (including quiet versions)
* Delete (including quiet version)
* Add, Set, Replace, append, prepend (including quiet versions)
* Subdoc operations
