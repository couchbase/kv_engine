# Unordered Execution Mode

Initially all connections to memcached is in an ordered execution
mode. That means that the server completes one command _before_ it
starts executing the next command. This mode has a number of pros and
a number of cons, but the biggest problem is that one slow operation
blocks the entire pipeline of commands.

To work around that problem the client may toggle the connection into
unordered execution mode by using [HELO](BinaryProtocol.md#0x1f-helo)
command. When enabled the client tells the server that it is free
to optimize the execution order of commands with
[reorder](BinaryProtocol.md#request-header-with-flexible-framing-extras")
specified. If the client send the following pipeline:

    cmd1
    cmd2 [reorder]
    cmd3 [reorder]
    cmd4
    
The server must execute `cmd1` _before_ it may start execution of
`cmd2`. The server may execute `cmd2` and `cmd3` in any order it
like (even in parallel), but it has to wait until both commands is
completed before it may start executing `cmd4`.

The server gives the client full freedom to do stupid things like:

    SET foo {somevalue} [reorder]
    SET foo {someothervalue} [reorder]
    APPEND foo {somethirdvalue} [reorder]
    GET foo [reorder]

NOTE: Unordered Execution Mode is mutually exclusive with DCP. You
can't enable unordered execution mode on a connection configured for
DCP, and you cannot start DCP on a connection set in unordered execution
mode.

The client may use the opaque field in the request to identify the
which request the response belongs to.

## Commands to be supported in the server (initially)

The following is a list of commands the client may use
with the reorder flag set and if something weird happens
it should be considered an ERROR in the server and a bug
report should be filed.

* Get (including quiet versions with and without key)
* Get Replica
* Get locked
* Get and touch
* Touch
* Unlock
* Incr / decr (including quiet versions)
* Delete (including quiet version)
* Add, Set, Replace, append, prepend (including quiet versions)
