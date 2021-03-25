# Unordered Execution Mode

By default all connections to memcached operate in an _ordered_ execution
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

The server _may_ start executing `cmd1` and `cmd2`, but has to wait
for _both_ commands to complete before it may start executing `cmd3`.
It *must* wait for `cmd3` to complete before it may start executing `cmd4`.

Unordered-execution for given requests isn't _guaranteed_. Typically the
server will only start executing a later command before an earlier (e.g.
`cmd2` before `cmd1` in the above example) if the earlier command cannot
be completed immediately. For example if `cmd1` is a GET for a key which is
non-resident, `cmd1` must wait for a background fetch to disk to occur before
it can be completed. With Unordered Exeuction, the server can start to execute
`cmd2` while it is waiting for the background fetch for `cmd1` to complete.

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

The client should use the opaque field in the request to identify the
which request the response belongs to.

## Commands to be supported in the server

The following is a list of commands the client may expect that
the server _may_ reorder. Note the commands below are not guaranteed to be
reordered - the server may not reorder ones it is capable of due to resource
limitations etc.

* Get
* Getk
* Set
* Add
* Replace
* Delete
* Increment
* Decrement
* Append
* Prepend
* Gat
* Touch
* EvictKey
* GetLocked
* UnlockKey
* GetReplica
* SubdocGet
* SubdocExists
* SubdocDictAdd
* SubdocDictUpsert
* SubdocDelete
* SubdocReplace
* SubdocArrayPushLast
* SubdocArrayPushFirst
* SubdocArrayInsert
* SubdocArrayAddUnique
* SubdocCounter
* SubdocMultiLookup
* SubdocMultiMutation
* SubdocGetCount
* SubdocReplaceBodyWithXattr

(Reference: `cb::mcbp::is_reorder_supported()`)
