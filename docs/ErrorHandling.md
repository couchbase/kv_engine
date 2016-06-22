# KV-Engine Error Handling Best Practices

Date: 2016-04-06

Author: Dave Rigby

## Introduction

KV-Engine is by definition the *system of record* for user data in
Couchbase Server. It is therefore essential that any errors
encountered at runtime are handled in such a way that (a) **Minimises
any potential data loss** and (b) **Maximises the availability of that
data**.

This document attempts to outline the different error scenarios which
may be encountered by KV-engine and what the best practices are in
handling each scenario.

## Table of Contents

* [General Error Handling Guidelines](#general-error-handling-guidelines)
* [Possible Error Responses](#possible-error-responses)
* [Classes of Errors](#classes-of-errors)
* [Implementation Guidelines](#implementation-guidelines)
* [Examples](#examples)
* [References](#references)
* [Appendix A: Future Enhancements](#appendix-a-future-enhancements)

## General Error Handling Guidelines

These guidelines apply to most error scenarios:

1. **Attempt to be as robust as possible**. In performing any
   operation, a good guideline is to be slow and cautious when
   starting a task (check all input data / preconditions before
   committing to anything), but once a task has been started go to
   whatever lengths are required to complete it (e.g. once data is
   queued to the disk queue try not to terminate without flushing the
   queue).

2. **Isolate errors from the rest of the system**. Related to (1),
   if an error occurs in one part of the system / for one user,
   attempt to minimise the effect the error has on the rest of the
   system / other users. For example: a failure of one user’s request
   should not affect other users; the failure of one vBucket shouldn’t
   affect other vBuckets. When using exceptions consider adding
   catch-all handler(s) at the boundaries of components to ensure that
   exceptions are not unnecessarily propagated.

3. **Validate any external input before using it**. Typically
   anything outside the immediate component / module should be
   considered as external and hence untrusted. Ensure that input
   values are checked for validity, file paths exist and are
   accessible, pointers are non-null before attempting to access them,
   etc.

4. **Ensure that non-fatal errors are appropriately recorded**. While
   the general theme of these guidelines is to maintain availability
   of the system, it is important that unexpected circumstances are
   appropriately logged, so we have a record of the issues reported by
   the system. (By default only NOTICE and upwards are printed to
   memcached.log):

    * Use `DEBUG` for debugging messages.

    * Use `INFO` for routine events which are expected to occur in
      normal use - e.g. client authenticating successfully.

    * Use `NOTICE` for significant, expected events - e.g. kv-engine
      clean startup/shutdown

    * Use `WARN` for unexpected, significant events - e.g. send
      failure to client connection.

5. **Prefer exceptions / error codes to assert() / abort()**. Related
   to (1) and (2), abort() and similar fatal errors give the rest of
   the system no say in how an error should be resolved. Exceptions
   provide much more flexibility; the caller can catch and handle if
   desired, but if they do not then the exception will propagate up
   the stack (potentially turning into a fatal error if not
   caught). Error codes are more efficient if the error is expected to
   be common (and the only option in C), but require that the caller
   checks for them, meaning they require much more rigor to ensure
   that errors are detected.

## Possible Error Responses

Before we describe in detail the different types of errors, it is
useful to summarise some of the possible error responses code can take
when an error is detected. Roughly ordered by severity:

1. **Handled Locally**. Current operation can resolve the issue
   locally, or the error is so benign that no alternative action is
   required. Depending on the error, it may still be logged.

2. **Return error code**. Stop the current operation, and return a
   non-success code back to the caller. The caller can then decide on
   what further action to take (if any). Typically used for expected
   but uncommon situations and/or when the current function does not
   have the ability to take further action and must be handled at a
   higher level.

3. **Raise exception**. Stop the current operation, and raise an
   exception for some code at a higher level to (potentially) catch,
   or terminate the program if not caught. Typically used for
   unexpected situations and/or when the immediate caller does not
   have the ability to take further action and must be handled at a
   higher level. *(Note: error codes are preferred to exceptions when
   "error" is common, as exceptions are more expensive to throw than
   simply returning an error code).*

4. **Clean shutdown**. Operation cannot continue, but the system is
   still in a state where we can (attempt) to flush outstanding disk /
   DCP queues, and otherwise quiesce the system before exiting.

5. **Immediate shutdown**. Operation cannot continue, and the system
   is in a known-bad or unknown state such that it may be unsafe to
   flush outstanding disk / replication queue data.

## Classes of Errors

Here is a brief taxonomy of *some of* the classes of errors we might
encounter in KV-engine, along with recommended methods of handling
them:

### ‘User’ Input Errors

Any input from "real", external users. In the context of kv-engine
this primarily consists of binary protocol messages from both normal
connections (SDKs, N1QL, DCP), and privileged connections (ns_server).

#### How to handle

All external input should be considered untrusted, and SHOULD be
*validated* before being accepted and passed down to other components.

1. Syntax errors which can be detected without any additional context
   (e.g. correct number of arguments, valid length, valid options etc)
   MUST be detected in memcached validator functions. Any errors found
   SHOULD result in returning an `EINVAL` response back to the client.

2. Semantic errors typically require more context than is present in
   the raw request (e.g. CAS mismatch requires we examine the current
   CAS value of an item). Errors SHOULD be checked for as early as
   possible, and return an appropriate status code back to the client
   (e.g. `EEXISTS` for a CAS mismatch).

3. Most binary protocol commands are request-response and are
   stateless between commands, so it is typically safe to just return
   an error for the request. For commands which are stateful (e.g
   DCP), it MAY be appropriate to also close the connection (after
   sending an appropriate status code) if the connection is in an
   inconsistent state.

4. Privileged connections should be treated the same as user
   connections for the purposes of validation - while we in general do
   trust ns_server; we cannot preclude bugs in it and so should still
   validate all commands it sends us.

See
[protocol_binary.h](https://github.com/couchbase/memcached/blob/master/include/memcached/protocol_binary.h)
for the list of available status codes.

### Resource Errors

Any errors arising from failure to acquire the resources kv-engine
needs. For example: memory allocation failure, insufficient disk
space, exhausting available file descriptors, etc.

Resource errors can occur in different places in the system
(e.g. frontend memcached, mid-tier HashTable, backend
storage). Additionally they may be temporary (e.g. memory allocation
failures may resolve once more ejection has completed), or
(semi-)permanent - disk space is unlikely to reduce by itself.

#### How to handle

Resource errors should be handled differently depending on where in
the system they are detected, and if they are likely to be temporary
or permanent:

**For temporary resource errors**, try to constrain the error to the
  client / operation in progress. Where it is possible to explicitly
  communicate the error upwards (e.g. a client request could not be
  completed due to not enough memory for result buffer), stop the
  current operation and return a suitable error code
  (e.g. `ENOMEM`). This is particularly suitable if the caller can
  retry the operation (hopefully after some kind of delay).

Where possible try to reduce the usage of the resource which has been
exhausted - for example if no memory could be allocated for a request
try to reduce memory usage - for example consider shrinking buffers,
freeing temporary resources, closing idle connections etc.

**For permanent resource errors**, if may be the case that no more
  forward progress can be made, and hence that we must make use of the
  larger-scale RAS features built in the product
  (e.g. failover). Consider what the most graceful way to fail is -
  instead of abort()ing (and losing all data in any queues), can we
  instead attempt to shutdown, flushing any existing data to disk /
  DCP consumer before terminating)?

### Data Errors

This category encompass errors which occur when kv-engine attempts to
access its data files, such as corrupted / missing data, or failure to
read / write. Data *files* includes:

1. Actual user data (i.e. couchstore / forestdb files)

2. Log files (`memcached.log`, `auditd.log`)

3. Configuration files (`memcached.json`).

Data can also refer to internal, intermediate data stored for example
in the HashTable, disk queues or caches.

#### How to handle

The type of file affected will influence the way the error is handled:

**Configuration files**: Some configuration files
  (e.g. memcached.json) are likely to be mandatory, and operation
  simply cannot proceed if they cannot be opened at startup so it is
  appropriate to treat them as a fatal error. However once the system
  is running, failure to later open a changed config file should NOT
  cause memcached to terminate.

**Log files**: Handling of log files will typically be similar to
  config files - while not strictly *as* essential as configuration
  files, log files are extremely useful to reason about the behaviour
  of a system and hence failing to open/create them at startup should
  result in a fatal error. However as per configuration files, if a
  log file later becomes inaccessible (e.g. unable to write) then the
  system SHOULD attempt to continue operation.

**User data files**: These are the most critical files in the
  system. Care should be taken to ensure that they remain error-free,
  however if errors *do* occur the system should attempt to maximise
  the data which is available.

* *If a failure occurs when attempting to access data, consider if an
  alternative version exists.* For example, in couchstore document may
  be locatable using a different (older) checkpoint header - if the
  seqno of a document is known this would allow access to the document
  which otherwise would have been lost. Note this shouldn’t replace or
  prevent notifying ns_server of the data-file issue.

* *Error handling within storage libraries (couchstore, forestdb)
  should be as thorough as possible.* The appropriate response to an
  error in the storage layer will frequently need to be determined by
  the user of that (ep-engine, 2i) and hence error conditions should
  be typically propagated up to the application using them, via error
  codes or exceptions. *It should only be in extreme circumstances
  that a storage engine should trigger a fatal error (i.e. `abort()`)
  itself*. Even *if* the storage layer itself is in a situation which
  it cannot continue at all, other parts of the system (e.g. DCP) may
  have the chance to move data off-node.

**Internal Data Structures**: Errors in the data contained in internal
  structures like ep-engine HashTables, DCP queues or similar are
  potentially difficult to handle, as at the point the erroneous data
  is detected it can be hard to determine how widespread it
  is. Consider the extent of the error - if only a single entity
  (e.g. SDK connection, DCP consumer) is affected then it may be
  possible to only shutdown that one connection. If the problem
  affects all clients (e.g. a vBucket data in memory is corrupt) then
  clearly kv-engine cannot continue in a reliable fashion and a
  termination (immediate or graceful) should be considered.

### Logic Errors

A.k.a "The program should never get into this state". This category
covers things like (supposedly) unreachable code or exceeding
implementation-defined limits.

#### How to handle

Logic errors vary considerably, and so handling will likely be very
instance-specific. Here are a few common examples and suggested ways
to handle:

**Unhandled switch case errors** - where a switch statement encounters
  an unexpected (default) case. Avoid using a fatal error in the
  default case, as that gives the caller no choice in how the error
  should be handled. Raising of an exception (C++) or returning an
  explicit error code (C) should be preferred.

Suitable exceptions to raise: `std::logic_error` or
`std::invalid_argument`, or a subclass thereof.

**Invalid arguments** - Where a function / class is passed invalid
  values. These should be explicitly checked, raising a suitable
  exception if invalid.

Suitable exceptions to raise: `std::invalid_argument` or subclass.

**Arithmetic / computational errors**: Where the result of a
  computation is not as expected, for example of out of range /
  unexpected NaN or zero.

Suitable exceptions to raise: `std::range_error`, or
`std::overflow_error`, `std::underflow_error`.

## Implementation Guidelines

This section describes specific implementation details (i.e. actual
C++ code) on how to implement the recommendations given.

### Throwing exceptions

1. When creating exceptions to throw, ALWAYS inherit from
   [std::exception](http://en.cppreference.com/w/cpp/error/exception). This
   gives a consistent interface to obtain the description (`what()`),
   and provides a common base-class for all exceptions used. All
   exceptions generated by the standard library inherit from
   `std::exception`.

2. The `what()` message should be sufficiently descriptive to identify
   the source of the exception. Include the class/method name, or
   other uniquely identifying information.

The Boost
[Error and Exception Handling](http://www.boost.org/community/error_handling.html)
page is a good reference for exception handling best practices.

### Catching exceptions

1. Catch the most explicit exception type you expect. It’s better to
   propagate an exception your code isn’t expecting (and let a higher
   level handle it) than incorrectly handle it.

## Examples

#### Unhandled switch statement case

```C++
bool found = false;
switch (enumVar) {
case CmdStat::TOTAL_RETRIVAL:
    found = true;
    break;
...
-default:
-    cb_assert(false);
+}
+
+if (!found) {
+    throw std::invalid_argument("Class::method: enumVar (which is " +
+        std::to_string(int(enumVar)) + " is not a valid EnumType");
+}
```

Replace the `default: assert(false)` case with a check if the enum
being switch on was found, if not raise a `std::invalid_argument`
exception.

The example above uses an explicit `found` flag, alternatively if each
case sets a variable then check for the variable still being at it’s
default value (e.g. `-1`/`nullptr`). Another variation (if all useful
work is done in the switch statement) is to return at the end of each
case statement, then unconditionally throw an exception if
control-flow reaches the end of the switch.

## References

* The Boost
  [Error and Exception Handling](http://www.boost.org/community/error_handling.html)
  page.

## Appendix A: Future Enhancements

This section exists to capture *potential* future improvements to
kv-engine error handling. Note these are outside the scope of
*current* kv-engine error-handling.

### Automatic bucket-level restart

[JimWalker]: I have an idea which may work for some of these classes,
another "error-tool" for us to consider. If the logic error is on the
data path of a bucket, we restart the bucket (and warmup). Global to
ep-engine is some restart counter and we basically allow a certain
amount of restarts, so if the logic error keeps happening we will say
"ok enough, fall back to a more heavy-weight error handler". There
could also be some time based heuristic which says if 1 sec/min/hour
goes by, reset the buckets restart counter.

* [DJR]: This would likely need additional communication with
  ns_server for it to decide if it’s better to re-attempt a
  (potentially long) warmup or just to failover the node and promote a
  replica.*

### Bucket (or vBucket) level failover

[DavidH]: Applying the principle of isolation, If this
[data corruption] only affects a single vbucket, perhaps there should
be the means to perform a failover at that granularity.

If it implies some greater underlying problem (dodgy RAM / hardware)
then agreed best move is to fail the node.

*[DJR]: This would require collaboration with ns_server.*

### Improved Data Availability

[DavidH] *(in the context of user data files, and the importance in
maximizing what data which is availaible):*

This highlights the fact that we need a better recovery tool. If no
other version is available we should have the means to repair / make
sense of the existing one or recreate it. e.g. in non-DGM scenarios
the ability to say "write in-memory vbucket to new couchstore
file". Or make compaction more robust to corrupt files. "if I can't
read this btree header I'll use the previous one. If I can't read this
full document I'll log it but still read the remainder"

*[DJR]: Agreed. This is actually an area where we have regressed, as
 (IIRC) prior to 3.0 one could replace the data files underneath
 ep-engine without any issue.*

### Read-only mode

[DavidH] Completely o/t but the thought occurred so I'll jot it here -
is there any merit in having a 'read-only' mode for KV engine where it
rejects any updates (and possibly new connections) at the front-end
but still provides access to the data. e.g. in a scenario where
auto-failover is not possible because there are other nodes that are
down.

*[DJR] Quite possibly, but that would need a more elaborate / powerful
 error communication between kv-engine and ns_server. *

*For example kv-engine would have to communicate "hey, I can't write
 any more data", and ns_server could then make the decision of (a)
 terminating kv-engine (if replicas are available) or (b) telling it
 "ok, go into read-only mode).*
