# Architectural Overview

_Insert high level diagram here_

Main components:
* memcached
* platform
* ep-engine

# Server

* a.k.a `memcached`
* Container process for buckets
* Manages front of house operations - incoming client connections, request, response.

## Startup and Initialization

## Connection management

### Threads

memcached uses a number of threads:

* The main thread, after initialization listens on all of the server
  sockets, and accepts a client connection which is then dispatched to
  the worker threads on a round robin model.

* The TAP thread is responsible for serving all TAP communication.

* The number of worker threads is specified with the `threads`
  parameter in the configuration. The worker threads are responsible for
  serving the clients requests, and the number of worker threads cannot
  be increased at runtime.

Some commands are implemented by dispatching separate threads to execute
the user's command. There is currently no checks trying to protect ourself
from clients trying to allocate too many threads (but the commands themselves
is not available to the average user. See RBAC.json)

* Bucket creation/deletion
* Reload of the SASL password database

### Connection object

The `Connection` class represents a Socket (it is used by both clients and
server objects). We should refactor the code so that it represents client
connections and have a separate object (and state machines) for a server
object.

The Connection object is bound to a thread object, and never changes (except
for clients running TAP, which is moved over to the TAP thread after the
initial packet...)

### Locking scheme

A client is bound to its worker thread when the client is created (let's ignore
the TAP threads, they're special and should hopefully die ;-)). When the
clients' event loop is triggered, it acquires the threads lock and runs the
entire state machine. This means that the thread can safely access any
resource available in the LIBEVENT thread object. Given that it already holds
a lock to a LIBEVENT thread object it is *NOT* allowed to try to acquire *ANY*
other thread locks. That would lead to a deadlock.

Given that all of the clients share the same set of worker threads, the
clients cannot block while waiting for a resource to become available. The
model in use is that we're expecting the engines to return EWOULDBLOCK and
we're expecting the client to run the task in a _different_ thread and call
`notify_io_complete` when the resource is available. The reason it has to
be a _different_ thread is that `notify_io_complete` WILL try to lock the
thread object where the connection is bound to (which would cause a deadlock
if called while holding a thread lock).

* libevent

## Client request / response

## Multi-tenancy (buckets)

# Engines

## ep-engine

## default engine

# Experimental and Test Engines

## crash

## ewouldblock
