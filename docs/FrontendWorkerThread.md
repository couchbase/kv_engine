# Frontend Worker Threads

The memcached binary use a given set of threads to perform network IO and
execute the commands received from the clients. These threads are
named [FrontEndThread](../daemon/front_end_thread.h) and is utilizing
libevent to implement an event driven framework and at the "core" it
is running the "event base loop" where libevent calls into the operating
system to fetch a set of sockets where an action (read/write) may
be performed. Given that the event driven nature these threads are
*not* allowed to block or run a long-running task as that would
affect all other connections served by the same thread.

libevent offers the base core functionality to let one operate on the
socket directly with read/write, but it also offers a higher level
of abstraction called "bufferevents" which offers a "stream" interface
with callbacks once data is sent/received from the socket (the actual
send / receive to the socket is hidden) and these streams may used
with and without TLS hiding all of these details from the send/receive
code throughout our code.

Another abstraction we're using on top of libevent is folly's EventBase
class which extends the functionality provided by libevents `event_base`
with some nice helper methods to execute functions in the same thread
which runs the event base. Under the cover this is implemented by
folly using a queue (with its own "event" added to libevent, which
drains the queue with a certain number of events; default 10). It
drives its own event loop, but includes a call to libevents
base_event_loop (with EVLOOP_ONCE) within its loop.

## Connection

Each connection use a single bufferevent structure registered in the
event base for notification. We utilize bufferevents read watermark
to reduce the number of notifications into our code to up to two
when we don't have the next frame available. Initially the read
watermark is set to the fixed 24 byte header of a packet. In that
callback we would check if we have the rest of the frame available,
and in the case we don't have the rest of the frame available we'll
set a new watermark for the entire packet. Please note that these
watermarks do *NOT* stop libevent from reading more data from the
kernel, but how often it'll notify the bufferevent.

Once the entire packet is available we'll start by validating the
packet (making sure that it contains the required fields etc) before
we start executing the packet. If execution would have been blocked
by having to go to disk (or connect to another node) for some reason
that operation is handed over to another thread and the read events
for the connection is *disabled* and we stop executing the command.
The command is resumed once the thread we scheduled the task to
completes and use the "run in thread" feature in folly's EventBase
mentioned above which marks the cookie as ready and tries to
notify the schedule a new "bufferevent notification" to resume
execution (if none is not set already). (This is a
simlified story without all the tiny details ;))

The reply isn't sent *immediately* to the socket when we try
to send data from our code to the bufferevent. It is scheduled
to be sent within the bufferevent code (using other write callbacks
from libevent draining that queue, and it is sent in chucks of 16k
bytes (the default size of the internal buffer used by libevent).
Once all data is transferred to the kernel space a "WRITE" event is
triggered for the connection.

The connection use two different ways to send data: Copy or pass
a reference to data. When we copy data to the bufferevent we
use `bufferevent_write` which allocates space internally in
and copies the data we want to send to that space and chains
it internally. This is the typical use pattern for us, but in
some cases where we already have the data in a buffer we may
pass on a unique_ptr to a "[SendBuffer](../daemon/sendbuffer.h)"
where we provide a "view" to the data we want to send and a callback
method libevent should call once all the data is sent.
