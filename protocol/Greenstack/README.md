# Greenstack

Project Greenstack is intended to build a unified protocol all
components of Couchbase may utilize to communicate with and within the
cluster. The protocol is designed to address the shortcomings of the
memcached binary protocol, and still be simple and efficient. See the
one-pager for a full description of the rationale behind creating a
new protocol.

## High level description

The protocol is full duplex, meaning that both parties may send and
receive packets at all times. This differs from the memcached binary
protocol where we had a notion of a client and a server, where the
client would send requests to the server and the server would only
send responses to the requests. Being able to send notifications from
memcached to clients/ns_server is something we’ve missed from the
binary protocol. Examples for use cases could be:

* I'm starting to run out of memory, please slow down.. Today we're
  just accepting data until we hit a threshold and we start refusing
  stuff... we could have told the client earlier to back off..
* I got a new vbucket configuration map..
* I've initiated a shutdown of the bucket.. expect it to go away..
* I'm currently doing warmup, I’m done doing warmup
* Send messages back to ns_server for things to pop up in the UI (ex:
  we’ve had n number of incorrect logins the last minute, is the app
  misconfigured or is are we under “attack”)

Note: This means that client authors needs to be prepared for
receiving other packets than a response for their request when they
try to fetch the next frame off the network (and handle the command;
which could be nothing more than sending not supported/unknown
command)

Another difference from the memcached binary protocol is that the end
receiving the commands may process and send responses out of order (as
long as the fence bit isn’t set).

## On the wire description

Everything that flows on the wire belongs to a frame, which is built
up by a header, an optional extra header (referred to as Flex header)
and a body. The first header describes the packet, and contains all of
the information a simple proxy should have to look at. In the mandatory
header there is also an optional flex header that may be utilized to
build extended features or carry extra information. Finally there is a
body where the payload for the command should go. There is a design
decision to keep the header and the flex header within our own format,
and just use Google FlatBuffers/Protobuffers in the payload. This allows
proxies or command dispatcher to transparently do work with the frame
without having to decode the frame.

All values in the protocol are specified in network byte order.

### Frame

All data visible on the network belongs to a frame with the following
layout:

<table>
  <tr>
    <th># bytes</th>
    <th>Description</th>
  </tr>
  <tr>
    <td>4</td>
    <td>Frame length. This is the number of bytes in the frame body
        (specified as n in the table).</td>
  </tr>
  <tr>
    <td><em>n</em></td>
    <td>Frame body</td>
  </tr>
</table>

### Frame body

The frame body consist of the following layout.

<table>
  <tr>
    <th># bytes</th>
    <th>Description</th>
  </tr>
  <tr>
    <td>4</td>
    <td>Opaque</td>
  </tr>
  <tr>
    <td>2</td>
    <td>Opcode</td>
  </tr>
  <tr>
    <td>1*<em>n</em></td>
    <td>One or more flag bytes. See description below for the definition
        of the values (and how to determine the amount of flag bytes).</td>
  </tr>
  <tr>
    <td>[ 2 ]</td>
    <td>Status code (see flag description)</td>
  </tr>
  <tr>
    <td>[ 4 ]</td>
    <td>Flex header length (see flag description)</td>
  </tr>
  <tr>
    <td>[ n ]</td>
    <td>Flex header (see flag description)</td>
  </tr>
  <tr>
    <td>Rest</td>
    <td>Command payload</td>
  </tr>
</table>

The minimum size of a packet is 7 bytes for a request and 9 bytes for
a response (11 bytes and 13 including the frame header).

### Opaque

The opaque field is an array of 4 bytes the “client” in a request may
use as a personal reference to identify the request in the
response. The “server” for a request must provide the same value in
the response.

### Opcode

The opcode is the actual action requested. It is defined per component:

<table>
  <tr>
    <th>Start</th><th>Stop</th><th>Component</th>
  </tr>
  <tr>
    <td>0</td>
    <td>1023</td>
    <td>Generic</td>
  </tr>
  <tr>
    <td>1024</td>
    <td>2047</td>
    <td>Memcached</td>
  </tr>
  <tr>
    <td>2048</td>
    <td>3071</td>
    <td>Clients</td>
  </tr>
  <tr>
    <td>3072</td>
    <td>65k</td>
    <td>Unassigned</td>
  </tr>
</table>

All opcodes should be defined in a document with a description of the
opcode and its stability tag (volatile, uncommitted, committed). See
http://www.lehman.cuny.edu/cgi-bin/man-cgi?attributes+5 for a
description of volatile, uncommitted and committed (TODO we need to
adapt those terms to our own definition and update the document with
that)

### Description of flags

The flag section of the frame contains fields that is needed by
protocol parsers that otherwise don’t need to decode the flex
header. The flag section is defined in a future extensible way by
allowing additional flag bytes to be defined. There should however be
a justification for adding features as flags compared to adding it to
the flex header.

#### First flag byte

<table>
  <tr>
    <th>Bit</th>
    <th>Description</th>
  </tr>
  <tr>
    <td>0</td>
    <td>Type - If cleared this is a request, if set this is a response
        packet. For response packets a status code is present following
        the flag section</td>
  </tr>
  <tr>
    <td>1</td>
    <td>Presence of a flex header</td>
  </tr>
  <tr>
    <td>2</td>
    <td>Fence - All operations sent in the same lane (see flex header)
        prior to the presence of this command must be completed before
        the response for this packet is sent. Do not start processing
        more commands until this command is completed.</td>
  </tr>
  <tr>
    <td>3</td>
    <td>More - There will be more frames for this logical unit</td>
  </tr>
  <tr>
    <td>4</td>
    <td>Quiet - Do not send a response for this packet unless an error
        occurs</td>
  </tr>
  <tr>
    <td>5</td>
    <td>Unassigned</td>
  </tr>
  <tr>
    <td>6</td>
    <td>Unassigned</td>
  </tr>
  <tr>
    <td>7</td>
    <td>Presence of a next flag byte (none is currently defined)</td>
  </tr>
</table>

### Status code

The status code is a two byte value. It is defined per component with
the following range:

<table>
  <tr><th>Start</th><th>Stop</th><th>Component</th></tr>
  <tr><td>0</td><td>1023</td><td>Generic</td></tr>
  <tr><td>1024</td><td>2047</td><td>Memcached</td></tr>
  <tr><td>2048</td><td>3071</td><td>Clients</td></tr>
  <tr><td>3072</td><td>65k</td><td>Unassigned</td></tr>
</table>

The current set of status codes is defined in
<a href="include/libgreenstack/Status.h">include/libgreenstack/Status.h</a><

### Flex header length

The flex header length field is only present if the bit for the
presence of a flex header is set in the flags section. The flex header
length contains the number of bytes in the flex header.

### Flex header

The flex header allows for a future extensible way to pass arbitrary
information to each command.

#### Format

Each entry in the flex header contains the following three mandatory
attributes (the value may however be of 0 length).

<table>
  <tr>
    <th>Key (2 bytes)</th>
    <th>Length (2 bytes)</th>
    <th>Value (length bytes)</th>
  </tr>
</table>

#### Keys

The following keys are currently defined. The length field in the
table below defines the legal value for the length and *must* be
present even if it is specified as a fixed width. No knowledge of the
keys should be necessary in order to parse the flex header to pick out
a certain field.

NOTE: We won't implement all of these initially, they're added here
when we thought of them and they may be dropped or changed

<table>
<tr>
<th>Value</th><th>Key</th><th>Length</th><th>Description</th>
</tr>
<tr>
  <td>0x0000</td>
  <td>Lane ID</td>
  <td>Variable</td>
  <td>
      Specifies a logical channel (this information shall be present in
      a response). A logical channel shares the authentication context
      with the "root channel", and inherits all of the other properties
      from the root channel upon creation time (but may change them
      to it's private values. Like switching buckets within a memcached
      connection). A barrier bit set applies to the lane, and there is
      no way to synchronize lanes (apart from setting the barrier bit
      on all of them and wait for all of the responses.. you have no
      control of the ordering you receive the responses for the barrier.
  </td>
</tr>
<tr>
  <td>0x0004</td>
  <td>TXID</td>
  <td>Variable</td>
  <td>A transaction identifier</td>
</tr>
<tr>
  <td>0x0005</td>
  <td>Priority</td>
  <td>1</td>
  <td>The priority for the request. Lower is better</td>
</tr>
<tr>
  <td>0x0006</td>
  <td>DCP-ID</td>
  <td>Variable</td>
  <td></td>
</tr>
<tr>
  <td>0x0007</td>
  <td>VBucket ID</td>
  <td>2</td>
  <td>The vbucket the document belongs to</td>
</tr>
<tr>
  <td>0x0008</td>
  <td>Hash</td>
  <td>4</td>
  <td>The raw hash value used to map the request to the vbucket id. This
      is used in the case where you want to co locate multiple related
      documents in the same vbucket. In these cases you’d hash with a
      common key, and this field should contain the calculated hash value.</td>
</tr>
<tr>
  <td>0x0009</td>
  <td>Ignore unless executed before</td>
  <td>4</td>
  <td>Ignore the command unless it is executed before the specified time
      (@@@ todo spec this properly @@@)</td>
</tr>
<tr>
  <td>0x000a</td>
  <td>Command timings</td>
  <td>variable</td>
  <td>Ignore the command unless it is executed before the specified time
      (@@@ todo spec this properly @@@)</td>
</tr>
</table>

# Connection lifecycle

After connecting to the advertised port the actor connecting to the port
must start by sending the `HELLO` command to the other end to identify
itself. Note that you may receive commands from the other end before
(or instead of) the `HELLO` reply in the case the other end have
other information it needs to notify you about (e.g. out of resources,
not ready to accept clients at this time etc).

After a successful `HELLO` exchange the you should normally authenticate
to the other end if applicable.

## Memcached connections

After you've identified yourself to memcached with the `HELLO` section
you're not connected to any bucket, and have to run `SELECT BUCKET` in
order to associate the connection with a bucket. By default you only
have access to the "default" bucket, but if you authenticate to the
server you may gain access to more buckets. This differs from the
memcached binary protocol where running `SASL AUTH` authenticates
and select the bucket.

# Generic Commands

The following section defines all commands that are considered generic
and may be implemented by multiple components.

I'll be using the term client and server in the following chapters. A
client is the party that initiates the connect, and the server is the
party that the client connects to. It may very well be two servers
communicating with each other.

## HELLO

<table>
  <tr>
    <th>Attribute</th>
    <th>Value</th>
  </tr>
  <tr>
    <td>Opcode</td>
    <td>0x0001</td>
  </tr>
  <tr>
    <td>Request payload</td>
    <td><a href="payload/HelloRequest.fbs">payload/HelloRequest.fbs</a></td>
  </tr>
  <tr>
    <td>Response payload</td>
    <td><a href="payload/HelloResponse.fbs">payload/HelloResponse.fbs</a></td>
  </tr>
  <tr>
    <td>Visibility</td>
    <td>Internal and External</td>
  </tr>
  <tr>
    <td>Interface stability</td>
    <td>Volatile</td>
  </tr>
  <tr>
    <td>Privileged</td>
    <td>No</td>
  </tr>
</table>

## SASL Auth

<table>
  <tr>
    <th>Attribute</th>
    <th>Value</th>
  </tr>
  <tr>
    <td>Opcode</td>
    <td>0x0002</td>
  </tr>
  <tr>
    <td>Request payload</td>
    <td><a href="payload/SaslAuthRequest.fbs">payload/SaslAuthRequest.fbs</a></td>
  </tr>
  <tr>
    <td>Response payload</td>
    <td><a href="payload/SaslAuthResponse.fbs">payload/SaslAuthResponse.fbs</a></td>
  </tr>
  <tr>
    <td>Visibility</td>
    <td>Internal and External</td>
  </tr>
  <tr>
    <td>Interface stability</td>
    <td>Volatile</td>
  </tr>
  <tr>
    <td>Privileged</td>
    <td>No</td>
  </tr>
</table>

# Memcached Commands

The following section defines all commands memcached provides.

## Mutation

Mutations in Greenstack differs from the memcached binary protocol
in the way that they're all implemented through a "mutation" command
with an extra field in the command specifying the actual operation
to perform. The motivation for doing this is that they all share
the exact same code path within the memcached core, except for
when the object is inserted into the underlying hash table.
It makes it easier to extend the support for new kinds of mutation
support if it means that we just have to update one location rather
than updating the entire state machinery with a new opcode etc.

<table>
  <tr>
    <th>Subcommand</th>
    <th>Descripion</th>
  </tr>
  <tr>
    <td>Add</td>
    <td>Add this document. Fail if it already exists (cas must
        be set to 0)</td>
  </tr>
  <tr>
    <td>Set</td>
    <td>Store this document unconditionally</td>
  </tr>
  <tr>
    <td>Replace</td>
    <td>Store this document only if a document with the same
        identifier already exists</td>
  </tr>
  <tr>
    <td>Append</td>
    <td>Append the content of this document to the
        existing document.</td>
  </tr>
  <tr>
    <td>Prepend</td>
    <td>Prepend the content of this document to the
        existing document.</td>
  </tr>
  <tr>
    <td>Patch</td>
    <td>Apply the attached patch to the existing document</td>
  </tr>
</table>

<table>
  <tr>
    <th>Attribute</th>
    <th>Value</th>
  </tr>
  <tr>
    <td>Opcode</td>
    <td>0x0405</td>
  </tr>
  <tr>
    <td>Request payload</td>
    <td><a href="payload/MutationRequest.fbs">payload/MutationRequest.fbs</a></td>
  </tr>
  <tr>
    <td>Response payload</td>
    <td><a href="payload/MutationResponse.fbs">payload/MutationResponse.fbs</a></td>
  </tr>
  <tr>
    <td>Visibility</td>
    <td>Internal and External</td>
  </tr>
  <tr>
    <td>Interface stability</td>
    <td>Volatile</td>
  </tr>
  <tr>
    <td>Privileged</td>
    <td>No</td>
  </tr>
</table>

## SELECT BUCKET

<table>
  <tr>
    <th>Attribute</th>
    <th>Value</th>
  </tr>
  <tr>
    <td>Opcode</td>
    <td>0x0400</td>
  </tr>
  <tr>
    <td>Request payload</td>
    <td><a href="payload/SelectBucketRequest.fbs">payload/SelectBucketRequest.fbs</a></td>
  </tr>
  <tr>
    <td>Response payload</td>
    <td>None</td>
  </tr>
  <tr>
    <td>Visibility</td>
    <td>Internal and External</td>
  </tr>
  <tr>
    <td>Interface stability</td>
    <td>Volatile</td>
  </tr>
  <tr>
    <td>Privileged</td>
    <td>No</td>
  </tr>
</table>

## LIST BUCKETS

<table>
  <tr>
    <th>Attribute</th>
    <th>Value</th>
  </tr>
  <tr>
    <td>Opcode</td>
    <td>0x0401</td>
  </tr>
  <tr>
    <td>Request payload</td>
    <td>None</td>
  </tr>
  <tr>
    <td>Response payload</td>
    <td><a href="payload/ListBucketsResponse.fbs">payload/ListBucketsResponse.fbs</a></td>
  </tr>
  <tr>
    <td>Visibility</td>
    <td>Internal and External</td>
  </tr>
  <tr>
    <td>Interface stability</td>
    <td>Volatile</td>
  </tr>
  <tr>
    <td>Privileged</td>
    <td>No</td>
  </tr>
</table>

List buckets will only list the buckets you have access to

<!--
## CREATE BUCKET

<table>
  <tr>
    <th>Attribute</th>
    <th>Value</th>
  </tr>
  <tr>
    <td>Opcode</td>
    <td>0x0402</td>
  </tr>
  <tr>
    <td>Request payload</td>
    <td><a href="payload/CreateBucketRequest.fbs">payload/CreateBucketRequest.fbs</a></td>
  </tr>
  <tr>
    <td>Response payload</td>
    <td>None</td>
  </tr>
  <tr>
    <td>Visibility</td>
    <td>Internal</td>
  </tr>
  <tr>
    <td>Interface stability</td>
    <td>Volatile</td>
  </tr>
  <tr>
    <td>Privileged</td>
    <td>Yes</td>
  </tr>
</table>

## DELETE BUCKET

<table>
  <tr>
    <th>Attribute</th>
    <th>Value</th>
  </tr>
  <tr>
    <td>Opcode</td>
    <td>0x0403</td>
  </tr>
  <tr>
    <td>Request payload</td>
    <td><a href="payload/DeleteBucketRequest.fbs">payload/DeleteBucketRequest.fbs</a></td>
  </tr>
  <tr>
    <td>Response payload</td>
    <td>None</td>
  </tr>
  <tr>
    <td>Visibility</td>
    <td>Internal</td>
  </tr>
  <tr>
    <td>Interface stability</td>
    <td>Volatile</td>
  </tr>
  <tr>
    <td>Privileged</td>
    <td>Yes</td>
  </tr>
</table>

## ASSUME ROLE

<table>
  <tr>
    <th>Attribute</th>
    <th>Value</th>
  </tr>
  <tr>
    <td>Opcode</td>
    <td>0x0404</td>
  </tr>
  <tr>
    <td>Request payload</td>
    <td><a href="payload/AssumeRoleRequest.fbs">payload/AssumeRoleRequest.fbs</a></td>
  </tr>
  <tr>
    <td>Response payload</td>
    <td>None</td>
  </tr>
  <tr>
    <td>Visibility</td>
    <td>Internal and External</td>
  </tr>
  <tr>
    <td>Interface stability</td>
    <td>Volatile</td>
  </tr>
  <tr>
    <td>Privileged</td>
    <td>No</td>
  </tr>
</table>

You can only assume roles you have access to
-->

# Milestones

In order to track progress and make it easier for external parties to
integrate with Greenstack, the development of the server follows the
following plan.

It is a bit hard to set dates for some of the milestones at this
time. As part of moving to Greenstack we'll be creating a detailed
documentation of the new commands; may have to change the engine API
and write unit tests. A rough estimate would be 1 1/2 day per command
in average. When I've added support for a few of them its easier
to predict the future (and the work involved..)

<table>
  <tr>
    <th>Milestone</th>
    <th>Date</th>
    <th>Content</th>
  </tr>
  <tr>
    <td>1</td>
    <td>20150601</td>
    <td>Minimal support for Greenstack. Clients may connect and
        authenticate, and select buckets on the server. This
        milestone creates the _infrastructure_ in memcached
        used by the following milestones.
    </td>
  </tr>
  <tr>
    <td>2</td>
    <td>20150701</td>
    <td>Allow for storing and retrieving data</td>
  </tr>
  <tr>
    <td>3</td>
    <td>20150801</td>
    <td>Support all commands specified in "Normal client access"
        profile.</td>
  </tr>
  <tr>
    <td>4</td>
    <td>TBD</td>
    <td>Support all admin commands</td>
  </tr>
  <tr>
    <td>5</td>
    <td>TBD</td>
    <td>Support DCP</td>
  </tr>
  <tr>
    <td>6</td>
    <td>TBD</td>
    <td>Support out of order replies (with barrier bits for the lanes)</td>
  </tr>
  <tr>
    <td>7</td>
    <td>TBD</td>
    <td>Performance measurement and optimizations</td>
  </tr>
  <!-- tr>
    <td></td>
    <td>TBD</td>
    <td></td>
  </tr -->
</table>

# Development plan

## Minimal support for Greenstack protocol in the server

### Allow for configuration of a dedicated port (done)

Greenstack is enabled by using `protocol=greenstack` for the interface
entry in `memcached.json`.

### Extend ns_server to enable greenstack protocol

ns_server needs to enable Greenstack protocol for a new ports (plain
and SSL). This is targeted for Milestone 3.

### Add support for parsing Greenstack frames in memcached (milestone 1)

`try_read_command` needs to be aware of the Greenstack protocol and
dispatch the opcodes to the right underlying protocol handler. Initially
we don't bother to try to be smart with respect to buffer handling (that's
planned for milestone 7 with a potential move to bufferevents in libevent)

We need to refactor the current executor pattern in the memcached core
so that both protocol reuse the same internal functions to implement
commands.

### More to come...


# TODOs

* For encode I should allow for an iovector (so I don't have to copy the payload twice)


# How to build

## Unix

    mkdir build
    cd build
    cmake ..
    gmake all test install

## Windows

    mkdir build
    cd build
    cmake -G "NMake Makefiles" ..
    nmake all test install

# Feedback

Please send feedback to greenstack-interests@couchbase.com
