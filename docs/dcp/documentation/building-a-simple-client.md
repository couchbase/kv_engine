# Building a Simple Client

In order to begin using DCP we first need a client capable of understanding the DCP protocol so that we can being streaming information. This page describes how to create a simple DCP client that will be able to connection to a single node Couchbase cluster and stream data. We will assume that the our single node cluster never crashes and that the client has no need for enabling any special mechanisms such as [flow control](flow-control.md) or [dead connection detection](dead-connections.md).

### DCP Is A Full Duplex Protocol

Responses for certain client requests can be received out of order and clients are required to inspect the opaque field of a DCP response in order to match it to a request. As a result client applications need to be able to read and write to a socket at the same time.

In order to support a full duplex connection clients can for example be implemented in one of the following ways:

1. By creating a reader thread responsible for reading from your socket and also creating a writer thread that will write to the socket.
2. By using epoll()
3. By using kqueue()
4. By using select()

Note that there are other ways to implement a full duplex connection and the list above is meant to be examples for how to accomplish this. Also keep in mind that clients should not be creating a lot of connections. As a result developers should not worry about choosing mechanisms known to scale the number of connections your client can handle.

### Creating a connection

Once your client networking code is in place the next step is to verify that your client can create and close a DCP connection. Creating a connection simply involves sending an [Open Connection](commands/open-connection.md) message which simply names the connection. After your client sends the [Open Connection](open-connection.md) message the client should check to make sure the connection was successfully created on the server. If it was you should be able to request dcp stats from the server and see your connection stats listed in the stats output. This connection will remain open until your client closes its socket. If the connection closes unexpectedly then this means something unexpected happened and the logs should be inspected since an error is always logged if a connection is closed unexpectedly.

### Sending Control Message(s)
Once the connection is created, the client can send one or more [control](commands/control.md) messages to inform the server of any of its non-default capabilities or preferences.

### Creating a stream

Once you have a connection established with the server then the next thing to do is to open a stream to the server to stream out data for a specific VBucket. For a basic client the simplest thing to do is to always stream data starting with the first mutation that was received in the VBucket. To do this the Consumer should send [Stream Request](commands/stream-request.md) messages for each VBucket that it wants to recieve data for.

* VBucket - Set this to the VBucket ID that you want your client to receive data for. This number should always be between 0 and 1023 inclusive.
* Flags - The flags field is used to define specialized behavior for a stream. Since we don't need any specialized behavior we set the flags field to 0.
* Start Seqno - Should be set to 0 since sequence numbers are assigned starting at sequence number 1. The Start sequence number is the last sequence number that the Consumer received and since for our basic streaming case we want to always start from the beginning we send 0 in this field.
* End Seqno - For our basic client we want to recieve a continuous stream and receive all data as in enters Couchbase Server. To do this the highest sequence number possible should be specified. In this case the end sequence number should be 2^64-1.
* VBucket UUID - Since we that are starting to recieve data for the first time 0 should be specified.
* Snapshot Start Seqno - Since we that are starting to recieve data for the first time 0 should be specified.
* Snapshot End Seqno - Since we that are starting to recieve data for the first time 0 should be specified.


### Client Side State for a Stream

A DCP client has to maintain the following state variables for a stream.

* [Failover Log](failure-scenarios.md) from the server
* Last Recieved Seqno
* Last Snapshot Start Seqno
* Last Snapshot End Seqno

Everytime a stream start or re-starts and the server decides to continue based on the parameters passed by the client (that is it does not decide on the rollback), the server sends over the failover log to the client. The client should replace its previous failover log with the new failover log sent by the server. This is because DCP is a master-slave protocol where all the slaves (DCP clients) follow the master (active vbucket on the server) get eventually consistent data.

Last Snapshot Start Seqno, Last Snapshot End Seqno, Last Recieved Seqno can keep changing as the server keeps sending data on the stream. The client is supposed to save atleast the final copy of these 3 sequence numbers that it receives on the stream.

Maintaining these state variables help in restarting from the point where the client had left off.

### Restarting from where you left off
A DCP stream can get dropped due to a number of reasons like drop in the connection, an error for that stream on the server side, an error for that stream on client, etc. So it is quite common for the stream to re-start.

Resumability upon restart of a DCP stream is decided through the use of client side state variables. Upon every start or re-start, the client should sent latest VBucket UUID from the failover log, Last Recieved Seqno as Start Seqno, Last Snapshot Start Seqno as Snapshot Start Seqno and Last Snapshot End Seqno as Snapshot End Seqno. A correct request will have the below invariant

						Snap Start Seqno <= Start Seqno <= Snap End Seqno

The server decides to whether to resume from that start seqno or to ask client to rollback based on the [rollback logic](rollback.md).

If the server decides to resume the stream then the client has to maintain the state variables as explained in the [previous section](building-a-simple-client.md#client-side-state-for-a-stream). If the client is asked to rollback then it should do as explained in the [next section](building-a-simple-client.md#handling-a-rollback).

### Handling a rollback
A rollback is necessary when a DCP client has a different history from the server for that particular vbucket. Clients can choose to handle the rollback in different ways.

Simplest way to handle the rollback response from the server is to rollback to 0. This is very easy to implement but comes at a cost of having to get all the items from start again.

A more sophisticated approach can be maintaining the history of the snapshots and the entire failover log and rolling back to a point as near as possible. Clients can also choose the length of the snapshot history, and length of the failover log history to maintain.

When a client is asked to rollback to a 'rollback seqno' < 'start seqno', the client can rollback its snapshots one by one till it reaches the snapshot corresponding to the rollback seqno and also remove the failover entries with seqno > 'rollback seqno' . Then it can resend the stream request with the corresponding VBucket UUID and the snapshot parameters.

