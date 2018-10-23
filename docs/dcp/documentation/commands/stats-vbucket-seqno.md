## Stats VBucket-Seqno

The Stats VBucket-Seqno command is used to obtain the current High Sequence Number/VBucket UUID for one or all VBuckets on a Couchbase node.

#### Binary Implementation

See the stats command for details on the packet format for stats commands. Stats VBucket-Seqno can be used by placing the string `vbucket-seqno` into the key section of a stats packet to receive High Sequence Number/VBucket UUID pairs for all VBuckets on a given node. To receive a the High Sequence NUmber/VBucket UUID pair for a single VBucket then you can set the key in the stats command to `vbucket-seqno <vbid>`.

##### Returns

The stats response from a Stats VBucket-Seqno command will look as follows:

    vb_0:high_seqno 45
    vb_0:vb_uuid 2723958720985
    vb_1:high_seqno 78
    vb_1:vb_uuid 7851085629020
    vb_2:high_seqno 56
    vb_2:vb_uuid 4560392056902
    ...

##### Errors

**PROTOCOL_BINARY_RESPONSE_EINVAL (0x04)**

If data in this packet is malformed or incomplete then this error is returned.

**PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET (0x07)**

If you specified a specific VBucket and that VBucket does not exist.

##### Use Cases

This command is typically used before creating an UPR stream in order to find out what the sequence number is of the last mutation the server has for a given VBucket at the time the Stats VBucket-Seqno command was received.


