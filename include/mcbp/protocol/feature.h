/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <cstdint>
#include <string>

namespace cb::mcbp {

/**
 * Definition of hello's features.
 * Note regarding JSON:0x1. Previously this was named DATATYPE and
 * implied that when supported all bits of the protocol datatype byte would
 * be valid. DATATYPE was never enabled and has been renamed as
 * JSON. Clients are now required negotiate individual datatypes
 * with the server using the feature DataType_* feature codes. Note XATTR
 * is linked with general xattr support and the ability to set the xattr
 * datatype bit using set_with_meta.
 */
enum class Feature : uint16_t {
    /// Previously DATATYPE, now retired
    Invalid = 0x01,
    /// The client wants to TLS and send STARTTLS (Never implemented)
    TLS = 0x2,
    /// The client requests the server to set TCP NODELAY on the
    /// socket used by this connection.
    TCPNODELAY = 0x03,
    /// The client requests the server to add the sequence number
    ///  for a mutation to the response packet used in mutations.
    MUTATION_SEQNO = 0x04,
    /// The client requests the server to set TCP DELAY on the socket
    /// used by this connection.
    TCPDELAY = 0x05,
    /// The client requests the server to add XATTRs to the stream for
    /// commands where it makes sense (GetWithMeta, SetWithMeta,
    /// DcpMutation etc)
    XATTR = 0x06,
    /// The client requests the server to send extended error codes instead of
    /// disconnecting the client when new errors occur (note that some errors
    /// may be remapped to more generic error codes instead of disconnecting)
    XERROR = 0x07,
    /// This is purely informational (it does not enable/disable anything on the
    /// server). It may be used from the client to know if it should be able to
    /// run select bucket or not (select bucket was a privileged command
    /// pre-spock. In spock all users may run select bucket, but only to a
    /// bucket they have access to).
    SELECT_BUCKET = 0x08,
    /// Used to be the old collection prototype. No longer in use
    Invalid2 = 0x09,
    /// The client wants to enable support for Snappy compression. A client with
    /// support for Snappy compression must update the datatype field in the
    /// requests with the bit representing SNAPPY when sending snappy compressed
    /// data to the server. It _must_ be able to receive data from the server
    /// compressed with SNAPPY identified by the bit being set in the datatype
    /// field. Due to incorrect implementations in some clients the server
    /// will only use Snappy compression for CRUD operations.
    SNAPPY = 0x0a,
    /// The client wants to enable support for JSON. The client _must_ set this
    /// bit when storing JSON documents on the server. The server will set the
    /// appropriate bit in the datatype field when returning such documents to
    /// the client.
    JSON = 0x0b,
    /// The client allows for full duplex on the socket. This means that the
    /// server may send requests back to the client. These messages is
    /// identified by the magic values of 0x82 (request) and 0x83 (response).
    /// See the document docs/Duplex.md for more information.
    Duplex = 0x0c,
    /// The client wants the server to notify the client with new cluster maps
    /// whenever ns_server push them to memcached. (note that this notification
    /// is subject to deduplication of the vbucket map received as part of not
    /// my vbucket)
    ClustermapChangeNotification = 0x0d,
    /// The client allows the server to reorder the execution of commands. See
    /// the document docs/UnorderedExecution.md for more information
    UnorderedExecution = 0x0e,
    /// The client wants the server to include tracing information in the
    /// response packet
    Tracing = 0x0f,
    /// This is purely informational (it does not enable/disable anything on the
    /// server). It may be used from the client to know if it may send the
    /// alternative request packet (magic 0x08) containing FrameInfo segments.
    AltRequestSupport = 0x10,
    /// This is purely informational (it does not enable/disable anything on the
    /// server). It may be used from the client to know if it may use
    /// synchronous replication tags in the mutation requests.
    SyncReplication = 0x11,
    /// The client wants to enable support for Collections
    Collections = 0x12,
    /// Notify the server that it honors the Snappy datatype bit on
    /// response packets (not only for data retrieval operations)
    SnappyEverywhere = 0x13,
    /// This is purely informational (it does not enable / disable anything on
    /// the server). It may be used from the client to know if it may use
    /// PreserveTtl in the operations who carries the TTL for a document.
    PreserveTtl = 0x14,
    /// This is purely information (it does not enable / disable anything on the
    /// server). It may be used from the client to determine if the server
    /// supports VATTRs in a generic way (can request $<VATTR> and will either
    /// succeed or fail with SubdocXattrUnknownVattr). Requires XATTR.
    VAttr = 0x15,
    /// This is purely information (it does not enable / disable anything on the
    /// server). It may be used from the client to determine if the server
    /// supports Point in Time Recovery.
    PiTR = 0x16,
    /// This is purely informational (it does not enable / disable anything on
    /// the server). It may be used from the client to determine if the server
    /// supports the subdoc `CreateAsDeleted` doc flag.
    SubdocCreateAsDeleted = 0x17,
    /// This is purely information (it does not enable / disable anything on the
    /// server). It may be used from the client to determine if the server
    /// supports using the virtual attribute $document in macros. Requires XATTR
    SubdocDocumentMacroSupport = 0x18,
    /// This is purely information (it does not enable / disable anything on the
    /// server). It may be used from the client to determine if the server
    /// supports the command SubdocReplaceBodyWithXattr.
    SubdocReplaceBodyWithXattr = 0x19,
    /// When enabled the server will insert frame info field(s) in the response
    /// containing the amount of read and write units the command used on the
    /// server, and the time the command spent throttled on the server. The
    /// fields will only be inserted if non-zero.
    ReportUnitUsage = 0x1a,
    /// cause the server to return ewouldthrottle on a request instead of
    /// throttle the command.
    NonBlockingThrottlingMode = 0x1b,
    /// Does the server support the ReplicaRead option to subdoc lookup
    /// operations
    SubdocReplicaRead = 0x1c,
    /// This is purely information (it does not enable / disable anything on the
    /// server). It may be used from the client to determine if the server
    /// supports that the client provides its known version of the cluster
    /// map as part of GetClusterConfig
    GetClusterConfigWithKnownVersion = 0x1d,
    /// Notify the server that the client correctly deals with the optional
    /// payload in "not-my-vbucket" (which allows for deduplication of
    /// cluster map)
    DedupeNotMyVbucketClustermap = 0x1e,
    /// The client wants the server to notify the client with the version
    /// number whenever ns_server push them to memcached. (note that this
    /// notification is subject to deduplication of the vbucket map received as
    /// part of not my vbucket)
    ClustermapChangeNotificationBrief = 0x1f,
    /// This is purely information (it does not enable / disable anything on the
    /// server). It may be used from the client to determine if the server
    /// allows the client to operate on multiple XATTR keys within the same
    /// MultiMutation/MultiLookup operation
    SubdocAllowsAccessOnMultipleXattrKeys = 0x20,
};

} // namespace cb::mcbp

std::string to_string(cb::mcbp::Feature feature);
