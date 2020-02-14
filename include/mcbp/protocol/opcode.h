/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
#pragma once

#include <iosfwd>
#include <string>

namespace cb {
namespace mcbp {

/**
 * Defintion of the different command opcodes.
 * See section 3.3 Command Opcodes
 */
enum class ClientOpcode : uint8_t {
    Get = 0x00,
    Set = 0x01,
    Add = 0x02,
    Replace = 0x03,
    Delete = 0x04,
    Increment = 0x05,
    Decrement = 0x06,
    Quit = 0x07,
    Flush = 0x08,
    Getq = 0x09,
    Noop = 0x0a,
    Version = 0x0b,
    Getk = 0x0c,
    Getkq = 0x0d,
    Append = 0x0e,
    Prepend = 0x0f,
    Stat = 0x10,
    Setq = 0x11,
    Addq = 0x12,
    Replaceq = 0x13,
    Deleteq = 0x14,
    Incrementq = 0x15,
    Decrementq = 0x16,
    Quitq = 0x17,
    Flushq = 0x18,
    Appendq = 0x19,
    Prependq = 0x1a,
    Verbosity = 0x1b,
    Touch = 0x1c,
    Gat = 0x1d,
    Gatq = 0x1e,
    Hello = 0x1f,

    SaslListMechs = 0x20,
    SaslAuth = 0x21,
    SaslStep = 0x22,

    /* Control */
    IoctlGet = 0x23,
    IoctlSet = 0x24,

    /* Config */
    ConfigValidate = 0x25,
    ConfigReload = 0x26,

    /* Audit */
    AuditPut = 0x27,
    AuditConfigReload = 0x28,

    /* Shutdown the server */
    Shutdown = 0x29,

    /* These commands are used for range operations and exist within
     * this header for use in other projects.  Range operations are
     * not expected to be implemented in the memcached server itself.
     */
    Rget_Unsupported = 0x30,
    Rset_Unsupported = 0x31,
    Rsetq_Unsupported = 0x32,
    Rappend_Unsupported = 0x33,
    Rappendq_Unsupported = 0x34,
    Rprepend_Unsupported = 0x35,
    Rprependq_Unsupported = 0x36,
    Rdelete_Unsupported = 0x37,
    Rdeleteq_Unsupported = 0x38,
    Rincr_Unsupported = 0x39,
    Rincrq_Unsupported = 0x3a,
    Rdecr_Unsupported = 0x3b,
    Rdecrq_Unsupported = 0x3c,
    /* End Range operations */

    /* VBucket commands */
    SetVbucket = 0x3d,
    GetVbucket = 0x3e,
    DelVbucket = 0x3f,
    /* End VBucket commands */

    /* TAP commands */
    TapConnect_Unsupported = 0x40,
    TapMutation_Unsupported = 0x41,
    TapDelete_Unsupported = 0x42,
    TapFlush_Unsupported = 0x43,
    TapOpaque_Unsupported = 0x44,
    TapVbucketSet_Unsupported = 0x45,
    TapCheckpointStart_Unsupported = 0x46,
    TapCheckpointEnd_Unsupported = 0x47,
    /* End TAP */

    /* Vbucket command to get the VBUCKET sequence numbers for all
     * vbuckets on the node */
    GetAllVbSeqnos = 0x48,

    /* DCP */
    DcpOpen = 0x50,
    DcpAddStream = 0x51,
    DcpCloseStream = 0x52,
    DcpStreamReq = 0x53,
    DcpGetFailoverLog = 0x54,
    DcpStreamEnd = 0x55,
    DcpSnapshotMarker = 0x56,
    DcpMutation = 0x57,
    DcpDeletion = 0x58,
    DcpExpiration = 0x59,
    DcpFlush_Unsupported = 0x5a,
    DcpSetVbucketState = 0x5b,
    DcpNoop = 0x5c,
    DcpBufferAcknowledgement = 0x5d,
    DcpControl = 0x5e,
    DcpSystemEvent = 0x5f,
    DcpPrepare = 0x60,
    DcpSeqnoAcknowledged = 0x61,
    DcpCommit = 0x62,
    DcpAbort = 0x63,
    DcpOsoSnapshot = 0x65,
    /* End DCP */

    StopPersistence = 0x80,
    StartPersistence = 0x81,
    SetParam = 0x82,
    GetReplica = 0x83,

    /* Bucket engine */
    CreateBucket = 0x85,
    DeleteBucket = 0x86,
    ListBuckets = 0x87,
    SelectBucket = 0x89,

    ObserveSeqno = 0x91,
    Observe = 0x92,

    EvictKey = 0x93,
    GetLocked = 0x94,
    UnlockKey = 0x95,

    GetFailoverLog = 0x96,

    /**
     * Return the last closed checkpoint Id for a given VBucket.
     */
    LastClosedCheckpoint = 0x97,
    /**
     * Reset the replication chain from the node that receives
     * this command. For example, given the replication chain,
     * A->B->C, if A receives this command, it will reset all the
     * replica vbuckets on B and C, which are replicated from A.
     */
    ResetReplicationChain_Unsupported = 0x9f,

    /**
     * Close the TAP connection for the registered TAP client and
     * remove the checkpoint cursors from its registered vbuckets.
     */
    DeregisterTapClient_Unsupported = 0x9e,

    /**
     * CMD_GET_META is used to retrieve the meta section for an item.
     */
    GetMeta = 0xa0,
    GetqMeta = 0xa1,
    SetWithMeta = 0xa2,
    SetqWithMeta = 0xa3,
    AddWithMeta = 0xa4,
    AddqWithMeta = 0xa5,
    SnapshotVbStates_Unsupported = 0xa6,
    VbucketBatchCount_Unsupported = 0xa7,
    DelWithMeta = 0xa8,
    DelqWithMeta = 0xa9,

    /**
     * Command to create a new checkpoint on a given vbucket by force
     */
    CreateCheckpoint = 0xaa,
    NotifyVbucketUpdate_Unsupported = 0xac,
    /**
     * Command to enable data traffic after completion of warm
     */
    EnableTraffic = 0xad,
    /**
     * Command to disable data traffic temporarily
     */
    DisableTraffic = 0xae,
    /**
     * Command to change the vbucket filter for a given producer.
     */
    ChangeVbFilter_Unsupported = 0xb0,
    /**
     * Command to wait for the checkpoint persistence
     */
    CheckpointPersistence = 0xb1,
    /**
     * Command that returns meta data for typical memcached ops
     */
    ReturnMeta = 0xb2,
    /**
     * Command to trigger compaction of a vbucket
     */
    CompactDb = 0xb3,
    /**
     * Command to set cluster configuration
     */
    SetClusterConfig = 0xb4,
    /**
     * Command that returns cluster configuration
     */
    GetClusterConfig = 0xb5,
    GetRandomKey = 0xb6,
    /**
     * Command to wait for the dcp sequence number persistence
     */
    SeqnoPersistence = 0xb7,
    /**
     * Command to get all keys
     */
    GetKeys = 0xb8,

    /**
     * Command to set collections manifest
     */
    CollectionsSetManifest = 0xb9,

    /**
     * Command to get collections manifest
     */
    CollectionsGetManifest = 0xba,

    /**
     * Command to get a collection ID
     */
    CollectionsGetID = 0xbb,
    /**
     * Command to get a scope ID
     */
    CollectionsGetScopeID = 0xbc,

    /**
     * Commands for GO-XDCR
     */
    SetDriftCounterState_Unsupported = 0xc1,
    GetAdjustedTime_Unsupported = 0xc2,

    /**
     * Commands for the Sub-document API.
     */

    /* Retrieval commands */
    SubdocGet = 0xc5,
    SubdocExists = 0xc6,

    /* Dictionary commands */
    SubdocDictAdd = 0xc7,
    SubdocDictUpsert = 0xc8,

    /* Generic modification commands */
    SubdocDelete = 0xc9,
    SubdocReplace = 0xca,

    /* Array commands */
    SubdocArrayPushLast = 0xcb,
    SubdocArrayPushFirst = 0xcc,
    SubdocArrayInsert = 0xcd,
    SubdocArrayAddUnique = 0xce,

    /* Arithmetic commands */
    SubdocCounter = 0xcf,

    /* Multi-Path commands */
    SubdocMultiLookup = 0xd0,
    SubdocMultiMutation = 0xd1,

    /* Subdoc additions for Spock: */
    SubdocGetCount = 0xd2,

    /* Scrub the data */
    Scrub = 0xf0,
    /* Refresh the ISASL data */
    IsaslRefresh = 0xf1,
    /* Refresh the SSL certificates */
    SslCertsRefresh = 0xf2,
    /* Internal timer ioctl */
    GetCmdTimer = 0xf3,
    /* ns_server - memcached session validation */
    SetCtrlToken = 0xf4,
    GetCtrlToken = 0xf5,
    /**
     * Update an external users permissions
     *
     * This message causes memcached to update the entry in the privilege
     * database for the specified external user.
     *
     * The value contains the new rbac entry
     */
    UpdateExternalUserPermissions = 0xf6,
    /* Refresh the RBAC database */
    RbacRefresh = 0xf7,

    /// Offer to be an Auth[nz] provider
    AuthProvider = 0xf8,

    /**
     * Drop a privilege from the current privilege set.
     *
     * The intention of the DropPrivilege command is to ease unit tests
     * testing access to commands with and without the required privileges
     * without having to regenerate the privilege database and
     * re-authenticating all the time.
     *
     * The privilege is dropped from the effective set until the security
     * context object gets reset by:
     *    * Selecting a new bucket
     *    * The RBAC database is invalidated
     */
    DropPrivilege = 0xfb,

    /**
     * Command used by our test application to mock with gettimeofday.
     */
    AdjustTimeofday = 0xfc,

    /*
     * Command used to instruct the ewouldblock engine. This command
     * is used by the unit test suite to mock the underlying engines, and
     * should *not* be used in production (note that the none of the
     * underlying engines know of this command so _if_ it is used in
     * production you'll get an error message back. It is listed here
     * to avoid people using the opcode for anything else).
     */
    EwouldblockCtl = 0xfd,

    /* get error code mappings */
    GetErrorMap = 0xfe,

    /* Reserved for being able to signal invalid opcode */
    Invalid = 0xff
};

enum class ServerOpcode {
    /**
     * The client may subscribe to notifications for when the cluster
     * map changes for the current bucket.
     *
     * To enable push notifications of cluster maps the client needs to
     * hello with the feature ClustermapChangeNotification. The server will
     * then send a ClustermapChangeNotification message every time the
     * cluster map is changed. To save bandwidth (and the fact that
     * there may be a race between the when the client is notified and
     * the normal client ops) it will not be sent if the client received
     * the clustermap as part of the NOT MY VBUCKET response.
     *
     * The packet format is still volatile, but:
     *   revision number stored as an uint32_t in network byte order in extras
     *   key is the bucket name
     *   value is the actual cluster map
     */
    ClustermapChangeNotification = 0x01,
    /**
     * Authentication request
     *
     * The payload contains the following JSON:
     *
     *     {
     *       "step" : false,
     *       "context" : "",
     *       "challenge" : "base64encoded challenge sent from client"
     *     }
     *
     * `step` should be set to true if this is a continuation of an ongoing
     * authentication. If not present this is assumed to be set to false.
     *
     * `context` is an opaque context string returned from the external
     * provider _iff_ the authentication process needs multiple iterations.
     *
     * `challenge` is base64 encoding of the callenge sent from the client
     * to memcached.
     */
    Authenticate = 0x02,
    /**
     * The list of active external users (pushed to authentication providers)
     * and the payload of the message contains the list of these users
     * in the following format:
     *
     *     [ "joe", "smith", "perry" ]
     */
    ActiveExternalUsers = 0x03,
    /**
     * GetAuthorization request
     *
     * The request contains the name of the user in the key field,
     * no additional extras, value.
     */
    GetAuthorization = 0x04,
};

bool is_valid_opcode(ClientOpcode opcode);
bool is_valid_opcode(ServerOpcode opcode);

/// Does the provided opcode support durability or not
bool is_durability_supported(ClientOpcode opcode);

/// Does the provided opcode support reordering
bool is_reorder_supported(ClientOpcode opcode);

/// Does the command carry a key which contains a collection identifier
bool is_collection_command(ClientOpcode opcode);

/// Does the provided opcode support preserving TTL
bool is_preserve_ttl_supported(ClientOpcode opcode);

} // namespace mcbp
} // namespace cb

/**
 * Get a textual representation of the given opcode
 *
 * @throws std::invalid_argument for unknown opcodes
 */
std::string to_string(cb::mcbp::ClientOpcode opcode);
std::string to_string(cb::mcbp::ServerOpcode opcode);

/**
 * Convert a textual representation of an opcode to an Opcode
 *
 * @throws std::invalid_argument for unknown opcodes
 */
cb::mcbp::ClientOpcode to_opcode(const std::string& string);

std::ostream& operator<<(std::ostream& out, cb::mcbp::ClientOpcode opcode);
std::ostream& operator<<(std::ostream& out, cb::mcbp::ServerOpcode opcode);
