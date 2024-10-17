/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "mcbp_privileges.h"

#include "connection.h"
#include <logger/logger.h>
#include <memcached/protocol_binary.h>

using cb::rbac::Privilege;
using cb::rbac::PrivilegeAccess;

void McbpPrivilegeChains::setup(cb::mcbp::ClientOpcode command,
                                cb::rbac::PrivilegeAccess (*f)(Cookie&)) {
    commandChains[std::underlying_type<cb::mcbp::ClientOpcode>::type(command)]
            .push_unique(
                    makeFunction<cb::rbac::PrivilegeAccess,
                                 cb::rbac::PrivilegeAccess::getSuccessValue,
                                 Cookie&>(f));
}

PrivilegeAccess McbpPrivilegeChains::invoke(cb::mcbp::ClientOpcode command,
                                            Cookie& cookie) {
    auto& chain =
            commandChains[std::underlying_type<cb::mcbp::ClientOpcode>::type(
                    command)];
    if (chain.empty()) {
        return cb::rbac::PrivilegeAccessFail;
    }
    try {
        return chain.invoke(cookie);
    } catch (const std::bad_function_call&) {
        LOG_WARNING(
                "{}: bad_function_call caught while evaluating access "
                "control for opcode: {:x}",
                cookie.getConnectionId(),
                std::underlying_type<cb::mcbp::ClientOpcode>::type(command));
        // Let the connection catch the exception and shut down the
        // connection
        throw;
    }
}

template <Privilege T>
static PrivilegeAccess require(Cookie& cookie) {
    return cookie.checkPrivilege(T);
}

static PrivilegeAccess requirePrivilegesOnCurrentDocument(
        Cookie& cookie, Privilege system_privilege, Privilege user_privilege) {
    if (cookie.isAccessingSystemCollection()) {
        auto ret = cookie.checkPrivilege(system_privilege);
        if (ret.failed()) {
            return ret;
        }
    }
    return cookie.checkPrivilege(user_privilege);
}

static auto requireReadOnCurrentDocument(Cookie& cookie) {
    return requirePrivilegesOnCurrentDocument(
            cookie, Privilege::SystemCollectionLookup, Privilege::Read);
}

static auto requireUpsertOnCurrentDocument(Cookie& cookie) {
    return requirePrivilegesOnCurrentDocument(
            cookie, Privilege::SystemCollectionMutation, Privilege::Upsert);
}

static PrivilegeAccess requireDeleteOnCurrentDocument(Cookie& cookie) {
    return requirePrivilegesOnCurrentDocument(
            cookie, Privilege::SystemCollectionMutation, Privilege::Delete);
}

static PrivilegeAccess requireInsertOrUpsertOnCurrentDocument(Cookie& cookie) {
    if (cookie.isAccessingSystemCollection()) {
        auto ret = cookie.checkPrivilege(Privilege::SystemCollectionMutation);
        if (ret.failed()) {
            return ret;
        }
    }

    if (cookie.checkPrivilege(Privilege::Insert).success()) {
        return cb::rbac::PrivilegeAccessOk;
    }

    return cookie.checkPrivilege(Privilege::Upsert);
}

static PrivilegeAccess requireMetaWriteOnCurrentDocument(Cookie& cookie) {
    return requirePrivilegesOnCurrentDocument(
            cookie, Privilege::SystemCollectionMutation, Privilege::MetaWrite);
}

static PrivilegeAccess dcpConsumerOrProducer(Cookie& cookie) {
    switch (cookie.getConnection().getType()) {
    case Connection::Type::Normal:
        return cb::rbac::PrivilegeAccessFail;
    case Connection::Type::Producer:
        return cookie.checkPrivilege(Privilege::DcpProducer);
    case Connection::Type::Consumer:
        return cookie.checkPrivilege(Privilege::DcpConsumer);
    }
    throw std::invalid_argument(
            "dcpConsumerOrProducer(): Invalid connection type");
}

template <Privilege T>
static PrivilegeAccess requirePrivilegeInAtLeastOneCollection(Cookie& cookie) {
    return cookie.checkForPrivilegeAtLeastInOneCollection(T);
}

static PrivilegeAccess empty(Cookie& cookie) {
    return cb::rbac::PrivilegeAccessOk;
}

McbpPrivilegeChains::McbpPrivilegeChains() {
    setup(cb::mcbp::ClientOpcode::GetEx, requireReadOnCurrentDocument);
    setup(cb::mcbp::ClientOpcode::GetExReplica, requireReadOnCurrentDocument);
    setup(cb::mcbp::ClientOpcode::Get, requireReadOnCurrentDocument);
    setup(cb::mcbp::ClientOpcode::Getq, requireReadOnCurrentDocument);
    setup(cb::mcbp::ClientOpcode::Getk, requireReadOnCurrentDocument);
    setup(cb::mcbp::ClientOpcode::Getkq, requireReadOnCurrentDocument);
    setup(cb::mcbp::ClientOpcode::GetFailoverLog, require<Privilege::Read>);
    setup(cb::mcbp::ClientOpcode::Set, requireUpsertOnCurrentDocument);
    setup(cb::mcbp::ClientOpcode::Setq, requireUpsertOnCurrentDocument);
    setup(cb::mcbp::ClientOpcode::Add, requireInsertOrUpsertOnCurrentDocument);
    setup(cb::mcbp::ClientOpcode::Addq, requireInsertOrUpsertOnCurrentDocument);
    setup(cb::mcbp::ClientOpcode::Replace, requireUpsertOnCurrentDocument);
    setup(cb::mcbp::ClientOpcode::Replaceq, requireUpsertOnCurrentDocument);
    setup(cb::mcbp::ClientOpcode::Delete, requireDeleteOnCurrentDocument);
    setup(cb::mcbp::ClientOpcode::Deleteq, requireDeleteOnCurrentDocument);
    setup(cb::mcbp::ClientOpcode::Append, requireUpsertOnCurrentDocument);
    setup(cb::mcbp::ClientOpcode::Appendq, requireUpsertOnCurrentDocument);
    setup(cb::mcbp::ClientOpcode::Prepend, requireUpsertOnCurrentDocument);
    setup(cb::mcbp::ClientOpcode::Prependq, requireUpsertOnCurrentDocument);
    setup(cb::mcbp::ClientOpcode::Increment, requireReadOnCurrentDocument);
    setup(cb::mcbp::ClientOpcode::Increment, requireUpsertOnCurrentDocument);
    setup(cb::mcbp::ClientOpcode::Incrementq, requireReadOnCurrentDocument);
    setup(cb::mcbp::ClientOpcode::Incrementq, requireUpsertOnCurrentDocument);
    setup(cb::mcbp::ClientOpcode::Decrement, requireReadOnCurrentDocument);
    setup(cb::mcbp::ClientOpcode::Decrement, requireUpsertOnCurrentDocument);
    setup(cb::mcbp::ClientOpcode::Decrementq, requireReadOnCurrentDocument);
    setup(cb::mcbp::ClientOpcode::Decrementq, requireUpsertOnCurrentDocument);
    setup(cb::mcbp::ClientOpcode::Quit, empty);
    setup(cb::mcbp::ClientOpcode::Quitq, empty);
    setup(cb::mcbp::ClientOpcode::Noop, empty);
    setup(cb::mcbp::ClientOpcode::Version, empty);
    setup(cb::mcbp::ClientOpcode::Stat, empty);
    setup(cb::mcbp::ClientOpcode::Verbosity, require<Privilege::Administrator>);
    setup(cb::mcbp::ClientOpcode::Touch, requireUpsertOnCurrentDocument);
    setup(cb::mcbp::ClientOpcode::Gat, requireReadOnCurrentDocument);
    setup(cb::mcbp::ClientOpcode::Gat, requireUpsertOnCurrentDocument);
    setup(cb::mcbp::ClientOpcode::Gatq, requireReadOnCurrentDocument);
    setup(cb::mcbp::ClientOpcode::Gatq, requireUpsertOnCurrentDocument);
    setup(cb::mcbp::ClientOpcode::Hello, empty);
    setup(cb::mcbp::ClientOpcode::GetErrorMap, empty);
    setup(cb::mcbp::ClientOpcode::SaslListMechs, empty);
    setup(cb::mcbp::ClientOpcode::SaslAuth, empty);
    setup(cb::mcbp::ClientOpcode::SaslStep, empty);
    /* Control */
    setup(cb::mcbp::ClientOpcode::IoctlGet, require<Privilege::Administrator>);
    setup(cb::mcbp::ClientOpcode::IoctlSet, require<Privilege::Administrator>);

    /* Config */
    setup(cb::mcbp::ClientOpcode::ConfigValidate,
          require<Privilege::NodeSupervisor>);
    setup(cb::mcbp::ClientOpcode::ConfigReload,
          require<Privilege::NodeSupervisor>);

    /* Audit */
    setup(cb::mcbp::ClientOpcode::AuditPut, require<Privilege::Audit>);
    setup(cb::mcbp::ClientOpcode::AuditConfigReload,
          require<Privilege::NodeSupervisor>);

    /* Shutdown the server */
    setup(cb::mcbp::ClientOpcode::Shutdown, require<Privilege::NodeSupervisor>);

    setup(cb::mcbp::ClientOpcode::SetActiveEncryptionKeys,
          require<Privilege::NodeSupervisor>);

    setup(cb::mcbp::ClientOpcode::SetBucketThrottleProperties,
          require<Privilege::BucketThrottleManagement>);
    setup(cb::mcbp::ClientOpcode::SetBucketDataLimitExceeded,
          require<Privilege::BucketThrottleManagement>);
    setup(cb::mcbp::ClientOpcode::SetNodeThrottleProperties,
          require<Privilege::BucketThrottleManagement>);

    /* VBucket commands */
    setup(cb::mcbp::ClientOpcode::SetVbucket,
          require<Privilege::NodeSupervisor>);
    // The testrunner client seem to use this command..
    setup(cb::mcbp::ClientOpcode::GetVbucket, empty);
    setup(cb::mcbp::ClientOpcode::DelVbucket,
          require<Privilege::NodeSupervisor>);
    /* End VBucket commands */

    /* Vbucket command to get the VBUCKET sequence numbers for all
     * vbuckets on the node  - handled by engine due to various encodings */
    setup(cb::mcbp::ClientOpcode::GetAllVbSeqnos, empty);

    /* DCP */
    setup(cb::mcbp::ClientOpcode::DcpOpen, empty);
    setup(cb::mcbp::ClientOpcode::DcpAddStream,
          require<Privilege::DcpProducer>);
    setup(cb::mcbp::ClientOpcode::DcpCloseStream,
          require<Privilege::DcpProducer>);
    setup(cb::mcbp::ClientOpcode::DcpStreamReq,
          require<Privilege::DcpProducer>);
    setup(cb::mcbp::ClientOpcode::DcpGetFailoverLog,
          require<Privilege::DcpProducer>);
    setup(cb::mcbp::ClientOpcode::DcpStreamEnd,
          require<Privilege::DcpConsumer>);
    setup(cb::mcbp::ClientOpcode::DcpSnapshotMarker,
          require<Privilege::DcpConsumer>);
    setup(cb::mcbp::ClientOpcode::DcpMutation, require<Privilege::DcpConsumer>);
    setup(cb::mcbp::ClientOpcode::DcpDeletion, require<Privilege::DcpConsumer>);
    setup(cb::mcbp::ClientOpcode::DcpExpiration,
          require<Privilege::DcpConsumer>);
    setup(cb::mcbp::ClientOpcode::DcpSetVbucketState,
          require<Privilege::DcpConsumer>);
    setup(cb::mcbp::ClientOpcode::DcpNoop, dcpConsumerOrProducer);
    setup(cb::mcbp::ClientOpcode::DcpBufferAcknowledgement,
          dcpConsumerOrProducer);
    setup(cb::mcbp::ClientOpcode::DcpControl, dcpConsumerOrProducer);
    setup(cb::mcbp::ClientOpcode::DcpSystemEvent,
          require<Privilege::DcpConsumer>);
    setup(cb::mcbp::ClientOpcode::DcpPrepare, require<Privilege::DcpConsumer>);
    setup(cb::mcbp::ClientOpcode::DcpSeqnoAcknowledged,
          require<Privilege::DcpProducer>);
    setup(cb::mcbp::ClientOpcode::DcpCommit, require<Privilege::DcpConsumer>);
    setup(cb::mcbp::ClientOpcode::DcpAbort, require<Privilege::DcpConsumer>);
    setup(cb::mcbp::ClientOpcode::DcpSeqnoAdvanced,
          require<Privilege::DcpConsumer>);
    /* End DCP */

    setup(cb::mcbp::ClientOpcode::StopPersistence,
          require<Privilege::Administrator>);
    setup(cb::mcbp::ClientOpcode::StartPersistence,
          require<Privilege::Administrator>);
    setup(cb::mcbp::ClientOpcode::SetParam, require<Privilege::Administrator>);
    setup(cb::mcbp::ClientOpcode::GetReplica, requireReadOnCurrentDocument);

    /* Bucket engine */
    setup(cb::mcbp::ClientOpcode::CreateBucket,
          require<Privilege::NodeSupervisor>);
    setup(cb::mcbp::ClientOpcode::DeleteBucket,
          require<Privilege::NodeSupervisor>);
    setup(cb::mcbp::ClientOpcode::PauseBucket,
          require<Privilege::NodeSupervisor>);
    setup(cb::mcbp::ClientOpcode::ResumeBucket,
          require<Privilege::NodeSupervisor>);
    // Everyone should be able to list their own buckets
    setup(cb::mcbp::ClientOpcode::ListBuckets, empty);
    // And select the one they have access to
    setup(cb::mcbp::ClientOpcode::SelectBucket, empty);

    setup(cb::mcbp::ClientOpcode::ObserveSeqno,
          requirePrivilegeInAtLeastOneCollection<Privilege::Read>);

    // The payload of observe contains a list of keys to observe, and
    // the underlying engine check for the Read privilege
    setup(cb::mcbp::ClientOpcode::Observe, empty);

    setup(cb::mcbp::ClientOpcode::EvictKey, require<Privilege::Administrator>);
    setup(cb::mcbp::ClientOpcode::GetLocked, requireReadOnCurrentDocument);
    setup(cb::mcbp::ClientOpcode::UnlockKey, requireReadOnCurrentDocument);

    /**
     * CMD_GET_META is used to retrieve the meta section for an item.
     */
    setup(cb::mcbp::ClientOpcode::GetMeta, requireReadOnCurrentDocument);
    setup(cb::mcbp::ClientOpcode::GetqMeta, requireReadOnCurrentDocument);
    setup(cb::mcbp::ClientOpcode::SetWithMeta,
          requireMetaWriteOnCurrentDocument);
    setup(cb::mcbp::ClientOpcode::SetqWithMeta,
          requireMetaWriteOnCurrentDocument);
    setup(cb::mcbp::ClientOpcode::AddWithMeta,
          requireMetaWriteOnCurrentDocument);
    setup(cb::mcbp::ClientOpcode::AddqWithMeta,
          requireMetaWriteOnCurrentDocument);
    setup(cb::mcbp::ClientOpcode::DelWithMeta,
          requireMetaWriteOnCurrentDocument);
    setup(cb::mcbp::ClientOpcode::DelqWithMeta,
          requireMetaWriteOnCurrentDocument);

    /**
     * Command to enable data traffic after completion of warm
     */
    setup(cb::mcbp::ClientOpcode::EnableTraffic,
          require<Privilege::NodeSupervisor>);
    /**
     * Command to disable data traffic temporarily
     */
    setup(cb::mcbp::ClientOpcode::DisableTraffic,
          require<Privilege::NodeSupervisor>);
    /// Command to manage interfaces
    setup(cb::mcbp::ClientOpcode::Ifconfig, require<Privilege::NodeSupervisor>);
    /// Command that returns meta data for Set, Add, Del
    setup(cb::mcbp::ClientOpcode::ReturnMeta,
          requireMetaWriteOnCurrentDocument);
    /**
     * Command to trigger compaction of a vbucket
     */
    setup(cb::mcbp::ClientOpcode::CompactDb,
          require<Privilege::NodeSupervisor>);
    /**
     * Command to set cluster configuration
     */
    setup(cb::mcbp::ClientOpcode::SetClusterConfig,
          require<Privilege::NodeSupervisor>);
    /**
     * Command that returns cluster configuration (open to anyone)
     */
    setup(cb::mcbp::ClientOpcode::GetClusterConfig, empty);

    setup(cb::mcbp::ClientOpcode::GetRandomKey, require<Privilege::Read>);
    /**
     * Command to wait for the dcp sequence number persistence
     */
    setup(cb::mcbp::ClientOpcode::SeqnoPersistence,
          require<Privilege::NodeSupervisor>);
    /**
     * Command to get all keys
     */
    setup(cb::mcbp::ClientOpcode::GetKeys, require<Privilege::Read>);

    /**
     * Commands for the Sub-document API.
     */

    /* Retrieval commands */
    setup(cb::mcbp::ClientOpcode::SubdocGet, requireReadOnCurrentDocument);
    setup(cb::mcbp::ClientOpcode::SubdocExists, requireReadOnCurrentDocument);

    /* Dictionary commands */
    setup(cb::mcbp::ClientOpcode::SubdocDictAdd,
          requireUpsertOnCurrentDocument);
    setup(cb::mcbp::ClientOpcode::SubdocDictUpsert,
          requireUpsertOnCurrentDocument);

    /* Generic modification commands */
    setup(cb::mcbp::ClientOpcode::SubdocDelete, requireUpsertOnCurrentDocument);
    setup(cb::mcbp::ClientOpcode::SubdocReplace,
          requireUpsertOnCurrentDocument);

    /* Array commands */
    setup(cb::mcbp::ClientOpcode::SubdocArrayPushLast,
          requireUpsertOnCurrentDocument);
    setup(cb::mcbp::ClientOpcode::SubdocArrayPushFirst,
          requireUpsertOnCurrentDocument);
    setup(cb::mcbp::ClientOpcode::SubdocArrayInsert,
          requireUpsertOnCurrentDocument);
    setup(cb::mcbp::ClientOpcode::SubdocArrayAddUnique,
          requireUpsertOnCurrentDocument);
    setup(cb::mcbp::ClientOpcode::SubdocGetCount, requireReadOnCurrentDocument);

    /* Arithmetic commands */
    setup(cb::mcbp::ClientOpcode::SubdocCounter, requireReadOnCurrentDocument);
    setup(cb::mcbp::ClientOpcode::SubdocCounter,
          requireUpsertOnCurrentDocument);

    /* Multi-Path commands */
    setup(cb::mcbp::ClientOpcode::SubdocMultiLookup,
          requireReadOnCurrentDocument);
    setup(cb::mcbp::ClientOpcode::SubdocMultiMutation,
          requireUpsertOnCurrentDocument);

    setup(cb::mcbp::ClientOpcode::SubdocReplaceBodyWithXattr,
          requireUpsertOnCurrentDocument);

    /* Refresh the ISASL data */
    setup(cb::mcbp::ClientOpcode::IsaslRefresh,
          require<Privilege::NodeSupervisor>);
    /* Refresh the SSL certificates */
    setup(cb::mcbp::ClientOpcode::SslCertsRefresh,
          require<Privilege::NodeSupervisor>);
    /* Internal timer ioctl */
    setup(cb::mcbp::ClientOpcode::GetCmdTimer, empty);
    /* ns_server - memcached session validation */
    setup(cb::mcbp::ClientOpcode::SetCtrlToken,
          require<Privilege::NodeSupervisor>);
    setup(cb::mcbp::ClientOpcode::GetCtrlToken,
          require<Privilege::NodeSupervisor>);

    // Drop a privilege from the effective set
    setup(cb::mcbp::ClientOpcode::DropPrivilege, empty);

    setup(cb::mcbp::ClientOpcode::UpdateExternalUserPermissions,
          require<Privilege::NodeSupervisor>);

    /* Refresh the RBAC data */
    setup(cb::mcbp::ClientOpcode::RbacRefresh,
          require<Privilege::NodeSupervisor>);

    setup(cb::mcbp::ClientOpcode::AuthProvider,
          require<Privilege::NodeSupervisor>);

    setup(cb::mcbp::ClientOpcode::CollectionsSetManifest,
          require<Privilege::NodeSupervisor>);

    /// all clients may need to read the manifest
    setup(cb::mcbp::ClientOpcode::CollectionsGetManifest, empty);
    setup(cb::mcbp::ClientOpcode::CollectionsGetID, empty);
    setup(cb::mcbp::ClientOpcode::CollectionsGetScopeID, empty);

    // The RangeScan collection is embedded in a JSON payload (for create) and
    // then continue/cancel need to re-inspect the existing RangeScan object.
    // So setup as empty and ep-engine does the checks.
    setup(cb::mcbp::ClientOpcode::RangeScanCreate, empty);
    setup(cb::mcbp::ClientOpcode::RangeScanContinue, empty);
    setup(cb::mcbp::ClientOpcode::RangeScanCancel, empty);

    if (getenv("MEMCACHED_UNIT_TESTS") != nullptr) {
        // The opcode used to set the clock by our extension
        setup(cb::mcbp::ClientOpcode::AdjustTimeofday, empty);
        // The opcode used by ewouldblock
        setup(cb::mcbp::ClientOpcode::EwouldblockCtl, empty);
        // We have a unit tests that tries to fetch this opcode to detect
        // that we don't crash (we used to have an array which was too
        // small ;-)
        setup(cb::mcbp::ClientOpcode::Invalid, empty);
    }
}
