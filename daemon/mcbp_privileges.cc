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

using cb::mcbp::ClientOpcode;
using cb::rbac::Privilege;
using cb::rbac::PrivilegeAccess;
using cb::rbac::PrivilegeAccessFail;
using cb::rbac::PrivilegeAccessOk;

void McbpPrivilegeChains::setup(ClientOpcode command,
                                PrivilegeAccess (*f)(Cookie&)) {
    commandChains[static_cast<uint8_t>(command)].push_unique(
            makeFunction<PrivilegeAccess,
                         PrivilegeAccess::getSuccessValue,
                         Cookie&>(f));
}

PrivilegeAccess McbpPrivilegeChains::invoke(ClientOpcode command,
                                            Cookie& cookie) const {
    auto& chain = commandChains[static_cast<uint8_t>(command)];
    if (chain.empty()) {
        return PrivilegeAccessFail;
    }
    try {
        return chain.invoke(cookie);
    } catch (const std::bad_function_call&) {
        LOG_WARNING_CTX(
                "bad_function_call caught while evaluating access control",
                {"conn_id", cookie.getConnectionId()},
                {"opcode_number",
                 fmt::format("{:#x}", static_cast<uint8_t>(command))});
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
        return PrivilegeAccessOk;
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
        return PrivilegeAccessFail;
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

static PrivilegeAccess empty(Cookie&) {
    return PrivilegeAccessOk;
}

McbpPrivilegeChains::McbpPrivilegeChains() {
    setup(ClientOpcode::GetEx, requireReadOnCurrentDocument);
    setup(ClientOpcode::GetExReplica, requireReadOnCurrentDocument);
    setup(ClientOpcode::Get, requireReadOnCurrentDocument);
    setup(ClientOpcode::Getq, requireReadOnCurrentDocument);
    setup(ClientOpcode::Getk, requireReadOnCurrentDocument);
    setup(ClientOpcode::Getkq, requireReadOnCurrentDocument);
    setup(ClientOpcode::GetFailoverLog, require<Privilege::Read>);
    setup(ClientOpcode::Set, requireUpsertOnCurrentDocument);
    setup(ClientOpcode::Setq, requireUpsertOnCurrentDocument);
    setup(ClientOpcode::Add, requireInsertOrUpsertOnCurrentDocument);
    setup(ClientOpcode::Addq, requireInsertOrUpsertOnCurrentDocument);
    setup(ClientOpcode::Replace, requireUpsertOnCurrentDocument);
    setup(ClientOpcode::Replaceq, requireUpsertOnCurrentDocument);
    setup(ClientOpcode::Delete, requireDeleteOnCurrentDocument);
    setup(ClientOpcode::Deleteq, requireDeleteOnCurrentDocument);
    setup(ClientOpcode::Append, requireUpsertOnCurrentDocument);
    setup(ClientOpcode::Appendq, requireUpsertOnCurrentDocument);
    setup(ClientOpcode::Prepend, requireUpsertOnCurrentDocument);
    setup(ClientOpcode::Prependq, requireUpsertOnCurrentDocument);
    setup(ClientOpcode::Increment, requireReadOnCurrentDocument);
    setup(ClientOpcode::Increment, requireUpsertOnCurrentDocument);
    setup(ClientOpcode::Incrementq, requireReadOnCurrentDocument);
    setup(ClientOpcode::Incrementq, requireUpsertOnCurrentDocument);
    setup(ClientOpcode::Decrement, requireReadOnCurrentDocument);
    setup(ClientOpcode::Decrement, requireUpsertOnCurrentDocument);
    setup(ClientOpcode::Decrementq, requireReadOnCurrentDocument);
    setup(ClientOpcode::Decrementq, requireUpsertOnCurrentDocument);
    setup(ClientOpcode::Quit, empty);
    setup(ClientOpcode::Quitq, empty);
    setup(ClientOpcode::Noop, empty);
    setup(ClientOpcode::Version, empty);
    setup(ClientOpcode::Stat, empty);
    setup(ClientOpcode::Verbosity, require<Privilege::Administrator>);
    setup(ClientOpcode::Touch, requireUpsertOnCurrentDocument);
    setup(ClientOpcode::Gat, requireReadOnCurrentDocument);
    setup(ClientOpcode::Gat, requireUpsertOnCurrentDocument);
    setup(ClientOpcode::Gatq, requireReadOnCurrentDocument);
    setup(ClientOpcode::Gatq, requireUpsertOnCurrentDocument);
    setup(ClientOpcode::Hello, empty);
    setup(ClientOpcode::GetErrorMap, empty);
    setup(ClientOpcode::SaslListMechs, empty);
    setup(ClientOpcode::SaslAuth, empty);
    setup(ClientOpcode::SaslStep, empty);
    setup(ClientOpcode::IoctlGet, require<Privilege::Administrator>);
    setup(ClientOpcode::IoctlSet, require<Privilege::Administrator>);
    setup(ClientOpcode::ConfigValidate, require<Privilege::NodeSupervisor>);
    setup(ClientOpcode::ConfigReload, require<Privilege::NodeSupervisor>);
    setup(ClientOpcode::AuditPut, require<Privilege::Audit>);
    setup(ClientOpcode::AuditConfigReload, require<Privilege::NodeSupervisor>);
    setup(ClientOpcode::Shutdown, require<Privilege::NodeSupervisor>);
    setup(ClientOpcode::SetActiveEncryptionKeys,
          require<Privilege::NodeSupervisor>);
    setup(ClientOpcode::SetBucketThrottleProperties,
          require<Privilege::BucketThrottleManagement>);
    setup(ClientOpcode::SetBucketDataLimitExceeded,
          require<Privilege::BucketThrottleManagement>);
    setup(ClientOpcode::SetNodeThrottleProperties,
          require<Privilege::BucketThrottleManagement>);
    setup(ClientOpcode::SetVbucket, require<Privilege::NodeSupervisor>);
    setup(ClientOpcode::GetVbucket, empty);
    setup(ClientOpcode::DelVbucket, require<Privilege::NodeSupervisor>);
    setup(ClientOpcode::GetAllVbSeqnos, empty);
    setup(ClientOpcode::DcpOpen, empty);
    setup(ClientOpcode::DcpAddStream, require<Privilege::DcpProducer>);
    setup(ClientOpcode::DcpCloseStream, require<Privilege::DcpProducer>);
    setup(ClientOpcode::DcpStreamReq, require<Privilege::DcpProducer>);
    setup(ClientOpcode::DcpGetFailoverLog, require<Privilege::DcpProducer>);
    setup(ClientOpcode::DcpStreamEnd, require<Privilege::DcpConsumer>);
    setup(ClientOpcode::DcpSnapshotMarker, require<Privilege::DcpConsumer>);
    setup(ClientOpcode::DcpMutation, require<Privilege::DcpConsumer>);
    setup(ClientOpcode::DcpDeletion, require<Privilege::DcpConsumer>);
    setup(ClientOpcode::DcpExpiration, require<Privilege::DcpConsumer>);
    setup(ClientOpcode::DcpSetVbucketState, require<Privilege::DcpConsumer>);
    setup(ClientOpcode::DcpNoop, dcpConsumerOrProducer);
    setup(ClientOpcode::DcpBufferAcknowledgement, dcpConsumerOrProducer);
    setup(ClientOpcode::DcpControl, dcpConsumerOrProducer);
    setup(ClientOpcode::DcpSystemEvent, require<Privilege::DcpConsumer>);
    setup(ClientOpcode::DcpPrepare, require<Privilege::DcpConsumer>);
    setup(ClientOpcode::DcpSeqnoAcknowledged, require<Privilege::DcpProducer>);
    setup(ClientOpcode::DcpCommit, require<Privilege::DcpConsumer>);
    setup(ClientOpcode::DcpAbort, require<Privilege::DcpConsumer>);
    setup(ClientOpcode::DcpSeqnoAdvanced, require<Privilege::DcpConsumer>);
    setup(ClientOpcode::StopPersistence, require<Privilege::Administrator>);
    setup(ClientOpcode::StartPersistence, require<Privilege::Administrator>);
    setup(ClientOpcode::SetParam, require<Privilege::Administrator>);
    setup(ClientOpcode::GetReplica, requireReadOnCurrentDocument);
    setup(ClientOpcode::CreateBucket, require<Privilege::NodeSupervisor>);
    setup(ClientOpcode::DeleteBucket, require<Privilege::NodeSupervisor>);
    setup(ClientOpcode::PauseBucket, require<Privilege::NodeSupervisor>);
    setup(ClientOpcode::ResumeBucket, require<Privilege::NodeSupervisor>);
    setup(ClientOpcode::ListBuckets, empty);
    setup(ClientOpcode::SelectBucket, empty);
    setup(ClientOpcode::ObserveSeqno,
          requirePrivilegeInAtLeastOneCollection<Privilege::Read>);
    setup(ClientOpcode::Observe, empty);
    setup(ClientOpcode::EvictKey, require<Privilege::Administrator>);
    setup(ClientOpcode::GetLocked, requireReadOnCurrentDocument);
    setup(ClientOpcode::UnlockKey, requireReadOnCurrentDocument);
    setup(ClientOpcode::GetFileFragment, require<Privilege::Administrator>);
    setup(ClientOpcode::PrepareSnapshot, require<Privilege::Administrator>);
    setup(ClientOpcode::ReleaseSnapshot, require<Privilege::Administrator>);
    setup(ClientOpcode::DownloadSnapshot, require<Privilege::Administrator>);
    setup(ClientOpcode::GetMeta, requireReadOnCurrentDocument);
    setup(ClientOpcode::GetqMeta, requireReadOnCurrentDocument);
    setup(ClientOpcode::SetWithMeta, requireMetaWriteOnCurrentDocument);
    setup(ClientOpcode::SetqWithMeta, requireMetaWriteOnCurrentDocument);
    setup(ClientOpcode::AddWithMeta, requireMetaWriteOnCurrentDocument);
    setup(ClientOpcode::AddqWithMeta, requireMetaWriteOnCurrentDocument);
    setup(ClientOpcode::DelWithMeta, requireMetaWriteOnCurrentDocument);
    setup(ClientOpcode::DelqWithMeta, requireMetaWriteOnCurrentDocument);
    setup(ClientOpcode::EnableTraffic, require<Privilege::NodeSupervisor>);
    setup(ClientOpcode::DisableTraffic, require<Privilege::NodeSupervisor>);
    setup(ClientOpcode::Ifconfig, require<Privilege::NodeSupervisor>);
    setup(ClientOpcode::ReturnMeta, requireMetaWriteOnCurrentDocument);
    setup(ClientOpcode::CompactDb, require<Privilege::NodeSupervisor>);
    setup(ClientOpcode::SetClusterConfig, require<Privilege::NodeSupervisor>);
    setup(ClientOpcode::GetClusterConfig, empty);
    setup(ClientOpcode::GetRandomKey, require<Privilege::Read>);
    setup(ClientOpcode::SeqnoPersistence, require<Privilege::NodeSupervisor>);
    setup(ClientOpcode::GetKeys, require<Privilege::Read>);
    setup(ClientOpcode::SubdocGet, requireReadOnCurrentDocument);
    setup(ClientOpcode::SubdocExists, requireReadOnCurrentDocument);
    setup(ClientOpcode::SubdocDictAdd, requireUpsertOnCurrentDocument);
    setup(ClientOpcode::SubdocDictUpsert, requireUpsertOnCurrentDocument);
    setup(ClientOpcode::SubdocDelete, requireUpsertOnCurrentDocument);
    setup(ClientOpcode::SubdocReplace, requireUpsertOnCurrentDocument);
    setup(ClientOpcode::SubdocArrayPushLast, requireUpsertOnCurrentDocument);
    setup(ClientOpcode::SubdocArrayPushFirst, requireUpsertOnCurrentDocument);
    setup(ClientOpcode::SubdocArrayInsert, requireUpsertOnCurrentDocument);
    setup(ClientOpcode::SubdocArrayAddUnique, requireUpsertOnCurrentDocument);
    setup(ClientOpcode::SubdocGetCount, requireReadOnCurrentDocument);
    setup(ClientOpcode::SubdocCounter, requireReadOnCurrentDocument);
    setup(ClientOpcode::SubdocCounter, requireUpsertOnCurrentDocument);
    setup(ClientOpcode::SubdocMultiLookup, requireReadOnCurrentDocument);
    setup(ClientOpcode::SubdocMultiMutation, requireUpsertOnCurrentDocument);
    setup(ClientOpcode::SubdocReplaceBodyWithXattr,
          requireUpsertOnCurrentDocument);
    setup(ClientOpcode::IsaslRefresh, require<Privilege::NodeSupervisor>);
    setup(ClientOpcode::SslCertsRefresh_Unsupported,
          require<Privilege::NodeSupervisor>);
    setup(ClientOpcode::GetCmdTimer, empty);
    setup(ClientOpcode::SetCtrlToken, require<Privilege::NodeSupervisor>);
    setup(ClientOpcode::GetCtrlToken, require<Privilege::NodeSupervisor>);
    setup(ClientOpcode::DropPrivilege, empty);
    setup(ClientOpcode::UpdateExternalUserPermissions,
          require<Privilege::NodeSupervisor>);
    setup(ClientOpcode::RbacRefresh, require<Privilege::NodeSupervisor>);
    setup(ClientOpcode::AuthProvider, require<Privilege::NodeSupervisor>);
    setup(ClientOpcode::CollectionsSetManifest,
          require<Privilege::NodeSupervisor>);
    setup(ClientOpcode::CollectionsGetManifest, empty);
    setup(ClientOpcode::CollectionsGetID, empty);
    setup(ClientOpcode::CollectionsGetScopeID, empty);
    setup(ClientOpcode::RangeScanCreate, empty);
    setup(ClientOpcode::RangeScanContinue, empty);
    setup(ClientOpcode::RangeScanCancel, empty);
    setup(ClientOpcode::GetFusionStorageSnapshot,
          require<Privilege::NodeSupervisor>);
    setup(ClientOpcode::ReleaseFusionStorageSnapshot,
          require<Privilege::NodeSupervisor>);
    setup(ClientOpcode::MountFusionVbucket, require<Privilege::NodeSupervisor>);
    setup(ClientOpcode::SyncFusionLogstore, require<Privilege::NodeSupervisor>);
    setup(ClientOpcode::StartFusionUploader,
          require<Privilege::NodeSupervisor>);
    setup(cb::mcbp::ClientOpcode::StopFusionUploader,
          require<Privilege::NodeSupervisor>);

    if (getenv("MEMCACHED_UNIT_TESTS") != nullptr) {
        // The opcode used to set the clock by our extension
        setup(ClientOpcode::AdjustTimeofday, empty);
        // The opcode used by ewouldblock
        setup(ClientOpcode::EwouldblockCtl, empty);
        // We have a unit tests that tries to fetch this opcode to detect
        // that we don't crash (we used to have an array which was too
        // small ;-)
        setup(ClientOpcode::Invalid, empty);
    }
}
