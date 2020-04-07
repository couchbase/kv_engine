/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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
#include "mcbp_privileges.h"

#include "connection.h"
#include <logger/logger.h>
#include <memcached/protocol_binary.h>

using cb::rbac::Privilege;
using cb::rbac::PrivilegeAccess;

void McbpPrivilegeChains::setup(cb::mcbp::ClientOpcode command,
                                cb::rbac::PrivilegeAccess (*f)(Cookie&)) {
    commandChains[std::underlying_type<cb::mcbp::ClientOpcode>::type(command)]
            .push_unique(makeFunction<cb::rbac::PrivilegeAccess,
                                      cb::rbac::PrivilegeAccess::Ok,
                                      Cookie&>(f));
}

PrivilegeAccess McbpPrivilegeChains::invoke(cb::mcbp::ClientOpcode command,
                                            Cookie& cookie) {
    auto& chain =
            commandChains[std::underlying_type<cb::mcbp::ClientOpcode>::type(
                    command)];
    if (chain.empty()) {
        return cb::rbac::PrivilegeAccess::Fail;
    } else {
        try {
            return chain.invoke(cookie);
        } catch (const std::bad_function_call&) {
            LOG_WARNING(
                    "{}: bad_function_call caught while evaluating access "
                    "control for opcode: {:x}",
                    cookie.getConnection().getId(),
                    std::underlying_type<cb::mcbp::ClientOpcode>::type(
                            command));
            // Let the connection catch the exception and shut down the
            // connection
            throw;
        }
    }
}

template <Privilege T>
static PrivilegeAccess require(Cookie& cookie) {
    return cookie.checkPrivilege(T);
}

static PrivilegeAccess requireInsertOrUpsert(Cookie& cookie) {
    auto ret = cookie.checkPrivilege(Privilege::Insert);
    if (ret == PrivilegeAccess::Ok) {
        return PrivilegeAccess::Ok;
    } else {
        return cookie.checkPrivilege(Privilege::Upsert);
    }
}

static PrivilegeAccess empty(Cookie& cookie) {
    return PrivilegeAccess::Ok;
}

McbpPrivilegeChains::McbpPrivilegeChains() {
    setup(cb::mcbp::ClientOpcode::Get, require<Privilege::Read>);
    setup(cb::mcbp::ClientOpcode::Getq, require<Privilege::Read>);
    setup(cb::mcbp::ClientOpcode::Getk, require<Privilege::Read>);
    setup(cb::mcbp::ClientOpcode::Getkq, require<Privilege::Read>);
    setup(cb::mcbp::ClientOpcode::GetFailoverLog, require<Privilege::Read>);
    setup(cb::mcbp::ClientOpcode::Set, require<Privilege::Upsert>);
    setup(cb::mcbp::ClientOpcode::Setq, require<Privilege::Upsert>);
    setup(cb::mcbp::ClientOpcode::Add, requireInsertOrUpsert);
    setup(cb::mcbp::ClientOpcode::Addq, requireInsertOrUpsert);
    setup(cb::mcbp::ClientOpcode::Replace, require<Privilege::Upsert>);
    setup(cb::mcbp::ClientOpcode::Replaceq, require<Privilege::Upsert>);
    setup(cb::mcbp::ClientOpcode::Delete, require<Privilege::Delete>);
    setup(cb::mcbp::ClientOpcode::Deleteq, require<Privilege::Delete>);
    setup(cb::mcbp::ClientOpcode::Append, require<Privilege::Upsert>);
    setup(cb::mcbp::ClientOpcode::Appendq, require<Privilege::Upsert>);
    setup(cb::mcbp::ClientOpcode::Prepend, require<Privilege::Upsert>);
    setup(cb::mcbp::ClientOpcode::Prependq, require<Privilege::Upsert>);
    setup(cb::mcbp::ClientOpcode::Increment, require<Privilege::Read>);
    setup(cb::mcbp::ClientOpcode::Increment, require<Privilege::Upsert>);
    setup(cb::mcbp::ClientOpcode::Incrementq, require<Privilege::Read>);
    setup(cb::mcbp::ClientOpcode::Incrementq, require<Privilege::Upsert>);
    setup(cb::mcbp::ClientOpcode::Decrement, require<Privilege::Read>);
    setup(cb::mcbp::ClientOpcode::Decrement, require<Privilege::Upsert>);
    setup(cb::mcbp::ClientOpcode::Decrementq, require<Privilege::Read>);
    setup(cb::mcbp::ClientOpcode::Decrementq, require<Privilege::Upsert>);
    setup(cb::mcbp::ClientOpcode::Quit, empty);
    setup(cb::mcbp::ClientOpcode::Quitq, empty);
    setup(cb::mcbp::ClientOpcode::Flush, require<Privilege::BucketManagement>);
    setup(cb::mcbp::ClientOpcode::Flushq, require<Privilege::BucketManagement>);
    setup(cb::mcbp::ClientOpcode::Noop, empty);
    setup(cb::mcbp::ClientOpcode::Version, empty);
    setup(cb::mcbp::ClientOpcode::Stat, empty);
    setup(cb::mcbp::ClientOpcode::Verbosity,
          require<Privilege::NodeManagement>);
    setup(cb::mcbp::ClientOpcode::Touch, require<Privilege::Upsert>);
    setup(cb::mcbp::ClientOpcode::Gat, require<Privilege::Read>);
    setup(cb::mcbp::ClientOpcode::Gat, require<Privilege::Upsert>);
    setup(cb::mcbp::ClientOpcode::Gatq, require<Privilege::Read>);
    setup(cb::mcbp::ClientOpcode::Gatq, require<Privilege::Upsert>);
    setup(cb::mcbp::ClientOpcode::Hello, empty);
    setup(cb::mcbp::ClientOpcode::GetErrorMap, empty);
    setup(cb::mcbp::ClientOpcode::SaslListMechs, empty);
    setup(cb::mcbp::ClientOpcode::SaslAuth, empty);
    setup(cb::mcbp::ClientOpcode::SaslStep, empty);
    /* Control */
    setup(cb::mcbp::ClientOpcode::IoctlGet, require<Privilege::NodeManagement>);
    setup(cb::mcbp::ClientOpcode::IoctlSet, require<Privilege::NodeManagement>);

    /* Config */
    setup(cb::mcbp::ClientOpcode::ConfigValidate,
          require<Privilege::NodeManagement>);
    setup(cb::mcbp::ClientOpcode::ConfigReload,
          require<Privilege::NodeManagement>);

    /* Audit */
    setup(cb::mcbp::ClientOpcode::AuditPut, require<Privilege::Audit>);
    setup(cb::mcbp::ClientOpcode::AuditConfigReload,
          require<Privilege::AuditManagement>);

    /* Shutdown the server */
    setup(cb::mcbp::ClientOpcode::Shutdown, require<Privilege::NodeManagement>);

    /* VBucket commands */
    setup(cb::mcbp::ClientOpcode::SetVbucket,
          require<Privilege::BucketManagement>);
    // The testrunner client seem to use this command..
    setup(cb::mcbp::ClientOpcode::GetVbucket, empty);
    setup(cb::mcbp::ClientOpcode::DelVbucket,
          require<Privilege::BucketManagement>);
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
    setup(cb::mcbp::ClientOpcode::DcpNoop, empty);
    setup(cb::mcbp::ClientOpcode::DcpBufferAcknowledgement, empty);
    setup(cb::mcbp::ClientOpcode::DcpControl, empty);
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
          require<Privilege::NodeManagement>);
    setup(cb::mcbp::ClientOpcode::StartPersistence,
          require<Privilege::NodeManagement>);
    setup(cb::mcbp::ClientOpcode::SetParam, require<Privilege::NodeManagement>);
    setup(cb::mcbp::ClientOpcode::GetReplica, require<Privilege::Read>);

    /* Bucket engine */
    setup(cb::mcbp::ClientOpcode::CreateBucket,
          require<Privilege::BucketManagement>);
    setup(cb::mcbp::ClientOpcode::DeleteBucket,
          require<Privilege::BucketManagement>);
    // Everyone should be able to list their own buckets
    setup(cb::mcbp::ClientOpcode::ListBuckets, empty);
    // And select the one they have access to
    setup(cb::mcbp::ClientOpcode::SelectBucket, empty);

    setup(cb::mcbp::ClientOpcode::ObserveSeqno, require<Privilege::MetaRead>);
    setup(cb::mcbp::ClientOpcode::Observe, require<Privilege::MetaRead>);

    setup(cb::mcbp::ClientOpcode::EvictKey, require<Privilege::NodeManagement>);
    setup(cb::mcbp::ClientOpcode::GetLocked, require<Privilege::Read>);
    setup(cb::mcbp::ClientOpcode::UnlockKey, require<Privilege::Read>);

    /**
     * Return the last closed checkpoint Id for a given VBucket.
     */
    setup(cb::mcbp::ClientOpcode::LastClosedCheckpoint,
          require<Privilege::MetaRead>);

    /**
     * CMD_GET_META is used to retrieve the meta section for an item.
     */
    setup(cb::mcbp::ClientOpcode::GetMeta, require<Privilege::MetaRead>);
    setup(cb::mcbp::ClientOpcode::GetqMeta, require<Privilege::MetaRead>);
    setup(cb::mcbp::ClientOpcode::SetWithMeta, require<Privilege::MetaWrite>);
    setup(cb::mcbp::ClientOpcode::SetqWithMeta, require<Privilege::MetaWrite>);
    setup(cb::mcbp::ClientOpcode::AddWithMeta, require<Privilege::MetaWrite>);
    setup(cb::mcbp::ClientOpcode::AddqWithMeta, require<Privilege::MetaWrite>);
    setup(cb::mcbp::ClientOpcode::DelWithMeta, require<Privilege::MetaWrite>);
    setup(cb::mcbp::ClientOpcode::DelqWithMeta, require<Privilege::MetaWrite>);

    /**
     * Command to create a new checkpoint on a given vbucket by force
     */
    setup(cb::mcbp::ClientOpcode::CreateCheckpoint,
          require<Privilege::NodeManagement>);
    /**
     * Command to enable data traffic after completion of warm
     */
    setup(cb::mcbp::ClientOpcode::EnableTraffic,
          require<Privilege::NodeManagement>);
    /**
     * Command to disable data traffic temporarily
     */
    setup(cb::mcbp::ClientOpcode::DisableTraffic,
          require<Privilege::NodeManagement>);
    /**
     * Command to wait for the checkpoint persistence
     */
    setup(cb::mcbp::ClientOpcode::CheckpointPersistence,
          require<Privilege::NodeManagement>);
    /**
     * Command that returns meta data for typical memcached ops
     */
    setup(cb::mcbp::ClientOpcode::ReturnMeta, require<Privilege::MetaRead>);
    setup(cb::mcbp::ClientOpcode::ReturnMeta, require<Privilege::MetaWrite>);
    /**
     * Command to trigger compaction of a vbucket
     */
    setup(cb::mcbp::ClientOpcode::CompactDb,
          require<Privilege::NodeManagement>);
    /**
     * Command to set cluster configuration
     */
    setup(cb::mcbp::ClientOpcode::SetClusterConfig,
          require<Privilege::SecurityManagement>);
    /**
     * Command that returns cluster configuration (open to anyone)
     */
    setup(cb::mcbp::ClientOpcode::GetClusterConfig, empty);

    setup(cb::mcbp::ClientOpcode::GetRandomKey, require<Privilege::Read>);
    /**
     * Command to wait for the dcp sequence number persistence
     */
    setup(cb::mcbp::ClientOpcode::SeqnoPersistence,
          require<Privilege::NodeManagement>);
    /**
     * Command to get all keys
     */
    setup(cb::mcbp::ClientOpcode::GetKeys, require<Privilege::Read>);

    /**
     * Commands for the Sub-document API.
     */

    /* Retrieval commands */
    setup(cb::mcbp::ClientOpcode::SubdocGet, require<Privilege::Read>);
    setup(cb::mcbp::ClientOpcode::SubdocExists, require<Privilege::Read>);

    /* Dictionary commands */
    setup(cb::mcbp::ClientOpcode::SubdocDictAdd, require<Privilege::Upsert>);
    setup(cb::mcbp::ClientOpcode::SubdocDictUpsert, require<Privilege::Upsert>);

    /* Generic modification commands */
    setup(cb::mcbp::ClientOpcode::SubdocDelete, require<Privilege::Upsert>);
    setup(cb::mcbp::ClientOpcode::SubdocReplace, require<Privilege::Upsert>);

    /* Array commands */
    setup(cb::mcbp::ClientOpcode::SubdocArrayPushLast,
          require<Privilege::Upsert>);
    setup(cb::mcbp::ClientOpcode::SubdocArrayPushFirst,
          require<Privilege::Upsert>);
    setup(cb::mcbp::ClientOpcode::SubdocArrayInsert,
          require<Privilege::Upsert>);
    setup(cb::mcbp::ClientOpcode::SubdocArrayAddUnique,
          require<Privilege::Upsert>);
    setup(cb::mcbp::ClientOpcode::SubdocGetCount, require<Privilege::Read>);

    /* Arithmetic commands */
    setup(cb::mcbp::ClientOpcode::SubdocCounter, require<Privilege::Read>);
    setup(cb::mcbp::ClientOpcode::SubdocCounter, require<Privilege::Upsert>);

    /* Multi-Path commands */
    setup(cb::mcbp::ClientOpcode::SubdocMultiLookup, require<Privilege::Read>);
    setup(cb::mcbp::ClientOpcode::SubdocMultiMutation,
          require<Privilege::Read>);
    setup(cb::mcbp::ClientOpcode::SubdocMultiMutation,
          require<Privilege::Upsert>);

    /* Scrub the data */
    setup(cb::mcbp::ClientOpcode::Scrub, require<Privilege::NodeManagement>);
    /* Refresh the ISASL data */
    setup(cb::mcbp::ClientOpcode::IsaslRefresh,
          require<Privilege::SecurityManagement>);
    /* Refresh the SSL certificates */
    setup(cb::mcbp::ClientOpcode::SslCertsRefresh,
          require<Privilege::SecurityManagement>);
    /* Internal timer ioctl */
    setup(cb::mcbp::ClientOpcode::GetCmdTimer, empty);
    /* ns_server - memcached session validation */
    setup(cb::mcbp::ClientOpcode::SetCtrlToken,
          require<Privilege::SessionManagement>);
    setup(cb::mcbp::ClientOpcode::GetCtrlToken,
          require<Privilege::SessionManagement>);

    // Drop a privilege from the effective set
    setup(cb::mcbp::ClientOpcode::DropPrivilege, empty);

    setup(cb::mcbp::ClientOpcode::UpdateExternalUserPermissions,
          require<Privilege::SecurityManagement>);

    /* Refresh the RBAC data */
    setup(cb::mcbp::ClientOpcode::RbacRefresh,
          require<Privilege::SecurityManagement>);

    setup(cb::mcbp::ClientOpcode::AuthProvider,
          require<Privilege::SecurityManagement>);

    /// @todo change priv to CollectionManagement
    setup(cb::mcbp::ClientOpcode::CollectionsSetManifest,
          require<Privilege::BucketManagement>);

    /// all clients may need to read the manifest
    setup(cb::mcbp::ClientOpcode::CollectionsGetManifest, empty);
    setup(cb::mcbp::ClientOpcode::CollectionsGetID, empty);
    setup(cb::mcbp::ClientOpcode::CollectionsGetScopeID, empty);

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
