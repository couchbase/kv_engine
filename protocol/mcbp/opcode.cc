/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <fmt/format.h>
#include <mcbp/protocol/opcode.h>
#include <nlohmann/json.hpp>
#include <platform/string_hex.h>
#include <algorithm>
#include <array>
#include <bitset>
#include <cctype>
#include <stdexcept>

namespace cb::mcbp {

/// Enum describing the various attributes associated with an opcode
enum class Attribute {
    /// If the opcode is supported (at some point we might drop support
    /// for certain commands). This allows us to reply to the client that
    /// we know about the command, but we don't support it (rather than
    /// return "unknown command")
    Supported,
    /// Does the command support adding a durability specification in the
    /// frame info section of the command  (It does not make sense to
    /// add durability spec to a get operation for instance).
    Durability,
    /// May this command be reordered or is this a "full barrier"
    Reorder,
    /// Does the key field contain a key which may contain a collection
    /// identifier
    Collection,
    /// Does the command support adding a flag in frame info to instruct
    /// the operation to preserve the TTL from the existing document
    PreserveTtl,
    /// When throttling is enabled (serverless mode) only commands with
    /// the SubjectForThrottling set may be throttled when the user
    /// exceeds its quota
    SubjectForThrottling,
    /// Before removing commands they'll be deprecated for a while
    Deprecated,
    /// Does the command cause new data to be added to the system or not.
    /// These operations may be blocked when setting the system in
    /// "read only mode" (internal users are still allowed to override this
    /// for replication etc)
    ClientWritingData,
    /// MB-52884 Some commands returns data in different threads
    ///          than the front end thread, and will race trying
    ///          to access the incomming packet data. In these
    ///          cases we might need to copy out the incomming
    ///          packet.
    MustPreserveBuffer,
    /// Does the operation swallow the response in some cases
    IsQuiet,
    Count
};

/// Each opcode contains a name and a set of attributes.
struct OpcodeMeta {
    OpcodeMeta() = default;
    OpcodeMeta(std::string_view name, const std::vector<Attribute>& properties)
        : name(name) {
        for (const auto& attr : properties) {
            attributes.set(static_cast<int>(attr));
        }
    }

    bool check(Attribute attr) const {
        return attributes.test(static_cast<int>(attr));
    }

    std::string_view name;
    std::bitset<static_cast<std::size_t>(Attribute::Count)> attributes;
};

/// A singleton class containing information for all opcodes.
class OpcodeInformationService {
public:
    bool isValid(ClientOpcode opcode) const {
        return !opcodes[static_cast<int>(opcode)].name.empty();
    }
    std::string_view name(ClientOpcode opcode) const {
        if (!isValid(opcode)) {
            throw std::invalid_argument(fmt::format(
                    "OpcodeInformationService::name(): Unknown command {}",
                    to_hex(uint8_t(opcode))));
        }
        return opcodes[static_cast<int>(opcode)].name;
    }
    bool isSupported(ClientOpcode opcode) const {
        return isValid(opcode) &&
               opcodes[static_cast<int>(opcode)].check(Attribute::Supported);
    }
    bool isDurability(ClientOpcode opcode) const {
        ensureValid(opcode, __func__);
        return opcodes[static_cast<int>(opcode)].check(Attribute::Durability);
    }
    bool isReorder(ClientOpcode opcode) const {
        ensureValid(opcode, __func__);
        return opcodes[static_cast<int>(opcode)].check(Attribute::Reorder);
    }
    bool isCollection(ClientOpcode opcode) const {
        ensureValid(opcode, __func__);
        return opcodes[static_cast<int>(opcode)].check(Attribute::Collection);
    }
    bool isPreserveTtl(ClientOpcode opcode) const {
        ensureValid(opcode, __func__);
        return opcodes[static_cast<int>(opcode)].check(Attribute::PreserveTtl);
    }
    bool isSubjectForThrottling(ClientOpcode opcode) const {
        ensureValid(opcode, __func__);
        return opcodes[static_cast<int>(opcode)].check(
                Attribute::SubjectForThrottling);
    }
    bool isDeprecated(ClientOpcode opcode) const {
        ensureValid(opcode, __func__);
        return opcodes[static_cast<int>(opcode)].check(Attribute::Deprecated);
    }
    bool isClientWritingData(ClientOpcode opcode) const {
        ensureValid(opcode, __func__);
        return opcodes[static_cast<int>(opcode)].check(
                Attribute::ClientWritingData);
    }
    bool mustPreserveBuffer(ClientOpcode opcode) const {
        ensureValid(opcode, __func__);
        return opcodes[static_cast<int>(opcode)].check(
                Attribute::MustPreserveBuffer);
    }
    bool isQuiet(ClientOpcode opcode) const {
        ensureValid(opcode, __func__);
        return opcodes[static_cast<int>(opcode)].check(Attribute::IsQuiet);
    }

    /**
     * Try to look up the opcode with the provided name
     */
    ClientOpcode lookup(std::string_view name) const {
        for (std::size_t ii = 0; ii < opcodes.size(); ++ii) {
            if (opcodes[ii].name == name) {
                return static_cast<ClientOpcode>(ii);
            }
        }
        return ClientOpcode::Invalid;
    }

    OpcodeInformationService() {
        using namespace std::string_view_literals;
        setup(ClientOpcode::Get,
              {"GET"sv,
               {Attribute::Supported,
                Attribute::Reorder,
                Attribute::Collection,
                Attribute::SubjectForThrottling}});
        setup(ClientOpcode::Set,
              {"SET"sv,
               {Attribute::Supported,
                Attribute::Durability,
                Attribute::Reorder,
                Attribute::Collection,
                Attribute::PreserveTtl,
                Attribute::SubjectForThrottling,
                Attribute::ClientWritingData}});
        setup(ClientOpcode::Add,
              {"ADD"sv,
               {Attribute::Supported,
                Attribute::Durability,
                Attribute::Reorder,
                Attribute::Collection,
                Attribute::SubjectForThrottling,
                Attribute::ClientWritingData}});
        setup(ClientOpcode::Replace,
              {"REPLACE"sv,
               {Attribute::Supported,
                Attribute::Durability,
                Attribute::Reorder,
                Attribute::Collection,
                Attribute::PreserveTtl,
                Attribute::SubjectForThrottling,
                Attribute::ClientWritingData}});
        setup(ClientOpcode::Delete,
              {"DELETE"sv,
               {Attribute::Supported,
                Attribute::Durability,
                Attribute::Reorder,
                Attribute::Collection,
                Attribute::SubjectForThrottling}});
        setup(ClientOpcode::Increment,
              {"INCREMENT"sv,
               {Attribute::Supported,
                Attribute::Durability,
                Attribute::Reorder,
                Attribute::Collection,
                Attribute::PreserveTtl,
                Attribute::SubjectForThrottling,
                Attribute::ClientWritingData}});
        setup(ClientOpcode::Decrement,
              {"DECREMENT"sv,
               {Attribute::Supported,
                Attribute::Durability,
                Attribute::Reorder,
                Attribute::Collection,
                Attribute::PreserveTtl,
                Attribute::SubjectForThrottling,
                Attribute::ClientWritingData}});
        setup(ClientOpcode::Quit, {"QUIT"sv, {Attribute::Supported}});
        setup(ClientOpcode::Flush_Unsupported, {"FLUSH"sv, {}});
        setup(ClientOpcode::Getq,
              {"GETQ"sv,
               {Attribute::Supported,
                Attribute::Collection,
                Attribute::SubjectForThrottling,
                Attribute::Deprecated,
                Attribute::IsQuiet}});
        setup(ClientOpcode::Noop, {"NOOP"sv, {Attribute::Supported}});
        setup(ClientOpcode::Version, {"VERSION"sv, {Attribute::Supported}});
        setup(ClientOpcode::Getk,
              {"GETK"sv,
               {Attribute::Supported,
                Attribute::Reorder,
                Attribute::Collection,
                Attribute::SubjectForThrottling,
                Attribute::Deprecated}});
        setup(ClientOpcode::Getkq,
              {"GETKQ"sv,
               {Attribute::Supported,
                Attribute::Collection,
                Attribute::SubjectForThrottling,
                Attribute::Deprecated,
                Attribute::IsQuiet}});
        setup(ClientOpcode::Append,
              {"APPEND"sv,
               {Attribute::Supported,
                Attribute::Durability,
                Attribute::Reorder,
                Attribute::Collection,
                Attribute::PreserveTtl,
                Attribute::SubjectForThrottling,
                Attribute::ClientWritingData}});
        setup(ClientOpcode::Prepend,
              {"PREPEND"sv,
               {Attribute::Supported,
                Attribute::Durability,
                Attribute::Reorder,
                Attribute::Collection,
                Attribute::PreserveTtl,
                Attribute::SubjectForThrottling,
                Attribute::ClientWritingData}});
        setup(ClientOpcode::Stat,
              {"STAT"sv,
               {Attribute::Supported, Attribute::MustPreserveBuffer}});
        setup(ClientOpcode::Setq,
              {"SETQ"sv,
               {Attribute::Supported,
                Attribute::Collection,
                Attribute::PreserveTtl,
                Attribute::SubjectForThrottling,
                Attribute::Deprecated,
                Attribute::ClientWritingData,
                Attribute::IsQuiet}});
        setup(ClientOpcode::Addq,
              {"ADDQ"sv,
               {Attribute::Supported,
                Attribute::Collection,
                Attribute::SubjectForThrottling,
                Attribute::Deprecated,
                Attribute::ClientWritingData,
                Attribute::IsQuiet}});
        setup(ClientOpcode::Replaceq,
              {"REPLACEQ"sv,
               {Attribute::Supported,
                Attribute::Collection,
                Attribute::PreserveTtl,
                Attribute::SubjectForThrottling,
                Attribute::Deprecated,
                Attribute::ClientWritingData,
                Attribute::IsQuiet}});
        setup(ClientOpcode::Deleteq,
              {"DELETEQ"sv,
               {Attribute::Supported,
                Attribute::Collection,
                Attribute::SubjectForThrottling,
                Attribute::Deprecated,
                Attribute::IsQuiet}});
        setup(ClientOpcode::Incrementq,
              {"INCREMENTQ"sv,
               {Attribute::Supported,
                Attribute::Collection,
                Attribute::PreserveTtl,
                Attribute::SubjectForThrottling,
                Attribute::Deprecated,
                Attribute::ClientWritingData,
                Attribute::IsQuiet}});
        setup(ClientOpcode::Decrementq,
              {"DECREMENTQ"sv,
               {Attribute::Supported,
                Attribute::Collection,
                Attribute::PreserveTtl,
                Attribute::SubjectForThrottling,
                Attribute::Deprecated,
                Attribute::ClientWritingData,
                Attribute::IsQuiet}});
        setup(ClientOpcode::Quitq,
              {"QUITQ"sv,
               {Attribute::Supported,
                Attribute::Deprecated,
                Attribute::IsQuiet}});
        setup(ClientOpcode::Flushq_Unsupported, {"FLUSHQ"sv, {}});
        setup(ClientOpcode::Appendq,
              {"APPENDQ"sv,
               {Attribute::Supported,
                Attribute::Collection,
                Attribute::PreserveTtl,
                Attribute::SubjectForThrottling,
                Attribute::Deprecated,
                Attribute::ClientWritingData,
                Attribute::IsQuiet}});
        setup(ClientOpcode::Prependq,
              {"PREPENDQ"sv,
               {Attribute::Supported,
                Attribute::Collection,
                Attribute::PreserveTtl,
                Attribute::SubjectForThrottling,
                Attribute::Deprecated,
                Attribute::ClientWritingData,
                Attribute::IsQuiet}});
        setup(ClientOpcode::Verbosity, {"VERBOSITY"sv, {Attribute::Supported}});
        setup(ClientOpcode::Touch,
              {"TOUCH"sv,
               {Attribute::Supported,
                Attribute::Durability,
                Attribute::Reorder,
                Attribute::Collection,
                Attribute::SubjectForThrottling,
                Attribute::ClientWritingData}});
        setup(ClientOpcode::Gat,
              {"GAT"sv,
               {Attribute::Supported,
                Attribute::Durability,
                Attribute::Reorder,
                Attribute::Collection,
                Attribute::SubjectForThrottling,
                Attribute::ClientWritingData}});
        setup(ClientOpcode::Gatq,
              {"GATQ"sv,
               {Attribute::Supported,
                Attribute::Collection,
                Attribute::SubjectForThrottling,
                Attribute::ClientWritingData,
                Attribute::IsQuiet}});
        setup(ClientOpcode::Hello, {"HELLO"sv, {Attribute::Supported}});
        setup(ClientOpcode::SaslListMechs,
              {"SASL_LIST_MECHS"sv, {Attribute::Supported}});
        setup(ClientOpcode::SaslAuth, {"SASL_AUTH"sv, {Attribute::Supported}});
        setup(ClientOpcode::SaslStep, {"SASL_STEP"sv, {Attribute::Supported}});
        setup(ClientOpcode::IoctlGet, {"IOCTL_GET"sv, {Attribute::Supported}});
        setup(ClientOpcode::IoctlSet, {"IOCTL_SET"sv, {Attribute::Supported}});
        setup(ClientOpcode::ConfigValidate,
              {"CONFIG_VALIDATE"sv, {Attribute::Supported}});
        setup(ClientOpcode::ConfigReload,
              {"CONFIG_RELOAD"sv, {Attribute::Supported}});
        setup(ClientOpcode::AuditPut, {"AUDIT_PUT"sv, {Attribute::Supported}});
        setup(ClientOpcode::AuditConfigReload,
              {"AUDIT_CONFIG_RELOAD"sv, {Attribute::Supported}});
        setup(ClientOpcode::Shutdown, {"SHUTDOWN"sv, {Attribute::Supported}});
        setup(ClientOpcode::SetBucketThrottleProperties,
              {"SET_BUCKET_THROTTLE_PROPERTIES"sv,
               {Attribute::Supported, Attribute::Reorder}});
        setup(ClientOpcode::SetBucketDataLimitExceeded,
              {"SET_BUCKET_DATA_LIMIT_EXCEEDED"sv,
               {Attribute::Supported, Attribute::Reorder}});
        setup(ClientOpcode::SetNodeThrottleProperties_Unsupported,
              {"SET_NODE_THROTTLE_PROPERTIES"sv,
               {Attribute::Supported, Attribute::Reorder}});
        setup(ClientOpcode::SetActiveEncryptionKeys,
              {"SET_ACTIVE_ENCRYPTION_KEYS"sv,
               {Attribute::Supported, Attribute::Reorder}});
        setup(ClientOpcode::PruneEncryptionKeys,
              {"PRUNE_ENCRYPTION_KEYS"sv,
               {Attribute::Supported, Attribute::Reorder}});
        setup(ClientOpcode::Rget_Unsupported,
              {"RGET"sv, {Attribute::SubjectForThrottling}});
        setup(ClientOpcode::Rset_Unsupported,
              {"RSET"sv, {Attribute::SubjectForThrottling}});
        setup(ClientOpcode::Rsetq_Unsupported,
              {"RSETQ"sv,
               {Attribute::SubjectForThrottling, Attribute::IsQuiet}});
        setup(ClientOpcode::Rappend_Unsupported,
              {"RAPPEND"sv, {Attribute::SubjectForThrottling}});
        setup(ClientOpcode::Rappendq_Unsupported,
              {"RAPPENDQ"sv,
               {Attribute::SubjectForThrottling, Attribute::IsQuiet}});
        setup(ClientOpcode::Rprepend_Unsupported,
              {"RPREPEND"sv, {Attribute::SubjectForThrottling}});
        setup(ClientOpcode::Rprependq_Unsupported,
              {"RPREPENDQ"sv,
               {Attribute::SubjectForThrottling, Attribute::IsQuiet}});
        setup(ClientOpcode::Rdelete_Unsupported,
              {"RDELETE"sv, {Attribute::SubjectForThrottling}});
        setup(ClientOpcode::Rdeleteq_Unsupported,
              {"RDELETEQ"sv,
               {Attribute::SubjectForThrottling, Attribute::IsQuiet}});
        setup(ClientOpcode::Rincr_Unsupported,
              {"RINCR"sv, {Attribute::SubjectForThrottling}});
        setup(ClientOpcode::Rincrq_Unsupported,
              {"RINCRQ"sv,
               {Attribute::SubjectForThrottling, Attribute::IsQuiet}});
        setup(ClientOpcode::Rdecr_Unsupported,
              {"RDECR"sv, {Attribute::SubjectForThrottling}});
        setup(ClientOpcode::Rdecrq_Unsupported,
              {"RDECRQ"sv,
               {Attribute::SubjectForThrottling, Attribute::IsQuiet}});
        setup(ClientOpcode::SetVbucket,
              {"SET_VBUCKET"sv, {Attribute::Supported}});
        setup(ClientOpcode::GetVbucket,
              {"GET_VBUCKET"sv, {Attribute::Supported}});
        setup(ClientOpcode::DelVbucket,
              {"DEL_VBUCKET"sv, {Attribute::Supported}});
        setup(ClientOpcode::TapConnect_Unsupported, {"TAP_CONNECT"sv, {}});
        setup(ClientOpcode::TapMutation_Unsupported, {"TAP_MUTATION"sv, {}});
        setup(ClientOpcode::TapDelete_Unsupported, {"TAP_DELETE"sv, {}});
        setup(ClientOpcode::TapFlush_Unsupported, {"TAP_FLUSH"sv, {}});
        setup(ClientOpcode::TapOpaque_Unsupported, {"TAP_OPAQUE"sv, {}});
        setup(ClientOpcode::TapVbucketSet_Unsupported,
              {"TAP_VBUCKET_SET"sv, {}});
        setup(ClientOpcode::TapCheckpointStart_Unsupported,
              {"TAP_CHECKPOINT_START"sv, {}});
        setup(ClientOpcode::TapCheckpointEnd_Unsupported,
              {"TAP_CHECKPOINT_END"sv, {}});
        setup(ClientOpcode::GetAllVbSeqnos,
              {"GET_ALL_VB_SEQNOS"sv,
               {Attribute::Supported, Attribute::Reorder}});
        setup(ClientOpcode::GetEx,
              {"GET_EX"sv,
               {Attribute::Supported,
                Attribute::Reorder,
                Attribute::Collection,
                Attribute::SubjectForThrottling}});
        setup(ClientOpcode::GetExReplica,
              {"GET_EX_REPLICA"sv,
               {Attribute::Supported,
                Attribute::Reorder,
                Attribute::Collection,
                Attribute::SubjectForThrottling}});
        setup(ClientOpcode::DcpOpen, {"DCP_OPEN"sv, {Attribute::Supported}});
        setup(ClientOpcode::DcpAddStream,
              {"DCP_ADD_STREAM"sv, {Attribute::Supported}});
        setup(ClientOpcode::DcpCloseStream,
              {"DCP_CLOSE_STREAM"sv, {Attribute::Supported}});
        setup(ClientOpcode::DcpStreamReq,
              {"DCP_STREAM_REQ"sv,
               {Attribute::Supported, Attribute::MustPreserveBuffer}});
        setup(ClientOpcode::DcpGetFailoverLog,
              {"DCP_GET_FAILOVER_LOG"sv, {Attribute::Supported}});
        setup(ClientOpcode::DcpStreamEnd,
              {"DCP_STREAM_END"sv, {Attribute::Supported}});
        setup(ClientOpcode::DcpSnapshotMarker,
              {"DCP_SNAPSHOT_MARKER"sv, {Attribute::Supported}});
        setup(ClientOpcode::DcpMutation,
              {"DCP_MUTATION"sv, {Attribute::Supported}});
        setup(ClientOpcode::DcpDeletion,
              {"DCP_DELETION"sv, {Attribute::Supported}});
        setup(ClientOpcode::DcpExpiration,
              {"DCP_EXPIRATION"sv, {Attribute::Supported}});
        setup(ClientOpcode::DcpFlush_Unsupported, {"DCP_FLUSH"sv, {}});
        setup(ClientOpcode::DcpSetVbucketState,
              {"DCP_SET_VBUCKET_STATE"sv, {Attribute::Supported}});
        setup(ClientOpcode::DcpNoop, {"DCP_NOOP"sv, {Attribute::Supported}});
        setup(ClientOpcode::DcpBufferAcknowledgement,
              {"DCP_BUFFER_ACKNOWLEDGEMENT"sv, {Attribute::Supported}});
        setup(ClientOpcode::DcpControl,
              {"DCP_CONTROL"sv, {Attribute::Supported}});
        setup(ClientOpcode::DcpSystemEvent,
              {"DCP_SYSTEM_EVENT"sv, {Attribute::Supported}});
        setup(ClientOpcode::DcpPrepare,
              {"DCP_PREPARE"sv, {Attribute::Supported}});
        setup(ClientOpcode::DcpSeqnoAcknowledged,
              {"DCP_SEQNO_ACKNOWLEDGED"sv, {Attribute::Supported}});
        setup(ClientOpcode::DcpCommit,
              {"DCP_COMMIT"sv, {Attribute::Supported}});
        setup(ClientOpcode::DcpAbort, {"DCP_ABORT"sv, {Attribute::Supported}});
        setup(ClientOpcode::DcpSeqnoAdvanced,
              {"DCP_SEQNO_ADVANCED"sv, {Attribute::Supported}});
        setup(ClientOpcode::DcpCachedValue,
              {"DCP_CACHED_VALUE"sv, {Attribute::Supported}});
        setup(ClientOpcode::DcpCachedKeyMeta,
              {"DCP_CACHED_KEY_META"sv, {Attribute::Supported}});
        setup(ClientOpcode::DcpCacheTransferEnd,
              {"DCP_CACHE_TRANSFER_END"sv, {Attribute::Supported}});
        setup(ClientOpcode::DcpOsoSnapshot,
              {"DCP_OSO_SNAPSHOT"sv, {Attribute::Supported}});
        setup(ClientOpcode::StopPersistence,
              {"STOP_PERSISTENCE"sv, {Attribute::Supported}});
        setup(ClientOpcode::StartPersistence,
              {"START_PERSISTENCE"sv, {Attribute::Supported}});
        setup(ClientOpcode::SetParam, {"SET_PARAM"sv, {Attribute::Supported}});
        setup(ClientOpcode::GetReplica,
              {"GET_REPLICA"sv,
               {Attribute::Supported,
                Attribute::Reorder,
                Attribute::Collection,
                Attribute::SubjectForThrottling}});
        setup(ClientOpcode::CreateBucket,
              {"CREATE_BUCKET"sv, {Attribute::Supported}});
        setup(ClientOpcode::DeleteBucket,
              {"DELETE_BUCKET"sv, {Attribute::Supported}});
        setup(ClientOpcode::ListBuckets,
              {"LIST_BUCKETS"sv, {Attribute::Supported}});
        setup(ClientOpcode::SelectBucket,
              {"SELECT_BUCKET"sv, {Attribute::Supported}});
        setup(ClientOpcode::PauseBucket,
              {"PAUSE_BUCKET"sv, {Attribute::Supported}});
        setup(ClientOpcode::ResumeBucket,
              {"RESUME_BUCKET"sv, {Attribute::Supported}});
        setup(ClientOpcode::ObserveSeqno,
              {"OBSERVE_SEQNO"sv,
               {Attribute::Supported, Attribute::SubjectForThrottling}});
        setup(ClientOpcode::Observe,
              {"OBSERVE"sv,
               {Attribute::Supported, Attribute::SubjectForThrottling}});
        setup(ClientOpcode::EvictKey,
              {"EVICT_KEY"sv,
               {Attribute::Supported,
                Attribute::Reorder,
                Attribute::Collection}});
        setup(ClientOpcode::GetLocked,
              {"GET_LOCKED"sv,
               {Attribute::Supported,
                Attribute::Reorder,
                Attribute::Collection,
                Attribute::SubjectForThrottling}});
        setup(ClientOpcode::UnlockKey,
              {"UNLOCK_KEY"sv,
               {Attribute::Supported,
                Attribute::Reorder,
                Attribute::Collection,
                Attribute::SubjectForThrottling}});
        setup(ClientOpcode::GetFailoverLog,
              {"GET_FAILOVER_LOG"sv, {Attribute::Supported}});
        setup(ClientOpcode::LastClosedCheckpoint_Unsupported,
              {"LAST_CLOSED_CHECKPOINT"sv, {}});
        setup(ClientOpcode::DeregisterTapClient_Unsupported,
              {"DEREGISTER_TAP_CLIENT"sv, {}});
        setup(ClientOpcode::ResetReplicationChain_Unsupported,
              {"RESET_REPLICATION_CHAIN"sv, {}});
        setup(ClientOpcode::GetMeta,
              {"GET_META"sv,
               {Attribute::Supported,
                Attribute::Collection,
                Attribute::SubjectForThrottling}});
        setup(ClientOpcode::GetqMeta,
              {"GETQ_META"sv,
               {Attribute::Supported,
                Attribute::Collection,
                Attribute::SubjectForThrottling,
                Attribute::Deprecated,
                Attribute::IsQuiet}});
        setup(ClientOpcode::SetWithMeta,
              {"SET_WITH_META"sv,
               {Attribute::Supported,
                Attribute::Collection,
                Attribute::SubjectForThrottling,
                Attribute::ClientWritingData}});
        setup(ClientOpcode::SetqWithMeta,
              {"SETQ_WITH_META"sv,
               {Attribute::Supported,
                Attribute::Collection,
                Attribute::SubjectForThrottling,
                Attribute::Deprecated,
                Attribute::ClientWritingData,
                Attribute::IsQuiet}});
        setup(ClientOpcode::AddWithMeta,
              {"ADD_WITH_META"sv,
               {Attribute::Supported,
                Attribute::Collection,
                Attribute::SubjectForThrottling,
                Attribute::ClientWritingData}});
        setup(ClientOpcode::AddqWithMeta,
              {"ADDQ_WITH_META"sv,
               {Attribute::Supported,
                Attribute::Collection,
                Attribute::SubjectForThrottling,
                Attribute::Deprecated,
                Attribute::ClientWritingData,
                Attribute::IsQuiet}});
        setup(ClientOpcode::SnapshotVbStates_Unsupported,
              {"SNAPSHOT_VB_STATES"sv, {}});
        setup(ClientOpcode::VbucketBatchCount_Unsupported,
              {"VBUCKET_BATCH_COUNT"sv, {}});
        setup(ClientOpcode::DelWithMeta,
              {"DEL_WITH_META"sv,
               {Attribute::Supported,
                Attribute::Collection,
                Attribute::SubjectForThrottling}});
        setup(ClientOpcode::DelqWithMeta,
              {"DELQ_WITH_META"sv,
               {Attribute::Supported,
                Attribute::Collection,
                Attribute::SubjectForThrottling,
                Attribute::Deprecated,
                Attribute::IsQuiet}});
        setup(ClientOpcode::CreateCheckpoint_Unsupported,
              {"CREATE_CHECKPOINT"sv, {}});
        setup(ClientOpcode::NotifyVbucketUpdate_Unsupported,
              {"NOTIFY_VBUCKET_UPDATE"sv, {}});
        setup(ClientOpcode::EnableTraffic,
              {"ENABLE_TRAFFIC"sv, {Attribute::Supported}});
        setup(ClientOpcode::DisableTraffic,
              {"DISABLE_TRAFFIC"sv, {Attribute::Supported}});
        setup(ClientOpcode::Ifconfig, {"IFCONFIG"sv, {Attribute::Supported}});
        setup(ClientOpcode::ChangeVbFilter_Unsupported,
              {"CHANGE_VB_FILTER"sv, {}});
        setup(ClientOpcode::CheckpointPersistence_Unsupported,
              {"CHECKPOINT_PERSISTENCE"sv, {}});
        setup(ClientOpcode::ReturnMeta,
              {"RETURN_META"sv,
               {Attribute::Supported,
                Attribute::Collection,
                Attribute::SubjectForThrottling,
                Attribute::ClientWritingData}});
        setup(ClientOpcode::CompactDb,
              {"COMPACT_DB"sv, {Attribute::Supported}});
        setup(ClientOpcode::SetClusterConfig,
              {"SET_CLUSTER_CONFIG"sv, {Attribute::Supported}});
        setup(ClientOpcode::GetClusterConfig,
              {"GET_CLUSTER_CONFIG"sv,
               {Attribute::Supported, Attribute::Reorder}});
        setup(ClientOpcode::GetRandomKey,
              {"GET_RANDOM_KEY"sv,
               {Attribute::Supported, Attribute::SubjectForThrottling}});
        setup(ClientOpcode::SeqnoPersistence,
              {"SEQNO_PERSISTENCE"sv, {Attribute::Supported}});
        setup(ClientOpcode::GetKeys,
              {"GET_KEYS"sv,
               {Attribute::Supported,
                Attribute::Collection,
                Attribute::SubjectForThrottling,
                Attribute::MustPreserveBuffer}});
        setup(ClientOpcode::CollectionsSetManifest,
              {"COLLECTIONS_SET_MANIFEST"sv, {Attribute::Supported}});
        setup(ClientOpcode::CollectionsGetManifest,
              {"COLLECTIONS_GET_MANIFEST"sv, {Attribute::Supported}});
        setup(ClientOpcode::CollectionsGetID,
              {"COLLECTIONS_GET_ID"sv, {Attribute::Supported}});
        setup(ClientOpcode::CollectionsGetScopeID,
              {"COLLECTIONS_GET_SCOPE_ID"sv, {Attribute::Supported}});
        setup(ClientOpcode::SetDriftCounterState_Unsupported,
              {"SET_DRIFT_COUNTER_STATE"sv, {}});
        setup(ClientOpcode::GetAdjustedTime_Unsupported,
              {"GET_ADJUSTED_TIME"sv, {}});
        setup(ClientOpcode::SubdocGet,
              {"SUBDOC_GET"sv,
               {Attribute::Supported,
                Attribute::Reorder,
                Attribute::Collection,
                Attribute::SubjectForThrottling}});
        setup(ClientOpcode::SubdocExists,
              {"SUBDOC_EXISTS"sv,
               {Attribute::Supported,
                Attribute::Reorder,
                Attribute::Collection,
                Attribute::SubjectForThrottling}});
        setup(ClientOpcode::SubdocDictAdd,
              {"SUBDOC_DICT_ADD"sv,
               {Attribute::Supported,
                Attribute::Durability,
                Attribute::Reorder,
                Attribute::Collection,
                Attribute::PreserveTtl,
                Attribute::SubjectForThrottling,
                Attribute::ClientWritingData}});
        setup(ClientOpcode::SubdocDictUpsert,
              {"SUBDOC_DICT_UPSERT"sv,
               {Attribute::Supported,
                Attribute::Durability,
                Attribute::Reorder,
                Attribute::Collection,
                Attribute::PreserveTtl,
                Attribute::SubjectForThrottling,
                Attribute::ClientWritingData}});
        setup(ClientOpcode::SubdocDelete,
              {"SUBDOC_DELETE"sv,
               {Attribute::Supported,
                Attribute::Durability,
                Attribute::Reorder,
                Attribute::Collection,
                Attribute::PreserveTtl,
                Attribute::SubjectForThrottling}});
        setup(ClientOpcode::SubdocReplace,
              {"SUBDOC_REPLACE"sv,
               {Attribute::Supported,
                Attribute::Durability,
                Attribute::Reorder,
                Attribute::Collection,
                Attribute::PreserveTtl,
                Attribute::SubjectForThrottling,
                Attribute::ClientWritingData}});
        setup(ClientOpcode::SubdocArrayPushLast,
              {"SUBDOC_ARRAY_PUSH_LAST"sv,
               {Attribute::Supported,
                Attribute::Durability,
                Attribute::Reorder,
                Attribute::Collection,
                Attribute::PreserveTtl,
                Attribute::SubjectForThrottling,
                Attribute::ClientWritingData}});
        setup(ClientOpcode::SubdocArrayPushFirst,
              {"SUBDOC_ARRAY_PUSH_FIRST"sv,
               {Attribute::Supported,
                Attribute::Durability,
                Attribute::Reorder,
                Attribute::Collection,
                Attribute::PreserveTtl,
                Attribute::SubjectForThrottling,
                Attribute::ClientWritingData}});
        setup(ClientOpcode::SubdocArrayInsert,
              {"SUBDOC_ARRAY_INSERT"sv,
               {Attribute::Supported,
                Attribute::Durability,
                Attribute::Reorder,
                Attribute::Collection,
                Attribute::PreserveTtl,
                Attribute::SubjectForThrottling,
                Attribute::ClientWritingData}});
        setup(ClientOpcode::SubdocArrayAddUnique,
              {"SUBDOC_ARRAY_ADD_UNIQUE"sv,
               {Attribute::Supported,
                Attribute::Durability,
                Attribute::Reorder,
                Attribute::Collection,
                Attribute::PreserveTtl,
                Attribute::SubjectForThrottling,
                Attribute::ClientWritingData}});
        setup(ClientOpcode::SubdocCounter,
              {"SUBDOC_COUNTER"sv,
               {Attribute::Supported,
                Attribute::Durability,
                Attribute::Reorder,
                Attribute::Collection,
                Attribute::PreserveTtl,
                Attribute::SubjectForThrottling,
                Attribute::ClientWritingData}});
        setup(ClientOpcode::SubdocMultiLookup,
              {"SUBDOC_MULTI_LOOKUP"sv,
               {Attribute::Supported,
                Attribute::Reorder,
                Attribute::Collection,
                Attribute::SubjectForThrottling}});
        setup(ClientOpcode::SubdocMultiMutation,
              {"SUBDOC_MULTI_MUTATION"sv,
               {Attribute::Supported,
                Attribute::Durability,
                Attribute::Reorder,
                Attribute::Collection,
                Attribute::PreserveTtl,
                Attribute::SubjectForThrottling,
                Attribute::ClientWritingData}});
        setup(ClientOpcode::SubdocGetCount,
              {"SUBDOC_GET_COUNT"sv,
               {Attribute::Supported,
                Attribute::Reorder,
                Attribute::Collection,
                Attribute::SubjectForThrottling}});
        setup(ClientOpcode::SubdocReplaceBodyWithXattr,
              {"SUBDOC_REPLACE_BODY_WITH_XATTR"sv,
               {Attribute::Supported,
                Attribute::Durability,
                Attribute::Reorder,
                Attribute::Collection,
                Attribute::PreserveTtl,
                Attribute::SubjectForThrottling,
                Attribute::ClientWritingData}});
        setup(ClientOpcode::RangeScanCreate,
              {"RANGE_SCAN_CREATE"sv,
               {Attribute::Supported,
                Attribute::Reorder,
                Attribute::SubjectForThrottling,
                Attribute::MustPreserveBuffer}});
        setup(ClientOpcode::RangeScanContinue,
              {"RANGE_SCAN_CONTINUE"sv,
               {Attribute::Supported,
                Attribute::SubjectForThrottling,
                Attribute::MustPreserveBuffer}});
        setup(ClientOpcode::RangeScanCancel,
              {"RANGE_SCAN_CANCEL"sv, {Attribute::Supported}});
        setup(ClientOpcode::PrepareSnapshot,
              {"PREPARE_SNAPSHOT"sv, {Attribute::Supported}});
        setup(ClientOpcode::ReleaseSnapshot,
              {"RELEASE_SNAPSHOT"sv, {Attribute::Supported}});
        setup(ClientOpcode::DownloadSnapshot,
              {"DOWNLOAD_SNAPSHOT"sv, {Attribute::Supported}});
        setup(ClientOpcode::GetFileFragment,
              {"GET_FILE_FRAGMENT"sv, {Attribute::Supported}});
        setup(ClientOpcode::Scrub_Unsupported, {"SCRUB"sv, {}});
        setup(ClientOpcode::IsaslRefresh,
              {"ISASL_REFRESH"sv, {Attribute::Supported}});
        setup(ClientOpcode::SslCertsRefresh_Unsupported,
              {"SSL_CERTS_REFRESH"sv, {}});
        setup(ClientOpcode::GetCmdTimer,
              {"GET_CMD_TIMER"sv, {Attribute::Supported}});
        setup(ClientOpcode::SetCtrlToken,
              {"SET_CTRL_TOKEN"sv, {Attribute::Supported}});
        setup(ClientOpcode::GetCtrlToken,
              {"GET_CTRL_TOKEN"sv, {Attribute::Supported}});
        setup(ClientOpcode::UpdateExternalUserPermissions,
              {"UPDATE_USER_PERMISSIONS"sv, {Attribute::Supported}});
        setup(ClientOpcode::RbacRefresh,
              {"RBAC_REFRESH"sv, {Attribute::Supported}});
        setup(ClientOpcode::AuthProvider,
              {"AUTH_PROVIDER"sv, {Attribute::Supported}});
        setup(ClientOpcode::DropPrivilege,
              {"DROP_PRIVILEGES"sv, {Attribute::Supported}});
        setup(ClientOpcode::AdjustTimeofday,
              {"ADJUST_TIMEOFDAY"sv, {Attribute::Supported}});
        setup(ClientOpcode::EwouldblockCtl,
              {"EWB_CTL"sv, {Attribute::Supported}});
        setup(ClientOpcode::GetErrorMap,
              {"GET_ERROR_MAP"sv, {Attribute::Supported}});
        setup(ClientOpcode::GetFusionStorageSnapshot,
              {"GET_FUSION_STORAGE_SNAPSHOT"sv, {Attribute::Supported}});
        setup(ClientOpcode::ReleaseFusionStorageSnapshot,
              {"RELEASE_FUSION_STORAGE_SNAPSHOT"sv, {Attribute::Supported}});
        setup(ClientOpcode::MountFusionVbucket,
              {"MOUNT_FUSION_VBUCKET"sv, {Attribute::Supported}});
        setup(ClientOpcode::UnmountFusionVbucket,
              {"UNMOUNT_FUSION_VBUCKET"sv, {Attribute::Supported}});
        setup(ClientOpcode::SyncFusionLogstore,
              {"SYNC_FUSION_LOGSTORE"sv, {Attribute::Supported}});
        setup(ClientOpcode::StartFusionUploader,
              {"START_FUSION_UPLOADER"sv, {Attribute::Supported}});
        setup(ClientOpcode::StopFusionUploader,
              {"STOP_FUSION_UPLOADER"sv, {Attribute::Supported}});
        setup(ClientOpcode::SetChronicleAuthToken,
              {"SET_CHRONICLE_AUTH_TOKEN"sv, {Attribute::Supported}});
        setup(ClientOpcode::DeleteFusionNamespace,
              {"DELETE_FUSION_NAMESPACE"sv, {Attribute::Supported}});
        setup(ClientOpcode::GetFusionNamespaces,
              {"GET_FUSION_NAMESPACES"sv, {Attribute::Supported}});
    }

protected:
    void ensureValid(ClientOpcode opcode, const char* method) const {
        // @todo look at the use pattern for the various methods to see
        //       if its even possible that we're trying to call the operations
        //       with invalid values (that would only be from reading the
        //       command off the socket until we call "validate" (and the
        //       failure path for invalid packets)
        if (!isValid(opcode)) {
            throw std::runtime_error(fmt::format(
                    "OpcodeInformationService::{}(): Unknown command {}",
                    method,
                    to_hex(uint8_t(opcode))));
        }
    }

    void setup(ClientOpcode opcode, OpcodeMeta meta) {
        opcodes[static_cast<int>(opcode)] = std::move(meta);
    }
    std::array<OpcodeMeta, 256> opcodes;
};

/// The one and only instance
static const OpcodeInformationService opcodeInformationServiceInstance;

bool is_valid_opcode(ClientOpcode opcode) {
    return opcodeInformationServiceInstance.isValid(opcode);
}

bool is_valid_opcode(ServerOpcode opcode) {
    switch (opcode) {
    case ServerOpcode::ClustermapChangeNotification:
    case ServerOpcode::Authenticate:
    case ServerOpcode::ActiveExternalUsers:
    case ServerOpcode::GetAuthorization:
        return true;
    }
    return false;
}

bool is_supported_opcode(ClientOpcode opcode) {
    return opcodeInformationServiceInstance.isSupported(opcode);
}

bool is_durability_supported(ClientOpcode opcode) {
    return opcodeInformationServiceInstance.isDurability(opcode);
}

bool is_reorder_supported(ClientOpcode opcode) {
    return opcodeInformationServiceInstance.isReorder(opcode);
}

bool is_collection_command(ClientOpcode opcode) {
    return opcodeInformationServiceInstance.isCollection(opcode);
}

bool is_deprecated(ClientOpcode opcode) {
    if (!is_valid_opcode(opcode) || !is_supported_opcode(opcode)) {
        return false;
    }
    return opcodeInformationServiceInstance.isDeprecated(opcode);
}

bool is_preserve_ttl_supported(ClientOpcode opcode) {
    return opcodeInformationServiceInstance.isPreserveTtl(opcode);
}

bool is_subject_for_throttling(ClientOpcode opcode) {
    return opcodeInformationServiceInstance.isSubjectForThrottling(opcode);
}

bool is_client_writing_data(ClientOpcode opcode) {
    return opcodeInformationServiceInstance.isClientWritingData(opcode);
}

bool must_preserve_buffer(ClientOpcode opcode) {
    return opcodeInformationServiceInstance.mustPreserveBuffer(opcode);
}

bool is_quiet(ClientOpcode opcode) {
    return opcodeInformationServiceInstance.isQuiet(opcode);
}

std::ostream& operator<<(std::ostream& out, const ClientOpcode& opcode) {
    out << opcodeInformationServiceInstance.name(opcode);
    return out;
}

std::ostream& operator<<(std::ostream& out,
                         const cb::mcbp::ServerOpcode& opcode) {
    out << format_as(opcode);
    return out;
}

std::string format_as(ClientOpcode opcode) {
    return std::string{opcodeInformationServiceInstance.name(opcode)};
}

std::string format_as(ServerOpcode opcode) {
    return ::to_string(opcode);
}

void to_json(nlohmann::json& j, const ClientOpcode& opcode) {
    j = format_as(opcode);
}

void to_json(nlohmann::json& j, const ServerOpcode& opcode) {
    j = format_as(opcode);
}

ClientOpcode to_client_opcode(std::string_view string) {
    std::string input;
    std::transform(
            string.begin(), string.end(), std::back_inserter(input), ::toupper);
    // If the user used space between the words, replace with '_'
    std::ranges::replace(input, ' ', '_');

    auto ret = opcodeInformationServiceInstance.lookup(input);
    if (ret != ClientOpcode::Invalid) {
        return ret;
    }

    throw std::invalid_argument(
            fmt::format(R"(to_client_opcode(): unknown opcode: "{}")", string));
}
} // namespace cb::mcbp

std::string to_string(cb::mcbp::ClientOpcode opcode) {
    return format_as(opcode);
}

std::string to_string(cb::mcbp::ServerOpcode opcode) {
    using namespace cb::mcbp;
    switch (opcode) {
    case ServerOpcode::ClustermapChangeNotification:
        return "ClustermapChangeNotification";
    case ServerOpcode::Authenticate:
        return "Authenticate";
    case ServerOpcode::ActiveExternalUsers:
        return "ActiveExternalUsers";
    case ServerOpcode::GetAuthorization:
        return "GetAuthorization";
    }
    throw std::invalid_argument(
            "to_string(cb::mcbp::ServerOpcode): Invalid opcode: " +
            std::to_string(int(opcode)));
}
