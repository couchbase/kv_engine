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

#include <mcbp/protocol/opcode.h>
#include <algorithm>
#include <cctype>
#include <iterator>
#include <stdexcept>

std::string to_string(cb::mcbp::ClientOpcode opcode) {
    using namespace cb::mcbp;

    switch (opcode) {
    case ClientOpcode::Get:
        return "GET";
    case ClientOpcode::Set:
        return "SET";
    case ClientOpcode::Add:
        return "ADD";
    case ClientOpcode::Replace:
        return "REPLACE";
    case ClientOpcode::Delete:
        return "DELETE";
    case ClientOpcode::Increment:
        return "INCREMENT";
    case ClientOpcode::Decrement:
        return "DECREMENT";
    case ClientOpcode::Quit:
        return "QUIT";
    case ClientOpcode::Flush:
        return "FLUSH";
    case ClientOpcode::Getq:
        return "GETQ";
    case ClientOpcode::Noop:
        return "NOOP";
    case ClientOpcode::Version:
        return "VERSION";
    case ClientOpcode::Getk:
        return "GETK";
    case ClientOpcode::Getkq:
        return "GETKQ";
    case ClientOpcode::Append:
        return "APPEND";
    case ClientOpcode::Prepend:
        return "PREPEND";
    case ClientOpcode::Stat:
        return "STAT";
    case ClientOpcode::Setq:
        return "SETQ";
    case ClientOpcode::Addq:
        return "ADDQ";
    case ClientOpcode::Replaceq:
        return "REPLACEQ";
    case ClientOpcode::Deleteq:
        return "DELETEQ";
    case ClientOpcode::Incrementq:
        return "INCREMENTQ";
    case ClientOpcode::Decrementq:
        return "DECREMENTQ";
    case ClientOpcode::Quitq:
        return "QUITQ";
    case ClientOpcode::Flushq:
        return "FLUSHQ";
    case ClientOpcode::Appendq:
        return "APPENDQ";
    case ClientOpcode::Prependq:
        return "PREPENDQ";
    case ClientOpcode::Verbosity:
        return "VERBOSITY";
    case ClientOpcode::Touch:
        return "TOUCH";
    case ClientOpcode::Gat:
        return "GAT";
    case ClientOpcode::Gatq:
        return "GATQ";
    case ClientOpcode::Hello:
        return "HELLO";
    case ClientOpcode::SaslListMechs:
        return "SASL_LIST_MECHS";
    case ClientOpcode::SaslAuth:
        return "SASL_AUTH";
    case ClientOpcode::SaslStep:
        return "SASL_STEP";
    case ClientOpcode::IoctlGet:
        return "IOCTL_GET";
    case ClientOpcode::IoctlSet:
        return "IOCTL_SET";
    case ClientOpcode::ConfigValidate:
        return "CONFIG_VALIDATE";
    case ClientOpcode::ConfigReload:
        return "CONFIG_RELOAD";
    case ClientOpcode::AuditPut:
        return "AUDIT_PUT";
    case ClientOpcode::AuditConfigReload:
        return "AUDIT_CONFIG_RELOAD";
    case ClientOpcode::Shutdown:
        return "SHUTDOWN";
    case ClientOpcode::Rget:
        return "RGET";
    case ClientOpcode::Rset:
        return "RSET";
    case ClientOpcode::Rsetq:
        return "RSETQ";
    case ClientOpcode::Rappend:
        return "RAPPEND";
    case ClientOpcode::Rappendq:
        return "RAPPENDQ";
    case ClientOpcode::Rprepend:
        return "RPREPEND";
    case ClientOpcode::Rprependq:
        return "RPREPENDQ";
    case ClientOpcode::Rdelete:
        return "RDELETE";
    case ClientOpcode::Rdeleteq:
        return "RDELETEQ";
    case ClientOpcode::Rincr:
        return "RINCR";
    case ClientOpcode::Rincrq:
        return "RINCRQ";
    case ClientOpcode::Rdecr:
        return "RDECR";
    case ClientOpcode::Rdecrq:
        return "RDECRQ";
    case ClientOpcode::SetVbucket:
        return "SET_VBUCKET";
    case ClientOpcode::GetVbucket:
        return "GET_VBUCKET";
    case ClientOpcode::DelVbucket:
        return "DEL_VBUCKET";
    case ClientOpcode::TapConnect:
        return "TAP_CONNECT";
    case ClientOpcode::TapMutation:
        return "TAP_MUTATION";
    case ClientOpcode::TapDelete:
        return "TAP_DELETE";
    case ClientOpcode::TapFlush:
        return "TAP_FLUSH";
    case ClientOpcode::TapOpaque:
        return "TAP_OPAQUE";
    case ClientOpcode::TapVbucketSet:
        return "TAP_VBUCKET_SET";
    case ClientOpcode::TapCheckpointStart:
        return "TAP_CHECKPOINT_START";
    case ClientOpcode::TapCheckpointEnd:
        return "TAP_CHECKPOINT_END";
    case ClientOpcode::GetAllVbSeqnos:
        return "GET_ALL_VB_SEQNOS";
    case ClientOpcode::DcpOpen:
        return "DCP_OPEN";
    case ClientOpcode::DcpAddStream:
        return "DCP_ADD_STREAM";
    case ClientOpcode::DcpCloseStream:
        return "DCP_CLOSE_STREAM";
    case ClientOpcode::DcpStreamReq:
        return "DCP_STREAM_REQ";
    case ClientOpcode::DcpGetFailoverLog:
        return "DCP_GET_FAILOVER_LOG";
    case ClientOpcode::DcpStreamEnd:
        return "DCP_STREAM_END";
    case ClientOpcode::DcpSnapshotMarker:
        return "DCP_SNAPSHOT_MARKER";
    case ClientOpcode::DcpMutation:
        return "DCP_MUTATION";
    case ClientOpcode::DcpDeletion:
        return "DCP_DELETION";
    case ClientOpcode::DcpExpiration:
        return "DCP_EXPIRATION";
    case ClientOpcode::DcpSetVbucketState:
        return "DCP_SET_VBUCKET_STATE";
    case ClientOpcode::DcpNoop:
        return "DCP_NOOP";
    case ClientOpcode::DcpBufferAcknowledgement:
        return "DCP_BUFFER_ACKNOWLEDGEMENT";
    case ClientOpcode::DcpControl:
        return "DCP_CONTROL";
    case ClientOpcode::DcpSystemEvent:
        return "DCP_SYSTEM_EVENT";
    case ClientOpcode::StopPersistence:
        return "STOP_PERSISTENCE";
    case ClientOpcode::StartPersistence:
        return "START_PERSISTENCE";
    case ClientOpcode::SetParam:
        return "SET_PARAM";
    case ClientOpcode::GetReplica:
        return "GET_REPLICA";
    case ClientOpcode::CreateBucket:
        return "CREATE_BUCKET";
    case ClientOpcode::DeleteBucket:
        return "DELETE_BUCKET";
    case ClientOpcode::ListBuckets:
        return "LIST_BUCKETS";
    case ClientOpcode::SelectBucket:
        return "SELECT_BUCKET";
    case ClientOpcode::ObserveSeqno:
        return "OBSERVE_SEQNO";
    case ClientOpcode::Observe:
        return "OBSERVE";
    case ClientOpcode::EvictKey:
        return "EVICT_KEY";
    case ClientOpcode::GetLocked:
        return "GET_LOCKED";
    case ClientOpcode::UnlockKey:
        return "UNLOCK_KEY";
    case ClientOpcode::GetFailoverLog:
        return "GET_FAILOVER_LOG";
    case ClientOpcode::LastClosedCheckpoint:
        return "LAST_CLOSED_CHECKPOINT";
    case ClientOpcode::ResetReplicationChain:
        return "RESET_REPLICATION_CHAIN";
    case ClientOpcode::DeregisterTapClient:
        return "DEREGISTER_TAP_CLIENT";
    case ClientOpcode::GetMeta:
        return "GET_META";
    case ClientOpcode::GetqMeta:
        return "GETQ_META";
    case ClientOpcode::SetWithMeta:
        return "SET_WITH_META";
    case ClientOpcode::SetqWithMeta:
        return "SETQ_WITH_META";
    case ClientOpcode::AddWithMeta:
        return "ADD_WITH_META";
    case ClientOpcode::AddqWithMeta:
        return "ADDQ_WITH_META";
    case ClientOpcode::SnapshotVbStates:
        return "SNAPSHOT_VB_STATES";
    case ClientOpcode::VbucketBatchCount:
        return "VBUCKET_BATCH_COUNT";
    case ClientOpcode::DelWithMeta:
        return "DEL_WITH_META";
    case ClientOpcode::DelqWithMeta:
        return "DELQ_WITH_META";
    case ClientOpcode::CreateCheckpoint:
        return "CREATE_CHECKPOINT";
    case ClientOpcode::NotifyVbucketUpdate:
        return "NOTIFY_VBUCKET_UPDATE";
    case ClientOpcode::EnableTraffic:
        return "ENABLE_TRAFFIC";
    case ClientOpcode::DisableTraffic:
        return "DISABLE_TRAFFIC";
    case ClientOpcode::ChangeVbFilter:
        return "CHANGE_VB_FILTER";
    case ClientOpcode::CheckpointPersistence:
        return "CHECKPOINT_PERSISTENCE";
    case ClientOpcode::ReturnMeta:
        return "RETURN_META";
    case ClientOpcode::CompactDb:
        return "COMPACT_DB";
    case ClientOpcode::SetClusterConfig:
        return "SET_CLUSTER_CONFIG";
    case ClientOpcode::GetClusterConfig:
        return "GET_CLUSTER_CONFIG";
    case ClientOpcode::GetRandomKey:
        return "GET_RANDOM_KEY";
    case ClientOpcode::SeqnoPersistence:
        return "SEQNO_PERSISTENCE";
    case ClientOpcode::GetKeys:
        return "GET_KEYS";
    case ClientOpcode::CollectionsSetManifest:
        return "COLLECTIONS_SET_MANIFEST";
    case ClientOpcode::CollectionsGetManifest:
        return "COLLECTIONS_GET_MANIFEST";
    case ClientOpcode::CollectionsGetID:
        return "COLLECTIONS_GET_ID";
    case ClientOpcode::SetDriftCounterState:
        return "SET_DRIFT_COUNTER_STATE";
    case ClientOpcode::GetAdjustedTime:
        return "GET_ADJUSTED_TIME";
    case ClientOpcode::SubdocGet:
        return "SUBDOC_GET";
    case ClientOpcode::SubdocExists:
        return "SUBDOC_EXISTS";
    case ClientOpcode::SubdocDictAdd:
        return "SUBDOC_DICT_ADD";
    case ClientOpcode::SubdocDictUpsert:
        return "SUBDOC_DICT_UPSERT";
    case ClientOpcode::SubdocDelete:
        return "SUBDOC_DELETE";
    case ClientOpcode::SubdocReplace:
        return "SUBDOC_REPLACE";
    case ClientOpcode::SubdocArrayPushLast:
        return "SUBDOC_ARRAY_PUSH_LAST";
    case ClientOpcode::SubdocArrayPushFirst:
        return "SUBDOC_ARRAY_PUSH_FIRST";
    case ClientOpcode::SubdocArrayInsert:
        return "SUBDOC_ARRAY_INSERT";
    case ClientOpcode::SubdocArrayAddUnique:
        return "SUBDOC_ARRAY_ADD_UNIQUE";
    case ClientOpcode::SubdocCounter:
        return "SUBDOC_COUNTER";
    case ClientOpcode::SubdocMultiLookup:
        return "SUBDOC_MULTI_LOOKUP";
    case ClientOpcode::SubdocMultiMutation:
        return "SUBDOC_MULTI_MUTATION";
    case ClientOpcode::SubdocGetCount:
        return "SUBDOC_GET_COUNT";
    case ClientOpcode::Scrub:
        return "SCRUB";
    case ClientOpcode::IsaslRefresh:
        return "ISASL_REFRESH";
    case ClientOpcode::SslCertsRefresh:
        return "SSL_CERTS_REFRESH";
    case ClientOpcode::GetCmdTimer:
        return "GET_CMD_TIMER";
    case ClientOpcode::SetCtrlToken:
        return "SET_CTRL_TOKEN";
    case ClientOpcode::GetCtrlToken:
        return "GET_CTRL_TOKEN";
    case ClientOpcode::UpdateExternalUserPermissions:
        return "UPDATE_USER_PERMISSIONS";
    case ClientOpcode::RbacRefresh:
        return "RBAC_REFRESH";
    case ClientOpcode::AuthProvider:
        return "AUTH_PROVIDER";
    case ClientOpcode::DropPrivilege:
        return "DROP_PRIVILEGES";
    case ClientOpcode::AdjustTimeofday:
        return "ADJUST_TIMEOFDAY";
    case ClientOpcode::EwouldblockCtl:
        return "EWB_CTL";
    case ClientOpcode::GetErrorMap:
        return "GET_ERROR_MAP";
    case ClientOpcode::Invalid:
        break;
    }

    throw std::invalid_argument(
            "to_string(cb::mcbp::ClientOpcode): Invalid opcode: " +
            std::to_string(int(opcode)));
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
    }
    throw std::invalid_argument(
            "to_string(cb::mcbp::ServerOpcode): Invalid opcode: " +
            std::to_string(int(opcode)));
}

cb::mcbp::ClientOpcode to_opcode(const std::string& string) {
    std::string input;
    std::transform(
            string.begin(), string.end(), std::back_inserter(input), ::toupper);
    // If the user used space between the words, replace with '_'
    std::replace(input.begin(), input.end(), ' ', '_');
    for (int ii = 0; ii < int(cb::mcbp::ClientOpcode::Invalid); ++ii) {
        try {
            auto str = to_string(cb::mcbp::ClientOpcode(ii));
            if (str == input) {
                return cb::mcbp::ClientOpcode(ii);
            }
        } catch (const std::invalid_argument&) {
            // ignore
        }
    }

    throw std::invalid_argument("to_opcode(): unknown opcode: \"" + string +
                                "\"");
}

std::ostream& operator<<(std::ostream& out, cb::mcbp::ClientOpcode opcode) {
    out << to_string(opcode);
    return out;
}

std::ostream& operator<<(std::ostream& out, cb::mcbp::ServerOpcode opcode) {
    out << to_string(opcode);
    return out;
}
