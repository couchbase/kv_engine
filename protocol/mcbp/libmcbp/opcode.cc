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

#include <mcbp/protocol/opcode.h>
#include <algorithm>
#include <cctype>
#include <iterator>
#include <stdexcept>

std::string to_string(cb::mcbp::Opcode opcode) {
    using namespace cb::mcbp;

    switch (opcode) {
    case Opcode::Get:
        return "GET";
    case Opcode::Set:
        return "SET";
    case Opcode::Add:
        return "ADD";
    case Opcode::Replace:
        return "REPLACE";
    case Opcode::Delete:
        return "DELETE";
    case Opcode::Increment:
        return "INCREMENT";
    case Opcode::Decrement:
        return "DECREMENT";
    case Opcode::Quit:
        return "QUIT";
    case Opcode::Flush:
        return "FLUSH";
    case Opcode::Getq:
        return "GETQ";
    case Opcode::Noop:
        return "NOOP";
    case Opcode::Version:
        return "VERSION";
    case Opcode::Getk:
        return "GETK";
    case Opcode::Getkq:
        return "GETKQ";
    case Opcode::Append:
        return "APPEND";
    case Opcode::Prepend:
        return "PREPEND";
    case Opcode::Stat:
        return "STAT";
    case Opcode::Setq:
        return "SETQ";
    case Opcode::Addq:
        return "ADDQ";
    case Opcode::Replaceq:
        return "REPLACEQ";
    case Opcode::Deleteq:
        return "DELETEQ";
    case Opcode::Incrementq:
        return "INCREMENTQ";
    case Opcode::Decrementq:
        return "DECREMENTQ";
    case Opcode::Quitq:
        return "QUITQ";
    case Opcode::Flushq:
        return "FLUSHQ";
    case Opcode::Appendq:
        return "APPENDQ";
    case Opcode::Prependq:
        return "PREPENDQ";
    case Opcode::Verbosity:
        return "VERBOSITY";
    case Opcode::Touch:
        return "TOUCH";
    case Opcode::Gat:
        return "GAT";
    case Opcode::Gatq:
        return "GATQ";
    case Opcode::Hello:
        return "HELLO";
    case Opcode::SaslListMechs:
        return "SASL_LIST_MECHS";
    case Opcode::SaslAuth:
        return "SASL_AUTH";
    case Opcode::SaslStep:
        return "SASL_STEP";
    case Opcode::IoctlGet:
        return "IOCTL_GET";
    case Opcode::IoctlSet:
        return "IOCTL_SET";
    case Opcode::ConfigValidate:
        return "CONFIG_VALIDATE";
    case Opcode::ConfigReload:
        return "CONFIG_RELOAD";
    case Opcode::AuditPut:
        return "AUDIT_PUT";
    case Opcode::AuditConfigReload:
        return "AUDIT_CONFIG_RELOAD";
    case Opcode::Shutdown:
        return "SHUTDOWN";
    case Opcode::Rget:
        return "RGET";
    case Opcode::Rset:
        return "RSET";
    case Opcode::Rsetq:
        return "RSETQ";
    case Opcode::Rappend:
        return "RAPPEND";
    case Opcode::Rappendq:
        return "RAPPENDQ";
    case Opcode::Rprepend:
        return "RPREPEND";
    case Opcode::Rprependq:
        return "RPREPENDQ";
    case Opcode::Rdelete:
        return "RDELETE";
    case Opcode::Rdeleteq:
        return "RDELETEQ";
    case Opcode::Rincr:
        return "RINCR";
    case Opcode::Rincrq:
        return "RINCRQ";
    case Opcode::Rdecr:
        return "RDECR";
    case Opcode::Rdecrq:
        return "RDECRQ";
    case Opcode::SetVbucket:
        return "SET_VBUCKET";
    case Opcode::GetVbucket:
        return "GET_VBUCKET";
    case Opcode::DelVbucket:
        return "DEL_VBUCKET";
    case Opcode::TapConnect:
        return "TAP_CONNECT";
    case Opcode::TapMutation:
        return "TAP_MUTATION";
    case Opcode::TapDelete:
        return "TAP_DELETE";
    case Opcode::TapFlush:
        return "TAP_FLUSH";
    case Opcode::TapOpaque:
        return "TAP_OPAQUE";
    case Opcode::TapVbucketSet:
        return "TAP_VBUCKET_SET";
    case Opcode::TapCheckpointStart:
        return "TAP_CHECKPOINT_START";
    case Opcode::TapCheckpointEnd:
        return "TAP_CHECKPOINT_END";
    case Opcode::GetAllVbSeqnos:
        return "GET_ALL_VB_SEQNOS";
    case Opcode::DcpOpen:
        return "DCP_OPEN";
    case Opcode::DcpAddStream:
        return "DCP_ADD_STREAM";
    case Opcode::DcpCloseStream:
        return "DCP_CLOSE_STREAM";
    case Opcode::DcpStreamReq:
        return "DCP_STREAM_REQ";
    case Opcode::DcpGetFailoverLog:
        return "DCP_GET_FAILOVER_LOG";
    case Opcode::DcpStreamEnd:
        return "DCP_STREAM_END";
    case Opcode::DcpSnapshotMarker:
        return "DCP_SNAPSHOT_MARKER";
    case Opcode::DcpMutation:
        return "DCP_MUTATION";
    case Opcode::DcpDeletion:
        return "DCP_DELETION";
    case Opcode::DcpExpiration:
        return "DCP_EXPIRATION";
    case Opcode::DcpFlush:
        return "DCP_FLUSH";
    case Opcode::DcpSetVbucketState:
        return "DCP_SET_VBUCKET_STATE";
    case Opcode::DcpNoop:
        return "DCP_NOOP";
    case Opcode::DcpBufferAcknowledgement:
        return "DCP_BUFFER_ACKNOWLEDGEMENT";
    case Opcode::DcpControl:
        return "DCP_CONTROL";
    case Opcode::DcpSystemEvent:
        return "DCP_SYSTEM_EVENT";
    case Opcode::StopPersistence:
        return "STOP_PERSISTENCE";
    case Opcode::StartPersistence:
        return "START_PERSISTENCE";
    case Opcode::SetParam:
        return "SET_PARAM";
    case Opcode::GetReplica:
        return "GET_REPLICA";
    case Opcode::CreateBucket:
        return "CREATE_BUCKET";
    case Opcode::DeleteBucket:
        return "DELETE_BUCKET";
    case Opcode::ListBuckets:
        return "LIST_BUCKETS";
    case Opcode::SelectBucket:
        return "SELECT_BUCKET";
    case Opcode::ObserveSeqno:
        return "OBSERVE_SEQNO";
    case Opcode::Observe:
        return "OBSERVE";
    case Opcode::EvictKey:
        return "EVICT_KEY";
    case Opcode::GetLocked:
        return "GET_LOCKED";
    case Opcode::UnlockKey:
        return "UNLOCK_KEY";
    case Opcode::LastClosedCheckpoint:
        return "LAST_CLOSED_CHECKPOINT";
    case Opcode::ResetReplicationChain:
        return "RESET_REPLICATION_CHAIN";
    case Opcode::DeregisterTapClient:
        return "DEREGISTER_TAP_CLIENT";
    case Opcode::GetMeta:
        return "GET_META";
    case Opcode::GetqMeta:
        return "GETQ_META";
    case Opcode::SetWithMeta:
        return "SET_WITH_META";
    case Opcode::SetqWithMeta:
        return "SETQ_WITH_META";
    case Opcode::AddWithMeta:
        return "ADD_WITH_META";
    case Opcode::AddqWithMeta:
        return "ADDQ_WITH_META";
    case Opcode::SnapshotVbStates:
        return "SNAPSHOT_VB_STATES";
    case Opcode::VbucketBatchCount:
        return "VBUCKET_BATCH_COUNT";
    case Opcode::DelWithMeta:
        return "DEL_WITH_META";
    case Opcode::DelqWithMeta:
        return "DELQ_WITH_META";
    case Opcode::CreateCheckpoint:
        return "CREATE_CHECKPOINT";
    case Opcode::NotifyVbucketUpdate:
        return "NOTIFY_VBUCKET_UPDATE";
    case Opcode::EnableTraffic:
        return "ENABLE_TRAFFIC";
    case Opcode::DisableTraffic:
        return "DISABLE_TRAFFIC";
    case Opcode::ChangeVbFilter:
        return "CHANGE_VB_FILTER";
    case Opcode::CheckpointPersistence:
        return "CHECKPOINT_PERSISTENCE";
    case Opcode::ReturnMeta:
        return "RETURN_META";
    case Opcode::CompactDb:
        return "COMPACT_DB";
    case Opcode::SetClusterConfig:
        return "SET_CLUSTER_CONFIG";
    case Opcode::GetClusterConfig:
        return "GET_CLUSTER_CONFIG";
    case Opcode::GetRandomKey:
        return "GET_RANDOM_KEY";
    case Opcode::SeqnoPersistence:
        return "SEQNO_PERSISTENCE";
    case Opcode::GetKeys:
        return "GET_KEYS";
    case Opcode::CollectionsSetManifest:
        return "COLLECTIONS_SET_MANIFEST";
    case Opcode::SetDriftCounterState:
        return "SET_DRIFT_COUNTER_STATE";
    case Opcode::GetAdjustedTime:
        return "GET_ADJUSTED_TIME";
    case Opcode::SubdocGet:
        return "SUBDOC_GET";
    case Opcode::SubdocExists:
        return "SUBDOC_EXISTS";
    case Opcode::SubdocDictAdd:
        return "SUBDOC_DICT_ADD";
    case Opcode::SubdocDictUpsert:
        return "SUBDOC_DICT_UPSERT";
    case Opcode::SubdocDelete:
        return "SUBDOC_DELETE";
    case Opcode::SubdocReplace:
        return "SUBDOC_REPLACE";
    case Opcode::SubdocArrayPushLast:
        return "SUBDOC_ARRAY_PUSH_LAST";
    case Opcode::SubdocArrayPushFirst:
        return "SUBDOC_ARRAY_PUSH_FIRST";
    case Opcode::SubdocArrayInsert:
        return "SUBDOC_ARRAY_INSERT";
    case Opcode::SubdocArrayAddUnique:
        return "SUBDOC_ARRAY_ADD_UNIQUE";
    case Opcode::SubdocCounter:
        return "SUBDOC_COUNTER";
    case Opcode::SubdocMultiLookup:
        return "SUBDOC_MULTI_LOOKUP";
    case Opcode::SubdocMultiMutation:
        return "SUBDOC_MULTI_MUTATION";
    case Opcode::SubdocGetCount:
        return "SUBDOC_GET_COUNT";
    case Opcode::Scrub:
        return "SCRUB";
    case Opcode::IsaslRefresh:
        return "ISASL_REFRESH";
    case Opcode::SslCertsRefresh:
        return "SSL_CERTS_REFRESH";
    case Opcode::GetCmdTimer:
        return "GET_CMD_TIMER";
    case Opcode::SetCtrlToken:
        return "SET_CTRL_TOKEN";
    case Opcode::GetCtrlToken:
        return "GET_CTRL_TOKEN";
    case Opcode::InitComplete:
        return "INIT_COMPLETE";
    case Opcode::RbacRefresh:
        return "RBAC_REFRESH";
    case Opcode::DropPrivilege:
        return "DROP_PRIVILEGES";
    case Opcode::AdjustTimeofday:
        return "ADJUST_TIMEOFDAY";
    case Opcode::EwouldblockCtl:
        return "EWB_CTL";
    case Opcode::GetErrorMap:
        return "GET_ERROR_MAP";
    case Opcode::Invalid:
        break;
    }

    throw std::invalid_argument(
            "to_string(cb::mcbp::Opcode): Invalid opcode: " +
            std::to_string(int(opcode)));
}

cb::mcbp::Opcode to_opcode(const std::string& string) {
    std::string input;
    std::transform(
            string.begin(), string.end(), std::back_inserter(input), ::toupper);
    // If the user used space between the words, replace with '_'
    std::replace(input.begin(), input.end(), ' ', '_');
    for (int ii = 0; ii < int(cb::mcbp::Opcode::Invalid); ++ii) {
        try {
            auto str = to_string(cb::mcbp::Opcode(ii));
            if (str == input) {
                return cb::mcbp::Opcode(ii);
            }
        } catch (const std::invalid_argument&) {
            // ignore
        }
    }

    throw std::invalid_argument("to_opcode(): unknown opcode: \"" + string +
                                "\"");
}
