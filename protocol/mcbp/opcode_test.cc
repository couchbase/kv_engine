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

#include <gtest/gtest.h>
#include <mcbp/protocol/opcode.h>
#include <memcached/protocol_binary.h>
#include <algorithm>
#include <cctype>
#include <map>
#include <stdexcept>

using namespace cb::mcbp;

const std::map<cb::mcbp::ClientOpcode, std::string> client_blueprint = {
        {{ClientOpcode::Get, "GET"},
         {ClientOpcode::Set, "SET"},
         {ClientOpcode::Add, "ADD"},
         {ClientOpcode::Replace, "REPLACE"},
         {ClientOpcode::Delete, "DELETE"},
         {ClientOpcode::Increment, "INCREMENT"},
         {ClientOpcode::Decrement, "DECREMENT"},
         {ClientOpcode::Quit, "QUIT"},
         {ClientOpcode::Flush, "FLUSH"},
         {ClientOpcode::Getq, "GETQ"},
         {ClientOpcode::Noop, "NOOP"},
         {ClientOpcode::Version, "VERSION"},
         {ClientOpcode::Getk, "GETK"},
         {ClientOpcode::Getkq, "GETKQ"},
         {ClientOpcode::Append, "APPEND"},
         {ClientOpcode::Prepend, "PREPEND"},
         {ClientOpcode::Stat, "STAT"},
         {ClientOpcode::Setq, "SETQ"},
         {ClientOpcode::Addq, "ADDQ"},
         {ClientOpcode::Replaceq, "REPLACEQ"},
         {ClientOpcode::Deleteq, "DELETEQ"},
         {ClientOpcode::Incrementq, "INCREMENTQ"},
         {ClientOpcode::Decrementq, "DECREMENTQ"},
         {ClientOpcode::Quitq, "QUITQ"},
         {ClientOpcode::Flushq, "FLUSHQ"},
         {ClientOpcode::Appendq, "APPENDQ"},
         {ClientOpcode::Prependq, "PREPENDQ"},
         {ClientOpcode::Verbosity, "VERBOSITY"},
         {ClientOpcode::Touch, "TOUCH"},
         {ClientOpcode::Gat, "GAT"},
         {ClientOpcode::Gatq, "GATQ"},
         {ClientOpcode::Hello, "HELLO"},
         {ClientOpcode::SaslListMechs, "SASL_LIST_MECHS"},
         {ClientOpcode::SaslAuth, "SASL_AUTH"},
         {ClientOpcode::SaslStep, "SASL_STEP"},
         {ClientOpcode::IoctlGet, "IOCTL_GET"},
         {ClientOpcode::IoctlSet, "IOCTL_SET"},
         {ClientOpcode::ConfigValidate, "CONFIG_VALIDATE"},
         {ClientOpcode::ConfigReload, "CONFIG_RELOAD"},
         {ClientOpcode::AuditPut, "AUDIT_PUT"},
         {ClientOpcode::AuditConfigReload, "AUDIT_CONFIG_RELOAD"},
         {ClientOpcode::Shutdown, "SHUTDOWN"},
         {ClientOpcode::Rget, "RGET"},
         {ClientOpcode::Rset, "RSET"},
         {ClientOpcode::Rsetq, "RSETQ"},
         {ClientOpcode::Rappend, "RAPPEND"},
         {ClientOpcode::Rappendq, "RAPPENDQ"},
         {ClientOpcode::Rprepend, "RPREPEND"},
         {ClientOpcode::Rprependq, "RPREPENDQ"},
         {ClientOpcode::Rdelete, "RDELETE"},
         {ClientOpcode::Rdeleteq, "RDELETEQ"},
         {ClientOpcode::Rincr, "RINCR"},
         {ClientOpcode::Rincrq, "RINCRQ"},
         {ClientOpcode::Rdecr, "RDECR"},
         {ClientOpcode::Rdecrq, "RDECRQ"},
         {ClientOpcode::SetVbucket, "SET_VBUCKET"},
         {ClientOpcode::GetVbucket, "GET_VBUCKET"},
         {ClientOpcode::DelVbucket, "DEL_VBUCKET"},
         {ClientOpcode::TapConnect, "TAP_CONNECT"},
         {ClientOpcode::TapMutation, "TAP_MUTATION"},
         {ClientOpcode::TapDelete, "TAP_DELETE"},
         {ClientOpcode::TapFlush, "TAP_FLUSH"},
         {ClientOpcode::TapOpaque, "TAP_OPAQUE"},
         {ClientOpcode::TapVbucketSet, "TAP_VBUCKET_SET"},
         {ClientOpcode::TapCheckpointStart, "TAP_CHECKPOINT_START"},
         {ClientOpcode::TapCheckpointEnd, "TAP_CHECKPOINT_END"},
         {ClientOpcode::GetAllVbSeqnos, "GET_ALL_VB_SEQNOS"},
         {ClientOpcode::DcpOpen, "DCP_OPEN"},
         {ClientOpcode::DcpAddStream, "DCP_ADD_STREAM"},
         {ClientOpcode::DcpCloseStream, "DCP_CLOSE_STREAM"},
         {ClientOpcode::DcpStreamReq, "DCP_STREAM_REQ"},
         {ClientOpcode::DcpGetFailoverLog, "DCP_GET_FAILOVER_LOG"},
         {ClientOpcode::DcpStreamEnd, "DCP_STREAM_END"},
         {ClientOpcode::DcpSnapshotMarker, "DCP_SNAPSHOT_MARKER"},
         {ClientOpcode::DcpMutation, "DCP_MUTATION"},
         {ClientOpcode::DcpDeletion, "DCP_DELETION"},
         {ClientOpcode::DcpExpiration, "DCP_EXPIRATION"},
         {ClientOpcode::DcpSetVbucketState, "DCP_SET_VBUCKET_STATE"},
         {ClientOpcode::DcpNoop, "DCP_NOOP"},
         {ClientOpcode::DcpBufferAcknowledgement, "DCP_BUFFER_ACKNOWLEDGEMENT"},
         {ClientOpcode::DcpControl, "DCP_CONTROL"},
         {ClientOpcode::DcpSystemEvent, "DCP_SYSTEM_EVENT"},
         {ClientOpcode::StopPersistence, "STOP_PERSISTENCE"},
         {ClientOpcode::StartPersistence, "START_PERSISTENCE"},
         {ClientOpcode::SetParam, "SET_PARAM"},
         {ClientOpcode::GetReplica, "GET_REPLICA"},
         {ClientOpcode::CreateBucket, "CREATE_BUCKET"},
         {ClientOpcode::DeleteBucket, "DELETE_BUCKET"},
         {ClientOpcode::ListBuckets, "LIST_BUCKETS"},
         {ClientOpcode::SelectBucket, "SELECT_BUCKET"},
         {ClientOpcode::ObserveSeqno, "OBSERVE_SEQNO"},
         {ClientOpcode::Observe, "OBSERVE"},
         {ClientOpcode::EvictKey, "EVICT_KEY"},
         {ClientOpcode::GetLocked, "GET_LOCKED"},
         {ClientOpcode::UnlockKey, "UNLOCK_KEY"},
         {ClientOpcode::GetFailoverLog, "GET_FAILOVER_LOG"},
         {ClientOpcode::LastClosedCheckpoint, "LAST_CLOSED_CHECKPOINT"},
         {ClientOpcode::ResetReplicationChain, "RESET_REPLICATION_CHAIN"},
         {ClientOpcode::DeregisterTapClient, "DEREGISTER_TAP_CLIENT"},
         {ClientOpcode::GetMeta, "GET_META"},
         {ClientOpcode::GetqMeta, "GETQ_META"},
         {ClientOpcode::SetWithMeta, "SET_WITH_META"},
         {ClientOpcode::SetqWithMeta, "SETQ_WITH_META"},
         {ClientOpcode::AddWithMeta, "ADD_WITH_META"},
         {ClientOpcode::AddqWithMeta, "ADDQ_WITH_META"},
         {ClientOpcode::SnapshotVbStates, "SNAPSHOT_VB_STATES"},
         {ClientOpcode::VbucketBatchCount, "VBUCKET_BATCH_COUNT"},
         {ClientOpcode::DelWithMeta, "DEL_WITH_META"},
         {ClientOpcode::DelqWithMeta, "DELQ_WITH_META"},
         {ClientOpcode::CreateCheckpoint, "CREATE_CHECKPOINT"},
         {ClientOpcode::NotifyVbucketUpdate, "NOTIFY_VBUCKET_UPDATE"},
         {ClientOpcode::EnableTraffic, "ENABLE_TRAFFIC"},
         {ClientOpcode::DisableTraffic, "DISABLE_TRAFFIC"},
         {ClientOpcode::ChangeVbFilter, "CHANGE_VB_FILTER"},
         {ClientOpcode::CheckpointPersistence, "CHECKPOINT_PERSISTENCE"},
         {ClientOpcode::ReturnMeta, "RETURN_META"},
         {ClientOpcode::CompactDb, "COMPACT_DB"},
         {ClientOpcode::SetClusterConfig, "SET_CLUSTER_CONFIG"},
         {ClientOpcode::GetClusterConfig, "GET_CLUSTER_CONFIG"},
         {ClientOpcode::GetRandomKey, "GET_RANDOM_KEY"},
         {ClientOpcode::SeqnoPersistence, "SEQNO_PERSISTENCE"},
         {ClientOpcode::GetKeys, "GET_KEYS"},
         {ClientOpcode::CollectionsSetManifest, "COLLECTIONS_SET_MANIFEST"},
         {ClientOpcode::CollectionsGetManifest, "COLLECTIONS_GET_MANIFEST"},
         {ClientOpcode::SetDriftCounterState, "SET_DRIFT_COUNTER_STATE"},
         {ClientOpcode::GetAdjustedTime, "GET_ADJUSTED_TIME"},
         {ClientOpcode::SubdocGet, "SUBDOC_GET"},
         {ClientOpcode::SubdocExists, "SUBDOC_EXISTS"},
         {ClientOpcode::SubdocDictAdd, "SUBDOC_DICT_ADD"},
         {ClientOpcode::SubdocDictUpsert, "SUBDOC_DICT_UPSERT"},
         {ClientOpcode::SubdocDelete, "SUBDOC_DELETE"},
         {ClientOpcode::SubdocReplace, "SUBDOC_REPLACE"},
         {ClientOpcode::SubdocArrayPushLast, "SUBDOC_ARRAY_PUSH_LAST"},
         {ClientOpcode::SubdocArrayPushFirst, "SUBDOC_ARRAY_PUSH_FIRST"},
         {ClientOpcode::SubdocArrayInsert, "SUBDOC_ARRAY_INSERT"},
         {ClientOpcode::SubdocArrayAddUnique, "SUBDOC_ARRAY_ADD_UNIQUE"},
         {ClientOpcode::SubdocCounter, "SUBDOC_COUNTER"},
         {ClientOpcode::SubdocMultiLookup, "SUBDOC_MULTI_LOOKUP"},
         {ClientOpcode::SubdocMultiMutation, "SUBDOC_MULTI_MUTATION"},
         {ClientOpcode::SubdocGetCount, "SUBDOC_GET_COUNT"},
         {ClientOpcode::Scrub, "SCRUB"},
         {ClientOpcode::IsaslRefresh, "ISASL_REFRESH"},
         {ClientOpcode::SslCertsRefresh, "SSL_CERTS_REFRESH"},
         {ClientOpcode::GetCmdTimer, "GET_CMD_TIMER"},
         {ClientOpcode::SetCtrlToken, "SET_CTRL_TOKEN"},
         {ClientOpcode::GetCtrlToken, "GET_CTRL_TOKEN"},
         {ClientOpcode::UpdateUserPermissions, "UPDATE_USER_PERMISSIONS"},
         {ClientOpcode::RbacRefresh, "RBAC_REFRESH"},
         {ClientOpcode::AuthProvider, "AUTH_PROVIDER"},
         {ClientOpcode::GetActiveExternalUsers, "GET_ACTIVE_USERS"},
         {ClientOpcode::DropPrivilege, "DROP_PRIVILEGES"},
         {ClientOpcode::AdjustTimeofday, "ADJUST_TIMEOFDAY"},
         {ClientOpcode::EwouldblockCtl, "EWB_CTL"},
         {ClientOpcode::GetErrorMap, "GET_ERROR_MAP"}}};

TEST(ClientOpcode_to_string, LegalValues) {
    for (auto& entry : client_blueprint) {
        EXPECT_EQ(entry.second, to_string(entry.first));
    }

    EXPECT_THROW(to_string(ClientOpcode::Invalid), std::invalid_argument);
}

TEST(ClientOpcode_to_string, InvalidValues) {
    for (int ii = 0; ii < 0x100; ++ii) {
        ClientOpcode opcode = ClientOpcode(ii);
        if (client_blueprint.find(opcode) == client_blueprint.end()) {
            EXPECT_THROW(to_string(opcode), std::invalid_argument);
        }
    }
}

TEST(ClientOpcode_to_opcode, LegalValues) {
    for (auto& entry : client_blueprint) {
        EXPECT_EQ(entry.first, to_opcode(entry.second));
    }
}

TEST(ClientOpcode_to_opcode, UnknownValues) {
    EXPECT_THROW(to_opcode("asdfasdf"), std::invalid_argument);
}

TEST(ClientOpcode_to_opcode, CaseDontMatter) {
    for (auto& entry : client_blueprint) {
        std::string lower;
        std::transform(entry.second.begin(),
                       entry.second.end(),
                       std::back_inserter(lower),
                       ::tolower);
        EXPECT_EQ(entry.first, to_opcode(lower));
    }
}

TEST(ClientOpcode_to_opcode, SpaceMayBeUsed) {
    for (auto& entry : client_blueprint) {
        std::string input{entry.second};
        std::replace(input.begin(), input.end(), '_', ' ');
        EXPECT_EQ(entry.first, to_opcode(input));
    }
}

const std::map<cb::mcbp::ServerOpcode, std::string> server_blueprint = {
        {{ServerOpcode::ClustermapChangeNotification,
          "ClustermapChangeNotification"},
         {ServerOpcode::AuthRequest, "AuthRequest"}}};

TEST(ServerOpcode, to_string) {
    for (int ii = 0; ii < 0x100; ++ii) {
        ServerOpcode opcode = ServerOpcode(ii);
        const auto iter = server_blueprint.find(opcode);
        if (iter == server_blueprint.end()) {
            EXPECT_THROW(to_string(opcode), std::invalid_argument);
        } else {
            EXPECT_EQ(to_string(iter->first), iter->second);
        }
    }
}
