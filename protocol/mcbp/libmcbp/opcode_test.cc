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

#include <gtest/gtest.h>
#include <mcbp/protocol/opcode.h>
#include <memcached/protocol_binary.h>
#include <algorithm>
#include <cctype>
#include <map>
#include <stdexcept>

#include "utilities/protocol2text.h"

using namespace cb::mcbp;

const std::map<cb::mcbp::Opcode, std::string> blueprint = {
        {{Opcode::Get, "GET"},
         {Opcode::Set, "SET"},
         {Opcode::Add, "ADD"},
         {Opcode::Replace, "REPLACE"},
         {Opcode::Delete, "DELETE"},
         {Opcode::Increment, "INCREMENT"},
         {Opcode::Decrement, "DECREMENT"},
         {Opcode::Quit, "QUIT"},
         {Opcode::Flush, "FLUSH"},
         {Opcode::Getq, "GETQ"},
         {Opcode::Noop, "NOOP"},
         {Opcode::Version, "VERSION"},
         {Opcode::Getk, "GETK"},
         {Opcode::Getkq, "GETKQ"},
         {Opcode::Append, "APPEND"},
         {Opcode::Prepend, "PREPEND"},
         {Opcode::Stat, "STAT"},
         {Opcode::Setq, "SETQ"},
         {Opcode::Addq, "ADDQ"},
         {Opcode::Replaceq, "REPLACEQ"},
         {Opcode::Deleteq, "DELETEQ"},
         {Opcode::Incrementq, "INCREMENTQ"},
         {Opcode::Decrementq, "DECREMENTQ"},
         {Opcode::Quitq, "QUITQ"},
         {Opcode::Flushq, "FLUSHQ"},
         {Opcode::Appendq, "APPENDQ"},
         {Opcode::Prependq, "PREPENDQ"},
         {Opcode::Verbosity, "VERBOSITY"},
         {Opcode::Touch, "TOUCH"},
         {Opcode::Gat, "GAT"},
         {Opcode::Gatq, "GATQ"},
         {Opcode::Hello, "HELLO"},
         {Opcode::SaslListMechs, "SASL_LIST_MECHS"},
         {Opcode::SaslAuth, "SASL_AUTH"},
         {Opcode::SaslStep, "SASL_STEP"},
         {Opcode::IoctlGet, "IOCTL_GET"},
         {Opcode::IoctlSet, "IOCTL_SET"},
         {Opcode::ConfigValidate, "CONFIG_VALIDATE"},
         {Opcode::ConfigReload, "CONFIG_RELOAD"},
         {Opcode::AuditPut, "AUDIT_PUT"},
         {Opcode::AuditConfigReload, "AUDIT_CONFIG_RELOAD"},
         {Opcode::Shutdown, "SHUTDOWN"},
         {Opcode::Rget, "RGET"},
         {Opcode::Rset, "RSET"},
         {Opcode::Rsetq, "RSETQ"},
         {Opcode::Rappend, "RAPPEND"},
         {Opcode::Rappendq, "RAPPENDQ"},
         {Opcode::Rprepend, "RPREPEND"},
         {Opcode::Rprependq, "RPREPENDQ"},
         {Opcode::Rdelete, "RDELETE"},
         {Opcode::Rdeleteq, "RDELETEQ"},
         {Opcode::Rincr, "RINCR"},
         {Opcode::Rincrq, "RINCRQ"},
         {Opcode::Rdecr, "RDECR"},
         {Opcode::Rdecrq, "RDECRQ"},
         {Opcode::SetVbucket, "SET_VBUCKET"},
         {Opcode::GetVbucket, "GET_VBUCKET"},
         {Opcode::DelVbucket, "DEL_VBUCKET"},
         {Opcode::TapConnect, "TAP_CONNECT"},
         {Opcode::TapMutation, "TAP_MUTATION"},
         {Opcode::TapDelete, "TAP_DELETE"},
         {Opcode::TapFlush, "TAP_FLUSH"},
         {Opcode::TapOpaque, "TAP_OPAQUE"},
         {Opcode::TapVbucketSet, "TAP_VBUCKET_SET"},
         {Opcode::TapCheckpointStart, "TAP_CHECKPOINT_START"},
         {Opcode::TapCheckpointEnd, "TAP_CHECKPOINT_END"},
         {Opcode::GetAllVbSeqnos, "GET_ALL_VB_SEQNOS"},
         {Opcode::DcpOpen, "DCP_OPEN"},
         {Opcode::DcpAddStream, "DCP_ADD_STREAM"},
         {Opcode::DcpCloseStream, "DCP_CLOSE_STREAM"},
         {Opcode::DcpStreamReq, "DCP_STREAM_REQ"},
         {Opcode::DcpGetFailoverLog, "DCP_GET_FAILOVER_LOG"},
         {Opcode::DcpStreamEnd, "DCP_STREAM_END"},
         {Opcode::DcpSnapshotMarker, "DCP_SNAPSHOT_MARKER"},
         {Opcode::DcpMutation, "DCP_MUTATION"},
         {Opcode::DcpDeletion, "DCP_DELETION"},
         {Opcode::DcpExpiration, "DCP_EXPIRATION"},
         {Opcode::DcpFlush, "DCP_FLUSH"},
         {Opcode::DcpSetVbucketState, "DCP_SET_VBUCKET_STATE"},
         {Opcode::DcpNoop, "DCP_NOOP"},
         {Opcode::DcpBufferAcknowledgement, "DCP_BUFFER_ACKNOWLEDGEMENT"},
         {Opcode::DcpControl, "DCP_CONTROL"},
         {Opcode::DcpSystemEvent, "DCP_SYSTEM_EVENT"},
         {Opcode::StopPersistence, "STOP_PERSISTENCE"},
         {Opcode::StartPersistence, "START_PERSISTENCE"},
         {Opcode::SetParam, "SET_PARAM"},
         {Opcode::GetReplica, "GET_REPLICA"},
         {Opcode::CreateBucket, "CREATE_BUCKET"},
         {Opcode::DeleteBucket, "DELETE_BUCKET"},
         {Opcode::ListBuckets, "LIST_BUCKETS"},
         {Opcode::SelectBucket, "SELECT_BUCKET"},
         {Opcode::ObserveSeqno, "OBSERVE_SEQNO"},
         {Opcode::Observe, "OBSERVE"},
         {Opcode::EvictKey, "EVICT_KEY"},
         {Opcode::GetLocked, "GET_LOCKED"},
         {Opcode::UnlockKey, "UNLOCK_KEY"},
         {Opcode::LastClosedCheckpoint, "LAST_CLOSED_CHECKPOINT"},
         {Opcode::ResetReplicationChain, "RESET_REPLICATION_CHAIN"},
         {Opcode::DeregisterTapClient, "DEREGISTER_TAP_CLIENT"},
         {Opcode::GetMeta, "GET_META"},
         {Opcode::GetqMeta, "GETQ_META"},
         {Opcode::SetWithMeta, "SET_WITH_META"},
         {Opcode::SetqWithMeta, "SETQ_WITH_META"},
         {Opcode::AddWithMeta, "ADD_WITH_META"},
         {Opcode::AddqWithMeta, "ADDQ_WITH_META"},
         {Opcode::SnapshotVbStates, "SNAPSHOT_VB_STATES"},
         {Opcode::VbucketBatchCount, "VBUCKET_BATCH_COUNT"},
         {Opcode::DelWithMeta, "DEL_WITH_META"},
         {Opcode::DelqWithMeta, "DELQ_WITH_META"},
         {Opcode::CreateCheckpoint, "CREATE_CHECKPOINT"},
         {Opcode::NotifyVbucketUpdate, "NOTIFY_VBUCKET_UPDATE"},
         {Opcode::EnableTraffic, "ENABLE_TRAFFIC"},
         {Opcode::DisableTraffic, "DISABLE_TRAFFIC"},
         {Opcode::ChangeVbFilter, "CHANGE_VB_FILTER"},
         {Opcode::CheckpointPersistence, "CHECKPOINT_PERSISTENCE"},
         {Opcode::ReturnMeta, "RETURN_META"},
         {Opcode::CompactDb, "COMPACT_DB"},
         {Opcode::SetClusterConfig, "SET_CLUSTER_CONFIG"},
         {Opcode::GetClusterConfig, "GET_CLUSTER_CONFIG"},
         {Opcode::GetRandomKey, "GET_RANDOM_KEY"},
         {Opcode::SeqnoPersistence, "SEQNO_PERSISTENCE"},
         {Opcode::GetKeys, "GET_KEYS"},
         {Opcode::CollectionsSetManifest, "COLLECTIONS_SET_MANIFEST"},
         {Opcode::SetDriftCounterState, "SET_DRIFT_COUNTER_STATE"},
         {Opcode::GetAdjustedTime, "GET_ADJUSTED_TIME"},
         {Opcode::SubdocGet, "SUBDOC_GET"},
         {Opcode::SubdocExists, "SUBDOC_EXISTS"},
         {Opcode::SubdocDictAdd, "SUBDOC_DICT_ADD"},
         {Opcode::SubdocDictUpsert, "SUBDOC_DICT_UPSERT"},
         {Opcode::SubdocDelete, "SUBDOC_DELETE"},
         {Opcode::SubdocReplace, "SUBDOC_REPLACE"},
         {Opcode::SubdocArrayPushLast, "SUBDOC_ARRAY_PUSH_LAST"},
         {Opcode::SubdocArrayPushFirst, "SUBDOC_ARRAY_PUSH_FIRST"},
         {Opcode::SubdocArrayInsert, "SUBDOC_ARRAY_INSERT"},
         {Opcode::SubdocArrayAddUnique, "SUBDOC_ARRAY_ADD_UNIQUE"},
         {Opcode::SubdocCounter, "SUBDOC_COUNTER"},
         {Opcode::SubdocMultiLookup, "SUBDOC_MULTI_LOOKUP"},
         {Opcode::SubdocMultiMutation, "SUBDOC_MULTI_MUTATION"},
         {Opcode::SubdocGetCount, "SUBDOC_GET_COUNT"},
         {Opcode::Scrub, "SCRUB"},
         {Opcode::IsaslRefresh, "ISASL_REFRESH"},
         {Opcode::SslCertsRefresh, "SSL_CERTS_REFRESH"},
         {Opcode::GetCmdTimer, "GET_CMD_TIMER"},
         {Opcode::SetCtrlToken, "SET_CTRL_TOKEN"},
         {Opcode::GetCtrlToken, "GET_CTRL_TOKEN"},
         {Opcode::InitComplete, "INIT_COMPLETE"},
         {Opcode::RbacRefresh, "RBAC_REFRESH"},
         {Opcode::DropPrivilege, "DROP_PRIVILEGES"},
         {Opcode::AdjustTimeofday, "ADJUST_TIMEOFDAY"},
         {Opcode::EwouldblockCtl, "EWB_CTL"},
         {Opcode::GetErrorMap, "GET_ERROR_MAP"}}};

TEST(to_string, LegalValues) {
    for (auto& entry : blueprint) {
        EXPECT_EQ(entry.second, to_string(entry.first));
    }

    EXPECT_THROW(to_string(Opcode::Invalid), std::invalid_argument);
}

TEST(to_string, InvalidValues) {
    for (int ii = 0; ii < 0x100; ++ii) {
        Opcode opcode = Opcode(ii);
        if (blueprint.find(opcode) == blueprint.end()) {
            EXPECT_THROW(to_string(opcode), std::invalid_argument);
        }
    }
}

TEST(to_string, CompatWithOldCode) {
    // verify that we return the same strings as earlier
    for (auto& entry : blueprint) {
        if (entry.first == Opcode::AdjustTimeofday ||
            entry.first == Opcode::EwouldblockCtl ||
            entry.first == Opcode::ChangeVbFilter ||
            entry.first == Opcode::DeregisterTapClient ||
            entry.first == Opcode::ResetReplicationChain) {
            // Entries we didn't have in the old code..
            continue;
        }
        auto* val = memcached_opcode_2_text(uint16_t(entry.first));
        ASSERT_NE(nullptr, val) << to_string(entry.first);
        EXPECT_EQ(entry.second, val);
    }
}

TEST(to_opcode, LegalValues) {
    for (auto& entry : blueprint) {
        EXPECT_EQ(entry.first, to_opcode(entry.second));
    }
}

TEST(to_opcode, UnknownValues) {
    EXPECT_THROW(to_opcode("asdfasdf"), std::invalid_argument);
}

TEST(to_opcode, CaseDontMatter) {
    for (auto& entry : blueprint) {
        std::string lower;
        std::transform(entry.second.begin(),
                       entry.second.end(),
                       std::back_inserter(lower),
                       ::tolower);
        EXPECT_EQ(entry.first, to_opcode(lower));
    }
}

TEST(to_opcode, SpaceMayBeUsed) {
    for (auto& entry : blueprint) {
        std::string input{entry.second};
        std::replace(input.begin(), input.end(), '_', ' ');
        EXPECT_EQ(entry.first, to_opcode(input));
    }
}
