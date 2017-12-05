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

#include <mcbp/protocol/request.h>

bool cb::mcbp::Request::isQuiet() const {
    if (getMagic() == Magic::ClientRequest) {
        switch (getClientOpcode()) {
        case ClientOpcode::Get:
        case ClientOpcode::Set:
        case ClientOpcode::Add:
        case ClientOpcode::Replace:
        case ClientOpcode::Delete:
        case ClientOpcode::Increment:
        case ClientOpcode::Decrement:
        case ClientOpcode::Quit:
        case ClientOpcode::Flush:
        case ClientOpcode::Noop:
        case ClientOpcode::Version:
        case ClientOpcode::Getk:
        case ClientOpcode::Append:
        case ClientOpcode::Prepend:
        case ClientOpcode::Stat:
        case ClientOpcode::Verbosity:
        case ClientOpcode::Touch:
        case ClientOpcode::Gat:
        case ClientOpcode::Hello:
        case ClientOpcode::SaslListMechs:
        case ClientOpcode::SaslAuth:
        case ClientOpcode::SaslStep:
        case ClientOpcode::IoctlGet:
        case ClientOpcode::IoctlSet:
        case ClientOpcode::ConfigValidate:
        case ClientOpcode::ConfigReload:
        case ClientOpcode::AuditPut:
        case ClientOpcode::AuditConfigReload:
        case ClientOpcode::Shutdown:
        case ClientOpcode::Rget:
        case ClientOpcode::Rset:
        case ClientOpcode::Rappend:
        case ClientOpcode::Rprepend:
        case ClientOpcode::Rdelete:
        case ClientOpcode::Rincr:
        case ClientOpcode::Rdecr:
        case ClientOpcode::SetVbucket:
        case ClientOpcode::GetVbucket:
        case ClientOpcode::DelVbucket:
        case ClientOpcode::TapConnect:
        case ClientOpcode::TapMutation:
        case ClientOpcode::TapDelete:
        case ClientOpcode::TapFlush:
        case ClientOpcode::TapOpaque:
        case ClientOpcode::TapVbucketSet:
        case ClientOpcode::TapCheckpointStart:
        case ClientOpcode::TapCheckpointEnd:
        case ClientOpcode::GetAllVbSeqnos:
        case ClientOpcode::DcpOpen:
        case ClientOpcode::DcpAddStream:
        case ClientOpcode::DcpCloseStream:
        case ClientOpcode::DcpStreamReq:
        case ClientOpcode::DcpGetFailoverLog:
        case ClientOpcode::DcpStreamEnd:
        case ClientOpcode::DcpSnapshotMarker:
        case ClientOpcode::DcpMutation:
        case ClientOpcode::DcpDeletion:
        case ClientOpcode::DcpExpiration:
        case ClientOpcode::DcpFlush:
        case ClientOpcode::DcpSetVbucketState:
        case ClientOpcode::DcpNoop:
        case ClientOpcode::DcpBufferAcknowledgement:
        case ClientOpcode::DcpControl:
        case ClientOpcode::DcpSystemEvent:
        case ClientOpcode::StopPersistence:
        case ClientOpcode::StartPersistence:
        case ClientOpcode::SetParam:
        case ClientOpcode::GetReplica:
        case ClientOpcode::CreateBucket:
        case ClientOpcode::DeleteBucket:
        case ClientOpcode::ListBuckets:
        case ClientOpcode::SelectBucket:
        case ClientOpcode::ObserveSeqno:
        case ClientOpcode::Observe:
        case ClientOpcode::EvictKey:
        case ClientOpcode::GetLocked:
        case ClientOpcode::UnlockKey:
        case ClientOpcode::LastClosedCheckpoint:
        case ClientOpcode::ResetReplicationChain:
        case ClientOpcode::DeregisterTapClient:
        case ClientOpcode::GetMeta:
        case ClientOpcode::SetWithMeta:
        case ClientOpcode::AddWithMeta:
        case ClientOpcode::SnapshotVbStates:
        case ClientOpcode::VbucketBatchCount:
        case ClientOpcode::DelWithMeta:
        case ClientOpcode::CreateCheckpoint:
        case ClientOpcode::NotifyVbucketUpdate:
        case ClientOpcode::EnableTraffic:
        case ClientOpcode::DisableTraffic:
        case ClientOpcode::ChangeVbFilter:
        case ClientOpcode::CheckpointPersistence:
        case ClientOpcode::ReturnMeta:
        case ClientOpcode::CompactDb:
        case ClientOpcode::SetClusterConfig:
        case ClientOpcode::GetClusterConfig:
        case ClientOpcode::GetRandomKey:
        case ClientOpcode::SeqnoPersistence:
        case ClientOpcode::GetKeys:
        case ClientOpcode::CollectionsSetManifest:
        case ClientOpcode::CollectionsGetManifest:
        case ClientOpcode::SetDriftCounterState:
        case ClientOpcode::GetAdjustedTime:
        case ClientOpcode::SubdocGet:
        case ClientOpcode::SubdocExists:
        case ClientOpcode::SubdocDictAdd:
        case ClientOpcode::SubdocDictUpsert:
        case ClientOpcode::SubdocDelete:
        case ClientOpcode::SubdocReplace:
        case ClientOpcode::SubdocArrayPushLast:
        case ClientOpcode::SubdocArrayPushFirst:
        case ClientOpcode::SubdocArrayInsert:
        case ClientOpcode::SubdocArrayAddUnique:
        case ClientOpcode::SubdocCounter:
        case ClientOpcode::SubdocMultiLookup:
        case ClientOpcode::SubdocMultiMutation:
        case ClientOpcode::SubdocGetCount:
        case ClientOpcode::Scrub:
        case ClientOpcode::IsaslRefresh:
        case ClientOpcode::SslCertsRefresh:
        case ClientOpcode::GetCmdTimer:
        case ClientOpcode::SetCtrlToken:
        case ClientOpcode::GetCtrlToken:
        case ClientOpcode::RbacRefresh:
        case ClientOpcode::DropPrivilege:
        case ClientOpcode::AdjustTimeofday:
        case ClientOpcode::EwouldblockCtl:
        case ClientOpcode::GetErrorMap:
        case ClientOpcode::Invalid:
            return false;

        case ClientOpcode::Getq:
        case ClientOpcode::Getkq:
        case ClientOpcode::Setq:
        case ClientOpcode::Addq:
        case ClientOpcode::Replaceq:
        case ClientOpcode::Deleteq:
        case ClientOpcode::Incrementq:
        case ClientOpcode::Decrementq:
        case ClientOpcode::Quitq:
        case ClientOpcode::Flushq:
        case ClientOpcode::Appendq:
        case ClientOpcode::Prependq:
        case ClientOpcode::Gatq:
        case ClientOpcode::Rsetq:
        case ClientOpcode::Rappendq:
        case ClientOpcode::Rprependq:
        case ClientOpcode::Rdeleteq:
        case ClientOpcode::Rincrq:
        case ClientOpcode::Rdecrq:
        case ClientOpcode::GetqMeta:
        case ClientOpcode::SetqWithMeta:
        case ClientOpcode::AddqWithMeta:
        case ClientOpcode::DelqWithMeta:
            return true;
        }
    } else {
        switch (getServerOpcode()) {
        case ServerOpcode::ClustermapChangeNotification:
            return false;
        }
    }

    throw std::invalid_argument("Request::isQuiet: Uknown opcode");
}
