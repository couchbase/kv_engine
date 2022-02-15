/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2014-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "dcp/response.h"

#include <memcached/protocol_binary.h>
#include <xattr/utils.h>

#include <typeinfo>

/*
 * These constants are calculated from the size of the packets that are
 * created by each message when it gets sent over the wire. The packet
 * structures are located in the protocol_binary.h file in the memcached
 * project.
 */

const uint32_t StreamRequest::baseMsgBytes = 72;
const uint32_t AddStreamResponse::baseMsgBytes = 28;
const uint32_t SnapshotMarkerResponse::baseMsgBytes = 24;
const uint32_t SetVBucketStateResponse::baseMsgBytes = 24;
const uint32_t StreamEndResponse::baseMsgBytes = 28;
const uint32_t SetVBucketState::baseMsgBytes = 25;
const uint32_t SnapshotMarker::baseMsgBytes = 24;
const uint32_t OSOSnapshot::baseMsgBytes = 28;
const uint32_t SeqnoAdvanced::baseMsgBytes = 32;

const char* DcpResponse::to_string() const {
    switch (event_) {
    case Event::Mutation:
        return "mutation";
    case Event::Deletion:
        return "deletion";
    case Event::Expiration:
        return "expiration";
    case Event::Prepare:
        return "prepare";
    case Event::Commit:
        return "commit";
    case Event::Abort:
        return "abort";
    case Event::SetVbucket:
        return "set vbucket";
    case Event::StreamReq:
        return "stream req";
    case Event::StreamEnd:
        return "stream end";
    case Event::SnapshotMarker:
        return "snapshot marker";
    case Event::AddStream:
        return "add stream";
    case Event::SystemEvent:
        return "system event";
    case Event::SeqnoAcknowledgement:
        return "seqno acknowledgement";
    case Event::OSOSnapshot:
        return "OSO snapshot";
    case Event::SeqnoAdvanced:
        return "Seqno Advanced";
    }
    throw std::logic_error(
        "DcpResponse::to_string(): " + std::to_string(int(event_)));
}

bool operator==(const DcpResponse& lhs, const DcpResponse& rhs) {
    return typeid(lhs) == typeid(rhs) && lhs.isEqual(rhs);
}

bool operator!=(const DcpResponse& lhs, const DcpResponse& rhs) {
    return !(lhs == rhs);
}

bool DcpResponse::isEqual(const DcpResponse& other) const {
    return opaque_ == other.opaque_ && event_ == other.event_ &&
           sid == other.sid;
}

uint32_t MutationResponse::getDeleteLength() const {
    if ((enableExpiryOutput == EnableExpiryOutput::Yes) &&
        (item_->deletionSource() == DeleteSource::TTL)) {
        return expirationBaseMsgBytes;
    }
    if (includeDeleteTime == IncludeDeleteTime::Yes) {
        return deletionV2BaseMsgBytes;
    }
    return deletionBaseMsgBytes;
}

uint32_t MutationResponse::getHeaderSize() const {
    switch (item_->getOperation()) {
    case queue_op::mutation:
    case queue_op::system_event:
    case queue_op::commit_sync_write:
        // A commit_sync_write which is being sent as a vanilla DCP_MUTATION,
        // either because the DCP client doesn't support SyncWrites, or because
        // it does but it never received the corresponding prepare.
        return item_->isDeleted() ? getDeleteLength() : mutationBaseMsgBytes;
    case queue_op::pending_sync_write:
        return prepareBaseMsgBytes;
    default:
        throw std::logic_error(
                "MutationResponse::getHeaderSize: Invalid operation " +
                ::to_string(item_->getOperation()));
    }
}

uint32_t MutationResponse::getMessageSize() const {
    const uint32_t header = getHeaderSize();
    uint32_t keySize = 0;
    if (includeCollectionID == DocKeyEncodesCollectionId::Yes) {
        keySize = item_->getKey().size();
    } else {
        keySize = item_->getKey().makeDocKeyWithoutCollectionID().size();
    }
    uint32_t body = keySize + item_->getNBytes();
    body += (getStreamId() ? sizeof(cb::mcbp::DcpStreamIdFrameInfo) : 0);
    return header + body;
}

MutationConsumerMessage::MutationConsumerMessage(MutationResponse& response)
    : MutationResponse(response.getItem(),
                       response.getOpaque(),
                       response.getIncludeValue(),
                       response.getIncludeXattrs(),
                       response.getIncludeDeleteTime(),
                       response.getIncludeDeletedUserXattrs(),
                       response.getDocKeyEncodesCollectionId(),
                       response.getEnableExpiryOutput(),
                       response.getStreamId()),
      emd(nullptr) {
}

uint32_t MutationConsumerMessage::getMessageSize() const {
    uint32_t body = MutationResponse::getMessageSize();

    // Check to see if we need to include the extended meta data size.
    if (emd) {
        body += emd->getExtMeta().second;
    }

    return body;
}

std::ostream& operator<<(std::ostream& os, const DcpResponse& r) {
    os << "DcpResponse[" << &r << "] with"
       << " event:" << r.to_string();
    return os;
}

CommitSyncWrite::CommitSyncWrite(uint32_t opaque,
                                 Vbid vbucket,
                                 uint64_t preparedSeqno,
                                 uint64_t commitSeqno,
                                 const DocKey& key,
                                 DocKeyEncodesCollectionId includeCollectionID)
    : DcpResponse(Event::Commit, opaque, cb::mcbp::DcpStreamId{}),
      vbucket(vbucket),
      key(key),
      payload(preparedSeqno, commitSeqno),
      includeCollectionID(includeCollectionID) {
}

CommitSyncWriteConsumer::CommitSyncWriteConsumer(uint32_t opaque,
                                                 Vbid vbucket,
                                                 uint64_t preparedSeqno,
                                                 uint64_t commitSeqno,
                                                 const DocKey& key)
    : CommitSyncWrite(opaque,
                      vbucket,
                      preparedSeqno,
                      commitSeqno,
                      key,
                      key.getEncoding()) {
}

uint32_t CommitSyncWrite::getMessageSize() const {
    auto size = commitBaseMsgBytes;
    if (includeCollectionID == DocKeyEncodesCollectionId::Yes) {
        size += key.size();
    } else {
        size += key.makeDocKeyWithoutCollectionID().size();
    }
    return size;
}

AbortSyncWrite::AbortSyncWrite(uint32_t opaque,
                               Vbid vbucket,
                               const DocKey& key,
                               uint64_t preparedSeqno,
                               uint64_t abortSeqno,
                               DocKeyEncodesCollectionId includeCollectionID)
    : DcpResponse(Event::Abort, opaque, cb::mcbp::DcpStreamId{}),
      vbucket(vbucket),
      key(key),
      payload(preparedSeqno, abortSeqno),
      includeCollectionID(includeCollectionID) {
}

AbortSyncWriteConsumer::AbortSyncWriteConsumer(uint32_t opaque,
                                               Vbid vbucket,
                                               const DocKey& key,
                                               uint64_t preparedSeqno,
                                               uint64_t abortSeqno)
    : AbortSyncWrite(opaque,
                     vbucket,
                     key,
                     preparedSeqno,
                     abortSeqno,
                     key.getEncoding()) {
}

uint32_t AbortSyncWrite::getMessageSize() const {
    auto size = abortBaseMsgBytes;
    if (includeCollectionID == DocKeyEncodesCollectionId::Yes) {
        size += key.size();
    } else {
        size += key.makeDocKeyWithoutCollectionID().size();
    }
    return size;
}

uint32_t SnapshotMarker::getMessageSize() const {
    auto rv = baseMsgBytes;
    if (timestamp) {
        rv += sizeof(cb::mcbp::request::DcpSnapshotMarkerV2xPayload) +
              sizeof(cb::mcbp::request::DcpSnapshotMarkerV2_1Value);
    } else if (highCompletedSeqno || maxVisibleSeqno) {
        rv += sizeof(cb::mcbp::request::DcpSnapshotMarkerV2xPayload) +
              sizeof(cb::mcbp::request::DcpSnapshotMarkerV2_0Value);
    } else {
        rv += sizeof(cb::mcbp::request::DcpSnapshotMarkerV1Payload);
    }
    rv += (getStreamId() ? sizeof(cb::mcbp::DcpStreamIdFrameInfo) : 0);
    return rv;
}

bool StreamRequest::isEqual(const DcpResponse& rsp) const {
    const auto& other = static_cast<const StreamRequest&>(rsp);
    bool eq =
            startSeqno_ == other.startSeqno_ && endSeqno_ == other.endSeqno_ &&
            vbucketUUID_ == other.vbucketUUID_ &&
            snapStartSeqno_ == other.snapStartSeqno_ &&
            snapEndSeqno_ == other.snapEndSeqno_ && flags_ == other.flags_ &&
            vbucket_ == other.vbucket_ && requestValue_ == other.requestValue_;
    return eq && DcpResponse::isEqual(rsp);
}

bool AddStreamResponse::isEqual(const DcpResponse& rsp) const {
    const auto& other = static_cast<const AddStreamResponse&>(rsp);
    bool eq = streamOpaque_ == other.streamOpaque_ && status_ == other.status_;
    return eq && DcpResponse::isEqual(rsp);
}

bool SnapshotMarkerResponse::isEqual(const DcpResponse& rsp) const {
    const auto& other = static_cast<const SnapshotMarkerResponse&>(rsp);
    return status_ == other.status_ && DcpResponse::isEqual(rsp);
}

bool SetVBucketStateResponse::isEqual(const DcpResponse& rsp) const {
    const auto& other = static_cast<const SetVBucketStateResponse&>(rsp);
    return status_ == other.status_ && DcpResponse::isEqual(rsp);
}

bool StreamEndResponse::isEqual(const DcpResponse& rsp) const {
    const auto& other = static_cast<const StreamEndResponse&>(rsp);
    bool eq = flags_ == other.flags_ && vbucket_ == other.vbucket_;
    return eq && DcpResponse::isEqual(rsp);
}

bool SetVBucketState::isEqual(const DcpResponse& rsp) const {
    const auto& other = static_cast<const SetVBucketState&>(rsp);
    bool eq = vbucket_ == other.vbucket_ && state_ == other.state_;
    return eq && DcpResponse::isEqual(rsp);
}

bool SnapshotMarker::isEqual(const DcpResponse& rsp) const {
    const auto& other = static_cast<const SnapshotMarker&>(rsp);
    bool eq = vbucket_ == other.vbucket_ &&
              start_seqno_ == other.start_seqno_ &&
              end_seqno_ == other.end_seqno_ && flags_ == other.flags_ &&
              highCompletedSeqno == other.highCompletedSeqno &&
              maxVisibleSeqno == other.maxVisibleSeqno &&
              timestamp == other.timestamp;
    return eq && DcpResponse::isEqual(rsp);
}

bool MutationResponse::isEqual(const DcpResponse& rsp) const {
    const auto& other = static_cast<const MutationResponse&>(rsp);
    bool eq = *item_ == *other.item_ && includeValue == other.includeValue &&
              includeXattributes == other.includeXattributes &&
              includeDeletedUserXattrs == other.includeDeletedUserXattrs &&
              includeCollectionID == other.includeCollectionID &&
              enableExpiryOutput == other.enableExpiryOutput;

    return eq && DcpResponse::isEqual(rsp);
}

bool MutationConsumerMessage::isEqual(const DcpResponse& rsp) const {
    const auto& other = static_cast<const MutationConsumerMessage&>(rsp);

    auto compareEmd = [this](const MutationConsumerMessage& other) {
        if (emd && other.emd) {
            auto ext1 = emd->getExtMeta();
            auto ext2 = other.emd->getExtMeta();
            if (ext1.second == ext2.second) {
                return std::memcmp(ext1.first, ext2.first, ext1.second) == 0;
            }
        } else if (!emd && !other.emd) {
            return true;
        }
        return false;
    };
    return compareEmd(other) && MutationResponse::isEqual(rsp);
}

bool SeqnoAcknowledgement::isEqual(const DcpResponse& rsp) const {
    const auto& other = static_cast<const SeqnoAcknowledgement&>(rsp);
    return vbucket == other.vbucket &&
           payload.getPreparedSeqno() == other.payload.getPreparedSeqno() &&
           DcpResponse::isEqual(rsp);
}

bool CommitSyncWrite::isEqual(const DcpResponse& rsp) const {
    const auto& other = static_cast<const CommitSyncWrite&>(rsp);
    bool eq = vbucket == other.vbucket && key == other.key &&
              payload.getBuffer() == other.payload.getBuffer() &&
              includeCollectionID == other.includeCollectionID;
    return eq && DcpResponse::isEqual(rsp);
}

bool AbortSyncWrite::isEqual(const DcpResponse& rsp) const {
    const auto& other = static_cast<const AbortSyncWrite&>(rsp);
    bool eq = vbucket == other.vbucket && key == other.key &&
              payload.getBuffer() == other.payload.getBuffer() &&
              includeCollectionID == other.includeCollectionID;
    return eq && DcpResponse::isEqual(rsp);
}

bool SystemEventConsumerMessage::isEqual(const DcpResponse& rsp) const {
    const auto& other = static_cast<const SystemEventConsumerMessage&>(rsp);
    bool eq = event == other.event && bySeqno == other.bySeqno &&
              version == other.version && key == other.key &&
              eventData == other.eventData;
    return eq && DcpResponse::isEqual(rsp);
}

bool SystemEventProducerMessage::isEqual(const DcpResponse& rsp) const {
    const auto& other = static_cast<const SystemEventProducerMessage&>(rsp);
    return *item == *other.item && DcpResponse::isEqual(rsp);
}

bool CollectionCreateProducerMessage::isEqual(const DcpResponse& rsp) const {
    const auto& other =
            static_cast<const CollectionCreateProducerMessage&>(rsp);
    bool eq = key == other.key && eventData == other.eventData;
    return eq && SystemEventProducerMessage::isEqual(rsp);
}

bool CollectionCreateWithMaxTtlProducerMessage::isEqual(
        const DcpResponse& rsp) const {
    const auto& other =
            static_cast<const CollectionCreateWithMaxTtlProducerMessage&>(rsp);
    bool eq = key == other.key && eventData == other.eventData;
    return eq && SystemEventProducerMessage::isEqual(rsp);
}

bool CollectionDropProducerMessage::isEqual(const DcpResponse& rsp) const {
    const auto& other = static_cast<const CollectionDropProducerMessage&>(rsp);
    bool eq = key == other.key && eventData == other.eventData;
    return eq && SystemEventProducerMessage::isEqual(rsp);
}

bool ScopeCreateProducerMessage::isEqual(const DcpResponse& rsp) const {
    const auto& other = static_cast<const ScopeCreateProducerMessage&>(rsp);
    bool eq = key == other.key && eventData == other.eventData;
    return eq && SystemEventProducerMessage::isEqual(rsp);
}

bool ScopeDropProducerMessage::isEqual(const DcpResponse& rsp) const {
    const auto& other = static_cast<const ScopeDropProducerMessage&>(rsp);
    bool eq = key == other.key && eventData == other.eventData;
    return eq && SystemEventProducerMessage::isEqual(rsp);
}

bool OSOSnapshot::isEqual(const DcpResponse& rsp) const {
    const auto& other = static_cast<const OSOSnapshot&>(rsp);
    return vbucket == other.vbucket && start == other.start &&
           DcpResponse::isEqual(rsp);
}

bool SeqnoAdvanced::isEqual(const DcpResponse& rsp) const {
    const auto& other = static_cast<const SeqnoAdvanced&>(rsp);
    return vbucket == other.vbucket && advancedSeqno == other.advancedSeqno &&
           DcpResponse::isEqual(rsp);
}
