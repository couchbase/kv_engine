/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "collections/system_event_types.h"
#include "dcp/dcp-types.h"
#include "ep_types.h"
#include "item.h"
#include "systemevent_factory.h"

#include <mcbp/protocol/dcp_stream_end_status.h>
#include <memcached/dcp_stream_id.h>
#include <memcached/protocol_binary.h>
#include <memory>
#include <utility>

class DcpResponse {
public:
    enum class Event : uint8_t {
        Mutation,
        Deletion,
        Expiration,
        Prepare,
        Commit,
        Abort,
        SetVbucket,
        StreamReq,
        StreamEnd,
        SnapshotMarker,
        AddStream,
        SystemEvent,
        SeqnoAcknowledgement,
        OSOSnapshot,
        SeqnoAdvanced,
        CachedValue,
        CachedKeyMeta,
        CacheTransferToActiveStream
    };

    DcpResponse(Event event,
                uint32_t opaque,
                cb::mcbp::DcpStreamId sid,
                uint32_t flowControlSize)
        : opaque_(opaque),
          flowControlSize(flowControlSize),
          sid(sid),
          event_(event) {
    }

    DcpResponse(Event event, uint32_t opaque, cb::mcbp::DcpStreamId sid)
        : opaque_(opaque), sid(sid), event_(event) {
    }

    virtual ~DcpResponse() {}

    uint32_t getOpaque() const {
        return opaque_;
    }

    Event getEvent() const {
        return event_;
    }

    /**
     * Virtual method with the sole purpose of removing the need for
     * calling dynamic_cast<MutationResponse> on the returned response
     * object.
     */
    virtual bool isMutationResponse() const {
        return false;
    }

    /**
     * Not all DcpResponse sub-classes have a seqno. MutationResponse (events
     * Mutation, Deletion and Expiration), SystemEventMessage (SystemEvent) and
     * Durability Commit/Abort have a seqno and would return an OptionalSeqno
     * with a seqno.
     *
     * @return OptionalSeqno with no value - certain sub-classes may have a
     *         seqno.
     */
    virtual OptionalSeqno getBySeqno() const {
        return OptionalSeqno{/*no-seqno*/};
    }

    /**
     * Function used in backfill code to determine if the current queued item
     * represents a persisted item, e.g. a mutation or delete
     *
     * @return true if this event is persisted (something which has been
     * recombobulated from the KVStore )
     */
    bool isPersistedEvent() const {
        switch (event_) {
        case Event::Mutation:
        case Event::Deletion:
        case Event::Expiration:
        case Event::Prepare:
        case Event::Commit:
        case Event::Abort:
        case Event::SystemEvent:
            return true;
        case Event::SetVbucket:
        case Event::StreamReq:
        case Event::StreamEnd:
        case Event::SnapshotMarker:
        case Event::AddStream:
        case Event::SeqnoAcknowledgement:
        case Event::OSOSnapshot:
        case Event::SeqnoAdvanced:
        case Event::CachedValue:
        case Event::CachedKeyMeta:
        case Event::CacheTransferToActiveStream:
            return false;
        }
        throw std::invalid_argument(
                "DcpResponse::isPersistedEvent: Invalid event_ " +
                std::to_string(int(event_)));
    }

    virtual size_t getMessageSize() const {
        //  If this is the implementation, then the sub-class must of
        //  pre-calculated a non-zero size and passed that value to the
        //  constructor (and used the correct constructor).
        Expects(flowControlSize != 0);
        return flowControlSize;
    }

    /**
     * Return approximately how many bytes this response message is using
     * for use in buffered backfill accounting. Note that certain sub-classes
     * have never been accounted for, only MutationResponse is used in the
     * accounting, hence this abstract method returns 0.
     */
    virtual size_t getApproximateSize() const {
        return 0;
    }

    const char* to_string() const;

    cb::mcbp::DcpStreamId getStreamId() const {
        return sid;
    }

protected:
    /**
     * This is for use only by the friend == operator. Implemented by the
     * DcpResponse hierarchy and will cast and call up the hierarchy to get
     * the complete comparison.
     *
     * @return true if this == rsp
     */
    virtual bool isEqual(const DcpResponse& rsp) const;

    friend bool operator==(const DcpResponse&, const DcpResponse&);

private:
    uint32_t opaque_{0};
    uint32_t flowControlSize{0};
    cb::mcbp::DcpStreamId sid;
    Event event_{Event::Mutation};
};

/*
 * These constants are calculated from the size of the packets that are
 * created by each message when it gets sent over the wire. The packet
 * structures are located in the protocol_binary.h file in the memcached
 * project. These values are used for flow-control which is limited to u32 so we
 * can define these as u32 to avoid pointless casting. These are only relevant
 * to replication and used by consumer/producer code for accounting
 * flow-control.
 */
constexpr uint32_t SetVBucketStateFlowControlSize = 25;

std::ostream& operator<<(std::ostream& os, const DcpResponse& r);
bool operator==(const DcpResponse& lhs, const DcpResponse& rhs);
bool operator!=(const DcpResponse& lhs, const DcpResponse& rhs);

class StreamRequest : public DcpResponse {
public:
    StreamRequest(Vbid vbucket,
                  uint32_t opaque,
                  cb::mcbp::DcpAddStreamFlag flags,
                  uint64_t startSeqno,
                  uint64_t endSeqno,
                  uint64_t vbucketUUID,
                  uint64_t snapStartSeqno,
                  uint64_t snapEndSeqno,
                  const std::string& request_value)
        : DcpResponse(Event::StreamReq, opaque, {}, flowControlBytes),
          startSeqno_(startSeqno),
          endSeqno_(endSeqno),
          vbucketUUID_(vbucketUUID),
          snapStartSeqno_(snapStartSeqno),
          snapEndSeqno_(snapEndSeqno),
          flags_(flags),
          vbucket_(vbucket),
          requestValue_(request_value) {
    }

    ~StreamRequest() override {
    }

    Vbid getVBucket() {
        return vbucket_;
    }

    auto getFlags() const {
        return flags_;
    }

    uint64_t getStartSeqno() {
        return startSeqno_;
    }

    uint64_t getEndSeqno() {
        return endSeqno_;
    }

    uint64_t getVBucketUUID() {
        return vbucketUUID_;
    }

    uint64_t getSnapStartSeqno() {
        return snapStartSeqno_;
    }

    uint64_t getSnapEndSeqno() {
        return snapEndSeqno_;
    }

    const std::string getRequestValue() {
        return requestValue_;
    }

protected:
    bool isEqual(const DcpResponse& rsp) const override;

private:
    static constexpr uint32_t flowControlBytes{72};
    uint64_t startSeqno_;
    uint64_t endSeqno_;
    uint64_t vbucketUUID_;
    uint64_t snapStartSeqno_;
    uint64_t snapEndSeqno_;
    cb::mcbp::DcpAddStreamFlag flags_;
    Vbid vbucket_;
    std::string requestValue_;
};

class AddStreamResponse : public DcpResponse {
public:
    AddStreamResponse(uint32_t opaque,
                      uint32_t streamOpaque,
                      cb::mcbp::Status status)
        : DcpResponse(Event::AddStream, opaque, {}, flowControlBytes),
          streamOpaque_(streamOpaque),
          status_(status) {
    }

    ~AddStreamResponse() override = default;

    uint32_t getStreamOpaque() {
        return streamOpaque_;
    }

    cb::mcbp::Status getStatus() const {
        return status_;
    }

protected:
    bool isEqual(const DcpResponse& rsp) const override;

private:
    static constexpr uint32_t flowControlBytes{28};
    const uint32_t streamOpaque_;
    const cb::mcbp::Status status_;
};

class SnapshotMarkerResponse : public DcpResponse {
public:
    SnapshotMarkerResponse(uint32_t opaque, cb::mcbp::Status status)
        : DcpResponse(Event::SnapshotMarker, opaque, {}), status_(status) {
    }

    cb::mcbp::Status getStatus() const {
        return status_;
    }

    size_t getMessageSize() const override {
        // SnapshotMarkerResponse is only created by the consumer in response
        // to the producer message. Use of getMessageSize in that context is
        // just used in readyQ memory accounting, not for flow-control. This is
        // a weakness in the DcpResponse hierarchy that getMessageSize is used
        // for both. Return sizeof the object as the best approximation of the
        // allocation size.
        return sizeof(*this);
    }

protected:
    bool isEqual(const DcpResponse& rsp) const override;

private:
    const cb::mcbp::Status status_;
};

class SetVBucketStateResponse : public DcpResponse {
public:
    SetVBucketStateResponse(uint32_t opaque, cb::mcbp::Status status)
        : DcpResponse(Event::SetVbucket, opaque, {}), status_(status) {
    }

    cb::mcbp::Status getStatus() const {
        return status_;
    }

    size_t getMessageSize() const override {
        // SetVBucketStateResponse is only created by the consumer in response
        // to the producer message. Use of getMessageSize in that context is
        // just used in readyQ memory accounting, not for flow-control. This is
        // a weakness in the DcpResponse hierarchy that getMessageSize is used
        // for both. Return sizeof the object as the best approximation of the
        // allocation size.
        return sizeof(*this);
    }

protected:
    bool isEqual(const DcpResponse& rsp) const override;

private:
    const cb::mcbp::Status status_;
};

class StreamEndResponse : public DcpResponse {
public:
    StreamEndResponse(uint32_t opaque,
                      cb::mcbp::DcpStreamEndStatus flags,
                      Vbid vbucket,
                      cb::mcbp::DcpStreamId sid)
        : DcpResponse(Event::StreamEnd, opaque, sid, getFlowControlSize(sid)),
          flags_(statusToFlags(flags)),
          vbucket_(vbucket) {
    }

    static cb::mcbp::DcpStreamEndStatus statusToFlags(
            cb::mcbp::DcpStreamEndStatus status) {
        if (status == cb::mcbp::DcpStreamEndStatus::Rollback) {
            return cb::mcbp::DcpStreamEndStatus::StateChanged;
        }
        return status;
    }

    cb::mcbp::DcpStreamEndStatus getFlags() const {
        return flags_;
    }

    Vbid getVbucket() const {
        return vbucket_;
    }

    static uint32_t getFlowControlSize(const cb::mcbp::DcpStreamId sid) {
        return baseFlowControlSize +
               (sid ? sizeof(cb::mcbp::DcpStreamIdFrameInfo) : 0);
    }

protected:
    bool isEqual(const DcpResponse& rsp) const override;

private:
    static constexpr uint32_t baseFlowControlSize{28};
    cb::mcbp::DcpStreamEndStatus flags_;
    Vbid vbucket_;
};

/**
 * This is a DcpResponse object that is only queued by a CacheTransferStream. It
 * does not generate a DCP message, but exists to signal to the producer that
 * the CacheTransferStream is finshed and an ActiveStream must now replace it.
 *
 * Using a DcpResponse object in this way provides a way to avoid lock
 * inversions and ensures that the switch to ActiveStream occurs after all
 * readyQ items from cache transfer have been processed.
 */
class CacheTransferToActiveStreamResponse : public DcpResponse {
public:
    CacheTransferToActiveStreamResponse(uint32_t opaque,
                                        Vbid vbucket,
                                        cb::mcbp::DcpStreamId sid)
        : DcpResponse(Event::CacheTransferToActiveStream, opaque, sid),
          vbucket(vbucket) {
    }

    Vbid getVbucket() const {
        return vbucket;
    }

    size_t getMessageSize() const override {
        // Only used for readyQ memory accounting, not for flow-control
        return sizeof(*this);
    }

protected:
    bool isEqual(const DcpResponse& rsp) const override;

private:
    Vbid vbucket;
};

class SetVBucketState : public DcpResponse {
public:
    SetVBucketState(uint32_t opaque, Vbid vbucket, vbucket_state_t state)
        : DcpResponse(Event::SetVbucket,
                      opaque,
                      {/*no sid*/},
                      SetVBucketStateFlowControlSize),
          vbucket_(vbucket),
          state_(state) {
    }

    Vbid getVBucket() const {
        return vbucket_;
    }

    vbucket_state_t getState() const {
        return state_;
    }

protected:
    bool isEqual(const DcpResponse& rsp) const override;

private:
    Vbid vbucket_;
    vbucket_state_t state_;
};

class SnapshotMarker : public DcpResponse {
public:
    SnapshotMarker(uint32_t opaque,
                   Vbid vbucket,
                   uint64_t start_seqno,
                   uint64_t end_seqno,
                   cb::mcbp::request::DcpSnapshotMarkerFlag flags,
                   std::optional<uint64_t> highCompletedSeqno,
                   std::optional<uint64_t> highPreparedSeqno,
                   std::optional<uint64_t> maxVisibleSeqno,
                   std::optional<uint64_t> purgeSeqno,
                   cb::mcbp::DcpStreamId sid)
        : DcpResponse(Event::SnapshotMarker, opaque, sid),
          vbucket_(vbucket),
          start_seqno_(start_seqno),
          end_seqno_(end_seqno),
          flags_(flags),
          highCompletedSeqno(highCompletedSeqno),
          highPreparedSeqno(highPreparedSeqno),
          maxVisibleSeqno(maxVisibleSeqno),
          purgeSeqno(purgeSeqno) {
    }

    Vbid getVBucket() const {
        return vbucket_;
    }

    uint64_t getStartSeqno() const {
        return start_seqno_;
    }

    uint64_t getEndSeqno() const {
        return end_seqno_;
    }

    [[nodiscard]] cb::mcbp::request::DcpSnapshotMarkerFlag getFlags() const {
        return flags_;
    }

    size_t getMessageSize() const override;

    std::optional<uint64_t> getHighCompletedSeqno() const {
        return highCompletedSeqno;
    }

    std::optional<uint64_t> getHighPreparedSeqno() const {
        return highPreparedSeqno;
    }

    std::optional<uint64_t> getMaxVisibleSeqno() const {
        return maxVisibleSeqno;
    }

    std::optional<uint64_t> getPurgeSeqno() const {
        return purgeSeqno;
    }

protected:
    bool isEqual(const DcpResponse& rsp) const override;

private:
    Vbid vbucket_;
    uint64_t start_seqno_;
    uint64_t end_seqno_;
    cb::mcbp::request::DcpSnapshotMarkerFlag flags_;
    std::optional<uint64_t> highCompletedSeqno;
    std::optional<uint64_t> highPreparedSeqno;
    std::optional<uint64_t> maxVisibleSeqno;
    std::optional<uint64_t> purgeSeqno;
};

class MutationResponse : public DcpResponse {
public:
    /**
     * Construct a MutationResponse which is used to represent an outgoing or
     * incoming DCP mutation/deletion (stored in an Item)
     * The parameters includeDeleteTime, includeCollectionID and
     * enableExpiryOutput exist only to control the flow control size of the
     * message. E.g. A DCP stream which has not enabled collectins would send
     * less bytes than one which has enabled collections.
     *
     * @param item The Item (via shared pointer) the object represents
     * @param opaque DCP opaque value
     * @param includeDeleteTime Does DCP include the delete time for deletions
     * @param includeCollectionID If the incoming/outgoing key should contain
     *        the Collection-ID.
     * @param enableExpiryOutput Whether the response should utilise expiry
     *        opcode output (if item is deleted and deletion source is TTL)
     * @param sid The stream-ID
     * @param eventType Optional Event. If not defined the Item defines the
     * Event (see eventFromItem) else this defines the Event.
     */
    MutationResponse(queued_item item,
                     uint32_t opaque,
                     IncludeDeleteTime includeDeleteTime,
                     DocKeyEncodesCollectionId includeCollectionID,
                     EnableExpiryOutput enableExpiryOut,
                     cb::mcbp::DcpStreamId sid,
                     std::optional<Event> eventType = std::nullopt)
        : DcpResponse(eventType ? *eventType : eventFromItem(*item),
                      opaque,
                      sid,
                      calculateFlowControlSize(*item,
                                               includeDeleteTime,
                                               includeCollectionID,
                                               enableExpiryOut,
                                               sid)),
          item_(std::move(item)) {
    }

    bool isMutationResponse() const override {
        return true;
    }

    const queued_item& getItem() const {
        return item_;
    }

    Vbid getVBucket() {
        return item_->getVBucketId();
    }

    /**
     * @return OptionalSeqno with the underlying Item's seqno.
     */
    OptionalSeqno getBySeqno() const override {
        return OptionalSeqno{item_->getBySeqno()};
    }

    uint64_t getRevSeqno() {
        return item_->getRevSeqno();
    }

    /**
      * @return size of message to be sent over the wire to the consumer.
      */
    static uint32_t calculateFlowControlSize(
            Item& item,
            IncludeDeleteTime includeDeleteTime,
            DocKeyEncodesCollectionId includeCollectionID,
            EnableExpiryOutput enableExpiryOut,
            cb::mcbp::DcpStreamId sid);

    /**
     * @returns a size representing approximately the memory used, in this case
     * the item's size.
     */
    size_t getApproximateSize() const override {
        return item_->size();
    }

    /// Returns the Event type which should be used for the given item.
    static Event eventFromItem(const Item& item) {
        switch (item.getOperation()) {
        case queue_op::commit_sync_write:
            // Even though Item is CommittedViaPrepare we still use the same
            // Events as CommittedViaMutation.
            // This method is only used for Mutation-like Events -
            // An actual Commit event is handled by the CommitSyncWrite
            // subclass.
            // FALLTHROUGH
        case queue_op::mutation:
        case queue_op::system_event:
            if (item.isDeleted()) {
                return (item.deletionSource() == DeleteSource::TTL)
                               ? Event::Expiration
                               : Event::Deletion;
            }
            return Event::Mutation;
        case queue_op::pending_sync_write:
            return Event::Prepare;
        case queue_op::abort_sync_write:
            return Event::Abort;
        default:
            throw std::logic_error(
                    "MutationResponse::eventFromItem: Invalid operation " +
                    ::to_string(item.getOperation()));
        }
    }

    static const size_t mutationBaseMsgBytes = 55;
    static const size_t deletionBaseMsgBytes = 42;
    static const size_t deletionV2BaseMsgBytes = 45;
    static const size_t expirationBaseMsgBytes = 44;
    static const size_t prepareBaseMsgBytes = 57;

protected:
    bool isEqual(const DcpResponse& rsp) const override;

    /// Return the size of the header for this message.
    static uint32_t getHeaderSize(Item& item,
                                  IncludeDeleteTime includeDeleteTime,
                                  EnableExpiryOutput enableExpiryOut);

    static uint32_t getDeleteLength(Item& item,
                                    IncludeDeleteTime includeDeleteTime,
                                    EnableExpiryOutput enableExpiryOut);

    const queued_item item_;
};

/**
 * Represents a sequence number acknowledgement message sent from replication
 * consumer to producer to notify the producer what seqno the consumer has
 * prepared up to.
 */
class SeqnoAcknowledgement : public DcpResponse {
public:
    SeqnoAcknowledgement(uint32_t opaque, Vbid vbucket, uint64_t preparedSeqno)
        : DcpResponse(Event::SeqnoAcknowledgement,
                      opaque,
                      cb::mcbp::DcpStreamId{},
                      sizeof(cb::mcbp::Request) +
                              sizeof(cb::mcbp::request::
                                             DcpSeqnoAcknowledgedPayload)),
          vbucket(vbucket),
          payload(preparedSeqno) {
    }

    Vbid getVbucket() const {
        return vbucket;
    }

    uint64_t getPreparedSeqno() const {
        return payload.getPreparedSeqno();
    }

protected:
    bool isEqual(const DcpResponse& rsp) const override;

private:
    Vbid vbucket;
    cb::mcbp::request::DcpSeqnoAcknowledgedPayload payload;
};

/**
 * Represents the Commit of a prepared SyncWrite, producer side.  Producer side
 * requires the collection mode of the producer to determine how keys are
 * encoded.
 */
class CommitSyncWrite : public DcpResponse {
public:
    CommitSyncWrite(uint32_t opaque,
                    Vbid vbucket,
                    uint64_t preparedSeqno,
                    uint64_t commitSeqno,
                    const DocKeyView& key,
                    DocKeyEncodesCollectionId includeCollectionID);

    OptionalSeqno getBySeqno() const override {
        return OptionalSeqno{payload.getCommitSeqno()};
    }

    const StoredDocKey& getKey() const {
        return key;
    }

    Vbid getVbucket() const {
        return vbucket;
    }

    uint64_t getPreparedSeqno() const {
        return payload.getPreparedSeqno();
    }

    uint64_t getCommitSeqno() const {
        return payload.getCommitSeqno();
    }

    static constexpr size_t commitBaseMsgBytes =
            sizeof(cb::mcbp::Request) +
            sizeof(cb::mcbp::request::DcpCommitPayload);

    static uint32_t calculateFlowControlSize(
            const DocKeyView& key,
            DocKeyEncodesCollectionId includeCollectionID);

protected:
    bool isEqual(const DcpResponse& rsp) const override;

private:
    Vbid vbucket;
    StoredDocKey key;
    cb::mcbp::request::DcpCommitPayload payload;
};

/**
 * Represents the Commit of a prepared SyncWrite, consumer side. The key will
 * define how getMessageSize calculates the 'ack' size
 */
class CommitSyncWriteConsumer : public CommitSyncWrite {
public:
    CommitSyncWriteConsumer(uint32_t opaque,
                            Vbid vbucket,
                            uint64_t preparedSeqno,
                            uint64_t commitSeqno,
                            const DocKeyView& key);

protected:
    bool isEqual(const DcpResponse& rsp) const override {
        return CommitSyncWrite::isEqual(rsp);
    }
};

/**
 * Represents the Abort of a prepared SyncWrite. Producer side requires
 * the collection mode of the producer to determine how keys are encoded.
 */
class AbortSyncWrite : public DcpResponse {
public:
    AbortSyncWrite(uint32_t opaque,
                   Vbid vbucket,
                   const DocKeyView& key,
                   uint64_t preparedSeqno,
                   uint64_t abortSeqno,
                   DocKeyEncodesCollectionId includeCollectionID);

    Vbid getVbucket() const {
        return vbucket;
    }

    const StoredDocKey& getKey() const {
        return key;
    }

    uint64_t getPreparedSeqno() const {
        return payload.getPreparedSeqno();
    }

    uint64_t getAbortSeqno() const {
        return payload.getAbortSeqno();
    }

    OptionalSeqno getBySeqno() const override {
        return OptionalSeqno{payload.getAbortSeqno()};
    }

    static constexpr size_t abortBaseMsgBytes =
            sizeof(cb::mcbp::Request) +
            sizeof(cb::mcbp::request::DcpAbortPayload);

    static uint32_t calculateFlowControlSize(
            const DocKeyView& key,
            DocKeyEncodesCollectionId includeCollectionID);

protected:
    bool isEqual(const DcpResponse& rsp) const override;

private:
    Vbid vbucket;
    StoredDocKey key;
    cb::mcbp::request::DcpAbortPayload payload;
};

/**
 * Represents the Abort of a prepared SyncWrite.
 */
class AbortSyncWriteConsumer : public AbortSyncWrite {
public:
    AbortSyncWriteConsumer(uint32_t opaque,
                           Vbid vbucket,
                           const DocKeyView& key,
                           uint64_t preparedSeqno,
                           uint64_t abortSeqno);

    static constexpr size_t abortBaseMsgBytes =
            sizeof(cb::mcbp::Request) +
            sizeof(cb::mcbp::request::DcpAbortPayload);

protected:
    bool isEqual(const DcpResponse& rsp) const override {
        return AbortSyncWrite::isEqual(rsp);
    }
};

/**
 * SystemEventMessage defines the interface required by consumer and producer
 * message classes.
 */
class SystemEventMessage : public DcpResponse {
public:
    SystemEventMessage(uint32_t opaque, cb::mcbp::DcpStreamId sid)
        : DcpResponse(Event::SystemEvent, opaque, sid) {
    }
    // baseMsgBytes is the unpadded size of the
    // protocol_binary_request_dcp_system_event::body struct
    static const uint32_t baseMsgBytes = sizeof(cb::mcbp::Request) +
                                         sizeof(uint64_t) + sizeof(uint32_t) +
                                         sizeof(uint8_t);
    virtual mcbp::systemevent::id getSystemEvent() const = 0;
    virtual std::string_view getKey() const = 0;
    virtual std::string_view getEventData() const = 0;
    virtual mcbp::systemevent::version getVersion() const = 0;
};

/**
 * A SystemEventConsumerMessage is used by DcpConsumer and associated code
 * for storing the data of a SystemEvent. The key and event bytes must be
 * copied from the caller into the object's storage because the consumer
 * will queue the message for future processing.
 */
class SystemEventConsumerMessage : public SystemEventMessage {
public:
    SystemEventConsumerMessage(uint32_t opaque,
                               mcbp::systemevent::id ev,
                               int64_t seqno,
                               Vbid vbucket,
                               mcbp::systemevent::version version,
                               cb::const_byte_buffer _key,
                               cb::const_byte_buffer _eventData)
        : SystemEventMessage(opaque, {}),
          event(ev),
          bySeqno(seqno),
          version(version),
          key(reinterpret_cast<const char*>(_key.data()), _key.size()),
          eventData(_eventData.begin(), _eventData.end()) {
        if (seqno > std::numeric_limits<int64_t>::max()) {
            throw std::overflow_error(
                    "SystemEventMessage: overflow condition on seqno " +
                    std::to_string(seqno));
        }
    }

    size_t getMessageSize() const override {
        return SystemEventMessage::baseMsgBytes + key.size() + eventData.size();
    }

    mcbp::systemevent::id getSystemEvent() const override {
        return event;
    }

    OptionalSeqno getBySeqno() const override {
        return OptionalSeqno{bySeqno};
    }

    std::string_view getKey() const override {
        return key;
    }

    std::string_view getEventData() const override {
        return {reinterpret_cast<const char*>(eventData.data()),
                eventData.size()};
    }

    mcbp::systemevent::version getVersion() const override {
        return version;
    }

protected:
    bool isEqual(const DcpResponse& rsp) const override;

private:
    mcbp::systemevent::id event;
    int64_t bySeqno;
    mcbp::systemevent::version version;
    std::string key;
    std::vector<uint8_t> eventData;
};

/**
 * A SystemEventProducerMessage is used by DcpProducer and associated code
 * for storing the data of a SystemEvent. The class can just own a
 * queued_item (shared_ptr) and then read all data from the underlying
 * Item object.
 */
class SystemEventFlatBuffers;
class SystemEventProducerMessage : public SystemEventMessage {
public:
    /**
     * Note: the body of this factory method is in systemevent_factory.cc along
     *       side related code which decides how SystemEvents are managed.
     *
     * Note: creation of the SystemEventProducerMessage will up the ref-count
     * of item
     *
     * @return a SystemEventMessage unique pointer constructed from the
     *         queued_item data.
     */
    static std::unique_ptr<SystemEventProducerMessage> make(
            uint32_t opaque, queued_item& item, cb::mcbp::DcpStreamId sid);

    /**
     * Note: the body of this factory method is in systemevent_factory.cc along
     *       side related code which decides how SystemEvents are managed.
     *
     * Note: creation of the SystemEventProducerMessage will up the ref-count
     * of item
     *
     * @return a SystemEventMessage unique pointer constructed from the
     *         queued_item data with a FlatBuffers value
     */
    static std::unique_ptr<SystemEventFlatBuffers> makeWithFlatBuffersValue(
            uint32_t opaque, queued_item& item, cb::mcbp::DcpStreamId sid);

    size_t getMessageSize() const override {
        return SystemEventMessage::baseMsgBytes + getKey().size() +
               getEventData().size() +
               (getStreamId() ? sizeof(cb::mcbp::DcpStreamIdFrameInfo) : 0);
    }

    mcbp::systemevent::id getSystemEvent() const override {
        switch (SystemEvent(item->getFlags())) {
        case SystemEvent::Collection:
            return item->isDeleted() ? mcbp::systemevent::id::EndCollection
                                     : mcbp::systemevent::id::BeginCollection;
        case SystemEvent::ModifyCollection:
            return mcbp::systemevent::id::ModifyCollection;
        case SystemEvent::Scope:
            return item->isDeleted() ? mcbp::systemevent::id::DropScope
                                     : mcbp::systemevent::id::CreateScope;
        }

        throw std::logic_error(
                "SystemEventProducerMessage::getSystemEvent unexpected event:" +
                std::to_string(item->getFlags()));
    }

    OptionalSeqno getBySeqno() const override {
        return OptionalSeqno{item->getBySeqno()};
    }

    Vbid getVBucket() const {
        return item->getVBucketId();
    }

    /**
     * @returns a size representing approximately the memory used, in this case
     * the item's size.
     */
    size_t getApproximateSize() const override {
        return item->size();
    }

protected:
    bool isEqual(const DcpResponse& rsp) const override;

    SystemEventProducerMessage(uint32_t opaque,
                               const queued_item& itm,
                               cb::mcbp::DcpStreamId sid)
        : SystemEventMessage(opaque, sid), item(itm) {
    }

    queued_item item;
};

class SystemEventFlatBuffers : public SystemEventProducerMessage {
public:
    SystemEventFlatBuffers(uint32_t opaque,
                           std::string_view key,
                           const queued_item& item,
                           cb::mcbp::DcpStreamId sid);

    std::string_view getKey() const override {
        return {key.data(), key.size()};
    }

    std::string_view getEventData() const override {
        // It's possible that the Item has been given an XATTR value for the
        // storing of the default collection legacy "max DCP seqno". The XATTR
        // data must not be replicated. This function call will obtain the
        // correct view of the data if XATTRs are present.
        return item->getValueViewWithoutXattrs();
    }

    mcbp::systemevent::version getVersion() const override {
        return mcbp::systemevent::version::version2;
    }
    std::string key;
};

class CollectionCreateProducerMessage : public SystemEventProducerMessage {
public:
    CollectionCreateProducerMessage(uint32_t opaque,
                                    const queued_item& itm,
                                    const Collections::CollectionEventData& ev,
                                    cb::mcbp::DcpStreamId sid)
        : SystemEventProducerMessage(opaque, itm, sid),
          key(ev.metaData.name),
          eventData{ev} {
    }

    std::string_view getKey() const override {
        return {key.data(), key.size()};
    }

    std::string_view getEventData() const override {
        return {reinterpret_cast<const char*>(&eventData),
                Collections::CreateEventDcpData::size};
    }

    mcbp::systemevent::version getVersion() const override {
        return mcbp::systemevent::version::version0;
    }

protected:
    bool isEqual(const DcpResponse& rsp) const override;

private:
    std::string key;
    Collections::CreateEventDcpData eventData;
};

class CollectionCreateWithMaxTtlProducerMessage
    : public SystemEventProducerMessage {
public:
    CollectionCreateWithMaxTtlProducerMessage(
            uint32_t opaque,
            const queued_item& itm,
            const Collections::CollectionEventData& ev,
            cb::mcbp::DcpStreamId sid)
        : SystemEventProducerMessage(opaque, itm, sid),
          key(ev.metaData.name),
          eventData{ev} {
    }

    std::string_view getKey() const override {
        return {key.data(), key.size()};
    }

    std::string_view getEventData() const override {
        return {reinterpret_cast<const char*>(&eventData),
                Collections::CreateWithMaxTtlEventDcpData::size};
    }

    mcbp::systemevent::version getVersion() const override {
        return mcbp::systemevent::version::version1;
    }

protected:
    bool isEqual(const DcpResponse& rsp) const override;

private:
    std::string key;
    Collections::CreateWithMaxTtlEventDcpData eventData;
};

class CollectionDropProducerMessage : public SystemEventProducerMessage {
public:
    CollectionDropProducerMessage(uint32_t opaque,
                                  const queued_item& itm,
                                  const Collections::DropEventData& data,
                                  cb::mcbp::DcpStreamId sid)
        : SystemEventProducerMessage(opaque, itm, sid), eventData{data} {
    }

    std::string_view getKey() const override {
        return {/* no key value for a drop event*/};
    }

    std::string_view getEventData() const override {
        return {reinterpret_cast<const char*>(&eventData),
                Collections::DropEventDcpData::size};
    }

    mcbp::systemevent::version getVersion() const override {
        return mcbp::systemevent::version::version0;
    }

protected:
    bool isEqual(const DcpResponse& rsp) const override;

private:
    std::string key;
    Collections::DropEventDcpData eventData;
};

class ScopeCreateProducerMessage : public SystemEventProducerMessage {
public:
    ScopeCreateProducerMessage(uint32_t opaque,
                               const queued_item& itm,
                               const Collections::CreateScopeEventData& data,
                               cb::mcbp::DcpStreamId sid)
        : SystemEventProducerMessage(opaque, itm, sid),
          key(data.metaData.name),
          eventData{data} {
    }

    std::string_view getKey() const override {
        return {key.data(), key.size()};
    }

    std::string_view getEventData() const override {
        return {reinterpret_cast<const char*>(&eventData),
                Collections::CreateScopeEventDcpData::size};
    }

    mcbp::systemevent::version getVersion() const override {
        return mcbp::systemevent::version::version0;
    }

protected:
    bool isEqual(const DcpResponse& rsp) const override;

private:
    std::string key;
    Collections::CreateScopeEventDcpData eventData;
};

class ScopeDropProducerMessage : public SystemEventProducerMessage {
public:
    ScopeDropProducerMessage(uint32_t opaque,
                             const queued_item& itm,
                             const Collections::DropScopeEventData& data,
                             cb::mcbp::DcpStreamId sid)
        : SystemEventProducerMessage(opaque, itm, sid), eventData{data} {
    }

    std::string_view getKey() const override {
        return {/* no key value for a drop event*/};
    }

    std::string_view getEventData() const override {
        return {reinterpret_cast<const char*>(&eventData),
                Collections::DropScopeEventDcpData::size};
    }

    mcbp::systemevent::version getVersion() const override {
        return mcbp::systemevent::version::version0;
    }

protected:
    bool isEqual(const DcpResponse& rsp) const override;

private:
    std::string key;
    Collections::DropScopeEventDcpData eventData;
};

/**
 * CollectionsEvent provides a shim on top of SystemEventMessage for
 * when a SystemEvent is a Collection's SystemEvent.
 */
class CollectionsEvent {
public:
    int64_t getBySeqno() const {
        return *event.getBySeqno();
    }

protected:
    /**
     * @throws invalid_argument if the event data is not expectedSize
     */
    CollectionsEvent(const SystemEventMessage& e, size_t expectedSize)
        : event(e) {
        if (event.getEventData().size() != expectedSize) {
            throw std::invalid_argument(
                    "CollectionsEvent::CollectionsEvent invalid size "
                    "expectedSize:" +
                    std::to_string(expectedSize) + ", size:" +
                    std::to_string(event.getEventData().size()));
        }
    }

    const SystemEventMessage& event;
};

class CreateCollectionEvent : public CollectionsEvent {
public:
    explicit CreateCollectionEvent(const SystemEventMessage& e)
        : CollectionsEvent(
                  e,
                  e.getVersion() == mcbp::systemevent::version::version0
                          ? Collections::CreateEventDcpData::size
                          : Collections::CreateWithMaxTtlEventDcpData::size) {
    }

    std::string_view getKey() const {
        return event.getKey();
    }

    /**
     * @return the CollectionID of the event
     */
    CollectionID getCollectionID() const {
        const auto* dcpData =
                reinterpret_cast<const Collections::CreateEventDcpData*>(
                        event.getEventData().data());
        return dcpData->cid.to_host();
    }

    ScopeID getScopeID() const {
        const auto* dcpData =
                reinterpret_cast<const Collections::CreateEventDcpData*>(
                        event.getEventData().data());
        return dcpData->sid.to_host();
    }

    /**
     * @return manifest uid which triggered the create
     */
    Collections::ManifestUid getManifestUid() const {
        const auto* dcpData =
                reinterpret_cast<const Collections::CreateEventDcpData*>(
                        event.getEventData().data());
        return dcpData->manifestUid.to_host();
    }

    /**
     * @return maxTTL of collection
     */
    cb::ExpiryLimit getMaxTtl() const {
        if (event.getVersion() == mcbp::systemevent::version::version0) {
            return {};
        } else {
            const auto* dcpData = reinterpret_cast<
                    const Collections::CreateWithMaxTtlEventDcpData*>(
                    event.getEventData().data());
            return std::chrono::seconds(ntohl(dcpData->maxTtl));
        }
    }
};

class DropCollectionEvent : public CollectionsEvent {
public:
    explicit DropCollectionEvent(const SystemEventMessage& e)
        : CollectionsEvent(e, Collections::DropEventDcpData::size) {
    }

    /**
     * @return manifest uid which triggered the drop
     */
    Collections::ManifestUid getManifestUid() const {
        const auto* dcpData =
                reinterpret_cast<const Collections::DropEventDcpData*>(
                        event.getEventData().data());
        return dcpData->manifestUid.to_host();
    }

    /**
     * @return the CollectionID of the event
     */
    CollectionID getCollectionID() const {
        const auto* dcpData =
                reinterpret_cast<const Collections::DropEventDcpData*>(
                        event.getEventData().data());
        return dcpData->cid.to_host();
    }
};

class CreateScopeEvent : public CollectionsEvent {
public:
    explicit CreateScopeEvent(const SystemEventMessage& e)
        : CollectionsEvent(e, Collections::CreateScopeEventDcpData::size) {
    }

    std::string_view getKey() const {
        return event.getKey();
    }

    ScopeID getScopeID() const {
        const auto* dcpData =
                reinterpret_cast<const Collections::CreateScopeEventDcpData*>(
                        event.getEventData().data());
        return dcpData->sid.to_host();
    }

    /**
     * @return manifest uid which triggered the create
     */
    Collections::ManifestUid getManifestUid() const {
        const auto* dcpData =
                reinterpret_cast<const Collections::CreateScopeEventDcpData*>(
                        event.getEventData().data());
        return dcpData->manifestUid.to_host();
    }
};

class DropScopeEvent : public CollectionsEvent {
public:
    explicit DropScopeEvent(const SystemEventMessage& e)
        : CollectionsEvent(e, Collections::DropScopeEventDcpData::size) {
    }

    /**
     * @return manifest uid which triggered the drop
     */
    Collections::ManifestUid getManifestUid() const {
        const auto* dcpData =
                reinterpret_cast<const Collections::DropScopeEventDcpData*>(
                        event.getEventData().data());
        return dcpData->manifestUid.to_host();
    }

    ScopeID getScopeID() const {
        const auto* dcpData =
                reinterpret_cast<const Collections::DropScopeEventDcpData*>(
                        event.getEventData().data());
        return dcpData->sid.to_host();
    }
};

class OSOSnapshot : public DcpResponse {
public:
    OSOSnapshot(uint32_t opaque, Vbid vbucket, cb::mcbp::DcpStreamId sid)
        : DcpResponse(Event::OSOSnapshot, opaque, sid),
          vbucket(vbucket),
          start{true} {
    }

    struct End {};
    OSOSnapshot(uint32_t opaque, Vbid vbucket, cb::mcbp::DcpStreamId sid, End)
        : DcpResponse(Event::OSOSnapshot, opaque, sid),
          vbucket(vbucket),
          start{false} {
    }

    ~OSOSnapshot() override {
    }

    Vbid getVBucket() const {
        return vbucket;
    }

    bool isStart() const {
        return start;
    }

    bool isEnd() const {
        return !start;
    }

    size_t getMessageSize() const override {
        return baseFlowControlSize +
               (getStreamId() ? sizeof(cb::mcbp::DcpStreamIdFrameInfo) : 0);
    }

protected:
    bool isEqual(const DcpResponse& rsp) const override;

private:
    static constexpr uint32_t baseFlowControlSize{28};
    Vbid vbucket;
    bool start;
};

class SeqnoAdvanced : public DcpResponse {
public:
    SeqnoAdvanced(uint32_t opaque,
                  Vbid vbucket,
                  cb::mcbp::DcpStreamId sid,
                  uint64_t seqno)
        : DcpResponse(
                  Event::SeqnoAdvanced, opaque, sid, getFlowControlSize(sid)),
          vbucket(vbucket),
          advancedSeqno(seqno) {
    }

    ~SeqnoAdvanced() override = default;

    [[nodiscard]] Vbid getVBucket() const {
        return vbucket;
    }

    [[nodiscard]] OptionalSeqno getBySeqno() const override {
        return {advancedSeqno};
    }

protected:
    bool isEqual(const DcpResponse& rsp) const override;

    static uint32_t getFlowControlSize(const cb::mcbp::DcpStreamId sid) {
        return baseFlowControlSize +
               (sid ? sizeof(cb::mcbp::DcpStreamIdFrameInfo) : 0);
    }

private:
    static constexpr uint32_t baseFlowControlSize{32};
    Vbid vbucket;
    uint64_t advancedSeqno;
};
