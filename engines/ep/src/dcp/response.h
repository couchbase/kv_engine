/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013 Couchbase, Inc
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

#ifndef SRC_DCP_RESPONSE_H_
#define SRC_DCP_RESPONSE_H_ 1

#include "config.h"

#include "collections/collections_types.h"
#include "dcp/dcp-types.h"
#include "ep_types.h"
#include "ext_meta_parser.h"
#include "item.h"
#include "systemevent.h"

#include <memcached/protocol_binary.h>
#include <memory>

class DcpResponse {
public:
    enum class Event : uint8_t {
        Mutation,
        Deletion,
        Expiration,
        SetVbucket,
        StreamReq,
        StreamEnd,
        SnapshotMarker,
        AddStream,
        SystemEvent
    };

    DcpResponse(Event event, uint32_t opaque)
        : opaque_(opaque), event_(event) {}

    virtual ~DcpResponse() {}

    uint32_t getOpaque() {
        return opaque_;
    }

    Event getEvent() const {
        return event_;
    }

    /**
     * Not all DcpResponse sub-classes have a seqno. MutationResponse (events
     * Mutation, Deletion and Expiration) and SystemEventMessage (SystemEvent)
     * have a seqno and would return an OptionalSeqno with a seqno.
     *
     * @return OptionalSeqno with no value - certain sub-classes may have a
     *         seqno.
     */
    virtual OptionalSeqno getBySeqno() const {
        return OptionalSeqno{/*no-seqno*/};
    }

    /* Returns true if this response is a meta event (i.e. not an operation on
     * an actual user document.
     */
    bool isMetaEvent() const {
        switch (event_) {
        case Event::Mutation:
        case Event::Deletion:
        case Event::Expiration:
            return false;

        case Event::SetVbucket:
        case Event::StreamReq:
        case Event::StreamEnd:
        case Event::SnapshotMarker:
        case Event::AddStream:
        case Event::SystemEvent:
            return true;
        }
        throw std::invalid_argument(
                "DcpResponse::isMetaEvent: Invalid event_ " +
                std::to_string(int(event_)));
    }

    /**
     * Returns if the response is a system event
     */
    bool isSystemEvent() const {
        return (event_ == Event::SystemEvent);
    }

    virtual uint32_t getMessageSize() const = 0;

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

private:
    uint32_t opaque_;
    Event event_;
};

std::ostream& operator<<(std::ostream& os, const DcpResponse& r);

class StreamRequest : public DcpResponse {
public:
    StreamRequest(Vbid vbucket,
                  uint32_t opaque,
                  uint32_t flags,
                  uint64_t startSeqno,
                  uint64_t endSeqno,
                  uint64_t vbucketUUID,
                  uint64_t snapStartSeqno,
                  uint64_t snapEndSeqno)
        : DcpResponse(Event::StreamReq, opaque),
          startSeqno_(startSeqno),
          endSeqno_(endSeqno),
          vbucketUUID_(vbucketUUID),
          snapStartSeqno_(snapStartSeqno),
          snapEndSeqno_(snapEndSeqno),
          flags_(flags),
          vbucket_(vbucket) {
    }

    ~StreamRequest() {}

    Vbid getVBucket() {
        return vbucket_;
    }

    uint32_t getFlags() {
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

    uint32_t getMessageSize() const {
        return baseMsgBytes;
    }

    static const uint32_t baseMsgBytes;

private:
    uint64_t startSeqno_;
    uint64_t endSeqno_;
    uint64_t vbucketUUID_;
    uint64_t snapStartSeqno_;
    uint64_t snapEndSeqno_;
    uint32_t flags_;
    Vbid vbucket_;
};

class AddStreamResponse : public DcpResponse {
public:
    AddStreamResponse(uint32_t opaque,
                      uint32_t streamOpaque,
                      cb::mcbp::Status status)
        : DcpResponse(Event::AddStream, opaque),
          streamOpaque_(streamOpaque),
          status_(status) {
    }

    ~AddStreamResponse() = default;

    uint32_t getStreamOpaque() {
        return streamOpaque_;
    }

    cb::mcbp::Status getStatus() const {
        return status_;
    }

    uint32_t getMessageSize() const {
        return baseMsgBytes;
    }

    static const uint32_t baseMsgBytes;

private:
    const uint32_t streamOpaque_;
    const cb::mcbp::Status status_;
};

class SnapshotMarkerResponse : public DcpResponse {
public:
    SnapshotMarkerResponse(uint32_t opaque, cb::mcbp::Status status)
        : DcpResponse(Event::SnapshotMarker, opaque), status_(status) {
    }

    cb::mcbp::Status getStatus() const {
        return status_;
    }

    uint32_t getMessageSize() const {
        return baseMsgBytes;
    }

    static const uint32_t baseMsgBytes;

private:
    const cb::mcbp::Status status_;
};

class SetVBucketStateResponse : public DcpResponse {
public:
    SetVBucketStateResponse(uint32_t opaque, cb::mcbp::Status status)
        : DcpResponse(Event::SetVbucket, opaque), status_(status) {
    }

    cb::mcbp::Status getStatus() const {
        return status_;
    }

    uint32_t getMessageSize() const {
        return baseMsgBytes;
    }

    static const uint32_t baseMsgBytes;

private:
    const cb::mcbp::Status status_;
};

class StreamEndResponse : public DcpResponse {
public:
    StreamEndResponse(uint32_t opaque, end_stream_status_t flags, Vbid vbucket)
        : DcpResponse(Event::StreamEnd, opaque),
          flags_(statusToFlags(flags)),
          vbucket_(vbucket) {
    }

    static end_stream_status_t statusToFlags(end_stream_status_t status) {
        if (status == END_STREAM_ROLLBACK) {
            return END_STREAM_STATE;
        }
        return status;
    }

    end_stream_status_t getFlags() const {
        return flags_;
    }

    Vbid getVbucket() const {
        return vbucket_;
    }

    uint32_t getMessageSize() const {
        return baseMsgBytes;
    }

    static const uint32_t baseMsgBytes;

private:
    end_stream_status_t flags_;
    Vbid vbucket_;
};

class SetVBucketState : public DcpResponse {
public:
    SetVBucketState(uint32_t opaque, Vbid vbucket, vbucket_state_t state)
        : DcpResponse(Event::SetVbucket, opaque),
          vbucket_(vbucket),
          state_(state) {
    }

    Vbid getVBucket() {
        return vbucket_;
    }

    vbucket_state_t getState() {
        return state_;
    }

    uint32_t getMessageSize() const {
        return baseMsgBytes;
    }

    static const uint32_t baseMsgBytes;

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
                   uint32_t flags)
        : DcpResponse(Event::SnapshotMarker, opaque),
          vbucket_(vbucket),
          start_seqno_(start_seqno),
          end_seqno_(end_seqno),
          flags_(flags) {
    }

    Vbid getVBucket() {
        return vbucket_;
    }

    uint64_t getStartSeqno() {
        return start_seqno_;
    }

    uint64_t getEndSeqno() {
        return end_seqno_;
    }

    uint32_t getFlags() {
        return flags_;
    }

    uint32_t getMessageSize() const {
        return baseMsgBytes;
    }

    static const uint32_t baseMsgBytes;

private:
    Vbid vbucket_;
    uint64_t start_seqno_;
    uint64_t end_seqno_;
    uint32_t flags_;
};

class MutationResponse : public DcpResponse {
public:
    /**
     * Construct a MutationResponse which is used to represent an outgoing or
     * incoming DCP mutation/deletion (stored in an Item)
     * @param item The Item (via shared pointer) the object represents
     * @param opaque DCP opaque value
     * @param includeVal Does the DCP message contain a value
     * @param includeXattrs Does the DCP message contain xattrs
     * @param includeCollectionID If the incoming/outgoing key should contain
     *        the Collection-ID.
     */
    MutationResponse(queued_item item,
                     uint32_t opaque,
                     IncludeValue includeVal,
                     IncludeXattrs includeXattrs,
                     IncludeDeleteTime includeDeleteTime,
                     DocKeyEncodesCollectionId includeCollectionID)
        : DcpResponse(item->isDeleted()
                              ? ((item->deletionSource() == DeleteSource::TTL)
                                           ? Event::Expiration
                                           : Event::Deletion)
                              : Event::Mutation,
                      opaque),
          item_(std::move(item)),
          includeValue(includeVal),
          includeXattributes(includeXattrs),
          includeDeleteTime(includeDeleteTime),
          includeCollectionID(includeCollectionID) {
    }

    queued_item& getItem() {
        return item_;
    }

    std::unique_ptr<Item> getItemCopy() {
        return std::make_unique<Item>(*item_);
    }

    Vbid getVBucket() {
        return item_->getVBucketId();
    }

    /**
     * @return OptionalSeqno with the underlying Item's seqno.
     */
    OptionalSeqno getBySeqno() const {
        return OptionalSeqno{item_->getBySeqno()};
    }

    uint64_t getRevSeqno() {
        return item_->getRevSeqno();
    }

    /**
      * @return size of message to be sent over the wire to the consumer.
      */
    uint32_t getMessageSize() const;

    /**
     * @returns a size representing approximately the memory used, in this case
     * the item's size.
     */
    size_t getApproximateSize() const {
        return item_->size();
    }

    IncludeValue getIncludeValue() const {
        return includeValue;
    }
    IncludeXattrs getIncludeXattrs() const {
        return includeXattributes;
    }
    IncludeDeleteTime getIncludeDeleteTime() const {
        return includeDeleteTime;
    }
    DocKeyEncodesCollectionId getDocKeyEncodesCollectionId() const {
        return includeCollectionID;
    }

    static const uint32_t mutationBaseMsgBytes = 55;
    static const uint32_t deletionBaseMsgBytes = 42;
    static const uint32_t deletionV2BaseMsgBytes = 45;

protected:
    uint32_t getDeleteLength() const;

    queued_item item_;

    // Whether the response should contain the value
    IncludeValue includeValue;
    // Whether the response should contain the xattributes, if they exist.
    IncludeXattrs includeXattributes;
    // Whether the response should include delete-time (when a delete)
    IncludeDeleteTime includeDeleteTime;
    // Whether the response includes the collection-ID
    DocKeyEncodesCollectionId includeCollectionID;
};

/**
 * MutationConsumerMessage is a class for storing a mutation that has been
 * sent to a DcpConsumer/PassiveStream.
 *
 * The class differs from the DcpProducer version (MutationResponse) in that
 * it can optionally store 'ExtendedMetaData'
 *
 */
class MutationConsumerMessage : public MutationResponse {
public:
    MutationConsumerMessage(queued_item item,
                            uint32_t opaque,
                            IncludeValue includeVal,
                            IncludeXattrs includeXattrs,
                            IncludeDeleteTime includeDeleteTime,
                            DocKeyEncodesCollectionId includeCollectionID,
                            ExtendedMetaData* e)
        : MutationResponse(item,
                           opaque,
                           includeVal,
                           includeXattrs,
                           includeDeleteTime,
                           includeCollectionID),
          emd(e) {
    }

    // Used in test code for wiring a producer/consumer directly
    MutationConsumerMessage(MutationResponse& response)
        : MutationResponse(response.getItem(),
                           response.getOpaque(),
                           response.getIncludeValue(),
                           response.getIncludeXattrs(),
                           response.getIncludeDeleteTime(),
                           response.getDocKeyEncodesCollectionId()),
          emd(nullptr) {
    }

    /**
     * @return size of message on the wire
     */
    uint32_t getMessageSize() const;

    ExtendedMetaData* getExtMetaData() {
        return emd.get();
    }

protected:
    std::unique_ptr<ExtendedMetaData> emd;
};

/**
 * SystemEventMessage defines the interface required by consumer and producer
 * message classes.
 */
class SystemEventMessage : public DcpResponse {
public:
    SystemEventMessage(uint32_t opaque)
        : DcpResponse(Event::SystemEvent, opaque) {
    }
    // baseMsgBytes is the unpadded size of the
    // protocol_binary_request_dcp_system_event::body struct
    static const uint32_t baseMsgBytes =
            sizeof(protocol_binary_request_header) + sizeof(uint64_t) +
            sizeof(uint32_t) + sizeof(uint8_t);
    virtual mcbp::systemevent::id getSystemEvent() const = 0;
    virtual cb::const_char_buffer getKey() const = 0;
    virtual cb::const_byte_buffer getEventData() const = 0;
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
        : SystemEventMessage(opaque),
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

    uint32_t getMessageSize() const override {
        return SystemEventMessage::baseMsgBytes + key.size() + eventData.size();
    }

    mcbp::systemevent::id getSystemEvent() const override {
        return event;
    }

    OptionalSeqno getBySeqno() const override {
        return OptionalSeqno{bySeqno};
    }

    cb::const_char_buffer getKey() const override {
        return key;
    }

    cb::const_byte_buffer getEventData() const override {
        return {eventData.data(), eventData.size()};
    }

    mcbp::systemevent::version getVersion() const override {
        return version;
    }

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
class SystemEventProducerMessage : public SystemEventMessage {
public:
    /**
     * Note: the body of this factory method is in systemevent.cc along side
     *       related code which decides how SystemEvents are managed.
     *
     * Note: creation of the SystemEventProducerMessage will up the ref-count
     * of item
     *
     * @return a SystemEventMessage unique pointer constructed from the
     *         queued_item data.
     */
    static std::unique_ptr<SystemEventProducerMessage> make(
            uint32_t opaque, const queued_item& item);

    uint32_t getMessageSize() const override {
        return SystemEventMessage::baseMsgBytes + getKey().size() +
               getEventData().size();
    }

    mcbp::systemevent::id getSystemEvent() const override {
        mcbp::systemevent::id rv;
        // Map a deleted Collection to be an explicit BeginDelete event
        if (SystemEvent(item->getFlags()) == SystemEvent::Collection) {
            rv = item->isDeleted() ? mcbp::systemevent::id::DeleteCollection
                                   : mcbp::systemevent::id::CreateCollection;
        } else if (SystemEvent(item->getFlags()) == SystemEvent::Scope) {
            rv = item->isDeleted() ? mcbp::systemevent::id::DropScope
                                   : mcbp::systemevent::id::CreateScope;
        } else {
            throw std::logic_error("getSystemEvent incorrect event:" +
                                   std::to_string(item->getFlags()));
        }
        return rv;
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
    SystemEventProducerMessage(uint32_t opaque, const queued_item& itm)
        : SystemEventMessage(opaque), item(itm) {
    }

    queued_item item;
};

class CollectionCreateProducerMessage : public SystemEventProducerMessage {
public:
    CollectionCreateProducerMessage(uint32_t opaque,
                                    const queued_item& itm,
                                    const Collections::CreateEventData& data)
        : SystemEventProducerMessage(opaque, itm),
          key(data.name),
          eventData{data} {
    }

    cb::const_char_buffer getKey() const override {
        return {key.data(), key.size()};
    }

    cb::const_byte_buffer getEventData() const override {
        return {reinterpret_cast<const uint8_t*>(&eventData),
                Collections::CreateEventDcpData::size};
    }

    mcbp::systemevent::version getVersion() const override {
        return mcbp::systemevent::version::version0;
    }

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
            const Collections::CreateEventData& data)
        : SystemEventProducerMessage(opaque, itm),
          key(data.name),
          eventData{data} {
    }

    cb::const_char_buffer getKey() const override {
        return {key.data(), key.size()};
    }

    cb::const_byte_buffer getEventData() const override {
        return {reinterpret_cast<const uint8_t*>(&eventData),
                Collections::CreateWithMaxTtlEventDcpData::size};
    }

    mcbp::systemevent::version getVersion() const override {
        return mcbp::systemevent::version::version1;
    }

private:
    std::string key;
    Collections::CreateWithMaxTtlEventDcpData eventData;
};

class CollectionDropProducerMessage : public SystemEventProducerMessage {
public:
    CollectionDropProducerMessage(uint32_t opaque,
                                  const queued_item& itm,
                                  const Collections::DropEventData& data)
        : SystemEventProducerMessage(opaque, itm), eventData{data} {
    }

    cb::const_char_buffer getKey() const override {
        return {/* no key value for a drop event*/};
    }

    cb::const_byte_buffer getEventData() const override {
        return {reinterpret_cast<const uint8_t*>(&eventData),
                Collections::DropEventDcpData::size};
    }

    mcbp::systemevent::version getVersion() const override {
        return mcbp::systemevent::version::version0;
    }

private:
    std::string key;
    Collections::DropEventDcpData eventData;
};

class ScopeCreateProducerMessage : public SystemEventProducerMessage {
public:
    ScopeCreateProducerMessage(uint32_t opaque,
                               const queued_item& itm,
                               const Collections::CreateScopeEventData& data)
        : SystemEventProducerMessage(opaque, itm),
          key(data.name),
          eventData{data} {
    }

    cb::const_char_buffer getKey() const override {
        return {key.data(), key.size()};
    }

    cb::const_byte_buffer getEventData() const override {
        return {reinterpret_cast<const uint8_t*>(&eventData),
                Collections::CreateScopeEventDcpData::size};
    }

    mcbp::systemevent::version getVersion() const override {
        return mcbp::systemevent::version::version0;
    }

private:
    std::string key;
    Collections::CreateScopeEventDcpData eventData;
};

class ScopeDropProducerMessage : public SystemEventProducerMessage {
public:
    ScopeDropProducerMessage(uint32_t opaque,
                             const queued_item& itm,
                             const Collections::DropScopeEventData& data)
        : SystemEventProducerMessage(opaque, itm), eventData{data} {
    }

    cb::const_char_buffer getKey() const override {
        return {/* no key value for a drop event*/};
    }

    cb::const_byte_buffer getEventData() const override {
        return {reinterpret_cast<const uint8_t*>(&eventData),
                Collections::DropScopeEventDcpData::size};
    }

    mcbp::systemevent::version getVersion() const override {
        return mcbp::systemevent::version::version0;
    }

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
    CreateCollectionEvent(const SystemEventMessage& e)
        : CollectionsEvent(
                  e,
                  e.getVersion() == mcbp::systemevent::version::version0
                          ? Collections::CreateEventDcpData::size
                          : Collections::CreateWithMaxTtlEventDcpData::size) {
    }

    cb::const_char_buffer getKey() const {
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
     * @return max_ttl of collection
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
    DropCollectionEvent(const SystemEventMessage& e)
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
    CreateScopeEvent(const SystemEventMessage& e)
        : CollectionsEvent(e, Collections::CreateScopeEventDcpData::size) {
    }

    cb::const_char_buffer getKey() const {
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
    DropScopeEvent(const SystemEventMessage& e)
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

#endif  // SRC_DCP_RESPONSE_H_
