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

#include "ext_meta_parser.h"
#include "item.h"

enum dcp_event_t {
    DCP_MUTATION,
    DCP_DELETION,
    DCP_EXPIRATION,
    DCP_FLUSH,
    DCP_SET_VBUCKET,
    DCP_STREAM_REQ,
    DCP_STREAM_END,
    DCP_SNAPSHOT_MARKER,
    DCP_ADD_STREAM
};


enum dcp_marker_flag_t {
    MARKER_FLAG_MEMORY = 0x01,
    MARKER_FLAG_DISK   = 0x02,
    MARKER_FLAG_CHK    = 0x04,
    MARKER_FLAG_ACK    = 0x08
};

class DcpResponse {
public:
    DcpResponse(dcp_event_t event, uint32_t opaque)
        : opaque_(opaque), event_(event) {}

    virtual ~DcpResponse() {}

    uint32_t getOpaque() {
        return opaque_;
    }

    dcp_event_t getEvent() {
        return event_;
    }

    /* Returns true if this response is a meta event (i.e. not an operation on
     * an actual user document.
     */
    bool isMetaEvent() const {
        switch (event_) {
        case DCP_MUTATION:
        case DCP_DELETION:
        case DCP_EXPIRATION:
        case DCP_FLUSH:
            return false;

        case DCP_SET_VBUCKET:
        case DCP_STREAM_REQ:
        case DCP_STREAM_END:
        case DCP_SNAPSHOT_MARKER:
        case DCP_ADD_STREAM:
            return true;

        default:
            throw std::invalid_argument(
                    "DcpResponse::isMetaEvent: Invalid event_ " +
                    std::to_string(event_));
        }
    }

    virtual uint32_t getMessageSize() = 0;

private:
    uint32_t opaque_;
    dcp_event_t event_;
};

class StreamRequest : public DcpResponse {
public:
    StreamRequest(uint16_t vbucket, uint32_t opaque, uint32_t flags,
                  uint64_t startSeqno, uint64_t endSeqno, uint64_t vbucketUUID,
                  uint64_t snapStartSeqno, uint64_t snapEndSeqno)
        : DcpResponse(DCP_STREAM_REQ, opaque), startSeqno_(startSeqno),
          endSeqno_(endSeqno), vbucketUUID_(vbucketUUID),
          snapStartSeqno_(snapStartSeqno), snapEndSeqno_(snapEndSeqno),
          flags_(flags), vbucket_(vbucket) {}

    ~StreamRequest() {}

    uint16_t getVBucket() {
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

    uint32_t getMessageSize() {
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
    uint16_t vbucket_;
};

class AddStreamResponse : public DcpResponse {
public:
    AddStreamResponse(uint32_t opaque, uint32_t streamOpaque, uint16_t status)
        : DcpResponse(DCP_ADD_STREAM, opaque), streamOpaque_(streamOpaque),
          status_(status) {}

    ~AddStreamResponse() {}

    uint32_t getStreamOpaque() {
        return streamOpaque_;
    }

    uint16_t getStatus() {
        return status_;
    }

    uint32_t getMessageSize() {
        return baseMsgBytes;
    }

    static const uint32_t baseMsgBytes;

private:
    uint32_t streamOpaque_;
    uint16_t status_;
};

class SnapshotMarkerResponse : public DcpResponse {
public:
    SnapshotMarkerResponse(uint32_t opaque, uint16_t status)
        : DcpResponse(DCP_SNAPSHOT_MARKER, opaque), status_(status) {}

    uint16_t getStatus() {
        return status_;
    }

    uint32_t getMessageSize() {
        return baseMsgBytes;
    }

    static const uint32_t baseMsgBytes;

private:
    uint32_t status_;
};

class SetVBucketStateResponse : public DcpResponse {
public:
    SetVBucketStateResponse(uint32_t opaque, uint16_t status)
        : DcpResponse(DCP_SET_VBUCKET, opaque), status_(status) {}

    uint16_t getStatus() {
        return status_;
    }

    uint32_t getMessageSize() {
        return baseMsgBytes;
    }

    static const uint32_t baseMsgBytes;

private:
    uint32_t status_;
};

class StreamEndResponse : public DcpResponse {
public:
    StreamEndResponse(uint32_t opaque, uint32_t flags, uint16_t vbucket)
        : DcpResponse(DCP_STREAM_END, opaque), flags_(flags),
          vbucket_(vbucket) {}

    uint16_t getFlags() {
        return flags_;
    }

    uint32_t getVbucket() {
        return vbucket_;
    }

    uint32_t getMessageSize() {
        return baseMsgBytes;
    }

    static const uint32_t baseMsgBytes;

private:
    uint32_t flags_;
    uint16_t vbucket_;
};

class SetVBucketState : public DcpResponse {
public:
    SetVBucketState(uint32_t opaque, uint16_t vbucket, vbucket_state_t state)
        : DcpResponse(DCP_SET_VBUCKET, opaque), vbucket_(vbucket),
          state_(state) {}

    uint16_t getVBucket() {
        return vbucket_;
    }

    vbucket_state_t getState() {
        return state_;
    }

    uint32_t getMessageSize() {
        return baseMsgBytes;
    }

    static const uint32_t baseMsgBytes;

private:
    uint16_t vbucket_;
    vbucket_state_t state_;
};

class SnapshotMarker : public DcpResponse {
public:
    SnapshotMarker(uint32_t opaque, uint16_t vbucket, uint64_t start_seqno,
                   uint64_t end_seqno, uint32_t flags)
        : DcpResponse(DCP_SNAPSHOT_MARKER, opaque), vbucket_(vbucket),
          start_seqno_(start_seqno), end_seqno_(end_seqno), flags_(flags) {}

    uint32_t getVBucket() {
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

    uint32_t getMessageSize() {
        return baseMsgBytes;
    }

    static const uint32_t baseMsgBytes;

private:
    uint16_t vbucket_;
    uint64_t start_seqno_;
    uint64_t end_seqno_;
    uint32_t flags_;
};

class MutationResponse : public DcpResponse {
public:
    MutationResponse(queued_item item, uint32_t opaque,
                     ExtendedMetaData *e = NULL)
        : DcpResponse(item->isDeleted() ? DCP_DELETION : DCP_MUTATION, opaque),
          item_(item), emd(e) {}

    ~MutationResponse() {
        if (emd) {
            delete emd;
        }
    }

    queued_item& getItem() {
        return item_;
    }

    Item* getItemCopy() {
        return new Item(*item_);
    }

    uint16_t getVBucket() {
        return item_->getVBucketId();
    }

    uint64_t getBySeqno() {
        return item_->getBySeqno();
    }

    uint64_t getRevSeqno() {
        return item_->getRevSeqno();
    }

    uint32_t getMessageSize() {
        const uint32_t base = item_->isDeleted() ? deletionBaseMsgBytes :
                        mutationBaseMsgBytes;
        uint32_t body = item_->getKey().size() + item_->getNBytes();

        if (emd) {
            body += emd->getExtMeta().second;
        }
        return base + body;
    }

    ExtendedMetaData* getExtMetaData() {
        return emd;
    }

    static const uint32_t mutationBaseMsgBytes;
    static const uint32_t deletionBaseMsgBytes;

private:
    queued_item item_;
    ExtendedMetaData *emd;
};

#endif  // SRC_DCP_RESPONSE_H_
