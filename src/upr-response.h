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

#ifndef SRC_UPR_RESPONSE_H_
#define SRC_UPR_RESPONSE_H_ 1

#include "config.h"

#include "item.h"

typedef enum {
    UPR_MUTATION,
    UPR_DELETION,
    UPR_EXPIRATION,
    UPR_FLUSH,
    UPR_SET_VBUCKET,
    UPR_STREAM_REQ,
    UPR_STREAM_END,
    UPR_SNAPSHOT_MARKER,
    UPR_ADD_STREAM
} upr_event_t;


typedef enum {
    MARKER_FLAG_MEMORY,
    MARKER_FLAG_DISK
} upr_marker_flag_t;


class UprResponse {
public:
    UprResponse(upr_event_t event, uint32_t opaque)
        : opaque_(opaque), event_(event) {}

    virtual ~UprResponse() {}

    uint32_t getOpaque() {
        return opaque_;
    }

    upr_event_t getEvent() {
        return event_;
    }

    virtual uint32_t getMessageSize() = 0;

private:
    uint32_t opaque_;
    upr_event_t event_;
};

class StreamRequest : public UprResponse {
public:
    StreamRequest(uint16_t vbucket, uint32_t opaque, uint32_t flags,
                  uint64_t startSeqno, uint64_t endSeqno, uint64_t vbucketUUID,
                  uint64_t snapStartSeqno, uint64_t snapEndSeqno)
        : UprResponse(UPR_STREAM_REQ, opaque), startSeqno_(startSeqno),
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

class AddStreamResponse : public UprResponse {
public:
    AddStreamResponse(uint32_t opaque, uint32_t streamOpaque, uint16_t status)
        : UprResponse(UPR_ADD_STREAM, opaque), streamOpaque_(streamOpaque),
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

class SetVBucketStateResponse : public UprResponse {
public:
    SetVBucketStateResponse(uint32_t opaque, uint16_t status)
        : UprResponse(UPR_SET_VBUCKET, opaque), status_(status) {}

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

class StreamEndResponse : public UprResponse {
public:
    StreamEndResponse(uint32_t opaque, uint32_t flags, uint16_t vbucket)
        : UprResponse(UPR_STREAM_END, opaque), flags_(flags),
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

class SetVBucketState : public UprResponse {
public:
    SetVBucketState(uint32_t opaque, uint16_t vbucket, vbucket_state_t state)
        : UprResponse(UPR_SET_VBUCKET, opaque), vbucket_(vbucket),
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

class SnapshotMarker : public UprResponse {
public:
    SnapshotMarker(uint32_t opaque, uint16_t vbucket, uint64_t start_seqno,
                   uint64_t end_seqno, uint32_t flags)
        : UprResponse(UPR_SNAPSHOT_MARKER, opaque), vbucket_(vbucket),
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

class MutationResponse : public UprResponse {
public:
    MutationResponse(Item* item, uint32_t opaque)
        : UprResponse(item->isDeleted() ? UPR_DELETION : UPR_MUTATION, opaque),
          item_(item) {}

    Item* getItem() {
        return item_;
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
        uint32_t base = item_->isDeleted() ? mutationBaseMsgBytes :
                                             deletionBaseMsgBytes;
        uint32_t body = item_->getNKey() + item_->getNBytes();
        return base + body;
    }

    static const uint32_t mutationBaseMsgBytes;
    static const uint32_t deletionBaseMsgBytes;

private:
    Item* item_;
};

#endif  // SRC_UPR_RESPONSE_H_
