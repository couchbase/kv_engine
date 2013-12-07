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

typedef enum { UPR_MUTATION = 101,
               UPR_DELETION,
               UPR_EXPIRATION,
               UPR_FLUSH,
               UPR_VBUCKET_SET,
               UPR_DISCONNECT,
               UPR_STREAM_REQ,
               UPR_STREAM_END,
               UPR_SNAPSHOT_MARKER,
               UPR_ADD_STREAM
} upr_event_t;

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

private:
    uint32_t opaque_;
    upr_event_t event_;
};

class StreamRequest : public UprResponse {
public:
    StreamRequest(uint16_t vbucket, uint32_t opaque, uint32_t flags,
                  uint64_t startSeqno, uint64_t endSeqno, uint64_t vbucketUUID,
                  uint64_t highSeqno)
        : UprResponse(UPR_STREAM_REQ, opaque), startSeqno_(startSeqno),
          endSeqno_(endSeqno), vbucketUUID_(vbucketUUID), highSeqno_(highSeqno),
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

    uint64_t getHighSeqno() {
        return highSeqno_;
    }

private:
    uint64_t startSeqno_;
    uint64_t endSeqno_;
    uint64_t vbucketUUID_;
    uint64_t highSeqno_;
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

private:
    uint32_t streamOpaque_;
    uint16_t status_;
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

private:
    uint32_t flags_;
    uint16_t vbucket_;
};

class SnapshotMarker : public UprResponse {
public:
    SnapshotMarker(uint32_t opaque, uint16_t vbucket)
        : UprResponse(UPR_SNAPSHOT_MARKER, opaque), vbucket_(vbucket) {}

    uint32_t getVBucket() {
        return vbucket_;
    }

private:
    uint16_t vbucket_;
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

private:
    Item* item_;
};

#endif  // SRC_UPR_RESPONSE_H_
