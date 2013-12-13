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

#include "config.h"

#include "ep_engine.h"
#include "upr-stream.h"

bool UprConsumer::processCheckpointCommand(uint8_t event, uint16_t vbucket,
                                           uint64_t checkpointId) {
    // Not Implemented
    return false;
}

UprResponse* UprConsumer::peekNextItem() {
    if (!readyQ.empty()) {
        return readyQ.front();
    }
    return NULL;
}

void UprConsumer::popNextItem() {
    if (!readyQ.empty()) {
        UprResponse* op = readyQ.front();
        delete op;
        readyQ.pop();
    }
}

ENGINE_ERROR_CODE UprConsumer::addPendingStream(uint16_t vbucket,
                                                uint32_t opaque,
                                                uint32_t flags) {
    LockHolder lh(streamMutex);
    RCPtr<VBucket> vb = engine_.getVBucket(vbucket);
    if (!vb) {
        return ENGINE_NOT_MY_VBUCKET;
    }

    uint64_t start_seqno = vb->getHighSeqno();
    uint64_t end_seqno = std::numeric_limits<uint64_t>::max();
    uint64_t vbucket_uuid = 0;
    uint64_t high_seqno = start_seqno;
    uint32_t new_opaque = ++opaqueCounter;

    std::map<uint16_t, Stream*>::iterator itr = streams_.find(vbucket);
    if (itr != streams_.end() && itr->second->getState() != STREAM_DEAD) {
        return ENGINE_KEY_EEXISTS;
    } else if (itr != streams_.end()) {
        delete itr->second;
        streams_.erase(itr);
    }

    Stream *stream = new Stream(flags, new_opaque, vbucket, start_seqno,
                                end_seqno, vbucket_uuid, high_seqno,
                                STREAM_PENDING);
    streams_[vbucket] = stream;
    opaqueMap_[new_opaque] = std::make_pair(opaque, vbucket);
    readyQ.push(new StreamRequest(vbucket, new_opaque, flags, start_seqno,
                                  end_seqno, vbucket_uuid, high_seqno));
    return ENGINE_SUCCESS;
}

void UprConsumer::streamAccepted(uint32_t opaque, uint16_t status) {
    LockHolder lh(streamMutex);
    std::map<uint32_t, std::pair<uint32_t, uint16_t> >::iterator oitr =
        opaqueMap_.find(opaque);
    if (oitr != opaqueMap_.end()) {
        uint32_t add_opaque = oitr->second.first;
        uint16_t vbucket = oitr->second.second;
        std::map<uint16_t, Stream*>::iterator sitr = streams_.find(vbucket);
        if (sitr != streams_.end() && sitr->second->getOpaque() == opaque &&
            sitr->second->getState() == STREAM_PENDING) {

            if (status == ENGINE_SUCCESS) {
                sitr->second->setState(STREAM_ACTIVE);
            } else {
                sitr->second->setState(STREAM_DEAD);
            }

            readyQ.push(new AddStreamResponse(add_opaque, opaque, status));
        } else {
            LOG(EXTENSION_LOG_WARNING, "Trying to add stream, but none exists");
            readyQ.push(new AddStreamResponse(add_opaque, add_opaque,
                                              ENGINE_KEY_ENOENT));
        }
        opaqueMap_.erase(opaque);
    } else {
        LOG(EXTENSION_LOG_WARNING, "No opaque for add stream response");
    }
}

bool UprConsumer::isValidOpaque(uint32_t opaque, uint16_t vbucket) {
    LockHolder lh(streamMutex);
    std::map<uint16_t, Stream*>::iterator itr = streams_.find(vbucket);
    return itr != streams_.end() && itr->second->getOpaque() == opaque;
}

ENGINE_ERROR_CODE UprConsumer::closeStream(uint16_t vbucket)
{
    LockHolder lh(streamMutex);
    Stream *stream = NULL;
    {
        std::map<uint16_t, Stream*>::iterator itr = streams_.find(vbucket);
        if (itr == streams_.end()) {
            return ENGINE_NOT_MY_VBUCKET;
        }
        stream = itr->second;
        streams_.erase(itr);
    }

    {
        std::map<uint32_t, std::pair<uint32_t, uint16_t> >::iterator itr;
        itr = opaqueMap_.find(stream->getOpaque());
        assert(itr != opaqueMap_.end());
        opaqueMap_.erase(itr);
    }

    delete stream;
    return ENGINE_SUCCESS;
}

