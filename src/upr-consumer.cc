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

UprResponse* UprConsumer::getNextItem() {
    LockHolder lh(streamMutex);
    std::map<uint16_t, PassiveStream*>::iterator itr = streams_.begin();
    for (; itr != streams_.end(); ++itr) {
        UprResponse* op = itr->second->next();

        if (!op) {
            continue;
        }
        switch (op->getEvent()) {
            case UPR_STREAM_REQ:
            case UPR_ADD_STREAM:
                break;
            default:
                LOG(EXTENSION_LOG_WARNING, "%s Consumer is attempting to write"
                    " an unexpected event %d", logHeader(), op->getEvent());
                abort();
        }
        return op;
    }
    return NULL;
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

    std::map<uint16_t, PassiveStream*>::iterator itr = streams_.find(vbucket);
    if (itr != streams_.end() && itr->second->isActive()) {
        return ENGINE_KEY_EEXISTS;
    } else if (itr != streams_.end()) {
        delete itr->second;
        streams_.erase(itr);
    }

    streams_[vbucket] = new PassiveStream(conn_->name, flags, new_opaque,
                                          vbucket, start_seqno, end_seqno,
                                          vbucket_uuid, high_seqno);
    opaqueMap_[new_opaque] = std::make_pair(opaque, vbucket);
    return ENGINE_SUCCESS;
}

void UprConsumer::streamAccepted(uint32_t opaque, uint16_t status) {
    LockHolder lh(streamMutex);
    opaque_map::iterator oitr = opaqueMap_.find(opaque);
    if (oitr != opaqueMap_.end()) {
        uint32_t add_opaque = oitr->second.first;
        uint16_t vbucket = oitr->second.second;
        std::map<uint16_t, PassiveStream*>::iterator sitr = streams_.find(vbucket);
        if (sitr != streams_.end() && sitr->second->getOpaque() == opaque &&
            sitr->second->getState() == STREAM_PENDING) {
            sitr->second->acceptStream(status, add_opaque);
        } else {
            LOG(EXTENSION_LOG_WARNING, "%s Trying to add stream, but none "
                "exists (opaque: %d, add_opaque: %d)", logHeader(), opaque,
                add_opaque);
        }
        opaqueMap_.erase(opaque);
    } else {
        LOG(EXTENSION_LOG_WARNING, "%s No opaque for add stream response",
            logHeader());
    }
}

bool UprConsumer::isValidOpaque(uint32_t opaque, uint16_t vbucket) {
    LockHolder lh(streamMutex);
    std::map<uint16_t, PassiveStream*>::iterator itr = streams_.find(vbucket);
    return itr != streams_.end() && itr->second->getOpaque() == opaque;
}

ENGINE_ERROR_CODE UprConsumer::closeStream(uint16_t vbucket)
{
    LockHolder lh(streamMutex);
    Stream *stream = NULL;
    {
        std::map<uint16_t, PassiveStream*>::iterator itr = streams_.find(vbucket);
        if (itr == streams_.end()) {
            return ENGINE_NOT_MY_VBUCKET;
        }
        stream = itr->second;
        streams_.erase(itr);
    }

    {
        opaque_map::iterator itr = opaqueMap_.find(stream->getOpaque());
        assert(itr != opaqueMap_.end());
        opaqueMap_.erase(itr);
    }

    delete stream;
    return ENGINE_SUCCESS;
}

