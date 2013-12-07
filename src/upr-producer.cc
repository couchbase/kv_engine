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

#include "backfill.h"
#include "ep_engine.h"
#include "upr-stream.h"

ENGINE_ERROR_CODE UprProducer::addStream(uint16_t vbucket,
                                         uint32_t opaque,
                                         uint32_t stream_flags,
                                         uint64_t start_seqno,
                                         uint64_t end_seqno,
                                         uint64_t vb_uuid,
                                         uint64_t high_seqno,
                                         uint64_t *rollback_seqno) {
    RCPtr<VBucket> vb = engine_.getVBucket(vbucket);
    if (!vb) {
        return ENGINE_NOT_MY_VBUCKET;
    }

    std::map<uint16_t, ActiveStream*>::iterator itr = streams.find(vbucket);
    if (itr != streams.end()) {
        if (itr->second->getState() != STREAM_DEAD) {
            return ENGINE_KEY_EEXISTS;
        } else {
            delete itr->second;
            streams.erase(vbucket);
        }
    }

    if ((uint64_t)vb->getHighSeqno() < start_seqno || start_seqno > end_seqno) {
        return ENGINE_ERANGE;
    }

    *rollback_seqno = 0;
    if(vb->failovers.needsRollback(start_seqno, vb_uuid)) {
        *rollback_seqno = vb->failovers.findRollbackPoint(vb_uuid);
        if((*rollback_seqno) == 0) {
            // rollback point of 0 indicates that the entry was missing
            // entirely, report as key not found per transport spec.
            return ENGINE_KEY_ENOENT;
        }
        return ENGINE_ROLLBACK;
    }

    streams[vbucket] = new ActiveStream(&engine_, conn_->name, stream_flags,
                                        opaque, vbucket, start_seqno, end_seqno,
                                        vb_uuid, high_seqno);

    streams[vbucket]->setActive();

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE UprProducer::closeStream(uint16_t vbucket)
{
    std::map<uint16_t, ActiveStream*>::iterator itr;
    for (itr = streams.begin() ; itr != streams.end(); ++itr) {
        if (vbucket == itr->second->getVBucket()) {
            // Found the stream for the vbucket.. nuke it!
            // @todo is there any pending tasks that is running for the
            //       stream that I need to wait for?
            Stream *stream = itr->second;
            streams.erase(itr);
            delete stream;
            return ENGINE_SUCCESS;
        }
    }
    return ENGINE_NOT_MY_VBUCKET;
}

void UprProducer::addStats(ADD_STAT add_stat, const void *c) {
    ConnHandler::addStats(add_stat, c);

    LockHolder lh(queueLock);
    std::map<uint16_t, ActiveStream*>::iterator itr;
    for (itr = streams.begin(); itr != streams.end(); ++itr) {
        itr->second->addStats(add_stat, c);
    }
}

void UprProducer::aggregateQueueStats(ConnCounter* aggregator) {
    LockHolder lh(queueLock);
    if (!aggregator) {
        LOG(EXTENSION_LOG_WARNING,
           "%s Pointer to the queue stats aggregator is NULL!!!", logHeader());
        return;
    }

    aggregator->conn_queueBackfillRemaining += totalBackfillBacklogs;
}

UprResponse* UprProducer::getNextItem() {
    LockHolder lh(queueLock);
    std::map<uint16_t, ActiveStream*>::iterator itr = streams.begin();
    for (; itr != streams.end(); ++itr) {
        UprResponse* op = itr->second->next();

        if (!op) {
            continue;
        }
        switch (op->getEvent()) {
            case UPR_SNAPSHOT_MARKER:
            case UPR_MUTATION:
            case UPR_DELETION:
            case UPR_STREAM_END:
                break;
            default:
                LOG(EXTENSION_LOG_WARNING, "Producer is attempting to write"
                    " an unexpected event %d", op->getEvent());
                abort();
        }
        return op;
    }
    return NULL;
}

bool UprProducer::isValidStream(uint32_t opaque, uint16_t vbucket) {
    std::map<uint16_t, ActiveStream*>::iterator itr = streams.find(vbucket);
    if (itr != streams.end() && opaque == itr->second->getOpaque() &&
        itr->second->isActive()) {
        return true;
    }
    return false;
}

bool UprProducer::isTimeForNoop() {
    // Not Implemented
    return false;
}

void UprProducer::setTimeForNoop() {
    // Not Implemented
}

void UprProducer::clearQueues() {
    LockHolder lh(queueLock);
    std::map<uint16_t, ActiveStream*>::iterator itr = streams.begin();
    for (; itr != streams.end(); ++itr) {
        itr->second->clear();
    }
}

void UprProducer::appendQueue(std::list<queued_item> *q) {
    (void) q;
    abort(); // Not Implemented
}

size_t UprProducer::getBackfillQueueSize() {
    return totalBackfillBacklogs;
}

void UprProducer::completeBackfill() {
    abort();
}

void UprProducer::scheduleDiskBackfill() {
    abort();
}

void UprProducer::completeDiskBackfill() {
    abort();
}

bool UprProducer::isBackfillCompleted() {
    abort(); // Not Implemented
}

void UprProducer::completeBGFetchJob(Item *item, uint16_t vbid,
                                     bool implicitEnqueue) {
    (void) item;
    (void) vbid;
    (void) implicitEnqueue;
    abort();
}

bool UprProducer::windowIsFull() {
    abort(); // Not Implemented
}

void UprProducer::flush() {
    abort(); // Not Implemented
}
