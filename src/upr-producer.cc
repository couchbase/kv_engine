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
#include "backfill.h"

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

    std::map<uint16_t, Stream*>::iterator itr = streams.find(vbucket);
    if (itr != streams.end()) {
        if (itr->second->getState() != STREAM_DEAD) {
            return ENGINE_KEY_EEXISTS;
        } else {
            delete itr->second;
            streams.erase(vbucket);
        }
    }

    *rollback_seqno = 0;
    if(vb->failovers.needsRollback(start_seqno, vb_uuid)) {
        *rollback_seqno = vb->failovers.findRollbackPoint(vb_uuid);
        if((*rollback_seqno) == 0) {
            // rollback point of 0 indicates that the entry was missing entirely,
            // report as key not found per transport spec.
            return ENGINE_KEY_ENOENT;
        }
        return ENGINE_ROLLBACK;
    }

    if (start_seqno >= end_seqno) {
        readyQ.push(new StreamEndResponse(opaque, 0, vbucket));
    } else {
        streams[vbucket] = new Stream(stream_flags, opaque, vbucket,
                                      start_seqno, end_seqno, vb_uuid,
                                      high_seqno, STREAM_ACTIVE);
        scheduleBackfill(vb, start_seqno, end_seqno);
    }

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE UprProducer::closeStream(uint16_t vbucket)
{
    std::map<uint16_t, Stream*>::iterator itr;
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

void UprProducer::scheduleBackfill(RCPtr<VBucket> &vb, uint64_t start_seqno,
                                   uint64_t end_seqno) {
    item_eviction_policy_t policy = engine_.getEpStore()->getItemEvictionPolicy();
    double num_items = static_cast<double>(vb->getNumItems(policy));

    if (num_items == 0) {
        return;
    }

    KVStore *underlying(engine_.getEpStore()->getAuxUnderlying());
    LOG(EXTENSION_LOG_INFO, "Scheduling backfill from disk for vbucket %d"
        "for seqno %ld to %ld", vb->getId(), start_seqno, end_seqno);
    ExTask task = new BackfillDiskLoad(conn_->name, &engine_,
                                       engine_.getUprConnMap(),
                                       underlying, vb->getId(), start_seqno,
                                       end_seqno, conn_->getConnectionToken(),
                                       Priority::TapBgFetcherPriority,
                                       0, false);
    ExecutorPool::get()->schedule(task, AUXIO_TASK_IDX);
}

void UprProducer::addStats(ADD_STAT add_stat, const void *c) {
    ConnHandler::addStats(add_stat, c);

    LockHolder lh(queueLock);
    std::map<uint16_t, Stream*>::iterator itr;
    for (itr = streams.begin(); itr != streams.end(); ++itr) {
        const int bsize = 32;
        char buffer[bsize];
        Stream* s = itr->second;
        snprintf(buffer, bsize, "stream_%d_flags", s->getVBucket());
        addStat(buffer, s->getFlags(), add_stat, c);
        snprintf(buffer, bsize, "stream_%d_opaque", s->getVBucket());
        addStat(buffer, s->getOpaque(), add_stat, c);
        snprintf(buffer, bsize, "stream_%d_start_seqno", s->getVBucket());
        addStat(buffer, s->getStartSeqno(), add_stat, c);
        snprintf(buffer, bsize, "stream_%d_end_seqno", s->getVBucket());
        addStat(buffer, s->getEndSeqno(), add_stat, c);
        snprintf(buffer, bsize, "stream_%d_vb_uuid", s->getVBucket());
        addStat(buffer, s->getVBucketUUID(), add_stat, c);
        snprintf(buffer, bsize, "stream_%d_high_seqno", s->getVBucket());
        addStat(buffer, s->getHighSeqno(), add_stat, c);
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

UprResponse* UprProducer::peekNextItem() {
    LockHolder lh(queueLock);
    while (!readyQ.empty()) {
        bool skip = false;
        UprResponse* op = readyQ.front();
        switch (op->getEvent()) {
            case UPR_MUTATION:
            case UPR_DELETION:
            {
                MutationResponse *m = dynamic_cast<MutationResponse*>(op);
                skip = shouldSkipMutation(m->getBySeqno(), m->getVBucket());
                break;
            }
            case UPR_STREAM_END:
                break;
            default:
                LOG(EXTENSION_LOG_WARNING, "Producer is attempting to write an "
                    "unexpected event %d", op->getEvent());
                abort();
        }
        if (!skip) {
            return op;
        }
        delete op;
        readyQ.pop();
    }
    return NULL;
}

void UprProducer::popNextItem() {
    LockHolder lh(queueLock);
    if (!readyQ.empty()) {
        UprResponse* op = readyQ.front();
        delete op;
        readyQ.pop();
    }
}

bool UprProducer::shouldSkipMutation(uint64_t byseqno, uint16_t vbucket) {
    std::map<uint16_t, Stream*>::iterator itr = streams.find(vbucket);
    if (itr != streams.end() && itr->second->getState() == STREAM_ACTIVE) {
        uint32_t opaque = itr->second->getOpaque();
        if (byseqno >= itr->second->getEndSeqno()) {
            itr->second->setState(STREAM_DEAD);
            readyQ.push(new StreamEndResponse(opaque, 0, vbucket));
            if (byseqno > itr->second->getEndSeqno()) {
                return true;
            }
        }
        return false;
    } else {
        return true;
    }
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
    while (!readyQ.empty()) {
        UprResponse* resp = readyQ.front();
        readyQ.pop();
        delete resp;
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
    /* Upr connections should not call this function because upr
     * backfills should be scheduled through the BackfillDiskLoad
     * task and not through the BackfillTask. Scheduling through
     * the BackfillDiskLoad task allows the specification of start
     * and end sequence numbers and allows us to schedule one-off
     * backfills.
     */
    abort();
}

void UprProducer::scheduleDiskBackfill() {
    // Not Implemented
}

void UprProducer::completeDiskBackfill() {
    // Not Implemented
}

bool UprProducer::isBackfillCompleted() {
    abort(); // Not Implemented
}

void UprProducer::completeBGFetchJob(Item *item, uint16_t vbid,
                                     bool implicitEnqueue) {
    (void) implicitEnqueue;

    // TODO: This is the wrong way to assign an opaque
    std::map<uint16_t, Stream*>::iterator itr = streams.find(vbid);
    if (itr == streams.end()) {
        return;
    }

    stats.memOverhead.fetch_add(sizeof(Item *));
    assert(stats.memOverhead.load() < GIGANTOR);
    readyQ.push(new MutationResponse(item, itr->second->getOpaque()));
}

bool UprProducer::windowIsFull() {
    abort(); // Not Implemented
}

void UprProducer::flush() {
    abort(); // Not Implemented
}
