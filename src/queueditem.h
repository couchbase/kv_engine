/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 Couchbase, Inc
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

#ifndef SRC_QUEUEDITEM_H_
#define SRC_QUEUEDITEM_H_ 1

#include "config.h"

#include <string>

#include "common.h"
#include "item.h"
#include "stats.h"

enum queue_operation {
    queue_op_set,
    queue_op_get,
    queue_op_get_meta,
    queue_op_del,
    queue_op_flush,
    queue_op_empty,
    queue_op_commit,
    queue_op_checkpoint_start,
    queue_op_checkpoint_end
};

typedef enum {
    vbucket_del_success,
    vbucket_del_fail,
    vbucket_del_invalid
} vbucket_del_result;

/**
 * Representation of an item queued for persistence or tap.
 */
class QueuedItem : public RCValue {
public:
    QueuedItem(const std::string &k, const uint16_t vb,
               enum queue_operation o, const uint64_t revSeq,
               const int64_t bySeq)
        : key(k), revSeqno(revSeq), bySeqno(bySeq), queued(ep_current_time()),
          op(static_cast<uint16_t>(o)), vbucket(vb)
    {
        assert(bySeqno >= 0);
        ObjectRegistry::onCreateQueuedItem(this);
    }

    ~QueuedItem() {
        ObjectRegistry::onDeleteQueuedItem(this);
    }

    const std::string &getKey(void) const { return key; }
    uint16_t getVBucketId(void) const { return vbucket; }
    uint32_t getQueuedTime(void) const { return queued; }
    enum queue_operation getOperation(void) const {
        return static_cast<enum queue_operation>(op);
    }

    uint64_t getRevSeqno() const { return revSeqno; }

    int64_t getBySeqno() const { return bySeqno; }

    void setRevSeqno(uint64_t seqno) {
        revSeqno = seqno;
    }

    void setBySeqno(int64_t seqno) {
        bySeqno = seqno;
    }

    void setQueuedTime(uint32_t queued_time) {
        queued = queued_time;
    }

    void setOperation(enum queue_operation o) {
        op = static_cast<uint16_t>(o);
    }

    bool operator <(const QueuedItem &other) const {
        return getVBucketId() == other.getVBucketId() ?
            getKey() < other.getKey() : getVBucketId() < other.getVBucketId();
    }

    size_t size() {
        return sizeof(QueuedItem) + getKey().size();
    }

private:
    std::string key;
    uint64_t revSeqno;
    int64_t bySeqno;
    uint32_t queued;
    uint16_t op;
    uint16_t vbucket;

    DISALLOW_COPY_AND_ASSIGN(QueuedItem);
};

typedef SingleThreadedRCPtr<QueuedItem> queued_item;

/**
 * Order QueuedItem objects pointed by shared_ptr by their keys.
 */
class CompareQueuedItemsByKey {
public:
    CompareQueuedItemsByKey() {}
    bool operator()(const queued_item &i1, const queued_item &i2) {
        return i1->getKey() < i2->getKey();
    }
};

/**
 * Order QueuedItem objects by their vbucket ids and keys.
 */
class CompareQueuedItemsByVBAndKey {
public:
    CompareQueuedItemsByVBAndKey() {}
    bool operator()(const queued_item &i1, const queued_item &i2) {
        return i1->getVBucketId() == i2->getVBucketId()
            ? i1->getKey() < i2->getKey()
            : i1->getVBucketId() < i2->getVBucketId();
    }
};

#endif  // SRC_QUEUEDITEM_H_
