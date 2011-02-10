/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef QUEUEDITEM_HH
#define QUEUEDITEM_HH 1

#include "common.hh"

enum queue_operation {
    queue_op_set,
    queue_op_del,
    queue_op_flush,
    queue_op_empty,
    queue_op_commit
};

typedef enum {
    vbucket_del_success,
    vbucket_del_fail,
    vbucket_del_invalid
} vbucket_del_result;

/**
 * Representation of an item queued for persistence or tap.
 */
class QueuedItem {
public:
    QueuedItem(const std::string &k, const uint16_t vb,
               enum queue_operation o, const uint16_t vb_version = -1,
               const int64_t rid = -1)
        : key(k), op(o), vbucket(vb), vbucket_version(vb_version),
          row_id(rid), dirtied(ep_current_time()) {}

    const std::string &getKey(void) const { return key; }
    uint16_t getVBucketId(void) const { return vbucket; }
    uint16_t getVBucketVersion(void) const { return vbucket_version; }
    rel_time_t getDirtied(void) const { return dirtied; }
    enum queue_operation getOperation(void) const { return op; }

    bool operator <(const QueuedItem &other) const {
        return vbucket == other.vbucket ? key < other.key : vbucket < other.vbucket;
    }

    size_t size() const {
        return sizeof(QueuedItem) + key.size();
    }

    int64_t getRowId() { return row_id; }

private:
    std::string key;
    enum queue_operation op;
    uint16_t vbucket;
    uint16_t vbucket_version;
    int64_t row_id;
    rel_time_t dirtied;
};

typedef shared_ptr<QueuedItem> queued_item;

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
 * Order QueuedItem objects by their row ids.
 */
class CompareQueuedItemsByRowId {
public:
    CompareQueuedItemsByRowId() {}
    bool operator()(QueuedItem i1, QueuedItem i2) {
        return i1.getRowId() < i2.getRowId();
    }
};

/**
 * Order QueuedItem objects by their vbucket then row ids.
 */
class CompareQueuedItemsByVBAndRowId {
public:
    CompareQueuedItemsByVBAndRowId() {}
    bool operator()(QueuedItem i1, QueuedItem i2) {
        return i1.getVBucketId() == i2.getVBucketId()
            ? i1.getRowId() < i2.getRowId()
            : i1.getVBucketId() < i2.getVBucketId();
    }
};

/**
 * Compare two Schwartzian-transformed QueuedItems.
 */
template <typename T>
class TaggedQueuedItemComparator {
public:
    TaggedQueuedItemComparator() {}

    bool operator()(std::pair<T, QueuedItem> i1, std::pair<T, QueuedItem> i2) {
        return (i1.first == i2.first)
            ? (i1.second.getRowId() < i2.second.getRowId())
            : (i1 < i2);
    }
};

#endif /* QUEUEDITEM_HH */
