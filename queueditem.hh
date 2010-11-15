/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef QUEUEDITEM_HH
#define QUEUEDITEM_HH 1

#include "common.hh"

enum queue_operation {
    queue_op_set,
    queue_op_del,
    queue_op_flush
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
               enum queue_operation o, const uint16_t vb_version = -1)
        : key(k), op(o), vbucket(vb), vbucket_version(vb_version),
          dirtied(ep_current_time()) {}

    std::string getKey(void) const { return key; }
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

private:
    std::string key;
    enum queue_operation op;
    uint16_t vbucket;
    uint16_t vbucket_version;
    rel_time_t dirtied;
};

#endif /* QUEUEDITEM_HH */
