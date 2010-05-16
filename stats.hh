/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include <memcached/engine.h>

struct ep_stats {
    // How long it took us to load the data from disk.
    time_t warmupTime;
    // Whether we're warming up.
    bool warmupComplete;
    // Number of records warmed up.
    size_t warmedUp;
    // size of the input queue
    size_t queue_size;
    // Size of the in-process (output) queue.
    size_t flusher_todo;
    // Objects that were rejected from persistence for being too fresh.
    size_t tooYoung;
    // Objects that were forced into persistence for being too old.
    size_t tooOld;
    // Number of items persisted.
    size_t totalPersisted;
    // Cumulative number of items added to the queue.
    size_t totalEnqueued;
    // Number of times an item flush failed.
    size_t flushFailed;
    // Number of times a commit failed.
    size_t commitFailed;
    // How long an object is dirty before written.
    rel_time_t dirtyAge;
    rel_time_t dirtyAgeHighWat;
    // How old persisted data was when it hit the persistence layer
    rel_time_t dataAge;
    rel_time_t dataAgeHighWat;
    // How long does it take to do an entire flush cycle.
    rel_time_t flushDuration;
    rel_time_t flushDurationHighWat;
    // Amount of time spent in the commit phase.
    rel_time_t commit_time;
    // Total number of items; this would be total_items if we recycled
    // items, but we don't right now.
    size_t curr_items;
    // Beyond this point are config items
    // Minimum data age before a record can be persisted
    uint32_t min_data_age;
    // Maximum data age before a record is forced to be persisted
    uint32_t queue_age_cap;
    // Current tap queue size.
    size_t tap_queue;
    // Total number of tap messages sent.
    size_t tap_fetched;
};

struct key_stats {
    uint64_t cas;
    rel_time_t exptime;
    rel_time_t dirtied;
    rel_time_t data_age;
    uint32_t flags;
    bool dirty;
};
