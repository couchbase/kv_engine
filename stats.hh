/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include <memcached/engine.h>

#include "common.hh"

template <typename T>
class StatValue {
public:

    StatValue() : v(0) {}

    void incr(int by=1) {
        v += by;
    }

    void decr(int by=1) {
        v -= by;
    }

    void set(T to) {
        v = to;
    }

    T get(void) {
        return v;
    }

private:
    T v;
};

class EPStats {
public:

    EPStats() {}

    // How long it took us to load the data from disk.
    StatValue<time_t> warmupTime;
    // Whether we're warming up.
    StatValue<bool> warmupComplete;
    // Number of records warmed up.
    StatValue<size_t> warmedUp;
    // size of the input queue
    StatValue<size_t> queue_size;
    // Size of the in-process (output) queue.
    StatValue<size_t> flusher_todo;
    // Objects that were rejected from persistence for being too fresh.
    StatValue<size_t> tooYoung;
    // Objects that were forced into persistence for being too old.
    StatValue<size_t> tooOld;
    // Number of items persisted.
    StatValue<size_t> totalPersisted;
    // Cumulative number of items added to the queue.
    StatValue<size_t> totalEnqueued;
    // Number of times an item flush failed.
    StatValue<size_t> flushFailed;
    // Number of times a commit failed.
    StatValue<size_t> commitFailed;
    // How long an object is dirty before written.
    StatValue<rel_time_t> dirtyAge;
    StatValue<rel_time_t> dirtyAgeHighWat;
    // How old persisted data was when it hit the persistence layer
    StatValue<rel_time_t> dataAge;
    StatValue<rel_time_t> dataAgeHighWat;
    // How long does it take to do an entire flush cycle.
    StatValue<rel_time_t> flushDuration;
    StatValue<rel_time_t> flushDurationHighWat;
    // Amount of time spent in the commit phase.
    StatValue<rel_time_t> commit_time;
    // Total number of items; this would be total_items if we recycled
    // items, but we don't right now.
    StatValue<size_t> curr_items;
    // Beyond this point are config items
    // Minimum data age before a record can be persisted
    StatValue<int> min_data_age;
    // Maximum data age before a record is forced to be persisted
    StatValue<int> queue_age_cap;
    // Current tap queue size.
    StatValue<size_t> tap_queue;
    // Total number of tap messages sent.
    StatValue<size_t> tap_fetched;
};

struct key_stats {
    uint64_t cas;
    rel_time_t exptime;
    rel_time_t dirtied;
    rel_time_t data_age;
    uint32_t flags;
    bool dirty;
};
