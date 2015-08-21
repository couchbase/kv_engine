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

#ifndef SRC_ITEM_PAGER_H_
#define SRC_ITEM_PAGER_H_ 1

#include "config.h"

#include <set>
#include <string>
#include <utility>

#include "common.h"
#include "tasks.h"
#include "stats.h"

typedef std::pair<int64_t, int64_t> row_range_t;

// Forward declaration.
class EventuallyPersistentEngine;

/**
 * The item pager phase
 */
enum item_pager_phase {
    PAGING_UNREFERENCED,
    PAGING_RANDOM
};

/**
 * Item eviction policy
 */
enum item_eviction_policy_t {
    VALUE_ONLY, // Only evict an item's value.
    FULL_EVICTION // Evict an item's key, metadata and value together.
};

/**
 * Dispatcher job responsible for periodically pushing data out of
 * memory.
 */
class ItemPager : public GlobalTask {
public:

    /**
     * Construct an ItemPager.
     *
     * @param s the store (where we'll visit)
     * @param st the stats
     */
    ItemPager(EventuallyPersistentEngine *e, EPStats &st) :
        GlobalTask(e, Priority::ItemPagerPriority, 10, false),
        engine(e), stats(st), available(true), phase(PAGING_UNREFERENCED),
        doEvict(false) {}

    bool run(void);

    item_pager_phase getPhase() const {
        return phase;
    }

    void setPhase(item_pager_phase item_phase) {
        phase = item_phase;
    }

    std::string getDescription() { return std::string("Paging out items."); }

private:

    EventuallyPersistentEngine *engine;
    EPStats &stats;
    bool available;
    item_pager_phase phase;
    bool doEvict;
};

/**
 * Dispatcher job responsible for purging expired items from
 * memory and disk.
 */
class ExpiredItemPager : public GlobalTask {
public:

    /**
     * Construct an ExpiredItemPager.
     *
     * @param s the store (where we'll visit)
     * @param st the stats
     * @param stime number of seconds to wait between runs
     */
    ExpiredItemPager(EventuallyPersistentEngine *e, EPStats &st,
                     size_t stime, ssize_t taskTime = -1);

    bool run(void);

    std::string getDescription() {
        return std::string("Paging expired items.");
    }

private:
    /**
     *  This function is to update the next expiry pager
     *  task time, based on the current snooze time.
     */
    void updateExpPagerTime(double sleepSecs);

    EventuallyPersistentEngine *engine;
    EPStats                    &stats;
    double                     sleepTime;
    bool                       available;
};

#endif  // SRC_ITEM_PAGER_H_
