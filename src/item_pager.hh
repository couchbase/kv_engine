/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef ITEM_PAGER_HH
#define ITEM_PAGER_HH 1

#include <map>
#include <vector>
#include <list>

#include "common.hh"
#include "dispatcher.hh"
#include "stats.hh"

typedef std::pair<int64_t, int64_t> row_range_t;

// Forward declaration.
class EventuallyPersistentStore;

/**
 * The item pager phase
 */
typedef enum {
    PAGING_UNREFERENCED,
    PAGING_RANDOM
} item_pager_phase;

/**
 * Dispatcher job responsible for periodically pushing data out of
 * memory.
 */
class ItemPager : public DispatcherCallback {
public:

    /**
     * Construct an ItemPager.
     *
     * @param s the store (where we'll visit)
     * @param st the stats
     */
    ItemPager(EventuallyPersistentStore *s, EPStats &st) :
        store(*s), stats(st), available(true), phase(PAGING_UNREFERENCED) {}

    bool callback(Dispatcher &d, TaskId &t);

    item_pager_phase getPhase() const {
        return phase;
    }

    void setPhase(item_pager_phase item_phase) {
        phase = item_phase;
    }

    std::string description() { return std::string("Paging out items."); }

private:

    EventuallyPersistentStore &store;
    EPStats &stats;
    bool available;
    item_pager_phase phase;
};

/**
 * Dispatcher job responsible for purging expired items from
 * memory and disk.
 */
class ExpiredItemPager : public DispatcherCallback {
public:

    /**
     * Construct an ExpiredItemPager.
     *
     * @param s the store (where we'll visit)
     * @param st the stats
     * @param stime number of seconds to wait between runs
     */
    ExpiredItemPager(EventuallyPersistentStore *s, EPStats &st,
                     size_t stime) :
        store(*s), stats(st), sleepTime(static_cast<double>(stime)),
        available(true) {}

    bool callback(Dispatcher &d, TaskId &t);

    std::string description() { return std::string("Paging expired items."); }

private:
    EventuallyPersistentStore &store;
    EPStats                   &stats;
    double                     sleepTime;
    bool                       available;
};

#endif /* ITEM_PAGER_HH */
