/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef ITEM_PAGER_HH
#define ITEM_PAGER_HH 1

#include "common.hh"
#include "dispatcher.hh"
#include "stats.hh"

// Forward declaration.
class EventuallyPersistentStore;

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
        store(s), stats(st) {}

    bool callback(Dispatcher &d, TaskId t);

private:
    EventuallyPersistentStore *store;
    EPStats                   &stats;
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
        store(s), stats(st), sleepTime(static_cast<double>(stime)) {}

    bool callback(Dispatcher &d, TaskId t);

private:
    EventuallyPersistentStore *store;
    EPStats                   &stats;
    double                     sleepTime;
};

#endif /* ITEM_PAGER_HH */
