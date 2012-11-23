/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef SRC_ITEM_PAGER_H_
#define SRC_ITEM_PAGER_H_ 1

#include <map>
#include <vector>
#include <list>

#include "common.h"
#include "dispatcher.h"
#include "stats.h"

typedef std::pair<int64_t, int64_t> row_range_t;

// Forward declaration.
class EventuallyPersistentStore;

/**
 * ItemPager visits replica vbuckets and active vbuckets in one phases.
 * The config_t is a bool value and it indicates whether random items ejection
 * will be performed.
 */
class PagingConfig {
public:
    static const short int paging_max = 2;

    static const short int paging_unreferenced = 0;
    static const short int paging_random = 1;

    static const bool phaseConfig[paging_max];
};

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
        store(*s), stats(st), available(true), phase(PagingConfig::paging_unreferenced) {}

    bool callback(Dispatcher &d, TaskId &t);

    std::string description() { return std::string("Paging out items."); }

private:
    bool checkAccessScannerTask();

    EventuallyPersistentStore &store;
    EPStats                   &stats;
    bool                       available;
    short int                  phase;
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

#endif  // SRC_ITEM_PAGER_H_
