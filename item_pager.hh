/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef ITEM_PAGER_HH
#define ITEM_PAGER_HH 1

#include <map>
#include <vector>
#include <list>

#include "common.hh"
#include "dispatcher.hh"
#include "stats.hh"

typedef std::pair<int64_t, int64_t> row_range;

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
        store(s), stats(st), available(true) {}

    bool callback(Dispatcher &d, TaskId t);

    std::string description() { return std::string("Paging out items."); }

private:
    EventuallyPersistentStore *store;
    EPStats                   &stats;
    bool                       available;
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
        store(s), stats(st), sleepTime(static_cast<double>(stime)),
        available(true) {}

    bool callback(Dispatcher &d, TaskId t);

    std::string description() { return std::string("Paging expired items."); }

private:
    EventuallyPersistentStore *store;
    EPStats                   &stats;
    double                     sleepTime;
    bool                       available;
};

/**
 * Dispatcher job responsible for purging invalid items with the old vbucket version
 * from disk.
 */
class InvalidItemDbPager : public DispatcherCallback {
public:

    /**
     * Construct an InvalidItemDbPager.
     *
     * @param s the store
     * @param st the stats
     */
    InvalidItemDbPager(EventuallyPersistentStore *s, EPStats &st,
                       size_t deletion_size) :
        store(s), stats(st), chunk_size(deletion_size) { }

    ~InvalidItemDbPager() {
        std::map<uint16_t, std::vector<int64_t>* >::iterator it;
        for (it = vb_items.begin(); it != vb_items.end(); it++) {
            delete (*it).second;
        }
    }

    bool callback(Dispatcher &d, TaskId t);

    void addInvalidItem(Item *item, uint16_t vb_version);

    void createRangeList();

    std::string description() {
        return std::string("Removing items with the old vbucket version from disk.");
    }

private:
    EventuallyPersistentStore                    *store;
    EPStats                                      &stats;
    size_t                                        chunk_size;
    std::map<uint16_t, uint16_t>                  vb_versions;
    std::map<uint16_t, std::vector<int64_t>* >    vb_items;
    std::map<uint16_t, std::list<row_range> >     vb_row_ranges;
};

#endif /* ITEM_PAGER_HH */
