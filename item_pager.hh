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
     * @param l lower boundary (how far down we'll aim to go when evicting)
     * @param u the upper boundary (threshold at which we begin evicting)
     */
    ItemPager(EventuallyPersistentStore *s, EPStats &st,
              size_t l, size_t u) : store(s), stats(st),
                                    lower(l), upper(u) {}

    bool callback(Dispatcher &d, TaskId t);

private:
    EventuallyPersistentStore *store;
    EPStats                   &stats;
    size_t                     lower;
    size_t                     upper;
};

#endif /* ITEM_PAGER_HH */
