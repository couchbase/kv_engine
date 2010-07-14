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

    ItemPager(EventuallyPersistentStore *s, EPStats &st) : store(s), stats(st) {}

    bool callback(Dispatcher &d, TaskId t);

private:
    EventuallyPersistentStore *store;
    EPStats                   &stats;
};

#endif /* ITEM_PAGER_HH */
