/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef HTRESIZER_HH
#define HTRESIZER_HH 1

#include "config.h"

#include "dispatcher.hh"

class EventuallyPersistentStore;

/**
 * Look around at hash tables and verify they're all sized
 * appropriately.
 */
class HashtableResizer : public DispatcherCallback {
public:

    HashtableResizer(EventuallyPersistentStore *s) : store(s) {}

    bool callback(Dispatcher &d, TaskId t);

    std::string description() {
        return std::string("Adjusting hash table sizes.");
    }

private:
    EventuallyPersistentStore *store;
};

#endif // HTRESIZER_HH
