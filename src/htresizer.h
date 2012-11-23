/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef SRC_HTRESIZER_H_
#define SRC_HTRESIZER_H_ 1

#include "config.h"

#include "dispatcher.h"

class EventuallyPersistentStore;

/**
 * Look around at hash tables and verify they're all sized
 * appropriately.
 */
class HashtableResizer : public DispatcherCallback {
public:

    HashtableResizer(EventuallyPersistentStore *s) : store(s) {}

    bool callback(Dispatcher &d, TaskId &t);

    std::string description() {
        return std::string("Adjusting hash table sizes.");
    }

private:
    EventuallyPersistentStore *store;
};

#endif  // SRC_HTRESIZER_H_
