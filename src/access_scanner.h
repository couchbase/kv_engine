/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef SRC_ACCESS_SCANNER_H_
#define SRC_ACCESS_SCANNER_H_ 1

#include "config.h"

#include <string>

#include "common.h"
#include "dispatcher.h"
#include "ep_engine.h"

// Forward declaration.
class EventuallyPersistentStore;
class AccessScannerValueChangeListener;

class AccessScanner : public DispatcherCallback {
    friend class AccessScannerValueChangeListener;
public:
    AccessScanner(EventuallyPersistentStore &_store, EPStats &st,
                  size_t sleetime);
    bool callback(Dispatcher &d, TaskId &t);
    std::string description();
    size_t startTime();

private:
    EventuallyPersistentStore &store;
    EPStats &stats;
    size_t sleepTime;
    bool available;
};

#endif  // SRC_ACCESS_SCANNER_H_
