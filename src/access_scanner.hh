/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef ACCESS_SCANNER_HH
#define ACCESS_SCANNER_HH 1

#include "common.hh"
#include "dispatcher.hh"

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

#endif /* ACCESS_SCANNER_HH */
