/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef ACCESS_SCANNER_HH
#define ACCESS_SCANNER_HH 1

#include "common.hh"
#include "dispatcher.hh"

// Forward declaration.
class EventuallyPersistentStore;

class AccessScanner : public DispatcherCallback {
public:

    AccessScanner(EventuallyPersistentStore &_store);
    bool callback(Dispatcher &d, TaskId t);
    std::string description();
    double getSleepTime() const;
    void setSleepTime(size_t t);

private:
    EventuallyPersistentStore &store;
    double sleepTime;
};

#endif /* ACCESS_SCANNER_HH */
