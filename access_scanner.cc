/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <iostream>

#include "ep_engine.h"
#include "access_scanner.hh"

class ItemAccessVisitor : public VBucketVisitor {
public:
    ItemAccessVisitor(EventuallyPersistentStore &_store) : store(_store),
                                                           startTime(ep_real_time())
    {
        Configuration &conf = store.getEPEngine().getConfiguration();
        name = conf.getAlogPath();
        prev = name + ".old";
        next = name + ".next";

        log = new MutationLog(next, conf.getAlogBlockSize());
        assert(log != NULL);
        log->open();
        if (!log->isOpen()) {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "FATAL: Failed to open access log: %s",
                             next.c_str());
            delete log;
            log = NULL;
        }
    }

    void visit(StoredValue *v) {
        if (log != NULL && v->isReferenced(true)) {
            if (v->isExpired(startTime) || v->isDeleted()) {
                getLogger()->log(EXTENSION_LOG_INFO, NULL,
                                 "INFO: Skipping expired/deleted item: %s",
                                 v->getKey().c_str());
            } else {
                log->newItem(currentBucket->getId(), v->getKey(), v->getId());
            }
        }
    }

    bool visitBucket(RCPtr<VBucket> &vb) {
        if (log == NULL) {
            return false;
        }

        return VBucketVisitor::visitBucket(vb);
    }

    virtual void complete() {
        if (log != NULL) {
            // I'd like a better way to do this!!!!
            log->commit1();
            log->commit2();
            delete log;
            log = NULL;

            if (access(prev.c_str(), F_OK) == 0 && remove(prev.c_str()) == -1) {
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "FATAL: Failed to remove '%s': %s",
                                 prev.c_str(), strerror(errno));
                remove(next.c_str());
            } else if (access(name.c_str(), F_OK) == 0 && rename(name.c_str(), prev.c_str()) == -1) {
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "FATAL: Failed to rename '%s' to '%s': %s",
                                 name.c_str(), prev.c_str(), strerror(errno));
                remove(next.c_str());
            } else if (rename(next.c_str(), name.c_str()) == -1) {
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "FATAL: Failed to rename '%s' to '%s': %s",
                                 next.c_str(), name.c_str(), strerror(errno));
                remove(next.c_str());
            }
        }
    }

private:
    EventuallyPersistentStore &store;
    rel_time_t startTime;
    std::string prev;
    std::string next;
    std::string name;

    MutationLog *log;
};

class AccessScannerValueChangeListener : public ValueChangedListener {
public:
    AccessScannerValueChangeListener(AccessScanner &as) : scanner(as) {
    }

    virtual void sizeValueChanged(const std::string &key, size_t value) {
        if (key.compare("alog_sleep_time") == 0) {
            scanner.setSleepTime(value);
        }
    }

private:
    AccessScanner &scanner;
};

AccessScanner::AccessScanner(EventuallyPersistentStore &_store) :
    store(_store)
{
    Configuration &config = store.getEPEngine().getConfiguration();
    config.addValueChangedListener("alog_sleep_time",
                                   new AccessScannerValueChangeListener(*this));
    setSleepTime(config.getAlogSleepTime());

}

bool AccessScanner::callback(Dispatcher &d, TaskId t) {
    // @todo we should be able to suspend this task to ensure that we're not
    //       running multiple in parallel
    shared_ptr<ItemAccessVisitor> pv(new ItemAccessVisitor(store));
    store.visit(pv, "Item access scanner", &d, Priority::ItemPagerPriority);
    d.snooze(t, sleepTime);
    return true;
}

std::string AccessScanner::description() {
    return std::string("Generating access log");
}

double AccessScanner::getSleepTime() const {
    return sleepTime;
}

void AccessScanner::setSleepTime(size_t t) {
    sleepTime = (double)(t * 60);
}
