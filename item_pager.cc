/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "config.h"
#include <iostream>
#include <cstdlib>

#include "common.hh"
#include "item_pager.hh"
#include "ep.hh"

static const double threshold = 75.0;

/**
 * As part of the ItemPager, visit all of the objects in memory and
 * eject some within a constrained probability
 */
class PagingVisitor : public VBucketVisitor {
public:

    /**
     * Construct a PagingVisitor that will attempt to evict the given
     * percentage of objects.
     *
     * @param pcnt percentage of objects to attempt to evict (0-1)
     */
    PagingVisitor(EPStats &st, double pcnt) : stats(st), percent(pcnt),
                                              ejected(0), failedEjects(0) {}

    void visit(StoredValue *v) {

        double r = static_cast<double>(std::rand()) / static_cast<double>(RAND_MAX);
        if (percent >= r) {
            if (v->ejectValue(stats)) {
                ++ejected;
            } else {
                ++failedEjects;
            }
        }
    }

    /**
     * Get the number of items ejected during the visit.
     */
    size_t numEjected() { return ejected; }

    /**
     * Get the number of ejection failures.
     *
     * An ejection failure is the state when an ejection was
     * requested, but did not work for some reason (either object was
     * dirty, or too small, or something).
     */
    size_t numFailedEjects() { return failedEjects; }

private:
    EPStats &stats;
    double   percent;
    size_t   ejected;
    size_t   failedEjects;
};

bool ItemPager::callback(Dispatcher &d, TaskId t) {
    double current = static_cast<double>(StoredValue::getCurrentSize(stats));
    double upper = static_cast<double>(stats.mem_high_wat);
    double lower = static_cast<double>(stats.mem_low_wat);
    if (current > upper) {

        ++stats.pagerRuns;

        double toKill = (current - static_cast<double>(lower)) / current;

        std::stringstream ss;
        ss << "Using " << StoredValue::getCurrentSize(stats)
           << " bytes of memory, paging out %0f%% of items." << std::endl;
        getLogger()->log(EXTENSION_LOG_INFO, NULL, ss.str().c_str(),
                         (toKill*100.0));
        PagingVisitor pv(stats, toKill);
        store->visit(pv);

        stats.numValueEjects.incr(pv.numEjected());
        stats.numNonResident.incr(pv.numEjected());
        stats.numFailedEjects.incr(pv.numFailedEjects());

        getLogger()->log(EXTENSION_LOG_INFO, NULL,
                         "Paged out %d values\n", pv.numEjected());
    }

    d.snooze(t, 10);
    return true;
}
