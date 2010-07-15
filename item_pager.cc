/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

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
    PagingVisitor(double pcnt) : percent(pcnt), ejected(0) {}

    void visit(StoredValue *v) {

        double r = static_cast<double>(std::rand()) / static_cast<double>(RAND_MAX);
        if (percent >= r && v->isResident() && v->ejectValue()) {
            ++ejected;
        }
    }

    /**
     * Get the number of items ejected during the visit.
     */
    size_t numEjected() { return ejected; }

private:
    double   percent;
    size_t   ejected;
};

bool ItemPager::callback(Dispatcher &d, TaskId t) {
    double current = static_cast<double>(StoredValue::getCurrentSize());
    if (current > upper) {

        double toKill = (current - static_cast<double>(lower)) / current;

        getLogger()->log(EXTENSION_LOG_INFO, NULL,
                         "Using %zd bytes of memory, paging out %0f%% of items.\n",
                         StoredValue::getCurrentSize(), (toKill*100.0));

        PagingVisitor pv(toKill);
        store->visit(pv);

        stats.numValueEjects.incr(pv.numEjected());
        stats.numNonResident.incr(pv.numEjected());

        getLogger()->log(EXTENSION_LOG_INFO, NULL,
                         "Paged out %d values\n", pv.numEjected());
    }

    d.snooze(t, 10);
    return true;
}
