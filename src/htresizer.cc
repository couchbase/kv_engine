/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "config.h"

#include "htresizer.hh"
#include "ep.hh"
#include "stored-value.hh"

static const double FREQUENCY(60.0);

/**
 * Look at all the hash tables and make sure they're sized appropriately.
 */
class ResizingVisitor : public VBucketVisitor {
public:

    ResizingVisitor() { }

    bool visitBucket(RCPtr<VBucket> &vb) {
        vb->ht.resize();
        return false;
    }

};

bool HashtableResizer::callback(Dispatcher &d, TaskId t) {
    shared_ptr<ResizingVisitor> pv(new ResizingVisitor);
    store->visit(pv, "Hashtable resizer", &d, Priority::ItemPagerPriority);

    d.snooze(t, FREQUENCY);
    return true;
}
