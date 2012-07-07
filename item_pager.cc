/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "config.h"
#include <iostream>
#include <cstdlib>
#include <utility>
#include <list>

#include "common.hh"
#include "item_pager.hh"
#include "ep.hh"

static const double EJECTION_RATIO_THRESHOLD(0.1);
static const size_t MAX_PERSISTENCE_QUEUE_SIZE = 1000000;

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
     * @param s the store that will handle the bulk removal
     * @param st the stats where we'll track what we've done
     * @param pcnt percentage of objects to attempt to evict (0-1)
     * @param sfin pointer to a bool to be set to true after run completes
     * @param pause flag indicating if PagingVisitor can pause between vbucket visits
     */
    PagingVisitor(EventuallyPersistentStore *s, EPStats &st, double pcnt,
                  bool *sfin, bool pause = false)
        : store(s), stats(st), percent(pcnt), ejected(0),
          totalEjected(0), totalEjectionAttempts(0),
          startTime(ep_real_time()), stateFinalizer(sfin), canPause(pause) {}

    void visit(StoredValue *v) {
        // Remember expired objects -- we're going to delete them.
        if ((v->isExpired(startTime) && !v->isDeleted()) || v->isTempItem()) {
            expired.push_back(std::make_pair(currentBucket->getId(), v->getKey()));
            return;
        }

        double r = static_cast<double>(std::rand()) / static_cast<double>(RAND_MAX);
        if (percent >= r) {
            ++totalEjectionAttempts;
            if (!v->eligibleForEviction()) {
                ++stats.numFailedEjects;
                return;
            }
            // Check if the key was already visited by all the cursors.
            bool can_evict =
                currentBucket->checkpointManager.eligibleForEviction(v->getKey());
            if (can_evict && v->ejectValue(stats, currentBucket->ht)) {
                ++ejected;
            }
        }
    }

    bool visitBucket(RCPtr<VBucket> &vb) {
         update();
         return VBucketVisitor::visitBucket(vb);
    }

    void update() {
        store->deleteExpiredItems(expired);

        if (numEjected() > 0) {
            getLogger()->log(EXTENSION_LOG_INFO, NULL,
                             "Paged out %ld values\n", numEjected());
        }

        size_t num_expired = expired.size();
        if (num_expired > 0) {
            getLogger()->log(EXTENSION_LOG_INFO, NULL,
                             "Purged %ld expired items\n", num_expired);
        }

        totalEjected += (ejected + num_expired);
        ejected = 0;
        expired.clear();
    }

    bool pauseVisitor() {
        size_t queueSize = stats.queue_size.get() + stats.flusher_todo.get();
        return canPause && queueSize >= MAX_PERSISTENCE_QUEUE_SIZE;
    }

    void complete() {
        update();
        if (stateFinalizer) {
            *stateFinalizer = true;
        }
    }

    /**
     * Get the number of items ejected during the visit.
     */
    size_t numEjected() { return ejected; }

    /**
     * Get the total number of items whose values are ejected or removed due to
     * the expiry time.
     */
    size_t getTotalEjected() { return totalEjected; }

    /**
     * Get the total number of ejection attempts.
     */
    size_t getTotalEjectionAttempts() { return totalEjectionAttempts; }

private:
    std::list<std::pair<uint16_t, std::string> > expired;

    EventuallyPersistentStore *store;
    EPStats                   &stats;
    double                     percent;
    size_t                     ejected;
    size_t                     totalEjected;
    size_t                     totalEjectionAttempts;
    time_t                     startTime;
    bool                      *stateFinalizer;
    bool                       canPause;
};

bool ItemPager::callback(Dispatcher &d, TaskId t) {
    double current = static_cast<double>(stats.getTotalMemoryUsed());
    double upper = static_cast<double>(stats.mem_high_wat);
    double lower = static_cast<double>(stats.mem_low_wat);
    if (available && current > upper) {

        ++stats.pagerRuns;

        double toKill = (current - static_cast<double>(lower)) / current;

        std::stringstream ss;
        ss << "Using " << stats.getTotalMemoryUsed()
           << " bytes of memory, paging out %0f%% of items." << std::endl;
        getLogger()->log(EXTENSION_LOG_INFO, NULL, ss.str().c_str(),
                         (toKill*100.0));

        available = false;
        shared_ptr<PagingVisitor> pv(new PagingVisitor(store, stats,
                                                       toKill, &available));
        std::srand(ep_real_time());
        store->visit(pv, "Item pager", &d, Priority::ItemPagerPriority);

        double total_eject_attms = static_cast<double>(pv->getTotalEjectionAttempts());
        double total_ejected = static_cast<double>(pv->getTotalEjected());
        double ejection_ratio =
            total_eject_attms > 0 ? total_ejected / total_eject_attms : 0;

        if (stats.getTotalMemoryUsed() > stats.mem_high_wat &&
            ejection_ratio < EJECTION_RATIO_THRESHOLD)
        {
            const VBucketMap &vbuckets = store->getVBuckets();
            size_t num_vbuckets = vbuckets.getSize();
            for (size_t i = 0; i < num_vbuckets; ++i) {
                assert(i <= std::numeric_limits<uint16_t>::max());
                uint16_t vbid = static_cast<uint16_t>(i);
                RCPtr<VBucket> vb = vbuckets.getBucket(vbid);
                if (!vb) {
                    continue;
                }
                vb->checkpointManager.setCheckpointExtension(false);
            }
        }
    }

    d.snooze(t, 10);
    return true;
}

bool ExpiredItemPager::callback(Dispatcher &d, TaskId t) {
    if (available) {
        ++stats.expiryPagerRuns;

        available = false;
        shared_ptr<PagingVisitor> pv(new PagingVisitor(store, stats,
                                                       -1, &available, true));
        store->visit(pv, "Expired item remover", &d, Priority::ItemPagerPriority,
                     true, 10);
    }
    d.snooze(t, sleepTime);
    return true;
}
