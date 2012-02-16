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
        if (v->isExpired(startTime) && !v->isDeleted()) {
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
            // Check if the key exists in the open or closed referenced checkpoints.
            bool foundInCheckpoints =
                currentBucket->checkpointManager.isKeyResidentInCheckpoints(v->getKey());
            if (!foundInCheckpoints && v->ejectValue(stats, currentBucket->ht)) {
                if (currentBucket->getState() == vbucket_state_replica) {
                    ++stats.numReplicaEjects;
                }
                ++ejected;
            }
        }
    }

    bool visitBucket(RCPtr<VBucket> &vb) {
         update();
         return VBucketVisitor::visitBucket(vb);
    }

    void update() {
        stats.expired.incr(expired.size());
        store->deleteExpiredItems(expired);

        if (numEjected() > 0) {
            getLogger()->log(EXTENSION_LOG_INFO, NULL,
                             "Paged out %d values\n", numEjected());
        }

        size_t num_expired = expired.size();
        if (num_expired > 0) {
            getLogger()->log(EXTENSION_LOG_INFO, NULL,
                             "Purged %d expired items\n", num_expired);
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
    double current = static_cast<double>(StoredValue::getCurrentSize(stats));
    double upper = static_cast<double>(stats.mem_high_wat);
    double lower = static_cast<double>(stats.mem_low_wat);
    if (available && current > upper) {

        ++stats.pagerRuns;

        double toKill = (current - static_cast<double>(lower)) / current;

        std::stringstream ss;
        ss << "Using " << StoredValue::getCurrentSize(stats)
           << " bytes of memory, paging out %0f%% of items." << std::endl;
        getLogger()->log(EXTENSION_LOG_INFO, NULL, ss.str().c_str(),
                         (toKill*100.0));

        available = false;
        shared_ptr<PagingVisitor> pv(new PagingVisitor(store, stats,
                                                       toKill, &available));
        store->visit(pv, "Item pager", &d, Priority::ItemPagerPriority);

        double total_eject_attms = static_cast<double>(pv->getTotalEjectionAttempts());
        double total_ejected = static_cast<double>(pv->getTotalEjected());
        double ejection_ratio =
            total_eject_attms > 0 ? total_ejected / total_eject_attms : 0;

        if (StoredValue::getCurrentSize(stats) > stats.mem_high_wat &&
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

void InvalidItemDbPager::addInvalidItem(Item *itm, uint16_t vbucket_version) {
    uint16_t vbucket_id = itm->getVBucketId();
    std::map<uint16_t, uint16_t>::iterator version_it = vb_versions.find(vbucket_id);
    if (version_it == vb_versions.end() || version_it->second < vbucket_version) {
        vb_versions[vbucket_id] = vbucket_version;
    }

    std::map<uint16_t, std::vector<int64_t>* >::iterator item_it = vb_items.find(vbucket_id);
    if (item_it != vb_items.end()) {
        item_it->second->push_back(itm->getId());
    } else {
        std::vector<int64_t> *item_list = new std::vector<int64_t>(chunk_size * 5);
        item_list->push_back(itm->getId());
        vb_items[vbucket_id] = item_list;
    }
}

void InvalidItemDbPager::createRangeList() {
    std::map<uint16_t, std::vector<int64_t>* >::iterator vbit;
    for (vbit = vb_items.begin(); vbit != vb_items.end(); vbit++) {
        std::sort(vbit->second->begin(), vbit->second->end());
        std::list<row_range_t> row_range_list;
        createChunkListFromArray<int64_t>(vbit->second, chunk_size, row_range_list);
        vb_row_ranges[vbit->first] = row_range_list;
        delete vbit->second;
    }
    vb_items.clear();
}

bool InvalidItemDbPager::callback(Dispatcher &d, TaskId t) {
    BlockTimer timer(&stats.diskInvaidItemDelHisto);
    std::map<uint16_t, std::list<row_range_t> >::iterator it = vb_row_ranges.begin();
    if (it == vb_row_ranges.end()) {
        stats.dbCleanerComplete.set(true);
        return false;
    }

    std::list<row_range_t>::iterator rit = it->second.begin();
    uint16_t vbid = it->first;
    uint16_t vb_version = vb_versions[vbid];
    if (store->getRWUnderlying()->delVBucket(vbid, vb_version, *rit)) {
        it->second.erase(rit);
        if (it->second.begin() == it->second.end()) {
            vb_row_ranges.erase(it);
        }
    }
    else {
        d.snooze(t, 10);
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                         "Reschedule to delete the old chunk of vbucket %d with",
                         " the version %d from disk\n",
                         vbid, vb_version);
    }
    return true;
}
