/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "config.h"
#include <iostream>
#include <cstdlib>
#include <utility>
#include <list>

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
                                              ejected(0), failedEjects(0),
                                              startTime(ep_real_time()) {}

    void visit(StoredValue *v) {

        // Remember expired objects -- we're going to delete them.
        if (v->isExpired(startTime)) {
            expired.push_back(std::make_pair(currentBucket->getId(), v->getKey()));
            return;
        }

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

    std::list<std::pair<uint16_t, std::string> > expired;

private:
    EPStats &stats;
    double   percent;
    size_t   ejected;
    size_t   failedEjects;
    time_t   startTime;
};

/**
 * As part of the ExpiredItemPager, visit all of the objects in memory and
 * purge all the expired items from memory and disk
 */
class ExpiryPagingVisitor : public VBucketVisitor {
public:

    /**
     * Construct a ExpiryPagingVisitor that will attempt to purge all the
     * expired items from memory and disk.
     *
     * @param st stats for engine
     */
    ExpiryPagingVisitor(EPStats &st) : stats(st), startTime(ep_real_time()) {}

    void visit(StoredValue *v) {
        if (v->isExpired(startTime)) {
            expired.push_back(std::make_pair(currentBucket->getId(), v->getKey()));
        }
    }

    std::list<std::pair<uint16_t, std::string> > expired;

private:
    EPStats &stats;
    time_t   startTime;
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
        stats.expired.incr(pv.expired.size());

        store->deleteMany(pv.expired);

        getLogger()->log(EXTENSION_LOG_INFO, NULL,
                         "Paged out %d values\n", pv.numEjected());
    }

    d.snooze(t, 10);
    return true;
}

bool ExpiredItemPager::callback(Dispatcher &d, TaskId t) {
    ++stats.expiryPagerRuns;

    PagingVisitor pv(stats, -1);
    store->visit(pv);

    stats.expired.incr(pv.expired.size());
    store->deleteMany(pv.expired);

    getLogger()->log(EXTENSION_LOG_INFO, NULL,
                     "Purged %d expired items\n", pv.expired.size());
    d.snooze(t, sleepTime);
    return true;
}

void InvalidItemDbPager::addInvalidItem(Item *item, uint16_t vbucket_version) {
    uint16_t vbucket_id = item->getVBucketId();
    std::map<uint16_t, uint16_t>::iterator version_it = vb_versions.find(vbucket_id);
    if (version_it == vb_versions.end() || version_it->second < vbucket_version) {
        vb_versions[vbucket_id] = vbucket_version;
    }

    std::map<uint16_t, std::vector<int64_t>* >::iterator item_it = vb_items.find(vbucket_id);
    if (item_it != vb_items.end()) {
        item_it->second->push_back(item->getId());
    } else {
        std::vector<int64_t> *item_list = new std::vector<int64_t>(chunk_size * 5);
        item_list->push_back(item->getId());
        vb_items[vbucket_id] = item_list;
    }
}

void InvalidItemDbPager::createRangeList() {
    std::map<uint16_t, std::vector<int64_t>* >::iterator vbit;
    for (vbit = vb_items.begin(); vbit != vb_items.end(); vbit++) {
        std::sort(vbit->second->begin(), vbit->second->end());
        std::list<row_range> row_range_list;
        createChunkListFromArray(vbit->second, chunk_size, row_range_list);
        vb_row_ranges[vbit->first] = row_range_list;
        delete vbit->second;
    }
    vb_items.clear();
}

bool InvalidItemDbPager::callback(Dispatcher &d, TaskId t) {
    BlockTimer timer(&stats.diskInvaidItemDelHisto);
    std::map<uint16_t, std::list<row_range> >::iterator it = vb_row_ranges.begin();
    if (it == vb_row_ranges.end()) {
        return false;
    }

    std::list<row_range>::iterator rit = it->second.begin();
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
