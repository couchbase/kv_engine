/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2012 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include "access_scanner.h"
#include "bucket_logger.h"
#include "ep_time.h"
#include "hash_table.h"
#include "kv_bucket.h"
#include "mutation_log.h"
#include "stats.h"
#include "vb_count_visitor.h"

#include <phosphor/phosphor.h>
#include <platform/dirutils.h>
#include <platform/platform_time.h>

#include <memory>
#include <numeric>

class ItemAccessVisitor : public CappedDurationVBucketVisitor,
                          public HashTableVisitor {
public:
    ItemAccessVisitor(KVBucket& _store,
                      Configuration& conf,
                      EPStats& _stats,
                      uint16_t sh,
                      std::atomic<bool>& sfin,
                      AccessScanner& aS,
                      uint64_t items_to_scan)
        : store(_store),
          stats(_stats),
          startTime(ep_real_time()),
          taskStart(std::chrono::steady_clock::now()),
          shardID(sh),
          stateFinalizer(sfin),
          as(aS),
          items_scanned(0),
          items_to_scan(items_to_scan) {
        setVBucketFilter(VBucketFilter(
                _store.getVBuckets().getShard(sh)->getVBuckets()));
        name = conf.getAlogPath();
        name = name + "." + std::to_string(shardID);
        prev = name + ".old";
        next = name + ".next";

        log = std::make_unique<MutationLog>(next, conf.getAlogBlockSize());
        log->open();
        if (!log->isOpen()) {
            EP_LOG_WARN("Failed to open access log: '{}'", next);
            log.reset();
        } else {
            EP_LOG_INFO(
                    "Attempting to generate new access file "
                    "'{}'",
                    next);
        }
    }

    bool visit(const HashTable::HashBucketLock& lh, StoredValue& v) override {
        // Record resident, Committed HashTable items as 'accessed'.
        if (log && v.isResident() && v.isCommitted()) {
            if (v.isExpired(startTime) || v.isDeleted()) {
                EP_LOG_DEBUG("Skipping expired/deleted item: {}",
                             v.getBySeqno());
            } else {
                accessed.push_back(StoredDocKey(v.getKey()));
                return ++items_scanned < items_to_scan;
            }
        }
        return true;
    }

    void update(Vbid vbid) {
        if (log != nullptr) {
            for (auto it = accessed.begin(); it != accessed.end(); ++it) {
                log->newItem(vbid, *it);
            }
        }
        accessed.clear();
    }

    void visitBucket(const VBucketPtr& vb) override {
        update(vb->getId());

        if (log == nullptr) {
            return;
        }
        HashTable::Position ht_start;
        if (vBucketFilter(vb->getId())) {
            while (ht_start != vb->ht.endPosition()) {
                ht_start = vb->ht.pauseResumeVisit(*this, ht_start);
                update(vb->getId());
                log->commit1();
                log->commit2();
                items_scanned = 0;
            }
        }
    }

    void complete() override {

        if (log == nullptr) {
            updateStateFinalizer(false);
        } else {
            size_t num_items = log->itemsLogged[int(MutationLogType::New)];
            log->commit1();
            log->commit2();
            log.reset();
            stats.alogRuntime.store(ep_real_time() - startTime);
            stats.alogNumItems.store(num_items);
            stats.accessScannerHisto.add(
                    std::chrono::duration_cast<std::chrono::microseconds>(
                            std::chrono::steady_clock::now() - taskStart));

            if (num_items == 0) {
                EP_LOG_INFO(
                        "The new access log file is empty. "
                        "Delete it without replacing the current access "
                        "log...");
                remove(next.c_str());
                updateStateFinalizer(true);
                return;
            }

            if (cb::io::isFile(prev) && remove(prev.c_str()) == -1) {
                EP_LOG_WARN(
                        "Failed to remove access log file "
                        "'{}': {}",
                        prev,
                        strerror(errno));
                remove(next.c_str());
                updateStateFinalizer(true);
                return;
            }
            EP_LOG_INFO("Removed old access log file: '{}'", prev);
            if (cb::io::isFile(name) &&
                rename(name.c_str(), prev.c_str()) == -1) {
                EP_LOG_WARN(
                        "Failed to rename access log file "
                        "from '{}' to '{}': {}",
                        name,
                        prev,
                        strerror(errno));
                remove(next.c_str());
                updateStateFinalizer(true);
                return;
            }
            EP_LOG_INFO(
                    "Renamed access log file from '{}' to "
                    "'{}'",
                    name,
                    prev);
            if (rename(next.c_str(), name.c_str()) == -1) {
                EP_LOG_WARN(
                        "Failed to rename access log file "
                        "from '{}' to '{}': {}",
                        next,
                        name,
                        strerror(errno));
                remove(next.c_str());
                updateStateFinalizer(true);
                return;
            }
            EP_LOG_INFO(
                    "New access log file '{}' created with "
                    "{} keys",
                    name,
                    static_cast<uint64_t>(num_items));
            updateStateFinalizer(true);
        }
    }

private:
    /**
     * Finalizer method called at the end of completing a visit.
     * @param created_log: Did we successfully create a MutationLog object on
     * this run?
     */
    void updateStateFinalizer(bool created_log) {
        if (++(as.completedCount) == store.getVBuckets().getNumShards()) {
            bool inverse = false;
            stateFinalizer.compare_exchange_strong(inverse, true);
        }
        if (created_log) {
            // Successfully created an access log - increment stat.
            // Done after the new file created
            // to aid in testing - once the stat has the new value the access.log
            // file can be safely checked.
            ++stats.alogRuns;
        }
    }

    VBucketFilter vBucketFilter;

    KVBucket& store;
    EPStats& stats;
    rel_time_t startTime;
    std::chrono::steady_clock::time_point taskStart;
    std::string prev;
    std::string next;
    std::string name;
    uint16_t shardID;

    std::vector<StoredDocKey> accessed;

    std::unique_ptr<MutationLog> log;
    std::atomic<bool> &stateFinalizer;
    AccessScanner &as;

    // The number items scanned since last pause
    uint64_t items_scanned;
    // The number of items to scan before we pause
    const uint64_t items_to_scan;
};

AccessScanner::AccessScanner(KVBucket& _store,
                             Configuration& conf,
                             EPStats& st,
                             double sleeptime,
                             bool useStartTime,
                             bool completeBeforeShutdown)
    : GlobalTask(&_store.getEPEngine(),
                 TaskId::AccessScanner,
                 sleeptime,
                 completeBeforeShutdown),
      completedCount(0),
      store(_store),
      conf(conf),
      stats(st),
      sleepTime(sleeptime),
      available(true) {
    residentRatioThreshold = conf.getAlogResidentRatioThreshold();
    alogPath = conf.getAlogPath();
    maxStoredItems = conf.getAlogMaxStoredItems();
    double initialSleep = sleeptime;
    if (useStartTime) {
        size_t startTime = conf.getAlogTaskTime();

        /*
         * Ensure startTime will always be within a range of (0, 23).
         * A validator is already in place in the configuration file.
         */
        startTime = startTime % 24;

        /*
         * The following logic calculates the amount of time this task
         * needs to sleep for initially so that it would wake up at the
         * designated task time, note that this logic kicks in only when
         * useStartTime argument in the constructor is set to TRUE.
         * Otherwise this task will wake up periodically in a time
         * specified by sleeptime.
         */
        const time_t now = ep_abs_time(ep_current_time());
        struct tm timeNow, timeTarget;
        cb_gmtime_r(&now, &timeNow);
        timeTarget = timeNow;
        if (timeNow.tm_hour >= (int)startTime) {
            timeTarget.tm_mday += 1;
        }
        timeTarget.tm_hour = startTime;
        timeTarget.tm_min = 0;
        timeTarget.tm_sec = 0;

        initialSleep = difftime(mktime(&timeTarget), mktime(&timeNow));
        snooze(initialSleep);
    }

    updateAlogTime(initialSleep);
}

bool AccessScanner::run() {
    TRACE_EVENT0("ep-engine/task", "AccessScanner");

    bool inverse = true;
    if (available.compare_exchange_strong(inverse, false)) {
        store.resetAccessScannerTasktime();
        completedCount = 0;

        bool deleteAccessLogFiles = false;
        /* Get the resident ratio */
        VBucketCountAggregator aggregator;
        VBucketCountVisitor activeCountVisitor(vbucket_state_active);
        aggregator.addVisitor(&activeCountVisitor);
        VBucketCountVisitor replicaCountVisitor(vbucket_state_replica);
        aggregator.addVisitor(&replicaCountVisitor);

        store.visit(aggregator);

        /* If the resident ratio is greater than 95% we do not want to generate
         access log and also we want to delete previously existing access log
         files*/
        if ((activeCountVisitor.getMemResidentPer() > residentRatioThreshold) &&
            (replicaCountVisitor.getMemResidentPer() > residentRatioThreshold))
        {
            deleteAccessLogFiles = true;
        }
        for (size_t i = 0; i < store.getVBuckets().getNumShards(); i++) {
            if (deleteAccessLogFiles) {
                std::string name(alogPath + "." + std::to_string(i));
                std::string prev(name + ".old");

                EP_LOG_INFO(
                        "Deleting access log files '{}' and "
                        "'{}' as resident ratio is over {}",
                        name,
                        prev,
                        residentRatioThreshold);

                /* Remove .old shard access log file */
                deleteAlogFile(prev);
                /* Remove shard access log file */
                deleteAlogFile(name);
                stats.accessScannerSkips++;
            } else {
                createAndScheduleTask(i);
            }
        }
    }
    snooze(sleepTime);
    updateAlogTime(sleepTime);

    return true;
}

void AccessScanner::updateAlogTime(double sleepSecs) {
    struct timeval _waketime;
    gettimeofday(&_waketime, nullptr);
    _waketime.tv_sec += sleepSecs;
    stats.alogTime.store(_waketime.tv_sec);
}

std::string AccessScanner::getDescription() {
    return "Generating access log";
}

std::chrono::microseconds AccessScanner::maxExpectedDuration() {
    // The actual 'AccessScanner' task doesn't do very much (most of the actual
    // work to generate the access logs is delegated to the per-vBucket
    // 'ItemAccessVisitor' tasks). As such we don't expect to execute for
    // very long.
    return std::chrono::milliseconds(100);
}

void AccessScanner::deleteAlogFile(const std::string& fileName) {
    if (cb::io::isFile(fileName) && remove(fileName.c_str()) == -1) {
        EP_LOG_WARN("Failed to remove '{}': {}", fileName, strerror(errno));
    }
}

/**
 * Helper method to create and schedule the VBCAdaptor task for the Access Log
 * generation.
 * @param shard vBucket shard being used to create the ItemAccessVisitor
 * @return True on successful creation, False if the task failed
 */
void AccessScanner::createAndScheduleTask(const size_t shard) {
    try {
        auto pv = std::make_unique<ItemAccessVisitor>(
                store, conf, stats, shard, available, *this, maxStoredItems);

        // p99.9 is typically ~200ms
        const auto maxExpectedDuration = 500ms;
        store.visitAsync(std::move(pv),
                         "Item Access Scanner",
                         TaskId::AccessScannerVisitor,
                         maxExpectedDuration);
    } catch (const std::exception& e) {
        EP_LOG_WARN(
                "Error creating Item Access Scanner task: '{}'. Please verify "
                "the "
                "location specified for the access logs is valid and exists. "
                "Current location is set at: '{}'",
                e.what(),
                conf.getAlogPath());
    }
}
