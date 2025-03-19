/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2012-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "access_scanner.h"
#include "bucket_logger.h"
#include "configuration.h"
#include "ep_time.h"
#include "hash_table.h"
#include "item_access_visitor.h"
#include "kv_bucket.h"
#include "mutation_log.h"
#include "stats.h"
#include "vb_count_visitor.h"
#include "vbucket.h"

#include <phosphor/phosphor.h>
#include <platform/dirutils.h>
#include <platform/platform_time.h>
#include <platform/semaphore_guard.h>

#include <memory>
#include <numeric>

ItemAccessVisitor::ItemAccessVisitor(KVBucket& _store,
                                     Configuration& conf,
                                     EPStats& _stats,
                                     uint16_t sh,
                                     cb::SemaphoreGuard<> guard,
                                     uint64_t items_to_scan,
                                     std::unique_ptr<mlog::FileIface> fileIface)
    : stats(_stats),
      startTime(ep_real_time()),
      taskStart(cb::time::steady_clock::now()),
      shardID(sh),
      semaphoreGuard(std::move(guard)),
      items_to_scan(items_to_scan) {
    Expects(semaphoreGuard.valid());
    setVBucketFilter(
            VBucketFilter(_store.getVBuckets().getShard(sh)->getVBuckets()));
    name = conf.getAlogPath();
    name = name + "." + std::to_string(shardID);
    prev = name + ".old";
    next = name + ".next";

    log = std::make_unique<MutationLog>(
            next, conf.getAlogBlockSize(), std::move(fileIface));
    log->open();
    if (!log->isOpen()) {
        EP_LOG_WARN_CTX("Failed to open access log", {"path", next});
        throw std::runtime_error(
                fmt::format("ItemAccessVisitor failed log->isOpen {}", next));
    }
    EP_LOG_INFO_CTX("Attempting to generate new access file", {"path", next});
}

ItemAccessVisitor::~ItemAccessVisitor() {
    ++stats.alogRuns;
}

bool ItemAccessVisitor::visit(const HashTable::HashBucketLock& lh,
                              StoredValue& v) {
    // Record resident, Committed HashTable items as 'accessed'.
    if (v.isResident() && v.isCommitted()) {
        if (v.isExpired(startTime) || v.isDeleted()) {
            EP_LOG_DEBUG_CTX("Skipping expired/deleted item",
                             {"seqno", v.getBySeqno()});
        } else {
            accessed.emplace_back(v.getKey());
            return ++items_scanned < items_to_scan;
        }
    }
    return true;
}

void ItemAccessVisitor::update(Vbid vbid) {
    for (auto& it : accessed) {
        log->newItem(vbid, it);
    }
    accessed.clear();
}

void ItemAccessVisitor::visitBucket(VBucket& vb) {
    try {
        update(vb.getId());

        HashTable::Position ht_start;
        if (vBucketFilter(vb.getId())) {
            while (ht_start != vb.ht.endPosition()) {
                ht_start = vb.ht.pauseResumeVisit(*this, ht_start);
                update(vb.getId());
                log->commit1();
                log->commit2();
                items_scanned = 0;
            }
        }
    } catch (const MutationLog::WriteException& e) {
        EP_LOG_WARN_CTX("Failed to write new access log to disk",
                        {"path", next},
                        {"error", e.what()});
        writeFailed = true;
        log->disable();
    }
}

void ItemAccessVisitor::complete() {
    size_t num_items = log->itemsLogged[int(MutationLogType::New)];
    try {
        log->commit1();
        log->commit2();
        log.reset();
    } catch (const MutationLog::WriteException& e) {
        EP_LOG_WARN_CTX("Failed to write new access log to disk",
                        {"path", next},
                        {"error", e.what()});
        writeFailed = true;
        log->disable();
    }

    // Writing the new access log to disk failed, skip replacing the current
    // access log.
    if (writeFailed) {
        remove(next.c_str());
        return;
    }

    stats.alogRuntime.store(ep_real_time() - startTime);
    stats.alogNumItems.store(num_items);
    stats.accessScannerHisto.add(
            std::chrono::duration_cast<std::chrono::microseconds>(
                    cb::time::steady_clock::now() - taskStart));

    if (num_items == 0) {
        EP_LOG_INFO_RAW(
                "The new access log file is empty. "
                "Delete it without replacing the current access "
                "log...");
        remove(next.c_str());
        return;
    }

    if (cb::io::isFile(prev) && remove(prev.c_str()) == -1) {
        EP_LOG_WARN_CTX("Failed to remove access log file",
                        {"path", prev},
                        {"error", strerror(errno)});
        remove(next.c_str());
        return;
    }
    EP_LOG_INFO_CTX("Removed old access log file", {"prev", prev});
    if (cb::io::isFile(name) && rename(name.c_str(), prev.c_str()) == -1) {
        EP_LOG_WARN_CTX("Failed to rename access log file",
                        {"from", name},
                        {"to", prev},
                        {"error", strerror(errno)});
        remove(next.c_str());
        return;
    }
    EP_LOG_INFO_CTX("Renamed access log file", {"from", name}, {"to", prev});
    if (rename(next.c_str(), name.c_str()) == -1) {
        EP_LOG_WARN_CTX("Failed to rename access log file",
                        {"from", next},
                        {"to", name},
                        {"error", strerror(errno)});
        remove(next.c_str());
        return;
    }
    EP_LOG_INFO_CTX("New access log file created",
                    {"name", name},
                    {"num_items", static_cast<uint64_t>(num_items)});
}

AccessScanner::AccessScanner(KVBucket& _store,
                             Configuration& conf,
                             EPStats& st,
                             double sleeptime,
                             bool useStartTime,
                             bool completeBeforeShutdown)
    : EpTask(_store.getEPEngine(),
             TaskId::AccessScanner,
             sleeptime,
             completeBeforeShutdown),
      completedCount(0),
      store(_store),
      conf(conf),
      stats(st),
      sleepTime(sleeptime),
      semaphore(store.getVBuckets().getNumShards()) {
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

    auto residentRatioThreshold = conf.getAlogResidentRatioThreshold();

    // Can we create a visitor per shard? We need 1 token per shard
    if (semaphore.try_acquire(store.getVBuckets().getNumShards())) {
        store.resetAccessScannerTasktime();

        bool deleteAccessLogFiles = false;
        /* Get the resident ratio */
        VBucketStatAggregator aggregator;
        VBucketCountVisitor activeCountVisitor(vbucket_state_active);
        aggregator.addVisitor(&activeCountVisitor);
        VBucketCountVisitor replicaCountVisitor(vbucket_state_replica);
        aggregator.addVisitor(&replicaCountVisitor);

        store.visit(aggregator);

        /* If the resident ratio is greater than 95% we do not want to generate
         access log and also we want to delete previously existing access log
         files*/
        if ((activeCountVisitor.getMemResidentPer() > residentRatioThreshold) &&
            (replicaCountVisitor.getMemResidentPer() >
             residentRatioThreshold)) {
            deleteAccessLogFiles = true;
        }

        for (size_t i = 0; i < store.getVBuckets().getNumShards(); i++) {
            cb::SemaphoreGuard<> semaphoreGuard(&semaphore,
                                                cb::adopt_token_t{});

            if (deleteAccessLogFiles) {
                std::string name(alogPath + "." + std::to_string(i));
                std::string prev(name + ".old");

                EP_LOG_INFO_CTX(
                        "Deleting access log files resident ratio is over the "
                        "threshold",
                        {"paths", {name, prev}},
                        {"resident_ratio_threshold", residentRatioThreshold});

                /* Remove .old shard access log file */
                deleteAlogFile(prev);
                /* Remove shard access log file */
                deleteAlogFile(name);
                stats.accessScannerSkips++;
            } else {
                createAndScheduleTask(i, std::move(semaphoreGuard));
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

std::string AccessScanner::getDescription() const {
    return "Generating access log";
}

std::chrono::microseconds AccessScanner::maxExpectedDuration() const {
    // The actual 'AccessScanner' task doesn't do very much (most of the actual
    // work to generate the access logs is delegated to the per-vBucket
    // 'ItemAccessVisitor' tasks). As such we don't expect to execute for
    // very long.
    return std::chrono::milliseconds(100);
}

void AccessScanner::deleteAlogFile(const std::string& fileName) {
    if (cb::io::isFile(fileName) && remove(fileName.c_str()) == -1) {
        EP_LOG_WARN_CTX("Failed to remove",
                        {"path", fileName},
                        {"error", strerror(errno)});
    }
}

/**
 * Helper method to create and schedule the VBCAdaptor task for the Access Log
 * generation.
 * @param shard vBucket shard being used to create the ItemAccessVisitor
 * @return True on successful creation, False if the task failed
 */
void AccessScanner::createAndScheduleTask(const size_t shard,
                                          cb::SemaphoreGuard<> semaphoreGuard) {
    try {
        auto pv = std::make_unique<ItemAccessVisitor>(store,
                                                      conf,
                                                      stats,
                                                      shard,
                                                      std::move(semaphoreGuard),
                                                      maxStoredItems);

        // p99.9 is typically ~200ms
        const auto maxExpectedDuration = 500ms;
        store.visitAsync(std::move(pv),
                         "Item Access Scanner",
                         TaskId::AccessScannerVisitor,
                         maxExpectedDuration);
    } catch (const std::exception& e) {
        EP_LOG_WARN_CTX(
                "Failed to create ItemAccessVisitor for shard. Please "
                "verify the location specified for the access logs is valid "
                "and exists",
                {"shard", shard},
                {"error", e.what()},
                {"path", conf.getAlogPath()});
    }
}
