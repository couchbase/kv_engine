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
#include "ep_engine.h"
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

static bool removeFile(const std::filesystem::path& path) {
    std::error_code ec;
    remove(path, ec);
    if (ec) {
        EP_LOG_WARN_CTX("Failed to remove access log file",
                        {"path", path.string()},
                        {"error", ec.message()});
        return false;
    }
    return true;
}

ItemAccessVisitor::ItemAccessVisitor(
        KVBucket& _store,
        Configuration& conf,
        EPStats& _stats,
        uint16_t sh,
        cb::SemaphoreGuard<> guard,
        uint64_t items_to_scan,
        std::vector<Vbid> vbuckets,
        std::function<void(std::string_view)> fileWriteTestHook)
    : stats(_stats),
      startTime(ep_real_time()),
      taskStart(cb::time::steady_clock::now()),
      encryptionKey(
              _store.getEPEngine().getEncryptionKeyProvider()->lookup({})),
      shardID(sh),
      semaphoreGuard(std::move(guard)),
      items_to_scan(items_to_scan) {
    Expects(semaphoreGuard.valid());
    setVBucketFilter(VBucketFilter(std::move(vbuckets)));
    Expects(!conf.getAlogPath().empty() && "No filename set");
    name = conf.getAlogPath();
    name = name + "." + std::to_string(shardID);
    prev = name + ".old";
    next = name + ".next";

    try {
        // Remove the file if one exists from a previous partial run
        // (e.g. due to a crash). We *don't* want to use the common
        // method removeFile, as we can't continue running the visitor
        // if we failed to remove the file.
        std::filesystem::remove_all(next);
    } catch (const std::exception& e) {
        EP_LOG_WARN_CTX("Failed to remove existing access log",
                        {"path", next},
                        {"error", e.what()});
        throw;
    }

    auto bs = conf.getAlogBlockSize();
    if (bs < MutationLogWriter::MinBlockSize ||
        bs > MutationLogWriter::MaxBlockSize) {
        const auto value =
                std::min(std::max(bs, MutationLogWriter::MinBlockSize),
                         MutationLogWriter::MaxBlockSize);
        EP_LOG_WARN_CTX("Configured block size is outside the legal range",
                        {"range",
                         {"min", MutationLogWriter::MinBlockSize},
                         {"max", MutationLogWriter::MaxBlockSize}},
                        {"configured", bs},
                        {"using", value});
        bs = value;
    }

    log = std::make_unique<MutationLogWriter>(next,
                                              bs,
                                              encryptionKey,
                                              cb::crypto::Compression::None,
                                              std::move(fileWriteTestHook));
    EP_LOG_INFO_CTX(
            "Attempting to generate new access file",
            {"path", next},
            {"encryption_key",
             encryptionKey ? encryptionKey->id
                           : cb::crypto::KeyDerivationKey::UnencryptedKeyId});
}

ItemAccessVisitor::~ItemAccessVisitor() {
    ++stats.alogRuns;
    removeFile(next);
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
    try {
        for (auto& it : accessed) {
            log->newItem(vbid, it);
        }
        accessed.clear();
    } catch (const std::exception& e) {
        EP_LOG_WARN_CTX("update(): Failed to update access log",
                        {"path", next},
                        {"error", e.what()});
        log->disable();
    }
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
    } catch (const std::exception& e) {
        EP_LOG_WARN_CTX("visitBucket(): Failed to write new access log to disk",
                        {"vb", vb.getId()},
                        {"path", next},
                        {"error", e.what()});
        writeFailed = true;
        log->disable();
    }
}

bool ItemAccessVisitor::cycleFile() {
    // we might want to rewrite the current file in the case where it was
    // previously unencrypted and the new one is encrypted (or the other way)
    // for now; just remove it if that is the case
    if (encryptionKey) {
        removeFile(name);
    } else {
        removeFile(name + ".cef");
    }

    std::error_code ec;
    std::filesystem::rename(name + ".cef", name + ".old.cef", ec);
    if (!ec) {
        // Success
        return true;
    }
    if (ec.value() != ENOENT) {
        EP_LOG_WARN_CTX("Failed to rename access log file",
                        {"from", name + ".cef"},
                        {"to", name + ".old.cef"},
                        {"error", ec.message()});
        return false;
    }
    std::filesystem::rename(name, name + ".old", ec);
    if (!ec) {
        // Success
        return true;
    }
    if (ec.value() != ENOENT) {
        EP_LOG_WARN_CTX("Failed to rename access log file",
                        {"from", name},
                        {"to", name + ".old"},
                        {"error", ec.message()});
        return false;
    }
    return true;
}

void ItemAccessVisitor::complete() {
    const auto num_items = log->getItemsLogged(MutationLogType::New);
    try {
        log->commit1();
        log->commit2();
        log->close();
        log.reset();
    } catch (const std::exception& e) {
        EP_LOG_WARN_CTX("complete(). Failed to write new access log to disk",
                        {"path", next},
                        {"error", e.what()});
        writeFailed = true;
        log.reset();
    }

    // Writing the new access log to disk failed, skip replacing the current
    // access log.
    if (writeFailed) {
        return;
    }

    stats.alogRuntime.store(ep_real_time() - startTime);
    stats.alogNumItems.store(num_items);
    stats.accessScannerHisto.add(
            std::chrono::duration_cast<std::chrono::microseconds>(
                    cb::time::steady_clock::now() - taskStart));

    if (num_items == 0) {
        EP_LOG_INFO_RAW(
                "The new access log file is empty. Leave the existing one in "
                "place");
        return;
    }

    // Try to rotate the "current" to ".old[.cef]"
    if (!removeFile(name + ".old") || !removeFile(name + ".old.cef") ||
        !cycleFile()) {
        return;
    }

    std::error_code ec;
    if (encryptionKey) {
        std::filesystem::rename(next, name + ".cef", ec);
    } else {
        std::filesystem::rename(next, name, ec);
    }
    if (ec) {
        EP_LOG_WARN_CTX("Failed to rename access log file",
                        {"from", next},
                        {"to", encryptionKey ? name + ".cef" : name},
                        {"error", strerror(errno)});
        return;
    }
    EP_LOG_INFO_CTX(
            "New access log file created",
            {"name", encryptionKey ? name + ".cef" : name},
            {"num_items", static_cast<uint64_t>(num_items)},
            {"encryption_key",
             encryptionKey ? encryptionKey->id
                           : cb::crypto::KeyDerivationKey::UnencryptedKeyId});
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
        initialSleep = calculateSleepTime();
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

        std::vector<std::filesystem::path> files;
        for (KVShard::id_type i = 0; i < store.getVBuckets().getNumShards();
             i++) {
            cb::SemaphoreGuard<> semaphoreGuard(&semaphore,
                                                cb::adopt_token_t{});

            if (deleteAccessLogFiles) {
                std::string name(alogPath + "." + std::to_string(i));
                std::error_code ec;
                for (const auto& extension : {"", ".old", ".cef", ".old.cef"}) {
                    std::filesystem::path filePath(name + extension);
                    if (std::filesystem::exists(filePath, ec)) {
                        files.push_back(filePath);
                    }
                }
                stats.accessScannerSkips++;
            } else {
                auto vbuckets = store.getVBuckets().getShard(i)->getVBuckets();
                // MB-69134: Only create a task if there are vbuckets mapped to
                // the shard.
                if (vbuckets.size() > 0) {
                    createAndScheduleTask(
                            i, std::move(semaphoreGuard), std::move(vbuckets));
                }
            }
        }

        if (deleteAccessLogFiles) {
            if (files.empty()) {
                EP_LOG_INFO_CTX(
                        "Not generating access log files as resident ratio is "
                        "over the threshold",
                        {"resident_ratio_threshold", residentRatioThreshold});
            } else {
                EP_LOG_INFO_CTX(
                        "Deleting access log files as resident ratio is over "
                        "the threshold",
                        {"paths", files},
                        {"resident_ratio_threshold", residentRatioThreshold});
                for (const auto& file : files) {
                    removeFile(file);
                }
            }
        }
    }

    // If task is scheduled to run once a day calculate the sleep time
    // relative to when the task was run in the case it was forcibly run
    // at a different time. If the user configured alog_sleep_time to another
    // value just run relative from now.
    const auto waitTime =
            conf.getAlogSleepTime() == 1440 ? calculateSleepTime() : sleepTime;

    snooze(waitTime);
    updateAlogTime(waitTime);

    return true;
}

void AccessScanner::updateAlogTime(double sleepSecs) {
    struct timeval _waketime;
    gettimeofday(&_waketime, nullptr);
    _waketime.tv_sec += sleepSecs;
    stats.alogTime.store(_waketime.tv_sec);
}

double AccessScanner::calculateSleepTime() const {
    // narrow to int for use with tm_hour. Config validates this to be 0 to 23
    auto startTime = gsl::narrow_cast<int>(conf.getAlogTaskTime());

    // Ensure startTime will always be within a range of (0, 23).
    // A validator is already in place in the configuration file.
    startTime = startTime % 24;

    // The following logic calculates the amount of time this task
    // needs to sleep for so that it would wake up at the
    // designated task time.
    const time_t now = ep_abs_time(ep_current_time());
    struct tm timeNow, timeTarget;
    cb_gmtime_r(&now, &timeNow);
    timeTarget = timeNow;
    if (timeNow.tm_hour >= startTime) {
        timeTarget.tm_mday += 1;
    }
    timeTarget.tm_hour = startTime;
    timeTarget.tm_min = 0;
    timeTarget.tm_sec = 0;

    return difftime(mktime(&timeTarget), mktime(&timeNow));
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

/**
 * Helper method to create and schedule the VBCAdaptor task for the Access Log
 * generation.
 * @param shard vBucket shard being used to create the ItemAccessVisitor
 * @return True on successful creation, False if the task failed
 */
void AccessScanner::createAndScheduleTask(const size_t shard,
                                          cb::SemaphoreGuard<> semaphoreGuard,
                                          std::vector<Vbid> vbuckets) {
    try {
        auto pv = std::make_unique<ItemAccessVisitor>(store,
                                                      conf,
                                                      stats,
                                                      shard,
                                                      std::move(semaphoreGuard),
                                                      maxStoredItems,
                                                      std::move(vbuckets));

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
