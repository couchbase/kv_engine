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

#include "config.h"

#include <iostream>

#include "access_scanner.h"
#include "ep_engine.h"
#include "mutation_log.h"

class ItemAccessVisitor : public VBucketVisitor {
public:
    ItemAccessVisitor(EventuallyPersistentStore &_store, EPStats &_stats,
                      uint16_t sh, std::atomic<bool> &sfin, AccessScanner &aS) :
        store(_store), stats(_stats), startTime(ep_real_time()),
        taskStart(gethrtime()), shardID(sh), stateFinalizer(sfin), as(aS)
    {
        Configuration &conf = store.getEPEngine().getConfiguration();
        name = conf.getAlogPath();
        std::stringstream s;
        s << shardID;
        name = name + "." + s.str();
        prev = name + ".old";
        next = name + ".next";

        log = new MutationLog(next, conf.getAlogBlockSize());
        log->open();
        if (!log->isOpen()) {
            LOG(EXTENSION_LOG_WARNING, "Failed to open access log: '%s'",
                next.c_str());
            delete log;
            log = NULL;
        } else {
            LOG(EXTENSION_LOG_NOTICE, "Attempting to generate new access file "
                "'%s'", next.c_str());
        }
    }

    void visit(StoredValue *v) {
        if (log != NULL && v->isResident()) {
            if (v->isExpired(startTime) || v->isDeleted()) {
                LOG(EXTENSION_LOG_INFO,
                "INFO: Skipping expired/deleted item: %s",v->getKey().c_str());
            } else {
                accessed.push_back(std::make_pair(v->getBySeqno(), v->getKey()));
            }
        }
    }

    void update() {
        if (log != NULL) {
            std::list<std::pair<uint64_t, std::string> >::iterator it;
            for (it = accessed.begin(); it != accessed.end(); ++it) {
                log->newItem(currentBucket->getId(), it->second, it->first);
            }
        }
        accessed.clear();
    }

    bool visitBucket(RCPtr<VBucket> &vb) {
        update();

        if (log == NULL) {
            return false;
        }

        return VBucketVisitor::visitBucket(vb);
    }

    virtual void complete() {
        update();

        if (log != NULL) {
            size_t num_items = log->itemsLogged[ML_NEW];
            log->commit1();
            log->commit2();
            delete log;
            log = NULL;
            ++stats.alogRuns;
            stats.alogRuntime.store(ep_real_time() - startTime);
            stats.alogNumItems.store(num_items);
            stats.accessScannerHisto.add((gethrtime() - taskStart) / 1000);

            if (num_items == 0) {
                LOG(EXTENSION_LOG_NOTICE, "The new access log file is empty. "
                    "Delete it without replacing the current access log...");
                remove(next.c_str());
                updateStateFinalizer();
                return;
            }

            if (access(prev.c_str(), F_OK) == 0 && remove(prev.c_str()) == -1){
                LOG(EXTENSION_LOG_WARNING, "Failed to remove access log file "
                    "'%s': %s", prev.c_str(), strerror(errno));
                remove(next.c_str());
                updateStateFinalizer();
                return;
            }
            LOG(EXTENSION_LOG_NOTICE, "Removed old access log file: '%s'",
                prev.c_str());
            if (access(name.c_str(), F_OK) == 0 && rename(name.c_str(),
                                                          prev.c_str()) == -1){
                LOG(EXTENSION_LOG_WARNING, "Failed to rename access log file "
                    "from '%s' to '%s': %s", name.c_str(), prev.c_str(),
                    strerror(errno));
                remove(next.c_str());
                updateStateFinalizer();
                return;
            }
            LOG(EXTENSION_LOG_NOTICE, "Renamed access log file from '%s' to "
                "'%s'", name.c_str(), prev.c_str());
            if (rename(next.c_str(), name.c_str()) == -1) {
                LOG(EXTENSION_LOG_WARNING, "Failed to rename access log file "
                    "from '%s' to '%s': %s", next.c_str(), name.c_str(),
                    strerror(errno));
                remove(next.c_str());
                updateStateFinalizer();
                return;
            }
            LOG(EXTENSION_LOG_NOTICE, "New access log file '%s' created with "
                "%" PRIu64 " keys", name.c_str(),
                static_cast<uint64_t>(num_items));
        }

        updateStateFinalizer();
    }

private:
    void updateStateFinalizer() {
        if (++(as.completedCount) == store.getVBuckets().getNumShards()) {
            bool inverse = false;
            stateFinalizer.compare_exchange_strong(inverse, true);
        }
    }

    EventuallyPersistentStore &store;
    EPStats &stats;
    rel_time_t startTime;
    hrtime_t taskStart;
    std::string prev;
    std::string next;
    std::string name;
    uint16_t shardID;

    std::list<std::pair<uint64_t, std::string> > accessed;

    MutationLog *log;
    std::atomic<bool> &stateFinalizer;
    AccessScanner &as;
};

AccessScanner::AccessScanner(EventuallyPersistentStore &_store, EPStats &st,
                             const Priority &p, double sleeptime,
                             bool useStartTime, bool completeBeforeShutdown)
    : GlobalTask(&_store.getEPEngine(), p, sleeptime,
                 completeBeforeShutdown),
      completedCount(0),
      store(_store),
      stats(st),
      sleepTime(sleeptime),
      available(true) {

    Configuration &conf = store.getEPEngine().getConfiguration();
    residentRatioThreshold = conf.getAlogResidentRatioThreshold();
    alogPath = conf.getAlogPath();
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
        time_t now = ep_abs_time(ep_current_time());
        struct tm timeNow, timeTarget;
        timeNow = *(gmtime(&now));
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
    bool inverse = true;
    if (available.compare_exchange_strong(inverse, false)) {
        store.resetAccessScannerTasktime();
        completedCount = 0;

        bool deleteAccessLogFiles = false;
        /* Get the resident ratio */
        VBucketCountAggregator aggregator;
        VBucketCountVisitor activeCountVisitor(store.getEPEngine(),
                                               vbucket_state_active);
        aggregator.addVisitor(&activeCountVisitor);
        VBucketCountVisitor replicaCountVisitor(store.getEPEngine(),
                                                vbucket_state_replica);
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

                LOG(EXTENSION_LOG_NOTICE, "Deleting access log files '%s' and "
                    "'%s' as resident ratio is over %" PRIu8,
                    name.c_str(), prev.c_str(), residentRatioThreshold);

                /* Remove .old shard access log file */
                deleteAlogFile(prev);
                /* Remove shard access log file */
                deleteAlogFile(name);
                stats.accessScannerSkips++;
            } else {
                std::shared_ptr<ItemAccessVisitor> pv(new ItemAccessVisitor(store,
                                                 stats, i, available, *this));
                std::shared_ptr<VBucketVisitor> vbv(pv);
                ExTask task = new VBucketVisitorTask(&store, vbv, i,
                                                     "Item Access Scanner",
                                                     sleepTime, true);
                ExecutorPool::get()->schedule(task, AUXIO_TASK_IDX);
            }
        }
    }
    snooze(sleepTime);
    updateAlogTime(sleepTime);

    return true;
}

void AccessScanner::updateAlogTime(double sleepSecs) {
    struct timeval _waketime;
    gettimeofday(&_waketime, NULL);
    _waketime.tv_sec += sleepSecs;
    stats.alogTime.store(_waketime.tv_sec);
}

std::string AccessScanner::getDescription() {
    return std::string("Generating access log");
}

void AccessScanner::deleteAlogFile(const std::string& fileName) {
    if (access(fileName.c_str(), F_OK) == 0 && remove(fileName.c_str()) == -1) {
        LOG(EXTENSION_LOG_WARNING, "Failed to remove '%s': %s",
            fileName.c_str(), strerror(errno));
    }
}
