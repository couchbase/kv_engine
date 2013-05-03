/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013 Couchbase, Inc.
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

#include "iomanager/iomanager.h"
#include "ep_engine.h"
#include "flusher.hh"
#include "bgfetcher.hh"

Mutex IOManager::initGuard;
IOManager *IOManager::instance = NULL;

IOManager *IOManager::get() {
    if (!instance) {
        LockHolder lh(initGuard);
        if (!instance) {
            Configuration &config =
                ObjectRegistry::getCurrentEngine()->getConfiguration();
            instance = new IOManager(config.getMaxNumShards(),
                                     config.getMaxNumShards());
        }
    }
    return instance;
}

size_t IOManager::scheduleFlusherTask(EventuallyPersistentEngine *engine,
                                      Flusher* flusher,
                                      const Priority &priority, int sid) {
    assert(sid < writers);
    ExTask task = new FlusherTask(engine, flusher, priority);
    flusher->setTaskId(task->getId());
    return schedule(task, sid);
}

size_t IOManager::scheduleVBSnapshot(EventuallyPersistentEngine *engine,
                                     const Priority &priority, int sid,
                                     int sleeptime, bool isDaemon) {
    assert(sid < writers);
    ExTask task = new VBSnapshotTask(engine, priority, sid, isDaemon);
    return schedule(task, sid);
}

size_t IOManager::scheduleVBDelete(EventuallyPersistentEngine *engine,
                                   const void* cookie, uint16_t vbucket,
                                   const Priority &priority, int sid,
                                   bool recreate, int sleeptime,
                                   bool isDaemon) {
    assert(sid < writers);
    ExTask task = new VBDeleteTask(engine, vbucket, cookie, priority, recreate,
                                   sleeptime, isDaemon);
    return schedule(task, sid);
}

size_t IOManager::scheduleStatsSnapshot(EventuallyPersistentEngine *engine,
                                        const Priority &priority, int sid,
                                        bool runOnce, int sleeptime,
                                        bool isDaemon, bool blockShutdown) {
    assert(sid < writers);
    ExTask task = new StatSnap(engine, priority, runOnce, sleeptime,
                               isDaemon, blockShutdown);
    return schedule(task, sid);
}

size_t IOManager::scheduleMLogCompactor(EventuallyPersistentEngine *engine,
                                        const Priority &priority, int sleeptime,
                                        bool isDaemon, bool blockShutdown) {
    ExTask task = new MutationLogCompactor(engine, priority, sleeptime,
                                          isDaemon, blockShutdown);
    return schedule(task, 0);
}

size_t IOManager::scheduleMultiBGFetcher(EventuallyPersistentEngine *engine,
                                         BgFetcher *b, const Priority &priority,
                                         int sid, int sleeptime, bool isDaemon,
                                         bool blockShutdown) {
    assert(sid < readers);
    ExTask task = new BgFetcherTask(engine, b, priority, sleeptime,
                                    isDaemon, blockShutdown);
    b->setTaskId(task->getId());
    return schedule(task, writers + sid);
}

size_t IOManager::scheduleVKeyFetch(EventuallyPersistentEngine *engine,
                                    const std::string &key, uint16_t vbid,
                                    uint64_t seqNum, const void *cookie,
                                    const Priority &priority, int sid,
                                    int sleeptime, size_t delay, bool isDaemon,
                                    bool blockShutdown) {
    assert(sid < readers);
    ExTask task = new VKeyStatBGFetchTask(engine, key, vbid, seqNum, cookie,
                                          priority, sleeptime, delay, isDaemon,
                                          blockShutdown);
    return schedule(task, writers + sid);
}

size_t IOManager::scheduleBGFetch(EventuallyPersistentEngine *engine,
                                  const std::string &key, uint16_t vbid,
                                  uint64_t seqNum, const void *cookie,
                                  bool isMeta, const Priority &priority,
                                  int sid, int sleeptime, size_t delay,
                                  bool isDaemon, bool blockShutdown) {
    assert(sid < readers);
    ExTask task = new BGFetchTask(engine, key, vbid, seqNum, cookie, isMeta,
                                  priority, sleeptime, delay, isDaemon,
                                  blockShutdown);
    return schedule(task, writers + sid);
}
