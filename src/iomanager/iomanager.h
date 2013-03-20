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

#ifndef SRC_IOMANAGER_IOMANAGER_H_
#define SRC_IOMANAGER_IOMANAGER_H_ 1

#include "config.h"

#include <string>

#include "scheduler.h"
#include "tasks.h"

class IOManager : public ExecutorPool {
public:
    static IOManager *get();

    size_t scheduleFlusherTask(EventuallyPersistentEngine *engine,
                               Flusher* flusher, const Priority &priority,
                               int sid);

    size_t scheduleVBSnapshot(EventuallyPersistentEngine *engine,
                              const Priority &priority, int sid,
                              int sleeptime = 0, bool isDaemon = false);

    size_t scheduleVBDelete(EventuallyPersistentEngine *engine,
                            const void* cookie, uint16_t vbucket,
                            const Priority &priority, int sid,
                            bool recreate = false, int sleeptime = 0,
                            bool isDaemon = false);

    size_t scheduleStatsSnapshot(EventuallyPersistentEngine *engine,
                                 const Priority &priority, int sid,
                                 bool runOnce = false, int sleeptime = 0,
                                 bool isDaemon = false,
                                 bool blockShutdown = false);

    size_t scheduleMultiBGFetcher(EventuallyPersistentEngine *engine,
                                  BgFetcher *bg, const Priority &priority,
                                  int sid, int sleeptime = 0,
                                  bool isDaemon = false,
                                  bool blockShutdown = false);

    size_t scheduleVKeyFetch(EventuallyPersistentEngine *engine,
                             const std::string &key, uint16_t vbid,
                             uint64_t seqNum, const void *cookie,
                             const Priority &priority, int sid,
                             int sleeptime = 0, size_t delay = 0,
                             bool isDaemon = false, bool blockShutdown = false);

    size_t scheduleBGFetch(EventuallyPersistentEngine *engine,
                           const std::string &key, uint16_t vbid,
                           uint64_t seqNum, const void *cookie, bool isMeta,
                           const Priority &priority, int sid, int sleeptime = 0,
                           size_t delay = 0, bool isDaemon = false,
                           bool blockShutdown = false);

    IOManager(int ro = 0, int wo = 0)
        : ExecutorPool(ro, wo) {}

private:
    static Mutex initGuard;
    static IOManager *instance;
};

#endif  // SRC_IOMANAGER_IOMANAGER_H_

