/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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

#ifndef SRC_ACCESS_SCANNER_H_
#define SRC_ACCESS_SCANNER_H_ 1

#include "config.h"

#include <string>

#include "tasks.h"

// Forward declaration.
class KVBucket;
class AccessScannerValueChangeListener;

class AccessScanner : public GlobalTask {
    friend class AccessScannerValueChangeListener;
public:
    AccessScanner(KVBucket& _store, EPStats& st,
                  double sleeptime = 0,
                  bool useStartTime = false,
                  bool completeBeforeShutdown = false);

    bool run();
    std::string getDescription();
    std::atomic<size_t> completedCount;

private:
    void updateAlogTime(double sleepSecs);
    void deleteAlogFile(const std::string& fileName);

    KVBucket& store;
    EPStats& stats;
    double sleepTime;
    std::string alogPath;
    std::atomic<bool> available;
    uint8_t residentRatioThreshold;
};

#endif  // SRC_ACCESS_SCANNER_H_
