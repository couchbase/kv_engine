/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 Couchbase, Inc
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

#ifndef SRC_HTRESIZER_H_
#define SRC_HTRESIZER_H_ 1

#include "config.h"

#include <string>

#include "kv_bucket_iface.h"
#include "tasks.h"

/**
 * Look around at hash tables and verify they're all sized
 * appropriately.
 */
class HashtableResizerTask : public GlobalTask {
public:

    HashtableResizerTask(KVBucketIface* s, double sleepTime) :
    GlobalTask(&s->getEPEngine(), TaskId::HashtableResizerTask, sleepTime, false),
    store(s) {}

    bool run(void);

    std::string getDescription() {
        return std::string("Adjusting hash table sizes.");
    }

private:
    KVBucketIface* store;
};

#endif  // SRC_HTRESIZER_H_
