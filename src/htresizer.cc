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

#include "config.h"

#include "htresizer.h"

#include <memory>

#include <phosphor/phosphor.h>
#include <platform/make_unique.h>

static const double FREQUENCY(60.0);

/**
 * Look at all the hash tables and make sure they're sized appropriately.
 */
class ResizingVisitor : public VBucketVisitor {
public:
    ResizingVisitor() { }

    void visitBucket(RCPtr<VBucket> &vb) override {
        vb->ht.resize();
    }
};

bool HashtableResizerTask::run(void) {
    TRACE_EVENT0("ep-engine/task", "HashtableResizerTask");
    auto pv = std::make_unique<ResizingVisitor>();
    store->visit(std::move(pv),
                 "Hashtable resizer",
                 NONIO_TASK_IDX,
                 TaskId::HashtableResizerVisitorTask);

    snooze(FREQUENCY);
    return true;
}
