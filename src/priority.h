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

#ifndef SRC_PRIORITY_H_
#define SRC_PRIORITY_H_ 1

#include "config.h"

#include <string>

#include "common.h"

/**
 * Task priority definition.
 */
class Priority {
public:
    // Priorities for Read-only tasks
    static const Priority BgFetcherPriority;
    static const Priority BgFetcherGetMetaPriority;
    static const Priority TapBgFetcherPriority;
    static const Priority VKeyStatBgFetcherPriority;
    static const Priority WarmupPriority;

    // Priorities for Read-Write tasks
    static const Priority VBucketPersistHighPriority;
    static const Priority VBucketDeletionPriority;
    static const Priority FlusherPriority;
    static const Priority FlushAllPriority;
    static const Priority CompactorPriority;
    static const Priority VBucketPersistLowPriority;
    static const Priority StatSnapPriority;
    static const Priority MutationLogCompactorPriority;
    static const Priority AccessScannerPriority;

    // Priorities for NON-IO tasks
    static const Priority TapConnNotificationPriority;
    static const Priority CheckpointRemoverPriority;
    static const Priority VBMemoryDeletionPriority;
    static const Priority CheckpointStatsPriority;
    static const Priority ItemPagerPriority;
    static const Priority BackfillTaskPriority;
    static const Priority TapResumePriority;
    static const Priority TapConnectionReaperPriority;
    static const Priority HTResizePriority;
    static const Priority PendingOpsPriority;
    static const Priority TapConnMgrPriority;

    bool operator==(const Priority &other) const {
        return other.getPriorityValue() == this->priority;
    }

    bool operator<(const Priority &other) const {
        return this->priority > other.getPriorityValue();
    }

    bool operator>(const Priority &other) const {
        return this->priority < other.getPriorityValue();
    }

    /**
     * Return a priority name.
     *
     * @return a priority name
     */
    const std::string &toString() const {
        return name;
    }

    /**
     * Return an integer value that represents a priority.
     *
     * @return a priority value
     */
    int getPriorityValue() const {
        return priority;
    }

    // gcc didn't like the idea of having a class with no constructor
    // available to anyone.. let's make it protected instead to shut
    // gcc up :(
protected:
    Priority(const char *nm, int p) : name(nm), priority(p) { }
    std::string name;
    int priority;

private:
    DISALLOW_COPY_AND_ASSIGN(Priority);
};

#endif  // SRC_PRIORITY_H_
