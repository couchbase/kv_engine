/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "notifiable_task.h"
#include "executorpool.h"

bool NotifiableTask::run() {
    snooze(getSleepTime());
    pendingRun.store(false);
    return runInner();
}

void NotifiableTask::wakeup() {
    bool expected = false;
    if (pendingRun.compare_exchange_strong(expected, true)) {
        ExecutorPool::get()->wake(static_cast<size_t>(uid));
    }
}