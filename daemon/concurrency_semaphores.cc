/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "concurrency_semaphores.h"

#include "settings.h"

ConcurrencySemaphores::ConcurrencySemaphores() {
    // Size the read_vbucket_chunk semaphore from the configured number of
    // file-based backfill moves per node.
    read_vbucket_chunk.setCapacity(
            Settings::instance().getFileBasedBackfillMovesPerNode());
}

ConcurrencySemaphores& ConcurrencySemaphores::instance() {
    static ConcurrencySemaphores inst;
    return inst;
}
