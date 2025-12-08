/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <platform/awaitable_semaphore.h>

/**
 * A singleton holding the various semaphores used to control the
 * concurrency of various components for cross bucket tasks
 */
class GlobalConcurrencySemaphores {
public:
    GlobalConcurrencySemaphores(const GlobalConcurrencySemaphores&) = delete;
    static GlobalConcurrencySemaphores& instance();

    /// Limits the number of concurrent snapshot downloads. Set to 1 initially
    /// but is increased during startup when we determine the size of the
    /// AuxIO thread pool.
    cb::AwaitableSemaphore download_snapshot{1};

protected:
    GlobalConcurrencySemaphores();
};
