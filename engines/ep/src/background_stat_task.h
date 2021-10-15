/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <executor/globaltask.h>
#include <memcached/cookie_iface.h>
#include <memcached/engine_common.h>
#include <memcached/engine_error.h>

#include <functional>
#include <string>
#include <utility>
#include <vector>

// forward decl
enum class TaskId : int;

/**
 * Base type for tasks which gather stats on a background task.
 *
 * For use when generating stats is likely to be expensive, to avoid
 * taking up frontend thread time.
 * Users should construct and schedule the task, then store it in the
 * cookie engine specific with
 *  EventuallyPersistentEngine::storeStatTask(...)
 * Once the task has notified the frontend, the task should be retrieved with
 *  EventuallyPersistentEngine::retrieveStatTask(...)
 */
class BackgroundStatTask : public GlobalTask {
public:
    using Callback = std::function<cb::engine_errc(EventuallyPersistentEngine*,
                                                   const AddStatFn&)>;
    BackgroundStatTask(EventuallyPersistentEngine* e,
                       const CookieIface* cookie,
                       TaskId taskId);

    bool run() override;

    /**
     * If the task was successful, write all gathered stats as responses.
     *
     * Should only be called from a frontend thread.
     * @param addStat frontend provided callback
     * @return errc reflecting status of the operation
     */
    cb::engine_errc maybeWriteResponse(const AddStatFn& addStat) const;

protected:
    /**
     * Do potentially expensive work to collect stats in a background task.
     * @return status of operation
     */
    virtual cb::engine_errc collectStats() = 0;

    /**
     * Get a callback used to store stats which have been computed by the
     * background task.
     *
     * Stats cannot be immediately written as responses from the background
     * task as doing so could be racy. Instead, store them for when the
     * frontend thread revisits this operation.
     */
    AddStatFn getDeferredAddStat();

    EventuallyPersistentEngine* e;
    const CookieIface* cookie;

    // stats which have been collected while running in a background task
    std::vector<std::pair<std::string, std::string>> stats;
    cb::engine_errc status = cb::engine_errc::success;
};