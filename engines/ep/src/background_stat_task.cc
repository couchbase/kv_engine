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

#include "background_stat_task.h"

#include "bucket_logger.h"
#include "ep_engine.h"
#include <phosphor/phosphor.h>
#include <string_view>

BackgroundStatTask::BackgroundStatTask(EventuallyPersistentEngine* e,
                                       const CookieIface* cookie,
                                       TaskId taskId)
    : GlobalTask(e, taskId, 0, false), e(e), cookie(cookie) {
}

bool BackgroundStatTask::run() {
    TRACE_EVENT0("ep-engine/task", "BackgroundStatTask");
    try {
        status = collectStats();
    } catch (const std::exception& e) {
        EP_LOG_WARN(
                "BackgroundStatTask: callback threw exception: \"{}\" task "
                "desc:\"{}\"",
                e.what(),
                getDescription());
    }
    // _must_ notify success to ensure the frontend calls back into
    // getStats - the second call is required to avoid leaking data
    // allocated to store in the engine specific.
    // The real status is stored and shall be retrieved later.
    e->notifyIOComplete(cookie, cb::engine_errc::success);
    return false;
}

cb::engine_errc BackgroundStatTask::maybeWriteResponse(
        const AddStatFn& addStat) const {
    if (status == cb::engine_errc::success) {
        for (const auto& [key, value] : stats) {
            addStat(key, value, cookie);
        }
    }
    return status;
}

AddStatFn BackgroundStatTask::getDeferredAddStat() {
    return [this](std::string_view key, std::string_view value, const void*) {
        this->stats.emplace_back(key, value);
    };
}