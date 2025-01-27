/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "background_thread_command_context.h"

#include <daemon/connection.h>
#include <daemon/log_macros.h>
#include <daemon/one_shot_limited_concurrency_task.h>
#include <executor/executorpool.h>

BackgroundThreadCommandContext::BackgroundThreadCommandContext(
        Cookie& cookie,
        TaskId id,
        std::string name,
        cb::AwaitableSemaphore& semaphore,
        std::chrono::microseconds expectedRuntime)
    : SteppableCommandContext(cookie),
      task(std::make_shared<OneShotLimitedConcurrencyTask>(
              id,
              std::move(name),
              [this]() { execute_task_and_notify(); },
              semaphore,
              expectedRuntime)) {
    // Reallocate the cookie to ensure that the request buffer is preserved
    // until the task is scheduled (so there won't be any dangling pointers
    // if one tries to query the request object from the task).
    cookie.preserveRequest();
}

cb::engine_errc BackgroundThreadCommandContext::done() {
    if (status != cb::engine_errc::success) {
        if (!response.empty()) {
            cookie.setErrorContext(response);
        }
        return status;
    }

    cookie.sendResponse(status, {}, {}, response, datatype, cas);
    return status;
}

cb::engine_errc BackgroundThreadCommandContext::step() {
    switch (state) {
    case State::ScheduleTask:
        try {
            ExecutorPool::get()->schedule(task);
            state = State::Done;
            return cb::engine_errc::would_block;
        } catch (const std::bad_alloc&) {
            return cb::engine_errc::no_memory;
        }

    case State::Done:
        std::atomic_thread_fence(std::memory_order_acquire);
        return done();
    }

    throw std::logic_error(
            "BackgroundThreadCommandContext::step: Invalid state " +
            std::to_string(static_cast<int>(state)));
}

void BackgroundThreadCommandContext::execute_task_and_notify() {
    try {
        status = execute();
        if (status == cb::engine_errc::would_block) {
            FATAL_ERROR_CTX(
                    EXIT_FAILURE,
                    "BackgroundThreadCommandContext::execute_task_and_notify: "
                    "execute() returned would_block",
                    {"cookie", cookie.to_json()},
                    {"description", cookie.getConnection().getDescription()});
        }
    } catch (const std::exception& e) {
        LOG_ERROR_CTX("Exception occurred in BackgroundThreadCommandContext",
                      {"error", e.what()});
        status = cb::engine_errc::disconnect;
    }
    std::atomic_thread_fence(std::memory_order_release);
    cookie.notifyIoComplete(cb::engine_errc::success);
}
