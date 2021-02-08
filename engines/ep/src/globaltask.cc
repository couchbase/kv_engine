/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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

#include <limits.h>

#include "bucket_logger.h"
#include "ep_engine.h"
#include "globaltask.h"
#include "objectregistry.h"

// These static_asserts previously were in priority_test.cc
static_assert(TaskPriority::VKeyStatBGFetchTask < TaskPriority::FlusherTask,
              "VKeyStatBGFetchTask not less than FlusherTask");

static_assert(TaskPriority::ItemPager < TaskPriority::BackfillManagerTask,
              "ItemPager not less than BackfillManagerTask");

std::atomic<size_t> GlobalTask::task_id_counter(1);

GlobalTask::GlobalTask(Taskable& t,
                       TaskId taskId,
                       double initialSleepTime,
                       bool completeBeforeShutdown)
    : blockShutdown(completeBeforeShutdown),
      state(TASK_RUNNING),
      uid(nextTaskId()),
      taskId(taskId),
      engine(nullptr),
      taskable(t),
      totalRuntime(0),
      previousRuntime(0),
      lastStartTime(0) {
    priority = getTaskPriority(taskId);
    snooze(initialSleepTime);
}

GlobalTask::GlobalTask(EventuallyPersistentEngine* e,
                       TaskId taskId,
                       double initialSleepTime,
                       bool completeBeforeShutdown)
    : GlobalTask(e->getTaskable(),
                 taskId,
                 initialSleepTime,
                 completeBeforeShutdown) {
    engine = e;
}

GlobalTask::~GlobalTask() {
    // Why is this here? We are dereferencing this pointer to try and catch in
    // CV any destruction ordering issues (where the engine was destructed
    // before the task). ASAN should catch such an issue. Note that the engine
    // can be null in some unit tests
    if (engine) {
        engine->getConfiguration();
    }
}

bool GlobalTask::execute() {
    // Invoke run with the engine as the target for alloc/dalloc
    try {
        BucketAllocationGuard guard(engine);
        return run();
    } catch (...) {
        EP_LOG_CRITICAL(
                "GlobalTask::execute(): Task '{}' id:{} threw an uncaught "
                "exception",
                getDescription(),
                getId());
        // Our terminate handler will print details of the exception.
        std::terminate();
    }
}

void GlobalTask::snooze(const double secs) {
    if (secs == INT_MAX) {
        setState(TASK_SNOOZED, TASK_RUNNING);
        updateWaketime(std::chrono::steady_clock::time_point::max());
        return;
    }

    const auto curTime = std::chrono::steady_clock::now();
    if (secs) {
        setState(TASK_SNOOZED, TASK_RUNNING);
        auto nano_secs = static_cast<int64_t>(secs * 1000000000);
        updateWaketime(curTime + std::chrono::nanoseconds(nano_secs));
    } else {
        updateWaketime(curTime);
    }
}

void GlobalTask::wakeUp() {
    updateWaketime(std::chrono::steady_clock::now());
}

/*
 * Generate a switch statement from tasks.def.h that maps TaskId to a
 * stringified value of the task's name.
 */
const char* GlobalTask::getTaskName(TaskId id) {
    switch(id) {
#define TASK(name, type, prio) \
    case TaskId::name: {       \
        return #name;          \
    }
#include "tasks.def.h"
#undef TASK
        case TaskId::TASK_COUNT: {
            throw std::invalid_argument("GlobalTask::getTaskName(TaskId::TASK_COUNT) called.");
        }
    }
    throw std::logic_error("GlobalTask::getTaskName() unknown id " +
                          std::to_string(static_cast<int>(id)));
    return nullptr;
}

/*
 * Generate a switch statement from tasks.def.h that maps TaskId to priority
 */
TaskPriority GlobalTask::getTaskPriority(TaskId id) {
   switch(id) {
#define TASK(name, type, prio)     \
    case TaskId::name: {           \
        return TaskPriority::name; \
    }
#include "tasks.def.h"
#undef TASK
        case TaskId::TASK_COUNT: {
            throw std::invalid_argument("GlobalTask::getTaskPriority(TaskId::TASK_COUNT) called.");
        }
    }
    throw std::logic_error("GlobalTask::getTaskPriority() unknown id " +
                           std::to_string(static_cast<int>(id)));
    return TaskPriority::PRIORITY_COUNT;
}

/*
 * Generate a switch statement from tasks.def.h that maps TaskId to task type
 */
task_type_t GlobalTask::getTaskType(TaskId id) {
    switch (id) {
#define TASK(name, type, prio) \
    case TaskId::name: {       \
        return type;           \
    }
#include "tasks.def.h"
#undef TASK
    case TaskId::TASK_COUNT: {
        throw std::invalid_argument(
                "GlobalTask::getTaskType(TaskId::TASK_COUNT) called.");
    }
    }
    throw std::logic_error("GlobalTask::getTaskType() unknown id " +
                           std::to_string(static_cast<int>(id)));
}

std::array<TaskId, static_cast<int>(TaskId::TASK_COUNT)> GlobalTask::allTaskIds = {{
#define TASK(name, type, prio) TaskId::name,
#include "tasks.def.h"
#undef TASK
}};

std::string to_string(task_state_t state) {
    switch (state) {
    case TASK_RUNNING:
        return "RUNNING";
    case TASK_SNOOZED:
        return "SNOOZED";
    case TASK_DEAD:
        return "DEAD";
    }
    throw std::invalid_argument("to_string(task_state_t) unknown state " +
                                std::to_string(static_cast<int>(state)));
}
