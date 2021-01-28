/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
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

#include "tracing.h"

#include "cookie.h"
#include "executorpool.h"
#include "memcached.h"
#include "task.h"
#include "tracing_types.h"

#include <daemon/protocol/mcbp/command_context.h>

#include <chrono>
#include <memory>
#include <mutex>

// TODO: MB-20640 The default config should be configurable from memcached.json
static phosphor::TraceConfig lastConfig{
        phosphor::TraceConfig(phosphor::BufferMode::ring, 20 * 1024 * 1024)};
static std::mutex configMutex;

ENGINE_ERROR_CODE ioctlGetTracingStatus(Cookie& cookie,
                                        const StrToStrMap&,
                                        std::string& value,
                                        cb::mcbp::Datatype& datatype) {
    value = PHOSPHOR_INSTANCE.isEnabled() ? "enabled" : "disabled";
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE ioctlGetTracingConfig(Cookie& cookie,
                                        const StrToStrMap&,
                                        std::string& value,
                                        cb::mcbp::Datatype& datatype) {
    std::lock_guard<std::mutex> lh(configMutex);
    value = *lastConfig.toString();
    return ENGINE_SUCCESS;
}

template <typename ContainerT, typename PredicateT>
void erase_if(ContainerT& items, const PredicateT& predicate) {
    for (auto it = items.begin(); it != items.end();) {
        if (predicate(*it)) {
            it = items.erase(it);
        } else {
            ++it;
        }
    }
}

Task::Status StaleTraceDumpRemover::periodicExecute() {
    const auto now = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lh(traceDumps.mutex);

    using value_type = decltype(TraceDumps::dumps)::value_type;
    erase_if(traceDumps.dumps, [now, this](const value_type& dump) {
        return (dump.second.last_touch + std::chrono::seconds(max_age)) <= now;
    });
    return Status::Continue; // always repeat
}

static TraceDumps traceDumps;
static std::shared_ptr<StaleTraceDumpRemover> dump_remover;

void initializeTracing() {
    // Currently just creating the stale dump remover periodic task
    // @todo make period and max_age configurable
    dump_remover = std::make_shared<StaleTraceDumpRemover>(
            traceDumps, std::chrono::seconds(60), std::chrono::seconds(300));
    std::shared_ptr<Task> task = dump_remover;
    {
        std::lock_guard<std::mutex> lg(task->getMutex());
        executorPool->schedule(task);
    }

    // and begin tracing.
    {
        std::lock_guard<std::mutex> lh(configMutex);
        PHOSPHOR_INSTANCE.start(lastConfig);
    }
}

void deinitializeTracing() {
    dump_remover.reset();
    phosphor::TraceLog::getInstance().stop();
    std::lock_guard<std::mutex> guard(traceDumps.mutex);
    traceDumps.dumps.clear();
}

/// Task to run on the executor pool to convert the TraceContext to JSON
class TraceFormatterTask : public Task {
public:
    TraceFormatterTask(Cookie& cookie, phosphor::TraceContext&& context)
        : Task(), cookie(cookie), context(std::move(context)) {
    }

    Status execute() override {
        phosphor::tools::JSONExport exporter(context);
        static const std::size_t chunksize = 1024 * 1024;
        size_t last_wrote;
        do {
            formatted.resize(formatted.size() + chunksize);
            last_wrote = exporter.read(&formatted[formatted.size() - chunksize],
                                       chunksize);
        } while (!exporter.done());
        formatted.resize(formatted.size() - (chunksize - last_wrote));
        return Status::Finished;
    }

    void notifyExecutionComplete() override {
        notifyIoComplete(cookie, ENGINE_SUCCESS);
    }

    Cookie& cookie;
    std::string formatted;
    phosphor::TraceContext context;
};

struct TraceFormatterContext : public CommandContext {
    explicit TraceFormatterContext(std::shared_ptr<TraceFormatterTask>& task)
        : task(task) {
    }

    std::shared_ptr<TraceFormatterTask> task;
};

phosphor::TraceContext getTraceContext() {
    // Lock the instance until we've grabbed the trace context
    std::lock_guard<phosphor::TraceLog> lh(PHOSPHOR_INSTANCE);
    if (PHOSPHOR_INSTANCE.isEnabled()) {
        PHOSPHOR_INSTANCE.stop(lh);
    }

    return PHOSPHOR_INSTANCE.getTraceContext(lh);
}

ENGINE_ERROR_CODE ioctlGetTracingBeginDump(Cookie& cookie,
                                           const StrToStrMap&,
                                           std::string& value,
                                           cb::mcbp::Datatype& datatype) {
    auto* cctx = cookie.getCommandContext();
    if (!cctx) {
        auto context = getTraceContext();
        if (!context.getBuffer()) {
            cookie.setErrorContext(
                    "Cannot begin a dump when there is no existing trace");
            return ENGINE_EINVAL;
        }

        // Create a task to format the dump
        auto task = std::make_shared<TraceFormatterTask>(cookie,
                                                         std::move(context));
        cookie.setCommandContext(new TraceFormatterContext{task});

        cookie.setEwouldblock(true);
        std::lock_guard<std::mutex> guard(task->getMutex());
        std::shared_ptr<Task> basicTask = task;
        executorPool->schedule(basicTask, true);

        return ENGINE_EWOULDBLOCK;
    }

    auto* ctx = dynamic_cast<TraceFormatterContext*>(cctx);
    if (!ctx) {
        throw std::runtime_error(
                "ioctlGetTracingBeginDump: Unknown value for command context");
    }
    const auto uuid = cb::uuid::random();
    {
        std::lock_guard<std::mutex> guard(traceDumps.mutex);
        traceDumps.dumps.emplace(uuid,
                                 DumpContext{std::move(ctx->task->formatted)});
    }

    value = to_string(uuid);
    cookie.setCommandContext();
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE ioctlGetTraceDump(Cookie& cookie,
                                    const StrToStrMap& arguments,
                                    std::string& value,
                                    cb::mcbp::Datatype& datatype) {
    auto id = arguments.find("id");
    if (id == arguments.end()) {
        cookie.setErrorContext("Dump ID must be specified as a key argument");
        return ENGINE_EINVAL;
    }

    cb::uuid::uuid_t uuid;
    try {
        uuid = cb::uuid::from_string(id->second);
    } catch (const std::invalid_argument&) {
        cookie.setErrorContext("Dump ID must be a valid UUID");
        return ENGINE_EINVAL;
    }

    {
        std::lock_guard<std::mutex> lh(traceDumps.mutex);
        auto iter = traceDumps.dumps.find(uuid);
        if (iter == traceDumps.dumps.end()) {
            cookie.setErrorContext(
                    "Dump ID must correspond to an existing dump");
            return ENGINE_KEY_ENOENT;
        }

        value.assign(iter->second.content);
    }

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE ioctlSetTracingClearDump(Cookie& cookie,
                                           const StrToStrMap& arguments,
                                           const std::string& value) {
    cb::uuid::uuid_t uuid;
    try {
        uuid = cb::uuid::from_string(value);
    } catch (const std::invalid_argument&) {
        cookie.setErrorContext("Dump ID must be a valid UUID");
        return ENGINE_EINVAL;
    }

    {
        std::lock_guard<std::mutex> lh(traceDumps.mutex);
        auto dump = traceDumps.dumps.find(uuid);
        if (dump == traceDumps.dumps.end()) {
            cookie.setErrorContext(
                    "Dump ID must correspond to an existing dump");
            return ENGINE_EINVAL;
        }

        traceDumps.dumps.erase(dump);
    }

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE ioctlSetTracingConfig(Cookie& cookie,
                                        const StrToStrMap&,
                                        const std::string& value) {
    if (value.empty()) {
        cookie.setErrorContext("Trace config cannot be empty");
        return ENGINE_EINVAL;
    }
    try {
        std::lock_guard<std::mutex> lh(configMutex);
        lastConfig = phosphor::TraceConfig::fromString(value);
    } catch (const std::invalid_argument& e) {
        cookie.setErrorContext(std::string("Trace config is illformed: ") +
                               e.what());
        return ENGINE_EINVAL;
    }
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE ioctlSetTracingStart(Cookie& cookie,
                                       const StrToStrMap&,
                                       const std::string& value) {
    std::lock_guard<std::mutex> lh(configMutex);
    PHOSPHOR_INSTANCE.start(lastConfig);
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE ioctlSetTracingStop(Cookie& cookie,
                                      const StrToStrMap&,
                                      const std::string& value) {
    PHOSPHOR_INSTANCE.stop();
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE ioctlGetTracingList(Cookie& cookie,
                                      const StrToStrMap& arguments,
                                      std::string& value,
                                      cb::mcbp::Datatype& datatype) {
    std::lock_guard<std::mutex> lh(traceDumps.mutex);
    std::vector<std::string> uuids;
    for (const auto& dump : traceDumps.dumps) {
        uuids.emplace_back(to_string(dump.first));
    }
    nlohmann::json json(uuids);
    value = json.dump();
    datatype = cb::mcbp::Datatype::JSON;
    return ENGINE_SUCCESS;
}
