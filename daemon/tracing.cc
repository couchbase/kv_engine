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
#include "settings.h"
#include "task.h"
#include "tracing_types.h"

#include <daemon/protocol/mcbp/command_context.h>

#include <chrono>
#include <memory>
#include <mutex>

static phosphor::TraceConfig lastConfig{
        phosphor::TraceConfig(phosphor::BufferMode::ring, 20 * 1024 * 1024)};
static std::mutex configMutex;

static void setTraceConfig(const std::string& config) {
    std::lock_guard<std::mutex> lh(configMutex);
    lastConfig = phosphor::TraceConfig::fromString(config);
}

cb::engine_errc ioctlGetTracingStatus(Cookie& cookie,
                                      const StrToStrMap&,
                                      std::string& value,
                                      cb::mcbp::Datatype& datatype) {
    value = PHOSPHOR_INSTANCE.isEnabled() ? "enabled" : "disabled";
    return cb::engine_errc::success;
}

cb::engine_errc ioctlGetTracingConfig(Cookie& cookie,
                                      const StrToStrMap&,
                                      std::string& value,
                                      cb::mcbp::Datatype& datatype) {
    std::lock_guard<std::mutex> lh(configMutex);
    value = *lastConfig.toString();
    return cb::engine_errc::success;
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

void initializeTracing(const std::string& traceConfig,
                       std::chrono::seconds interval,
                       std::chrono::seconds max_age) {
    // Currently just creating the stale dump remover periodic task
    dump_remover = std::make_shared<StaleTraceDumpRemover>(
            traceDumps, interval, max_age);
    std::shared_ptr<Task> task = dump_remover;
    {
        std::lock_guard<std::mutex> lg(task->getMutex());
        executorPool->schedule(task);
    }

    setTraceConfig(traceConfig);
    // and begin tracing.
    {
        std::lock_guard<std::mutex> lh(configMutex);
        PHOSPHOR_INSTANCE.start(lastConfig);
    }

    Settings::instance().addChangeListener(
            "phosphor_config", [](const std::string&, Settings& s) {
                setTraceConfig(s.getPhosphorConfig());
            });
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
        notifyIoComplete(cookie, cb::engine_errc::success);
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

cb::engine_errc ioctlGetTracingBeginDump(Cookie& cookie,
                                         const StrToStrMap&,
                                         std::string& value,
                                         cb::mcbp::Datatype& datatype) {
    auto* cctx = cookie.getCommandContext();
    if (!cctx) {
        auto context = getTraceContext();
        if (!context.getBuffer()) {
            cookie.setErrorContext(
                    "Cannot begin a dump when there is no existing trace");
            return cb::engine_errc::invalid_arguments;
        }

        // Create a task to format the dump
        auto task = std::make_shared<TraceFormatterTask>(cookie,
                                                         std::move(context));
        cookie.setCommandContext(new TraceFormatterContext{task});

        cookie.setEwouldblock(true);
        std::lock_guard<std::mutex> guard(task->getMutex());
        std::shared_ptr<Task> basicTask = task;
        executorPool->schedule(basicTask, true);

        return cb::engine_errc::would_block;
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
    return cb::engine_errc::success;
}

cb::engine_errc ioctlGetTraceDump(Cookie& cookie,
                                  const StrToStrMap& arguments,
                                  std::string& value,
                                  cb::mcbp::Datatype& datatype) {
    auto id = arguments.find("id");
    if (id == arguments.end()) {
        cookie.setErrorContext("Dump ID must be specified as a key argument");
        return cb::engine_errc::invalid_arguments;
    }

    cb::uuid::uuid_t uuid;
    try {
        uuid = cb::uuid::from_string(id->second);
    } catch (const std::invalid_argument&) {
        cookie.setErrorContext("Dump ID must be a valid UUID");
        return cb::engine_errc::invalid_arguments;
    }

    {
        std::lock_guard<std::mutex> lh(traceDumps.mutex);
        auto iter = traceDumps.dumps.find(uuid);
        if (iter == traceDumps.dumps.end()) {
            cookie.setErrorContext(
                    "Dump ID must correspond to an existing dump");
            return cb::engine_errc::no_such_key;
        }

        value.assign(iter->second.content);
    }

    return cb::engine_errc::success;
}

cb::engine_errc ioctlSetTracingClearDump(Cookie& cookie,
                                         const StrToStrMap& arguments,
                                         const std::string& value) {
    cb::uuid::uuid_t uuid;
    try {
        uuid = cb::uuid::from_string(value);
    } catch (const std::invalid_argument&) {
        cookie.setErrorContext("Dump ID must be a valid UUID");
        return cb::engine_errc::invalid_arguments;
    }

    {
        std::lock_guard<std::mutex> lh(traceDumps.mutex);
        auto dump = traceDumps.dumps.find(uuid);
        if (dump == traceDumps.dumps.end()) {
            cookie.setErrorContext(
                    "Dump ID must correspond to an existing dump");
            return cb::engine_errc::invalid_arguments;
        }

        traceDumps.dumps.erase(dump);
    }

    return cb::engine_errc::success;
}

cb::engine_errc ioctlSetTracingConfig(Cookie& cookie,
                                      const StrToStrMap&,
                                      const std::string& value) {
    if (value.empty()) {
        cookie.setErrorContext("Trace config cannot be empty");
        return cb::engine_errc::invalid_arguments;
    }
    try {
        std::lock_guard<std::mutex> lh(configMutex);
        lastConfig = phosphor::TraceConfig::fromString(value);
    } catch (const std::invalid_argument& e) {
        cookie.setErrorContext(std::string("Trace config is illformed: ") +
                               e.what());
        return cb::engine_errc::invalid_arguments;
    }
    return cb::engine_errc::success;
}

cb::engine_errc ioctlSetTracingStart(Cookie& cookie,
                                     const StrToStrMap&,
                                     const std::string& value) {
    std::lock_guard<std::mutex> lh(configMutex);
    PHOSPHOR_INSTANCE.start(lastConfig);
    return cb::engine_errc::success;
}

cb::engine_errc ioctlSetTracingStop(Cookie& cookie,
                                    const StrToStrMap&,
                                    const std::string& value) {
    PHOSPHOR_INSTANCE.stop();
    return cb::engine_errc::success;
}

cb::engine_errc ioctlGetTracingList(Cookie& cookie,
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
    return cb::engine_errc::success;
}
