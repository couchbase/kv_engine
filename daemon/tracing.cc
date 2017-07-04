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
#include "task.h"

#include <platform/processclock.h>

#include <mutex>

// TODO: MB-20640 The default config should be configurable from memcached.json
static phosphor::TraceConfig lastConfig{
        phosphor::TraceConfig(phosphor::BufferMode::ring, 20 * 1024 * 1024)};
static std::mutex configMutex;


ENGINE_ERROR_CODE ioctlGetTracingStatus(Connection*,
                                        const StrToStrMap&,
                                        std::string& value) {
    value = PHOSPHOR_INSTANCE.isEnabled() ? "enabled" : "disabled";
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE ioctlGetTracingConfig(Connection*,
                                        const StrToStrMap&,
                                        std::string& value) {
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
    {
        const auto now = ProcessClock::now();
        std::lock_guard<std::mutex> lh(traceDumps.mutex);

        using value_type = decltype(TraceDumps::dumps)::value_type;
        erase_if(traceDumps.dumps, [now, this](const value_type& dump) {
            // If the mutex is locked then a
            // chunk is being generated from this dump
            auto isLocked = dump.second->mutex.try_lock();
            if (!isLocked) {
                return false;
            }
            dump.second->mutex.unlock();

            if (dump.second->last_touch + std::chrono::seconds(max_age) <=
                now) {
                return true;
            }
            return false;
        });
    }
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
    std::lock_guard<std::mutex>(traceDumps.mutex);
    traceDumps.dumps.clear();
}

ENGINE_ERROR_CODE ioctlGetTracingBeginDump(Connection* c,
                                           const StrToStrMap&,
                                           std::string& value) {
    auto& connection = dynamic_cast<McbpConnection&>(*c);
    std::lock_guard<phosphor::TraceLog> lh(PHOSPHOR_INSTANCE);
    if (PHOSPHOR_INSTANCE.isEnabled()) {
        PHOSPHOR_INSTANCE.stop(lh);
    }

    phosphor::TraceContext context = PHOSPHOR_INSTANCE.getTraceContext(lh);
    if (context.getBuffer() == nullptr) {
        connection.getCookieObject().setErrorContext(
                "Cannot begin a dump when there is no existing trace");
        return ENGINE_EINVAL;
    }

    // Create the new dump associated with a random uuid
    cb::uuid::uuid_t uuid = cb::uuid::random();
    {
        std::lock_guard<std::mutex> lh(traceDumps.mutex);
        traceDumps.dumps.emplace(
                uuid, std::make_unique<DumpContext>(std::move(context)));
    }

    // Return the textual form of the uuid back to the user with success
    value = to_string(uuid);
    return ENGINE_SUCCESS;
}

/**
 * A task for generating tasks in the background on an
 * executor thread instead of a front-end thread
 */
class ChunkBuilderTask : public Task {
public:
    /// This constructor assumes that the dump's
    /// mutex has already been locked
    ChunkBuilderTask(McbpConnection& connection,
                     DumpContext& dump,
                     std::unique_lock<std::mutex> lck,
                     size_t chunk_size)
        : Task(), connection(connection), dump(dump), lck(std::move(lck)) {
        chunk.resize(chunk_size);
    }

    Status execute() override {
        size_t count = dump.json_export.read(&chunk[0], chunk.size());
        chunk.resize(count);
        return Status::Finished;
    }

    void notifyExecutionComplete() override {
        notify_io_complete(connection.getCookie(), ENGINE_SUCCESS);
    }

    std::string& getChunk() {
        return chunk;
    }

private:
    std::string chunk;
    McbpConnection& connection;
    DumpContext& dump;
    std::unique_lock<std::mutex> lck;
};

struct ChunkBuilderContext : public CommandContext {
    ChunkBuilderContext(std::shared_ptr<ChunkBuilderTask>& task) : task(task) {
    }

    std::shared_ptr<ChunkBuilderTask> task;
};

ENGINE_ERROR_CODE ioctlGetTracingDumpChunk(Connection* c,
                                           const StrToStrMap& arguments,
                                           std::string& value) {
    auto& connection = dynamic_cast<McbpConnection&>(*c);

    // If we have a context then we already generated the chunk
    auto* ctx =
            dynamic_cast<ChunkBuilderContext*>(connection.getCommandContext());
    if (ctx != nullptr) {
        value = std::move(ctx->task->getChunk());
        connection.resetCommandContext();
        return ENGINE_SUCCESS;
    }

    auto id = arguments.find("id");
    if (id == arguments.end()) {
        connection.getCookieObject().setErrorContext(
                "Dump ID must be specified as a key argument");
        return ENGINE_EINVAL;
    }

    cb::uuid::uuid_t uuid;
    try {
        uuid = cb::uuid::from_string(id->second);
    } catch (const std::invalid_argument&) {
        connection.getCookieObject().setErrorContext(
                "Dump ID must be a valid UUID");
        return ENGINE_EINVAL;
    }

    {
        std::lock_guard<std::mutex> lh(traceDumps.mutex);
        auto iter = traceDumps.dumps.find(uuid);
        if (iter == traceDumps.dumps.end()) {
            connection.getCookieObject().setErrorContext(
                    "Dump ID must correspond to an existing dump");
            return ENGINE_EINVAL;
        }
        auto& dump = *(iter->second);

        // @todo make configurable
        const size_t chunk_size = 1024 * 1024;

        if (dump.json_export.done()) {
            value = "";
            return ENGINE_SUCCESS;
        }

        std::unique_lock<std::mutex> lck(dump.mutex, std::try_to_lock);
        // A chunk is already being generated for this dump
        if (!lck) {
            value = "";
            connection.getCookieObject().setErrorContext(
                    "A chunk is already being fetched for this dump");
            return ENGINE_TMPFAIL;
        }

        // ChunkBuilderTask assumes the lock above is already held
        auto task = std::make_shared<ChunkBuilderTask>(
                connection, dump, std::move(lck), chunk_size);
        connection.setCommandContext(new ChunkBuilderContext{task});

        connection.setEwouldblock(true);
        std::lock_guard<std::mutex> guard(task->getMutex());
        std::shared_ptr<Task> basicTask = task;
        executorPool->schedule(basicTask, true);

        return ENGINE_EWOULDBLOCK;
    }
}

ENGINE_ERROR_CODE ioctlSetTracingClearDump(Connection* c,
                                           const StrToStrMap& arguments,
                                           const std::string& value) {
    auto& connection = dynamic_cast<McbpConnection&>(*c);
    cb::uuid::uuid_t uuid;
    try {
        uuid = cb::uuid::from_string(value);
    } catch (const std::invalid_argument&) {
        connection.getCookieObject().setErrorContext(
                "Dump ID must be a valid UUID");
        return ENGINE_EINVAL;
    }

    {
        std::lock_guard<std::mutex> lh(traceDumps.mutex);
        auto dump = traceDumps.dumps.find(uuid);
        if (dump == traceDumps.dumps.end()) {
            connection.getCookieObject().setErrorContext(
                    "Dump ID must correspond to an existing dump");
            return ENGINE_EINVAL;
        }

        traceDumps.dumps.erase(dump);
    }

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE ioctlSetTracingConfig(Connection* c,
                                        const StrToStrMap&,
                                        const std::string& value) {
    auto& connection = dynamic_cast<McbpConnection&>(*c);
    if (value == "") {
        connection.getCookieObject().setErrorContext(
                "Trace config cannot be empty");
        return ENGINE_EINVAL;
    }
    try {
        std::lock_guard<std::mutex> lh(configMutex);
        lastConfig = phosphor::TraceConfig::fromString(value);
    } catch (const std::invalid_argument& e) {
        connection.getCookieObject().setErrorContext(
                std::string("Trace config is illformed: ") + e.what());
        return ENGINE_EINVAL;
    }
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE ioctlSetTracingStart(Connection*,
                                       const StrToStrMap&,
                                       const std::string& value) {
    std::lock_guard<std::mutex> lh(configMutex);
    PHOSPHOR_INSTANCE.start(lastConfig);
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE ioctlSetTracingStop(Connection*,
                                      const StrToStrMap&,
                                      const std::string& value) {
    PHOSPHOR_INSTANCE.stop();
    return ENGINE_SUCCESS;
}
