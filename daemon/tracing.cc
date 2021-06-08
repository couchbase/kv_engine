/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "tracing.h"

#include "cookie.h"
#include "memcached.h"
#include "settings.h"

#include <daemon/protocol/mcbp/command_context.h>
#include <phosphor/phosphor.h>
#include <phosphor/tools/export.h>
#include <platform/uuid.h>

#include <executor/executor.h>
#include <logger/logger.h>
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

/**
 * DumpContext holds all the information required for an ongoing
 * trace dump
 */
struct DumpContext {
    explicit DumpContext(std::string content)
        : content(std::move(content)),
          last_touch(std::chrono::steady_clock::now()) {
    }

    const std::string content;
    const std::chrono::steady_clock::time_point last_touch;
};

/// The map to hold the trace dumps (needs synchronization as it is accessed
/// from multiple threads)
using TraceDumpMap =
        folly::Synchronized<std::map<cb::uuid::uuid_t, DumpContext>,
                            std::mutex>;

/**
 * StaleTraceDumpRemover is a periodic task that removes old dumps
 */
class StaleTraceDumpRemover {
public:
    StaleTraceDumpRemover(TraceDumpMap& traceDumps,
                          std::chrono::seconds, // interval (not used)
                          std::chrono::seconds max_age)
        : traceDumps(traceDumps), max_age(max_age) {
    }

    void periodicExecute() {
        const auto now = std::chrono::steady_clock::now();
        traceDumps.withLock([now, this](auto& map) {
            erase_if(map, [now, this](const auto& dump) {
                return (dump.second.last_touch +
                        std::chrono::seconds(max_age)) <= now;
            });
        });
    }

protected:
    TraceDumpMap& traceDumps;
    const std::chrono::seconds max_age;
};

static TraceDumpMap traceDumps;
static std::shared_ptr<StaleTraceDumpRemover> dump_remover;

void initializeTracing(const std::string& traceConfig,
                       std::chrono::seconds interval,
                       std::chrono::seconds max_age) {
    // Currently just creating the stale dump remover periodic task
    // @todo install as a snoozing task once we have the executor from
    //       ep-engine available in the core
    dump_remover = std::make_shared<StaleTraceDumpRemover>(
            traceDumps, interval, max_age);

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
    traceDumps.lock()->clear();
}

/// Task to run on the executor pool to convert the TraceContext to JSON
class TraceFormatterTask {
public:
    TraceFormatterTask(Cookie& cookie, phosphor::TraceContext&& context)
        : cookie(cookie), context(std::move(context)) {
    }

    void execute() {
        try {
            phosphor::tools::JSONExport exporter(context);
            static const std::size_t chunksize = 1024 * 1024;
            size_t last_wrote;
            do {
                formatted.resize(formatted.size() + chunksize);
                last_wrote = exporter.read(
                        &formatted[formatted.size() - chunksize], chunksize);
            } while (!exporter.done());
            formatted.resize(formatted.size() - (chunksize - last_wrote));
            notifyIoComplete(cookie, cb::engine_errc::success);
        } catch (const std::exception& e) {
            LOG_WARNING("TraceFormatterTask::execute: Received exception: {}",
                        e.what());
            notifyIoComplete(cookie, cb::engine_errc::failed);
        }
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
        cb::executor::get().add([task]() { task->execute(); });
        return cb::engine_errc::would_block;
    }

    auto* ctx = dynamic_cast<TraceFormatterContext*>(cctx);
    if (!ctx) {
        throw std::runtime_error(
                "ioctlGetTracingBeginDump: Unknown value for command context");
    }
    const auto uuid = cb::uuid::random();
    traceDumps.lock()->emplace(uuid,
                               DumpContext{std::move(ctx->task->formatted)});
    value = to_string(uuid);
    cookie.setCommandContext();
    return cb::engine_errc::success;
}

cb::engine_errc ioctlGetTraceDump(Cookie& cookie,
                                  const StrToStrMap& arguments,
                                  std::string& value,
                                  cb::mcbp::Datatype& datatype) {
    // @todo once we have the snoozed task we can stop this..
    dump_remover->periodicExecute();

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

    return traceDumps.withLock([&value, &uuid, &cookie](auto& map) {
        auto iter = map.find(uuid);
        if (iter == map.end()) {
            cookie.setErrorContext(
                    "Dump ID must correspond to an existing dump");
            return cb::engine_errc::no_such_key;
        }
        value.assign(iter->second.content);
        return cb::engine_errc::success;
    });
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

    return traceDumps.withLock([&uuid, &cookie](auto& map) {
        auto dump = map.find(uuid);
        if (dump == map.end()) {
            cookie.setErrorContext(
                    "Dump ID must correspond to an existing dump");
            return cb::engine_errc::invalid_arguments;
        }
        map.erase(dump);
        return cb::engine_errc::success;
    });
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
    std::vector<std::string> uuids;
    traceDumps.withLock([&uuids](auto& map) {
        for (const auto& dump : map) {
            uuids.emplace_back(to_string(dump.first));
        }
    });
    nlohmann::json json(uuids);
    value = json.dump();
    datatype = cb::mcbp::Datatype::JSON;
    return cb::engine_errc::success;
}
