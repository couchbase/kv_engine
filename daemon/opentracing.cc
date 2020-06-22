/*
 *     Copyright 2019 Couchbase, Inc
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

#include "opentracing.h"

#include "connection.h"
#include "cookie.h"
#include "cookie_trace_context.h"
#include "log_macros.h"
#include "opentracing_config.h"

#include <mcbp/protocol/request.h>
#include <nlohmann/json.hpp>
#include <sstream>

#include <platform/thread.h>

namespace cb {
using Thread = Couchbase::Thread;
using ThreadState = Couchbase::ThreadState;
} // namespace cb

#ifdef ENABLE_OPENTRACING
std::atomic_bool OpenTracing::enabled;

// Create a shorter version so that we don't have to wrap lines all the
// time
using ParentSpan =
        opentracing::expected<std::unique_ptr<opentracing::SpanContext>>;
#endif

std::unique_ptr<OpenTracing> OpenTracing::instance;

class OpenTracingThread : public OpenTracing, public cb::Thread {
public:
    OpenTracingThread(const OpenTracingConfig& config)
        : OpenTracing(config), cb::Thread("mcd:trace") {
    }

    void stop() {
        {
            std::lock_guard<std::mutex> guard(mutex);
            running = false;
        }
        condition_variable.notify_all();
    }

protected:
    /// The main loop of the thread
    void run() override;

    void pushOne(std::chrono::system_clock::time_point system_now,
                 std::chrono::steady_clock::time_point steady_now,
                 const CookieTraceContext& entry);

    /// move all of the contents to the internal list. We live under
    /// the assumption that only a few commands contains trace requests
    /// so there won't be too many elemnets.
    void push(CookieTraceContext& context) override {
        {
            std::lock_guard<std::mutex> guard(mutex);
            contexts.push_back(std::move(context));
        }
        condition_variable.notify_all();
    }

#ifdef ENABLE_OPENTRACING
    /// Try to decode the context as if it was encoded as a text map
    /// (key1=value1&key2=value2)
    ParentSpan decodeAsTextMap(const std::string& context);

    /// Try to decode teh context as if it was encoded as a binary blob
    ParentSpan decodeAsBinary(const std::string& context);
#endif

    bool running = true;

    /// The mutex variable used to protect access to _all_ the internal
    /// members
    std::mutex mutex;
    /// The daemon thread will block and wait on this variable when there
    /// isn't any work to do
    std::condition_variable condition_variable;

    /// The list of entries to submit
    std::vector<CookieTraceContext> contexts;
};

void OpenTracingThread::run() {
    setRunning();
    std::unique_lock<std::mutex> lock(mutex);
    while (running) {
        if (contexts.empty()) {
            // There isn't any work to do... wait until we get some
            condition_variable.wait(
                    lock, [this] { return !running || !contexts.empty(); });
        }

        // move the entries over to another vector so the clients don't have
        // to wait while I'm working
        auto entries = std::move(contexts);

        // Release the lock to the internal variables while I process the
        // batch of trace elements
        lock.unlock();

        if (isEnabled()) {
            // Unfortunately OpenTracing want system clock, and we operate
            // with steady clocks internally.. snapshot the two and try to
            // convert between them. (I don't want to cache this "forever"
            // as the system clock could have been changed)
            const auto system_now = std::chrono::system_clock::now();
            const auto steady_now = std::chrono::steady_clock::now();

            for (const auto& e : entries) {
                pushOne(system_now, steady_now, e);
            }
        }

        // make sure we run all destructors before we're grabbing the lock
        entries.clear();

        // acquire the lock
        lock.lock();
    }
}

#ifdef ENABLE_OPENTRACING
// This code is copied from the example at:
// https://github.com/opentracing/opentracing-cpp/blob/master/README.md)
//
// Some clients don't implement the binary encoding scheme (at least
// jaeger don't have an implementation) so we need to work around that
// by having the client use the space inefficient text-map implementation.
struct CustomCarrierReader : opentracing::TextMapReader {
    explicit CustomCarrierReader(
            const std::unordered_map<std::string, std::string>& data_)
        : data{data_} {
    }

    using F = std::function<opentracing::expected<void>(
            opentracing::string_view, opentracing::string_view)>;

    opentracing::expected<void> ForeachKey(F f) const override {
        // Iterate through all key-value pairs, the tracer will use the
        // relevant keys to extract a span context.
        for (auto& key_value : data) {
            auto was_successful = f(key_value.first, key_value.second);
            if (!was_successful) {
                // If the callback returns and unexpected value, bail out
                // of the loop.
                return was_successful;
            }
        }

        // Indicate successful iteration.
        return {};
    }

    // Optional, define TextMapReader::LookupKey to allow for faster extraction.
    opentracing::expected<opentracing::string_view> LookupKey(
            opentracing::string_view key) const override {
        auto iter = data.find(key);
        if (iter != data.end()) {
            return opentracing::make_unexpected(
                    opentracing::key_not_found_error);
        }
        return opentracing::string_view{iter->second};
    }

    const std::unordered_map<std::string, std::string>& data;
};

ParentSpan OpenTracingThread::decodeAsTextMap(const std::string& context) {
    std::unordered_map<std::string, std::string> data;
    std::istringstream istr(context);
    std::string token;
    while (std::getline(istr, token, '&')) {
        size_t pos = token.find('=');
        data[token.substr(0, pos)] = token.substr(pos + 1);
    }

    CustomCarrierReader carrier{data};
    return tracer->Extract(carrier);
}

ParentSpan OpenTracingThread::decodeAsBinary(const std::string& context) {
    std::istringstream istr(context);
    return tracer->Extract(istr);
}

#endif

void OpenTracingThread::pushOne(
        std::chrono::system_clock::time_point system_now,
        std::chrono::steady_clock::time_point steady_now,
        const CookieTraceContext& entry) {
#ifdef ENABLE_OPENTRACING
    try {
        ParentSpan parent;

        if (entry.context.find('=') == std::string::npos) {
            // this is most likely a binary encoded blob
            parent = decodeAsBinary(entry.context);
            if (!parent) {
                // fall back to text:
                parent = decodeAsTextMap(entry.context);
            }
        } else {
            // this is most likely a text-map
            parent = decodeAsTextMap(entry.context);
            if (!parent) {
                // fall back to binary blob
                parent = decodeAsBinary(entry.context);
            }
        }

        if (!parent) {
            LOG_WARNING("Failed to parse OpenTracing context");
            return;
        }

        for (const auto& d : entry.traceSpans) {
            // Convert the start time to our system clock
            const auto start =
                    system_now - std::chrono::duration_cast<
                                         std::chrono::system_clock::duration>(
                                         steady_now - d.start);

            std::string text;
            if (d.code == cb::tracing::Code::Request) {
                if (cb::mcbp::is_client_magic(entry.magic)) {
                    text = to_string(cb::mcbp::ClientOpcode(entry.opcode));
                } else {
                    text = to_string(cb::mcbp::ServerOpcode(entry.opcode));
                }
            } else {
                text = to_string(d.code);
            }

            auto span = tracer->StartSpan(text,
                                          {opentracing::ChildOf(parent->get()),
                                           opentracing::StartTimestamp(start)});
            if (span) {
                if (d.code == cb::tracing::Code::Request) {
                    if (!entry.rawKey.empty()) {
                        span->SetTag("key", entry.rawKey);
                    }
                    span->SetTag("opaque", entry.opaque);
                    span->SetTag("span.kind", "server");
                }

                span->Finish(
                        {opentracing::FinishTimestamp(d.start + d.duration)});
            }
        }
    } catch (const std::exception& e) {
        LOG_WARNING("Failed to generate OpenTracing entry: {}", e.what());
    }
#endif
}

OpenTracing::OpenTracing(const OpenTracingConfig& config) {
#ifdef ENABLE_OPENTRACING
    if (config.module.empty()) {
        throw std::runtime_error(
                "Configuration error: module must be specified");
    }

    LOG_INFO("Loading OpenTracing module: {}", config.module);
    std::string error;
    handle = opentracing::DynamicallyLoadTracingLibrary(config.module.c_str(),
                                                        error);
    if (!handle) {
        std::string msg = "Failed to load OpenTracing library: \"" +
                          config.module + "\": " + error;
        throw std::runtime_error(msg);
    }

    auto& tracer_factory = handle->tracer_factory();
    auto tracer_maybe = tracer_factory.MakeTracer(config.config.c_str(), error);
    if (!tracer_maybe) {
        std::string msg = "Failed to create OpenTracing tracer from \"" +
                          config.module + "\": " + error;
        throw std::runtime_error(msg);
    }
    tracer = *tracer_maybe;
#endif
}

void OpenTracing::pushTraceLog(CookieTraceContext&& context) {
    if (isEnabled()) {
        instance->push(context);
    }
}

void OpenTracing::updateConfig(const OpenTracingConfig& config) {
#ifdef ENABLE_OPENTRACING
    static std::mutex lock;
    std::lock_guard<std::mutex> guard(lock);

    if (!config.enabled) {
        enabled.store(false, std::memory_order_release);
        return;
    }

    // We want to enable tracing.. Only create one if we don't have one
    if (!instance) {
        try {
            instance = std::make_unique<OpenTracingThread>(config);
            dynamic_cast<OpenTracingThread*>(instance.get())->start();
        } catch (const std::exception& e) {
            LOG_ERROR("Failed to create OpenTracing: {}", e.what());
            return;
        }
    }

    enabled.store(true, std::memory_order_release);
#endif
}

void OpenTracing::shutdown() {
    if (!instance) {
        return;
    }

    LOG_INFO("Shutting down OpenTracing");
    auto thread = dynamic_cast<OpenTracingThread*>(instance.get());
    if (thread == nullptr) {
        throw std::runtime_error(
                "OpenTracing::shutdown: expected instance to be "
                "OpenTracingThread");
    }

    thread->stop();
    thread->waitForState(cb::ThreadState::Zombie);
    instance.reset();
}
