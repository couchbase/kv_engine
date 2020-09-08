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

#include "opentelemetry.h"

#include "connection.h"
#include "cookie_trace_context.h"
#include "log_macros.h"

#include <mcbp/protocol/request.h>
#include <sstream>

#include <platform/thread.h>

namespace cb {
using Thread = Couchbase::Thread;
using ThreadState = Couchbase::ThreadState;
} // namespace cb

std::unique_ptr<OpenTelemetry> OpenTelemetry::instance;

class OpenTelemetryThread : public OpenTelemetry, public cb::Thread {
public:
    explicit OpenTelemetryThread(const OpenTelemetryConfig& config)
        : OpenTelemetry(config), cb::Thread("mcd:trace") {
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

void OpenTelemetryThread::run() {
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
        std::vector<CookieTraceContext> entries{};
        std::swap(contexts, entries);

        // Release the lock to the internal variables while I process the
        // batch of trace elements
        lock.unlock();

        if (isEnabled()) {
            // Unfortunately OpenTelemetry want system clock, and we operate
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

void OpenTelemetryThread::pushOne(
        std::chrono::system_clock::time_point system_now,
        std::chrono::steady_clock::time_point steady_now,
        const CookieTraceContext& entry) {
}

OpenTelemetry::OpenTelemetry(const OpenTelemetryConfig& config) {
}

void OpenTelemetry::pushTraceLog(CookieTraceContext&& context) {
    if (isEnabled()) {
        instance->push(context);
    }
}

void OpenTelemetry::updateConfig(const OpenTelemetryConfig& config) {
}

void OpenTelemetry::shutdown() {
    if (!instance) {
        return;
    }

    LOG_INFO("Shutting down OpenTelemetry");
    auto thread = dynamic_cast<OpenTelemetryThread*>(instance.get());
    if (thread == nullptr) {
        throw std::runtime_error(
                "OpenTelemetry::shutdown: expected instance to be "
                "OpenTelemetryThread");
    }

    thread->stop();
    thread->waitForState(cb::ThreadState::Zombie);
    instance.reset();
}
