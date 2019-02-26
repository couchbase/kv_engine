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
#include "log_macros.h"
#include "opentracing_config.h"

#include <mcbp/protocol/request.h>
#include <nlohmann/json.hpp>

#ifdef ENABLE_OPENTRACING
std::atomic_bool OpenTracing::enabled;
#endif

std::unique_ptr<OpenTracing> OpenTracing::instance;

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

void OpenTracing::push(const Cookie& cookie) {
#ifdef ENABLE_OPENTRACING
    const auto& context = cookie.getOpenTracingContext();
    if (context.empty()) {
        return;
    }

    std::istringstream istr(context);
    auto parent = tracer->Extract(istr);
    if (!parent) {
        const auto& c = cookie.getConnection();
        LOG_WARNING("{}: Failed to parse OpenTracing context provided by {}",
                    c.getId(),
                    c.getDescription());
        return;
    }

    const auto steady_now = std::chrono::steady_clock::now();
    const auto system_now = std::chrono::system_clock::now();
    const cb::mcbp::Request* request = nullptr;
    if (cookie.getHeader().isRequest()) {
        request = &cookie.getHeader().getRequest();
    }

    try {
        const auto& cookieTracer = cookie.getTracer();
        for (const auto& d : cookieTracer.getDurations()) {
            // Convert the start time to our system clock
            const auto start = system_now - (steady_now - d.start);

            std::string text;

            if (d.code == cb::tracing::TraceCode::REQUEST &&
                request != nullptr) {
                if (cb::mcbp::is_client_magic(request->getMagic())) {
                    text = to_string(request->getClientOpcode());
                } else {
                    text = to_string(request->getServerOpcode());
                }
            } else {
                text = to_string(d.code);
            }

            auto span = tracer->StartSpan(text,
                                          {opentracing::ChildOf(parent->get()),
                                           opentracing::StartTimestamp(start)});
            if (span) {
                if (d.code == cb::tracing::TraceCode::REQUEST &&
                    request != nullptr) {
                    const std::string key = request->getPrintableKey();
                    if (!key.empty()) {
                        span->SetTag("key", request->getPrintableKey());
                    }
                    span->SetTag("opaque", request->getOpaque());
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
            instance = std::make_unique<OpenTracing>(config);
        } catch (const std::exception& e) {
            LOG_ERROR("Failed to create OpenTracing: {}", e.what());
            return;
        }
    }

    enabled.store(true, std::memory_order_release);
#endif
}
