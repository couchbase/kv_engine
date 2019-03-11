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
#pragma once

#include <atomic>
#include <memory>

#ifdef ENABLE_OPENTRACING
#include <opentracing/dynamic_load.h>
#endif

class CookieTraceContext;
class OpenTracingConfig;

/**
 * The OpenTracing class is used to provide access to OpenTracing in the
 * prototype. We don't offer a lot of flexibility right now, but allow the
 * user to dynamically enable / disable the functionality (note that we
 * will only load the library the first time, and if you try to change the
 * library after it's been loaded that is ignored).
 */
class OpenTracing {
public:
    virtual ~OpenTracing() = default;

    /**
     * Update the OpenTracing instance with the provided configuration.
     *
     * This _might_ load libraries and create a tracer
     */
    static void updateConfig(const OpenTracingConfig& config);

    /**
     * Shut down the tracer and release all allocated resources.
     * Using the object after shutdown results in undefined behavior
     */
    static void shutdown();

    /**
     * Push the trace to the OpenTracing module
     */
    static void pushTraceLog(CookieTraceContext&& context);

    /**
     * Is OpenTracing configured (and enabled). If built without
     * support for OpenTracing this method always returns false.
     *
     * @return true if OpenTracing is loaded and enabled, false otherwise
     */
    static bool isEnabled() {
#ifdef ENABLE_OPENTRACING
        return enabled.load(std::memory_order_acquire);

#else
        return false;
#endif
    }

    /**
     * Load the dynamic library specified in the configuration and create a
     * tracer with the provided configuration string.
     *
     * Don't create objects directly; use updateConfig instead
     *
     * @throws std::runtime_error if something goes wrong
     */
    explicit OpenTracing(const OpenTracingConfig& config);

protected:
    static std::unique_ptr<OpenTracing> instance;
    virtual void push(CookieTraceContext& context) = 0;

#ifdef ENABLE_OPENTRACING
    static std::atomic_bool enabled;
    opentracing::expected<opentracing::DynamicTracingLibraryHandle> handle;
    std::shared_ptr<opentracing::Tracer> tracer;
#endif
};
