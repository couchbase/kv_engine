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

struct CookieTraceContext;
class OpenTelemetryConfig;

/**
 * The OpenTelemetry class is used to provide access to OpenTelemetry in the
 * prototype. We don't offer a lot of flexibility right now, but allow the
 * user to dynamically enable / disable the functionality (note that we
 * will only load the library the first time, and if you try to change the
 * library after it's been loaded that is ignored).
 */
class OpenTelemetry {
public:
    virtual ~OpenTelemetry() = default;

    /**
     * Update the OpenTelemetry instance with the provided configuration.
     *
     * This _might_ load libraries and create a tracer
     */
    static void updateConfig(const OpenTelemetryConfig& config);

    /**
     * Shut down the tracer and release all allocated resources.
     * Using the object after shutdown results in undefined behavior
     */
    static void shutdown();

    /**
     * Push the trace to the OpenTelemetry module
     */
    static void pushTraceLog(CookieTraceContext&& context);

    /**
     * Is OpenTelemetry configured (and enabled). If built without
     * support for OpenTelemetry this method always returns false.
     *
     * @return true if OpenTelemetry is loaded and enabled, false otherwise
     */
    static bool isEnabled() {
        return false;
    }

    /**
     * Load the dynamic library specified in the configuration and create a
     * tracer with the provided configuration string.
     *
     * Don't create objects directly; use updateConfig instead
     *
     * @throws std::runtime_error if something goes wrong
     */
    explicit OpenTelemetry(const OpenTelemetryConfig& config);

protected:
    static std::unique_ptr<OpenTelemetry> instance;
    virtual void push(CookieTraceContext& context) = 0;
};
