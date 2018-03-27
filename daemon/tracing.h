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

#pragma once

#include "config.h"

#include "task.h"
#include "utilities/string_utilities.h"

#include <memcached/types.h>
#include <phosphor/phosphor.h>
#include <phosphor/tools/export.h>
#include <platform/processclock.h>
#include <platform/uuid.h>

#include <cstddef>
#include <map>

class Cookie;

/**
 * Initialises Tracing
 */
void initializeTracing();

/**
 * Deinitialises Tracing
 */
void deinitializeTracing();

/**
 * DumpContext holds all the information required for an ongoing
 * trace dump
 */
struct DumpContext {
    DumpContext(phosphor::TraceContext&& _context)
        : context(std::move(_context)),
          json_export(context),
          last_touch(ProcessClock::now()) {
    }

    // Moving is dangerous as json_export contains a reference to
    // context.
    DumpContext(DumpContext&& other) = delete;

    phosphor::TraceContext context;
    phosphor::tools::JSONExport json_export;
    ProcessClock::time_point last_touch;
    std::mutex mutex;
};

/**
 * Aggregate object to hold a map of dumps and a mutex protecting them
 */
struct TraceDumps {
    std::map<cb::uuid::uuid_t, std::unique_ptr<DumpContext>> dumps;
    std::mutex mutex;
};

/**
 * StaleTraceDumpRemover is a periodic task that removes old dumps
 */
class StaleTraceDumpRemover : public PeriodicTask {
public:
    StaleTraceDumpRemover(TraceDumps& traceDumps,
                          std::chrono::seconds period,
                          std::chrono::seconds max_age)
        : PeriodicTask(period), traceDumps(traceDumps), max_age(max_age) {
    }

    Status periodicExecute() override;

private:
    TraceDumps& traceDumps;
    const std::chrono::seconds max_age;
};

/**
 * IOCTL Get callback to get the tracing status
 * @param[out] value Either "enabled" or "disabled" depending on status
 */
ENGINE_ERROR_CODE ioctlGetTracingStatus(Cookie& cookie,
                                        const StrToStrMap& arguments,
                                        std::string& value);

/**
 * IOCTL Get callback to get the last used tracing config
 * @param[out] value The last Phoshor config used (re-encoded as a string)
 */
ENGINE_ERROR_CODE ioctlGetTracingConfig(Cookie& cookie,
                                        const StrToStrMap& arguments,
                                        std::string& value);

/**
 * IOCTL Get callback to create a new dump from the last trace
 * @param[out] value The uuid of the newly created dump
 */
ENGINE_ERROR_CODE ioctlGetTracingBeginDump(Cookie& cookie,
                                           const StrToStrMap&,
                                           std::string& value);

/**
 * IOCTL Get callback to generate and return chunks from the specified dump
 * @param arguments 'id' argument should be given to specify uuid of dump to
          continue
 * @param[out] value The contents of the next chunk (or empty if dump is done)
 */
ENGINE_ERROR_CODE ioctlGetTracingDumpChunk(Cookie& cookie,
                                           const StrToStrMap& arguments,
                                           std::string& value);

/**
 * IOCTL Set callback to clear a tracing dump
 * @param value The uuid of the dump to clear
 */
ENGINE_ERROR_CODE ioctlSetTracingClearDump(Cookie& cookie,
                                           const StrToStrMap& arguments,
                                           const std::string& value);

/**
 * IOCTL Set callback to set the tracing config to use when it starts
 * @param value The Phosphor trace config string to start tracing with
 */
ENGINE_ERROR_CODE ioctlSetTracingConfig(Cookie& cookie,
                                        const StrToStrMap& arguments,
                                        const std::string& value);

/**
 * IOCTL Set callback to start tracing
 */
ENGINE_ERROR_CODE ioctlSetTracingStart(Cookie& cookie,
                                       const StrToStrMap& arguments,
                                       const std::string& value);

/**
 * IOCTL Set callback to stop tracing
 */
ENGINE_ERROR_CODE ioctlSetTracingStop(Cookie& cookie,
                                      const StrToStrMap& arguments,
                                      const std::string& value);
