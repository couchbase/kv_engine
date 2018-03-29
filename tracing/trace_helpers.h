/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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

#include "tracer.h"

// forward declaration
struct server_handle_v1_t;
typedef struct server_handle_v1_t SERVER_HANDLE_V1;

// #define DISABLE_SESSION_TRACING 1

#ifndef DISABLE_SESSION_TRACING

/**
 * Traces a scope
 * Usage:
 *   {
 *     TRACE_SCOPE(api, cookie, "test1");
 *     ....
 *    }
 */
class ScopedTracer {
public:
    ScopedTracer(SERVER_HANDLE_V1* api,
                 const void* ck,
                 const cb::tracing::TraceCode code)
        : api(api), ck(ck), code(code) {
        if (api && ck) {
            api->tracing->begin_trace(ck, code);
        } else {
            api = nullptr;
            ck = nullptr;
        }
    }

    ~ScopedTracer() {
        if (api) {
            api->tracing->end_trace(ck, code);
        }
    }

protected:
    SERVER_HANDLE_V1* api;
    const void* ck;
    cb::tracing::TraceCode code;
};

#define TRACE_SCOPE(api, ck, code) ScopedTracer __st__##__LINE__(api, ck, code)

#else
/**
 * if DISABLE_SESSION_TRACING is set
 * unset all TRACE macros
 */
#define TRACE_SCOPE(api, ck, code)

#endif
