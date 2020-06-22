/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc.
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

#include <memcached/tracer.h>
#include <platform/sized_buffer.h>
#include <string>

namespace cb {
namespace mcbp {
enum class Magic : uint8_t;
} // namespace mcbp
} // namespace cb

/**
 * The CookieTraceContext contains the information we need to keep around
 * to generate an OpenTracing dump for a given cookie (command)
 */
struct CookieTraceContext {
    CookieTraceContext(cb::mcbp::Magic magic,
                       uint8_t opcode,
                       uint32_t opaque,
                       cb::const_byte_buffer rawKey,
                       std::string context,
                       std::vector<cb::tracing::Span> spans)
        : magic(magic),
          opcode(opcode),
          opaque(opaque),
          rawKey(std::string{reinterpret_cast<const char*>(rawKey.data()),
                             rawKey.size()}),
          context(std::move(context)),
          traceSpans(std::move(spans)) {
    }

    const cb::mcbp::Magic magic;
    const uint8_t opcode;
    const uint32_t opaque;
    const std::string rawKey;
    const std::string context;
    const std::vector<cb::tracing::Span> traceSpans;
};
