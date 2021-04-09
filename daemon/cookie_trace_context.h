/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <memcached/tracer.h>
#include <platform/sized_buffer.h>
#include <string>

namespace cb::mcbp {
enum class Magic : uint8_t;
} // namespace cb::mcbp

/**
 * The CookieTraceContext contains the information we need to keep around
 * to generate an OpenTelemetry dump for a given cookie (command)
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
