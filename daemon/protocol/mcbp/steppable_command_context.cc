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
#include "steppable_command_context.h"
#include "executors.h"

#include <daemon/buckets.h>
#include <daemon/connection.h>
#include <daemon/cookie.h>
#include <daemon/front_end_thread.h>
#include <daemon/memcached.h>
#include <daemon/stats.h>
#include <logger/logger.h>
#include <hdrhistogram/hdrhistogram.h>
#include <platform/histogram.h>
#include <platform/scope_timer.h>

SteppableCommandContext::SteppableCommandContext(Cookie& cookie_)
    : cookie(cookie_), connection(cookie.getConnection()) {
}

void SteppableCommandContext::drive() {
    auto ret = cookie.swapAiostat(cb::engine_errc::success);

    if (ret == cb::engine_errc::success) {
        try {
            ret = step();
        } catch (const cb::engine_error& error) {
            if (error.code() != cb::engine_errc::would_block) {
                LOG_WARNING_CTX("SteppableCommandContext::drive",
                                {"conn_id", connection.getId()},
                                {"description", connection.getDescription()},
                                {"error", error.what()});
            }
            ret = cb::engine_errc(error.code().value());
        }

        if (ret == cb::engine_errc::locked ||
            ret == cb::engine_errc::locked_tmpfail) {
            STATS_INCR(&connection, lock_errors);
        }
    }

    if (ret != cb::engine_errc::success) {
        handle_executor_status(cookie, ret);
    }
}

void SteppableCommandContext::setDatatypeJSONFromValue(
        const std::string_view value,
        protocol_binary_datatype_t& datatype) const {
    // Determine if document is JSON or not. We do not trust what the client
    // sent - instead we check for ourselves.
    if (connection.getThread().isValidJson(cookie, value)) {
        datatype |= PROTOCOL_BINARY_DATATYPE_JSON;
    } else {
        datatype &= ~PROTOCOL_BINARY_DATATYPE_JSON;
    }
}
