/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "flush_command_context.h"
#include "engine_wrapper.h"
#include <daemon/cookie.h>
#include <daemon/mcaudit.h>
#include <logger/logger.h>

cb::engine_errc FlushCommandContext::flushing() {
    state = State::Done;
    return bucket_flush(cookie);
}

void FlushCommandContext::done() {
    if (!connection.isInternal()) {
        audit_bucket_flush(cookie, connection.getBucket().name);
    }
    get_thread_stats(&connection)->cmd_flush++;
    cookie.sendResponse(cb::mcbp::Status::Success);
}

FlushCommandContext::FlushCommandContext(Cookie& cookie)
    : SteppableCommandContext(cookie), state(State::Flushing) {
    LOG_INFO("{}: flush b:{}", connection.getId(), connection.getBucket().name);
}
