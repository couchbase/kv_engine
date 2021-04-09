/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include <cbsasl/context.h>
#include <cbsasl/logging.h>

#include <relaxed_atomic.h>
#include <iostream>

namespace cb::sasl::logging {

cb::RelaxedAtomic<LogCallback> callback;

void log(Context* context, Level level, const std::string& message) {
    auto logger = callback.load();
    if (logger == nullptr) {
        return;
    }

    if (context == nullptr) {
        log(level, message);
        return;
    }

    if (level == Level::Error) {
        // We need to generate UUID
        std::string full = "UUID:[" + context->getUuid() + "] " + message;
        callback.load()(level, full);
    } else {
        callback.load()(level, message);
    }
}

void log(Level level, const std::string& message) {
    auto logger = callback.load();
    if (logger == nullptr) {
        return;
    }

    callback.load()(level, message);
}
} // namespace cb::sasl::logging
