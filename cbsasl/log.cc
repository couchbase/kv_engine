/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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
#include <cbsasl/cbsasl.h>
#include "cbsasl/cbsasl_internal.h"

#include <iostream>
#include <relaxed_atomic.h>

namespace cb {
namespace sasl {
namespace logging {

Couchbase::RelaxedAtomic<LogCallback> callback;

void log(cbsasl_conn_t& connection, Level level, const std::string& message) {
    auto logger = callback.load();
    if (logger == nullptr) {
        return;
    }

    if (level == Level::Error) {
        // We need to generate UUID
        auto& uuid = connection.get_uuid();
        std::string full;
        full.reserve(message.size() + uuid.size() + 10);
        full = "UUID:[" + uuid + "] " + message;
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
} // namespace logging
} // namespace sasl
} // namespace cb
