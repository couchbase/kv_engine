/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "error_handler.h"

#include <folly/lang/Assume.h>
#include <logger/logger.h>

namespace cb {

ErrorHandlingMethod getErrorHandlingMethod(std::string_view str) {
    using namespace std::string_view_literals;
    if (str == "abort"sv) {
        return ErrorHandlingMethod::Abort;
    }
    if (str == "log"sv) {
        return ErrorHandlingMethod::Log;
    }
    if (str == "throw"sv) {
        return ErrorHandlingMethod::Throw;
    }
    throw std::logic_error("Invalid error handler parameter");
}

void handleError(cb::logger::Logger& logger,
                 spdlog::level::level_enum logLevel,
                 std::string_view msg,
                 ErrorHandlingMethod method) {
    // Always worth logging
    logger.log(logLevel, "{}", msg);

    switch (method) {
    case ErrorHandlingMethod::Log:
        return;
    case ErrorHandlingMethod::Abort:
        cb::logger::flush();
        std::abort();
    case ErrorHandlingMethod::Throw:
        throw std::logic_error(std::string(msg));
    }
}
} // namespace cb
