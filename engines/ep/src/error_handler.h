/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "ep_types.h"
#include <spdlog/logger.h>

namespace cb {

ErrorHandlingMethod getErrorHandlingMethod(std::string_view);

/**
 * Handle an error as described by the ErrorHandlingMethod
 *
 * @param logger spdlog::logger reference to log to
 * @param logLevel log level at which we should log
 * @param msg message to be used when handling error
 * @param method method by which to handle the error
 */
void handleError(spdlog::logger& logger,
                 spdlog::level::level_enum logLevel,
                 std::string_view msg,
                 ErrorHandlingMethod method);
} // namespace cb
