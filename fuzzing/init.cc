/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "init.h"

#include <fmt/format.h>
#include <folly/portability/GTest.h>
#include <chrono>
#include <cstdio>

#ifdef HAVE_FUZZTEST
#include <fuzztest/init_fuzztest.h>
#endif

#ifdef _POSIX_VERSION
#include <fcntl.h>
#endif

namespace cb::fuzzing {

void initialize(int argc, char** argv) {
#ifdef HAVE_FUZZTEST
    auto originalFilter = GTEST_FLAG_GET(filter);
    const bool isWildcardFilter =
            originalFilter.find_first_of("*:-") == std::string::npos;
    // Initialise fuzztest. This should match fuzztest_gtest_main.cc.
    fuzztest::ParseAbslFlags(argc, argv);
    fuzztest::InitFuzzTest(&argc, &argv);
    // FuzzTest will extend the filter with a negative filter to remove anything
    // that is not a fuzz test. This generates a giant list which is printed,
    // and it not necessary unless the filter matches a non-fuzz test.
    if (isWildcardFilter) {
        GTEST_FLAG_SET(filter, originalFilter);
    }
    // When specified, log to a file.
    if (getenv("CB_FUZZTEST_LOG_FILE")) {
        auto fileSuffix =
                isWildcardFilter
                        ? originalFilter
                        : std::to_string(std::chrono::system_clock::now()
                                                 .time_since_epoch()
                                                 .count());
        // Log to a file with name of the gtest filter.
        auto file = fmt::format("{}_fuzztest_{}.log", argv[0], fileSuffix);
        // Open the file and use dup2() to redirect stdout and stderr to it.
        auto fd = ::open(file.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
        if (fd != -1) {
            fmt::print("Redirecting stdout & stderr to {}\n", file);
            std::fflush(stdout);
            ::dup2(fd, STDOUT_FILENO);
            ::dup2(fd, STDERR_FILENO);
        }
    }
#endif
}

} // namespace cb::fuzzing
