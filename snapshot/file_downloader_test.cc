/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "file_downloader.h"
#include "manifest.h"

#include <folly/ScopeGuard.h>
#include <folly/portability/GTest.h>
#include <platform/dirutils.h>
#include <protocol/connection/client_connection.h>

TEST(FileDownloaderTest, FailFastIfFileTooBig) {
    std::filesystem::path tmp = cb::io::mkdtemp("FileDownloaderTest");
    auto guard =
            folly::makeGuard([&tmp] { cb::io::rmrf(tmp.generic_string()); });

    cb::snapshot::FileDownloader downloader(
            {},
            tmp,
            "my-uuid",
            1024, // fsync interval
            1024, // write size
            0, // checksum
            true, // fail fast
            [](auto level, auto message, auto json) {
                EXPECT_EQ(level, spdlog::level::warn);
                EXPECT_EQ(message, "Not enough disk space to download file");
                EXPECT_TRUE(json.contains("path")) << json.dump();
                EXPECT_TRUE(json.contains("size")) << json.dump();
                EXPECT_TRUE(json.contains("available_space")) << json.dump();
            },
            [](auto bytes) {
                FAIL() << "Should not have called stats callback";
            });

    const auto space_info = std::filesystem::space(tmp);
    // Create a file info with size larger than the available space to trigger
    // the warning log and early return
    cb::snapshot::FileInfo info("file1", space_info.available + 1_GiB, 1);

    auto ret = downloader.download(info);
    EXPECT_EQ(cb::engine_errc::too_big, ret);
}
