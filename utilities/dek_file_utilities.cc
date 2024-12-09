/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "dek_file_utilities.h"
#include <fmt/format.h>
#include <charconv>
#include <chrono>
#include <thread>

namespace cb::dek::util {

void iterateKeyFiles(
        std::string_view id,
        const std::filesystem::path& dir,
        const std::function<void(const std::filesystem::path&)>& visitor) {
    const auto pattern = fmt::format("{}.key.", id);
    for (const auto& p : std::filesystem::directory_iterator(dir)) {
        const auto& path = p.path();
        if (path.filename().string().rfind(pattern, 0) == 0) {
            visitor(path);
        }
    }
}

std::optional<std::filesystem::path> locateNewestKeyFile(
        std::string_view id, const std::filesystem::path& dir) {
    std::optional<std::filesystem::path> ret;
    uint64_t highest = 0;

    iterateKeyFiles(id, dir, [&highest, &ret](const auto& path) {
        uint64_t rev = 0;
        auto ext = path.extension().string();
        if (!ext.empty() && ext.front() == '.') {
            ext = ext.substr(1);
        }
        auto [ptr, ec] =
                std::from_chars(ext.data(), ext.data() + ext.size(), rev);
        if ((ptr != ext.data() + ext.size()) || ec != std::errc()) {
            return;
        }
        if (rev >= highest) {
            ret = path;
            highest = rev;
        }
    });

    return ret;
}

std::filesystem::path copyKeyFile(std::string_view id,
                                  const std::filesystem::path& src,
                                  const std::filesystem::path& dest) {
    static constexpr size_t MaxRetries = 20;
    int retries = MaxRetries;

    do {
        if (retries != MaxRetries) {
            std::this_thread::sleep_for(std::chrono::milliseconds{100});
        }
        try {
            const auto source = locateNewestKeyFile(id, src);
            if (source.has_value()) {
                std::filesystem::path target = dest / source->filename();
                std::error_code ec;
                if (copy_file(*source, target, ec)) {
                    return target;
                }
                remove_all(target, ec);
            }
        } catch (const std::exception&) {
        }
    } while (--retries > 0);
    throw std::runtime_error(
            "cb::dek::util::copyKeyFile: failed to copy key file");
}

} // namespace cb::dek::util
