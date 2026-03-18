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
#include <nlohmann/json.hpp>

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
        if (path.filename().string().starts_with(pattern)) {
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
    if (!exists(src)) {
        throw std::runtime_error(
                fmt::format("cb::dek::util::copyKeyFile: source directory '{}' "
                            "does not exist",
                            src.string()));
    }

    if (!exists(dest)) {
        std::error_code ec;
        create_directories(dest, ec);
        if (ec) {
            throw std::runtime_error(fmt::format(
                    "cb::dek::util::copyKeyFile: failed to create destination "
                    "directory '{}': {}",
                    dest.string(),
                    ec.message()));
        }
    }

    static constexpr int MaxRetries = 20;
    int retries = MaxRetries;

    nlohmann::json errors = nlohmann::json::array();
    do {
        if (retries != MaxRetries) {
            std::this_thread::sleep_for(std::chrono::milliseconds{100});
        }
        try {
            const auto source = locateNewestKeyFile(id, src);
            if (source.has_value()) {
                auto target = dest / source->filename();
                try {
                    if (copy_file(*source, target)) {
                        return target;
                    }
                } catch (const std::exception& exception) {
                    errors.emplace_back(fmt::format("copy_file '{}': {}",
                                                    source->string(),
                                                    exception.what()));
                }

                std::error_code ec;
                remove_all(target, ec);
            } else {
                errors.emplace_back("no key file found");
            }
        } catch (const std::exception& exception) {
            errors.emplace_back(exception.what());
        }
    } while (--retries > 0);
    throw std::runtime_error(
            fmt::format("cb::dek::util::copyKeyFile: failed to copy key file "
                        "'{}' errors: {}",
                        id,
                        errors.dump()));
}

} // namespace cb::dek::util
