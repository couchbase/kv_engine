/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "configuration_types.h"

#include <fmt/format.h>
#include <charconv>
#include <optional>

namespace cb::config {

FeatureVersion::FeatureVersion(uint8_t major, uint8_t minor)
    : major_version(major), minor_version(minor) {
}

static std::optional<uint8_t> parseUint8(std::string_view num) {
    uint8_t number = 0;
    auto [ptr, ec] =
            std::from_chars(num.data(), num.data() + num.size(), number);
    if (ec != std::errc()) {
        return std::nullopt;
    }
    return number;
}

static std::pair<uint8_t, uint8_t> parseVersionComponents(
        std::string_view version) {
    std::optional<uint8_t> major;
    std::optional<uint8_t> minor;
    const auto sep = version.find('.');
    if (sep != std::string_view::npos) {
        major = parseUint8(version.substr(0, sep));
        minor = parseUint8(version.substr(sep + 1));
    }
    if (!major || !minor || sep + 2 != version.size()) {
        throw std::invalid_argument("Invalid FeatureVersion: " +
                                    std::string(version));
    }
    return {*major, *minor};
}

FeatureVersion FeatureVersion::max() {
    return {std::numeric_limits<uint8_t>::max(),
            std::numeric_limits<uint8_t>::max()};
}

FeatureVersion FeatureVersion::parse(std::string_view version) {
    auto [major, minor] = parseVersionComponents(version);
    return {major, minor};
}

std::strong_ordering FeatureVersion::operator<=>(
        const FeatureVersion&) const noexcept = default;

bool FeatureVersion::operator==(const FeatureVersion& other) const noexcept =
        default;

std::string format_as(const FeatureVersion& version) {
    if (version == FeatureVersion::max()) {
        return "<max>";
    }
    return fmt::format("{}.{}", version.major_version, version.minor_version);
}

} // namespace cb::config