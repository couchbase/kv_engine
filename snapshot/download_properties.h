/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <nlohmann/json_fwd.hpp>
#include <platform/byte_literals.h>
#include <optional>
#include <string>

namespace cb::snapshot {

/**
 * The DownloadProperties is structure containing the properties used
 * for the DownloadSnapshot command. It is transmitted to the server
 * in JSON. This class exists to keep the reading/writing of the actual
 * JSON in a single place
 */
struct DownloadProperties {
    struct Sasl {
        /// The mechanism to use. Leave empty to use SASL_LIST_MECH
        std::string mechanism;
        /// The username to authenticate with
        std::string username;
        /// The users password
        std::string password;

        bool operator==(const Sasl& other) const = default;
    };

    struct Tls {
        /// The certificate to use
        std::string cert;
        /// The key to use
        std::string key;
        /// The CA store to use
        std::string ca_store;
        /// Peer certificate verification
        bool ssl_peer_verify = true;
        /// The passphrase to decode the
        std::string passphrase;
        bool operator==(const Tls&) const = default;
    };

    bool operator==(const DownloadProperties&) const = default;

    /// The hostname to connect to
    std::string hostname;

    /// The port number on the server
    uint16_t port = 0;

    /// The bucket to connect to
    std::string bucket;

    /// Optional: The number of bytes between each call to fsync (server
    /// provides a default value)
    std::optional<std::size_t> fsync_interval;

    /// Optional:The number of bytes to write in each chunk (server provides a
    /// default value)
    std::optional<std::size_t> write_size;

    /// Optional: Allow to fail fast if there isn't disk space to download the
    /// file. If set to true, the downloader will check if there is enough
    /// disk space before attempting to download the file and fail
    /// immediately if there isn't enough space. If set to false, the
    /// downloader will try to download the file and only fail if the
    /// download fails due to lack of disk space. This is not a
    /// guarantee that the download will fail if there isn't enough
    /// disk space, but it can help avoid starting downloads that are
    /// likely to fail. Default behavior is true
    std::optional<bool> allow_fail_fast;

    /// The properties to use IF SASL should be used (should not be
    /// specified if cert based auth is used)
    std::optional<Sasl> sasl;

    /// The TLS properties to use if TLS is to used
    std::optional<Tls> tls;
};

void to_json(nlohmann::json& json, const DownloadProperties& prop);
void to_json(nlohmann::json& json, const DownloadProperties::Sasl& sasl);
void to_json(nlohmann::json& json, const DownloadProperties::Tls& tls);
void from_json(const nlohmann::json& json, DownloadProperties& prop);
void from_json(const nlohmann::json& json, DownloadProperties::Sasl& sasl);
void from_json(const nlohmann::json& json, DownloadProperties::Tls& tls);

} // namespace cb::snapshot