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

        friend bool operator==(const Sasl& lhs, const Sasl& rhs) {
            return lhs.mechanism == rhs.mechanism &&
                   lhs.username == rhs.username && lhs.password == rhs.password;
        }
        friend bool operator!=(const Sasl& lhs, const Sasl& rhs) {
            return !(lhs == rhs);
        }
    };

    struct Tls {
        /// The certificate to use
        std::string cert;
        /// The key to use
        std::string key;
        /// The CA store to use
        std::string ca_store;
        /// The passphrase to decode the
        std::string passphrase;
        friend bool operator==(const Tls& lhs, const Tls& rhs) {
            return lhs.cert == rhs.cert && lhs.key == rhs.key &&
                   lhs.ca_store == rhs.ca_store &&
                   lhs.passphrase == rhs.passphrase;
        }
        friend bool operator!=(const Tls& lhs, const Tls& rhs) {
            return !(lhs == rhs);
        }
    };

    friend bool operator==(const DownloadProperties& lhs,
                           const DownloadProperties& rhs) {
        return lhs.hostname == rhs.hostname && lhs.port == rhs.port &&
               lhs.bucket == rhs.bucket && lhs.sasl == rhs.sasl &&
               lhs.tls == rhs.tls;
    }
    friend bool operator!=(const DownloadProperties& lhs,
                           const DownloadProperties& rhs) {
        return !(lhs == rhs);
    }

    /// The hostname to connect to
    std::string hostname;

    /// The port number on the server
    uint16_t port = 0;

    /// The bucket to connect to
    std::string bucket;

    static constexpr std::size_t DefaultFsyncInterval = 50_MiB;
    /// The number of bytes between each call to fsync
    std::size_t fsync_interval = DefaultFsyncInterval;

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