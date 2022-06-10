/*
 *    Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <cbsasl/scram_password_meta_data.h>
#include <platform/base64.h>

namespace cb::sasl::pwdb {

ScramPasswordMetaData::ScramPasswordMetaData(const nlohmann::json& obj) {
    if (!obj.is_object()) {
        throw std::invalid_argument(
                "ScramPasswordMetaData(): must be an object");
    }

    for (const auto& [label, value] : obj.items()) {
        if (label == "stored_key") {
            stored_key = cb::base64::decode(value.get<std::string>());
        } else if (label == "server_key") {
            server_key = cb::base64::decode(value.get<std::string>());
        } else if (label == "iterations") {
            iteration_count = value.get<std::size_t>();
        } else if (label == "salt") {
            salt = value.get<std::string>();
            // verify that it is a legal base64 encoding
            cb::base64::decode(value.get<std::string>());
        } else {
            throw std::invalid_argument(
                    "ScramPasswordMetaData(): Invalid attribute: \"" + label +
                    "\"");
        }
    }

    if (stored_key.empty()) {
        throw std::invalid_argument(
                "ScramPasswordMetaData(): stored_key must be present");
    }
    if (server_key.empty()) {
        throw std::invalid_argument(
                "ScramPasswordMetaData(): server_key must be present");
    }
    if (salt.empty()) {
        throw std::invalid_argument(
                "ScramPasswordMetaData(): salt must be present");
    }
    if (iteration_count == 0) {
        throw std::invalid_argument(
                "ScramPasswordMetaData(): iteration_count must be present");
    }
}

ScramPasswordMetaData::ScramPasswordMetaData(const nlohmann::json& obj,
                                             cb::crypto::Algorithm algorithm) {
    if (!obj.is_object()) {
        throw std::invalid_argument(
                "ScramPasswordMetaData(): must be an object");
    }
    std::string hash;

    for (const auto& [label, value] : obj.items()) {
        if (label == "h") {
            hash = value.get<std::string>();
        } else if (label == "i") {
            iteration_count = value.get<std::size_t>();
        } else if (label == "s") {
            salt = value.get<std::string>();
        } else {
            throw std::invalid_argument(
                    "ScramPasswordMetaData(): Invalid attribute: \"" + label +
                    "\"");
        }
    }

    if (salt.empty()) {
        throw std::invalid_argument(
                "ScramPasswordMetaData(): salt must be present");
    }
    if (iteration_count == 0) {
        throw std::invalid_argument(
                "ScramPasswordMetaData(): iteration_count must be present");
    }
    if (hash.empty()) {
        throw std::invalid_argument(
                "ScramPasswordMetaData(): hash must be present");
    }

    const auto password = Couchbase::Base64::decode(hash);
    server_key = cb::crypto::HMAC(algorithm, password, "Server Key");
    stored_key = cb::crypto::digest(
            algorithm, cb::crypto::HMAC(algorithm, password, "Client Key"));
}

nlohmann::json ScramPasswordMetaData::to_json() const {
    return {{"iterations", iteration_count},
            {"salt", salt},
            {"server_key", cb::base64::encode(server_key)},
            {"stored_key", cb::base64::encode(stored_key)}};
}

} // namespace cb::sasl::pwdb
