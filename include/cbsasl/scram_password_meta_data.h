/*
 *    Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <cbcrypto/digest.h>
#include <nlohmann/json_fwd.hpp>
#include <string>
#include <vector>

namespace cb::sasl::pwdb {

/**
 * The ScramPasswordMetaData struct keeps track of the properties
 * needed to perform authentication over SCRAM
 */
class ScramPasswordMetaData {
public:
    explicit ScramPasswordMetaData(const nlohmann::json& obj);

    /// salt (kept base64 encoded in memory) as it is only to the
    /// client
    std::string salt;

    struct Keys {
        Keys(std::string stored, std::string server)
            : stored_key(std::move(stored)), server_key(std::move(server)) {
        }
        /// stored key kept in raw format
        std::string stored_key;
        /// server key kept in raw format
        std::string server_key;
    };
    std::vector<Keys> keys;

    /// number of iterations
    std::size_t iteration_count = 0;
};

/// Dump the object to JSON (used in unit tests)
void to_json(nlohmann::json& json, const ScramPasswordMetaData& spmd);

} // namespace cb::sasl::pwdb
