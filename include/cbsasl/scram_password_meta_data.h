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

#include <cbsasl/cbcrypto.h>
#include <nlohmann/json_fwd.hpp>
#include <string>

namespace cb::sasl::pwdb {

/**
 * The ScramPasswordMetaData struct keeps track of the properties
 * needed to perform authentication over SCRAM
 */
class ScramPasswordMetaData {
public:
    explicit ScramPasswordMetaData(const nlohmann::json& obj);

    /// Until ns_server supplies stored_key and server_key we need
    /// to generate them off the salted password hash.
    /// This code should be deleted
    ScramPasswordMetaData(const nlohmann::json& obj,
                          cb::crypto::Algorithm algorithm);

    /// Dump the object to JSON (used in unit tests)
    nlohmann::json to_json() const;

    /// salt (kept base64 encoded in memory)
    std::string salt;
    /// stored key kept in raw format
    std::string stored_key;
    /// server key kept in raw format
    std::string server_key;
    /// number of iterations
    std::size_t iteration_count = 0;
};

} // namespace cb::sasl::pwdb
