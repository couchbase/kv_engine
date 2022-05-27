/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <gsl/gsl-lite.hpp>
#include <nlohmann/json_fwd.hpp>
#include <cstdint>
#include <string>

namespace cb::crypto {
enum class Algorithm { SHA1, SHA256, SHA512 };

const int SHA1_DIGEST_SIZE = 20;
const int SHA256_DIGEST_SIZE = 32;
const int SHA512_DIGEST_SIZE = 64;

/**
 * Generate a HMAC digest of the key and data by using the given
 * algorithm
 *
 * @throws std::invalid_argument - unsupported algorithm
 *         std::runtime_error - Failures generating the HMAC
 */
std::string HMAC(Algorithm algorithm,
                 std::string_view key,
                 std::string_view data);

/**
 * Generate a PBKDF2_HMAC digest of the key and data by using the given
 * algorithm
 *
 * @throws std::invalid_argument - unsupported algorithm
 *         std::runtime_error - Failures generating the HMAC
 */
std::string PBKDF2_HMAC(Algorithm algorithm,
                        std::string_view pass,
                        std::string_view salt,
                        unsigned int iterationCount);

/**
 * Generate a digest by using the requested algorithm
 */
std::string digest(Algorithm algorithm, std::string_view data);

} // namespace cb::crypto
