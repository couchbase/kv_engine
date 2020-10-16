/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
#pragma once

#include <gsl.h>
#include <nlohmann/json_fwd.hpp>
#include <cstdint>
#include <string>

namespace cb::crypto {
enum class Algorithm { MD5, SHA1, SHA256, SHA512 };

bool isSupported(Algorithm algorithm);

const int MD5_DIGEST_SIZE = 16;
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
                        const std::string& pass,
                        std::string_view salt,
                        unsigned int iterationCount);

/**
 * Generate a digest by using the requested algorithm
 */
std::string digest(Algorithm algorithm, std::string_view data);

enum class Cipher { AES_256_cbc };

Cipher to_cipher(const std::string& str);

/**
 * Encrypt the specified data by using a given cipher
 *
 * @param cipher The cipher to use
 * @param key The key used for encryption
 * @param ivec The IV to use for encryption
 * @param data The Pointer to the data to encrypt
 * @return The encrypted data
 */
std::string encrypt(Cipher cipher,
                    std::string_view key,
                    std::string_view iv,
                    std::string_view data);

/**
 * Encrypt the specified data
 *
 * {
 *    "cipher" : "name of the cipher",
 *    "key" : "base64 of the raw key",
 *    "iv" : "base64 of the raw iv"
 * }
 *
 * @param meta the json description of the encryption to use
 * @param data Pointer to the data to encrypt
 * @param length the length of the data to encrypt
 * @return The encrypted data
 */
std::string encrypt(const nlohmann::json& json, std::string_view data);

/**
 * Decrypt the specified data by using a given cipher
 *
 * @param cipher The cipher to use
 * @param key The key used for encryption
 * @param ivec The IV to use for encryption
 * @param data The data to decrypt
 * @return The decrypted data
 */
std::string decrypt(Cipher cipher,
                    std::string_view key,
                    std::string_view iv,
                    std::string_view data);

/**
 * Decrypt the specified data
 *
 * {
 *    "cipher" : "name of the cipher",
 *    "key" : "base64 of the raw key",
 *    "iv" : "base64 of the raw iv"
 * }
 *
 * @param meta the json description of the encryption to use
 * @param data Pointer to the data to decrypt
 * @param length the length of the data to decrypt
 * @return The decrypted data
 */
std::string decrypt(const nlohmann::json& json, std::string_view data);

} // namespace cb::crypto
