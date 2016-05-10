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

#include <cbsasl/visibility.h>
#include <cstdint>
#include <string>
#include <vector>

namespace Couchbase {
    namespace Crypto {
        enum class Algorithm {
            MD5,
            SHA1,
            SHA256,
            SHA512
        };

        CBSASL_PUBLIC_API
        bool isSupported(const Algorithm algorithm);

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
        CBSASL_PUBLIC_API
        std::vector<uint8_t> HMAC(const Algorithm algorithm,
                                  const std::vector<uint8_t>& key,
                                  const std::vector<uint8_t>& data);

        /**
         * Generate a PBKDF2_HMAC digest of the key and data by using the given
         * algorithm
         *
         * @throws std::invalid_argument - unsupported algorithm
         *         std::runtime_error - Failures generating the HMAC
         */
        CBSASL_PUBLIC_API
        std::vector<uint8_t> PBKDF2_HMAC(const Algorithm algorithm,
                                         const std::string& pass,
                                         const std::vector<uint8_t>& salt,
                                         unsigned int iterationCount);

        /**
         * Generate a digest by using the requested algorithm
         */
        CBSASL_PUBLIC_API
        std::vector<uint8_t> digest(const Algorithm algorithm,
                                    const std::vector<uint8_t>& data);
    }
}
