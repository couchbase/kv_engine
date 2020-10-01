/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020 Couchbase, Inc.
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
#include "ssl_utils.h"
#include <folly/portability/GTest.h>
#include <openssl/ssl.h>

static const long DefaultMask =
        SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3 | SSL_OP_NO_RENEGOTIATION;

TEST(ssl_decode_protocol, EmptyString) {
    EXPECT_EQ(DefaultMask, decode_ssl_protocol(""));
}

TEST(ssl_decode_protocol, TLSv1) {
    const auto TLSv1_mask = DefaultMask;
    for (const auto& val :
         std::vector<std::string>{{"TLSv1", "tlsv1", "tLsV1"}}) {
        EXPECT_EQ(TLSv1_mask, decode_ssl_protocol(val))
                << "Failed to decode: " << val;
    }
}

TEST(ssl_decode_protocol, TLSv1_1) {
    const auto TLSv1_mask = DefaultMask | SSL_OP_NO_TLSv1;
    for (const auto& val : std::vector<std::string>{{"TLSv1.1",
                                                     "TLSv1_1",
                                                     "tlsv1.1",
                                                     "tlsv1_1",
                                                     "tLsV1.1",
                                                     "tLsV1_1"}}) {
        EXPECT_EQ(TLSv1_mask, decode_ssl_protocol(val))
                << "Failed to decode: " << val;
    }
}

TEST(ssl_decode_protocol, TLSv1_2) {
    const auto TLSv2_mask = DefaultMask | SSL_OP_NO_TLSv1 | SSL_OP_NO_TLSv1_1;
    for (const auto& val : std::vector<std::string>{{"TLSv1.2",
                                                     "TLSv1_2",
                                                     "tlsv1.2",
                                                     "tlsv1_2",
                                                     "tLsV1.2",
                                                     "tLsV1_2"}}) {
        EXPECT_EQ(TLSv2_mask, decode_ssl_protocol(val))
                << "Failed to decode: " << val;
    }
}

TEST(ssl_decode_protocol, TLSv1_3) {
    const auto TLSv3_mask = DefaultMask | SSL_OP_NO_TLSv1 | SSL_OP_NO_TLSv1_1 |
                            SSL_OP_NO_TLSv1_2;
    for (const auto& val : std::vector<std::string>{{"TLSv1.3",
                                                     "TLSv1_3",
                                                     "tlsv1.3",
                                                     "tlsv1_3",
                                                     "tLsV1.3",
                                                     "tLsV1_3"}}) {
        EXPECT_EQ(TLSv3_mask, decode_ssl_protocol(val))
                << "Failed to decode: " << val;
    }
}
