/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
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
