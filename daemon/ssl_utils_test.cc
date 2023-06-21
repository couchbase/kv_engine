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

static const long DefaultMask = (SSL_OP_NO_SSL_MASK | SSL_OP_NO_RENEGOTIATION) &
                                ~(SSL_OP_NO_TLSv1_2 | SSL_OP_NO_TLSv1_3);

TEST(ssl_decode_protocol, EmptyString) {
    EXPECT_EQ(DefaultMask, decode_ssl_protocol(""));
}

TEST(ssl_decode_protocol, TLSv1) {
    try {
        decode_ssl_protocol("TLS 1");
        FAIL() << "TLS 1 is no longer supported!";
    } catch (const std::invalid_argument& e) {
        EXPECT_STREQ("Unknown protocol: TLS 1", e.what());
    }
}

TEST(ssl_decode_protocol, TLSv1_1) {
    try {
        decode_ssl_protocol("TLS 1.1");
        FAIL() << "TLS 1.1 is no longer supported!";
    } catch (const std::invalid_argument& e) {
        EXPECT_STREQ("Unknown protocol: TLS 1.1", e.what());
    }
}

TEST(ssl_decode_protocol, TLSv1_2) {
    const auto TLSv2_mask = DefaultMask;
    const auto val = "TLS 1.2";
    EXPECT_EQ(TLSv2_mask, decode_ssl_protocol(val))
            << "Failed to decode: " << val;
}

TEST(ssl_decode_protocol, TLSv1_3) {
    const auto TLSv3_mask = DefaultMask | SSL_OP_NO_TLSv1_2;
    const auto val = "TLS 1.3";
    EXPECT_EQ(TLSv3_mask, decode_ssl_protocol(val))
            << "Failed to decode: " << val;
}
