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
    EXPECT_EQ(DefaultMask, decode_ssl_protocol("TLS 1"))
            << "Failed to decode \"TLS 1\"";
}

TEST(ssl_decode_protocol, TLSv1_1) {
    const auto TLSv1_mask = DefaultMask | SSL_OP_NO_TLSv1;
    const auto val = "TLS 1.1";
    EXPECT_EQ(TLSv1_mask, decode_ssl_protocol(val))
            << "Failed to decode: " << val;
}

TEST(ssl_decode_protocol, TLSv1_2) {
    const auto TLSv2_mask = DefaultMask | SSL_OP_NO_TLSv1 | SSL_OP_NO_TLSv1_1;
    const auto val = "TLS 1.2";
    EXPECT_EQ(TLSv2_mask, decode_ssl_protocol(val))
            << "Failed to decode: " << val;
}

TEST(ssl_decode_protocol, TLSv1_3) {
    const auto TLSv3_mask = DefaultMask | SSL_OP_NO_TLSv1 | SSL_OP_NO_TLSv1_1 |
                            SSL_OP_NO_TLSv1_2;
    const auto val = "TLS 1.3";
    EXPECT_EQ(TLSv3_mask, decode_ssl_protocol(val))
            << "Failed to decode: " << val;
}
