/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "doc_pre_expiry.h"
#include <folly/portability/GTest.h>
#include <gsl/gsl-lite.hpp>
#include <memcached/protocol_binary.h>
#include <memcached/types.h>
#include <xattr/blob.h>

TEST(PreExpiry, EmptyDocument) {
    item_info info{};
    EXPECT_TRUE(document_pre_expiry(info).empty());
}

TEST(PreExpiry, DocumentWithoutXattr) {
    uint8_t blob[10] = {0};
    item_info info{};
    info.value[0].iov_base = static_cast<void*>(blob);
    info.value[0].iov_len = sizeof(blob);
    info.nbytes = sizeof(blob);
    auto rv = document_pre_expiry(info);
    EXPECT_TRUE(rv.empty());
}

TEST(PreExpiry, DocumentWithUserXAttrOnly) {
    cb::xattr::Blob blob;
    blob.set("user", "1");
    auto body = blob.finalize();
    item_info info{};
    info.value[0].iov_base = const_cast<char*>(body.data());
    info.value[0].iov_len = body.size();
    info.nbytes = gsl::narrow<uint32_t>(body.size());
    info.datatype = PROTOCOL_BINARY_DATATYPE_XATTR;
    auto rv = document_pre_expiry(info);
    EXPECT_TRUE(rv.empty());
}

TEST(PreExpiry, DocumentWithSystemXattrOnly) {
    cb::xattr::Blob blob;
    blob.set("_system", "1");
    auto body = blob.finalize();
    item_info info{};
    info.value[0].iov_base = const_cast<char*>(body.data());
    info.value[0].iov_len = body.size();
    info.nbytes = gsl::narrow<uint32_t>(body.size());
    info.datatype = PROTOCOL_BINARY_DATATYPE_XATTR;
    auto rv = document_pre_expiry(info);
    EXPECT_FALSE(rv.empty());
}

TEST(PreExpiry, DocumentWithUserAndSystemXattr) {
    cb::xattr::Blob blob;
    blob.set("_system", "1");
    blob.set("user", "1");
    auto body = blob.finalize();
    item_info info{};
    info.value[0].iov_base = const_cast<char*>(body.data());
    info.value[0].iov_len = body.size();
    info.nbytes = gsl::narrow<uint32_t>(body.size());
    info.datatype = PROTOCOL_BINARY_DATATYPE_XATTR;
    auto rv = document_pre_expiry(info);
    EXPECT_FALSE(rv.empty());
    EXPECT_LT(rv.size(), body.size());
}

TEST(PreExpiry, DocumentWithJsonBodyAndXattrs) {
    cb::xattr::Blob blob;
    blob.set("_system", "1");
    blob.set("user", "1");
    auto xattr = blob.finalize();

    std::string body((const char*)xattr.data(), xattr.size());
    body.append(R"({"foo":"bar"})");

    item_info info{};
    info.value[0].iov_base = const_cast<char*>(body.data());
    info.value[0].iov_len = body.size();
    info.nbytes = gsl::narrow<uint32_t>(body.size());
    info.datatype =
            PROTOCOL_BINARY_DATATYPE_XATTR | PROTOCOL_BINARY_DATATYPE_JSON;
    auto rv = document_pre_expiry(info);
    EXPECT_FALSE(rv.empty());
    EXPECT_LT(rv.size(), body.size());
}
