/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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

#include <daemon/doc_pre_expiry.h>
#include <folly/portability/GTest.h>
#include <memcached/protocol_binary.h>
#include <memcached/types.h>
#include <xattr/blob.h>
#include <gsl/gsl>

TEST(PreExpiry, EmptyDocument) {
    item_info info{};
    EXPECT_FALSE(document_pre_expiry(info));
}

TEST(PreExpiry, DocumentWithoutXattr) {
    uint8_t blob[10] = {0};
    item_info info{};
    info.value[0].iov_base = static_cast<void*>(blob);
    info.value[0].iov_len = sizeof(blob);
    info.nbytes = sizeof(blob);
    EXPECT_FALSE(document_pre_expiry(info));
    EXPECT_EQ(PROTOCOL_BINARY_RAW_BYTES, info.datatype);
}

TEST(PreExpiry, DocumentWithUserXAttrOnly) {
    cb::xattr::Blob blob;
    blob.set("user", "1");
    auto body = blob.finalize();
    item_info info{};
    info.value[0].iov_base = static_cast<void*>(body.data());
    info.value[0].iov_len = body.size();
    info.nbytes = gsl::narrow<uint32_t>(body.size());
    info.datatype = PROTOCOL_BINARY_DATATYPE_XATTR;
    EXPECT_FALSE(document_pre_expiry(info));
    EXPECT_EQ(PROTOCOL_BINARY_DATATYPE_XATTR, info.datatype);
}

TEST(PreExpiry, DocumentWithSystemXattrOnly) {
    cb::xattr::Blob blob;
    blob.set("_system", "1");
    auto body = blob.finalize();
    item_info info{};
    info.value[0].iov_base = static_cast<void*>(body.data());
    info.value[0].iov_len = body.size();
    info.nbytes = gsl::narrow<uint32_t>(body.size());
    info.datatype = PROTOCOL_BINARY_DATATYPE_XATTR;
    EXPECT_TRUE(document_pre_expiry(info));
    EXPECT_EQ(PROTOCOL_BINARY_DATATYPE_XATTR, info.datatype);
}

TEST(PreExpiry, DocumentWithUserAndSystemXattr) {
    cb::xattr::Blob blob;
    blob.set("_system", "1");
    blob.set("user", "1");
    auto body = blob.finalize();
    item_info info{};
    info.value[0].iov_base = static_cast<void*>(body.data());
    info.value[0].iov_len = body.size();
    info.nbytes = gsl::narrow<uint32_t>(body.size());
    info.datatype = PROTOCOL_BINARY_DATATYPE_XATTR;
    EXPECT_TRUE(document_pre_expiry(info));
    EXPECT_EQ(PROTOCOL_BINARY_DATATYPE_XATTR, info.datatype);

    // The body should have been modified (shrinked by stripping off user attr)
    EXPECT_GT(body.size(), info.datatype);
}

TEST(PreExpiry, DocumentWithJsonBodyAndXattrs) {
    cb::xattr::Blob blob;
    blob.set("_system", "1");
    blob.set("user", "1");
    auto xattr = blob.finalize();

    std::string body((const char*)xattr.data(), xattr.size());
    body.append("{\"foo\":\"bar\"}");

    item_info info{};
    info.value[0].iov_base = const_cast<char*>(body.data());
    info.value[0].iov_len = body.size();
    info.nbytes = gsl::narrow<uint32_t>(body.size());
    info.datatype =
            PROTOCOL_BINARY_DATATYPE_XATTR | PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_TRUE(document_pre_expiry(info));
    // The JSON datatype should be stripped off
    EXPECT_EQ(PROTOCOL_BINARY_DATATYPE_XATTR, info.datatype);

    // The body should have been modified (shrinked by stripping off user attr)
    EXPECT_GT(body.size(), info.datatype);
}
