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
    body.append("{\"foo\":\"bar\"}");

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
