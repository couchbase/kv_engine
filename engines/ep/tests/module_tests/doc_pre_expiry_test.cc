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
#include <memcached/types.h>
#include <xattr/blob.h>

using namespace std::string_view_literals;

TEST(PreExpiry, EmptyDocument) {
    EXPECT_TRUE(document_pre_expiry({}, {}).empty());
}

TEST(PreExpiry, DocumentWithoutXattr) {
    auto rv = document_pre_expiry("   "sv, {});
    EXPECT_TRUE(rv.empty());
}

TEST(PreExpiry, DocumentWithUserXAttrOnly) {
    cb::xattr::Blob blob;
    blob.set("user", "1");
    auto body = blob.finalize();
    auto rv = document_pre_expiry(body, PROTOCOL_BINARY_DATATYPE_XATTR);
    EXPECT_TRUE(rv.empty());
}

TEST(PreExpiry, DocumentWithSystemXattrOnly) {
    cb::xattr::Blob blob;
    blob.set("_system", "1");
    auto body = blob.finalize();
    auto rv = document_pre_expiry(body, PROTOCOL_BINARY_DATATYPE_XATTR);
    EXPECT_FALSE(rv.empty());
}

TEST(PreExpiry, DocumentWithUserAndSystemXattr) {
    cb::xattr::Blob blob;
    blob.set("_system", "1");
    blob.set("user", "1");
    auto body = blob.finalize();
    auto rv = document_pre_expiry(body, PROTOCOL_BINARY_DATATYPE_XATTR);
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
    auto rv = document_pre_expiry(
            body,
            PROTOCOL_BINARY_DATATYPE_XATTR | PROTOCOL_BINARY_DATATYPE_JSON);
    EXPECT_FALSE(rv.empty());
    EXPECT_LT(rv.size(), body.size());
}
