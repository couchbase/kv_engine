/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <audit.h>
#include <audit_event_filter.h>
#include <auditd/couchbase_audit_events.h>
#include <folly/portability/GTest.h>
#include <nlohmann/json.hpp>

TEST(AuditEventFilterTest, CreateFromJson) {
    auto json =
            R"(
{
  "buckets": {
    "bucket2": {
      "enabled": [
        20480,
        20481,
        20482
      ],
      "filter_out": {
        "user/couchbase": [
          20482
        ],
        "user2/unknown": [
          20482
        ]
      }
    }
  },
  "default": {
    "enabled": [
      20480,
      20481,
      20482
    ],
    "filter_out": {
      "user/couchbase": [
        20480
      ],
      "user2/unknown": [
        20480
      ]
    }
  }
})"_json;

    auto filter = AuditEventFilter::create(AuditImpl::generation.load(), json);
    ASSERT_TRUE(filter) << "Failed to create filter from " << json.dump(2);
    EXPECT_TRUE(filter->isValid());

    EXPECT_TRUE(filter->isFilteredOut(
            20480, {"user", cb::rbac::Domain::Local}, {}, {}, {}, {}));
    EXPECT_TRUE(filter->isFilteredOut(
            20480, {"user2", cb::rbac::Domain::Unknown}, {}, {}, {}, {}));
    EXPECT_FALSE(filter->isFilteredOut(
            20480, {"user", cb::rbac::Domain::External}, {}, {}, {}, {}));
    EXPECT_FALSE(filter->isFilteredOut(
            20481, {"user", cb::rbac::Domain::Local}, {}, {}, {}, {}));
    EXPECT_FALSE(filter->isFilteredOut(
            20482, {"user", cb::rbac::Domain::Local}, {}, {}, {}, {}));
    EXPECT_FALSE(filter->isFilteredOut(
            20481, {"user2", cb::rbac::Domain::Unknown}, {}, {}, {}, {}));
    EXPECT_FALSE(filter->isFilteredOut(
            20482, {"user2", cb::rbac::Domain::Unknown}, {}, {}, {}, {}));

    // Verify that we fall back to the default bucket if we don't have a match
    // for the given bucket
    EXPECT_TRUE(filter->isFilteredOut(
            20480, {"user", cb::rbac::Domain::Local}, {}, "bucket", {}, {}));
    EXPECT_TRUE(filter->isFilteredOut(
            20480, {"user2", cb::rbac::Domain::Unknown}, {}, "bucket", {}, {}));
    EXPECT_FALSE(filter->isFilteredOut(
            20480, {"user", cb::rbac::Domain::External}, {}, "bucket", {}, {}));
    EXPECT_FALSE(filter->isFilteredOut(
            20481, {"user", cb::rbac::Domain::Local}, {}, "bucket", {}, {}));
    EXPECT_FALSE(filter->isFilteredOut(
            20482, {"user", cb::rbac::Domain::Local}, {}, "bucket", {}, {}));

    cb::rbac::UserIdent euid{"John", cb::rbac::Domain::Local};
    EXPECT_FALSE(filter->isFilteredOut(
            20480, {"user", cb::rbac::Domain::Local}, &euid, "bucket", {}, {}));
    EXPECT_FALSE(filter->isFilteredOut(
            20481, {"user", cb::rbac::Domain::Local}, &euid, "bucket", {}, {}));
    EXPECT_FALSE(filter->isFilteredOut(
            20482, {"user", cb::rbac::Domain::Local}, &euid, "bucket", {}, {}));
    EXPECT_FALSE(filter->isFilteredOut(20481,
                                       {"user2", cb::rbac::Domain::Unknown},
                                       &euid,
                                       "bucket",
                                       {},
                                       {}));
    EXPECT_FALSE(filter->isFilteredOut(20482,
                                       {"user2", cb::rbac::Domain::Unknown},
                                       &euid,
                                       "bucket",
                                       {},
                                       {}));

    // Verify that we pick the bucket specific config
    EXPECT_FALSE(filter->isFilteredOut(
            20480, {"user", cb::rbac::Domain::Local}, {}, "bucket2", {}, {}));
    EXPECT_TRUE(filter->isFilteredOut(
            20482, {"user", cb::rbac::Domain::Local}, {}, "bucket2", {}, {}));
}

TEST(AuditEventFilterTest, InvalidGeneration) {
    auto filter =
            AuditEventFilter::create(AuditImpl::generation.load() + 1, {});
    EXPECT_FALSE(filter->isValid());
}

TEST(AuditEventFilterTest, ValidGeneration) {
    auto filter = AuditEventFilter::create(AuditImpl::generation.load(), {});
    EXPECT_TRUE(filter->isValid());
}

TEST(AuditEventFilterTest, isFilteredOut) {
    cb::rbac::UserIdent user1Local{"user1", cb::rbac::Domain::Local};
    cb::rbac::UserIdent user1External{"user1", cb::rbac::Domain::External};
    cb::rbac::UserIdent user2Local{"user2", cb::rbac::Domain::Local};
    cb::rbac::UserIdent user2External{"user2", cb::rbac::Domain::External};

    auto json =
            R"({"default":{"enabled":[20481,20488],"filter_out":{"user1/couchbase":[20488],"user2/external":[20488]}}})"_json;
    auto filter = AuditEventFilter::create(AuditImpl::generation.load(), json);
    ASSERT_TRUE(filter) << "Failed to create filter from " << json.dump(2);

    EXPECT_TRUE(filter->isFilteredOut(
            MEMCACHED_AUDIT_DOCUMENT_READ, user1Local, {}, {}, {}, {}));
    EXPECT_FALSE(filter->isFilteredOut(
            MEMCACHED_AUDIT_DOCUMENT_READ, user1External, {}, {}, {}, {}));
    EXPECT_TRUE(filter->isFilteredOut(
            MEMCACHED_AUDIT_DOCUMENT_READ, user2External, {}, {}, {}, {}));
    EXPECT_FALSE(filter->isFilteredOut(
            MEMCACHED_AUDIT_DOCUMENT_READ, user2Local, {}, {}, {}, {}));

    // Verify that we don't filter out if the id doesn't allow filtering
    EXPECT_FALSE(filter->isFilteredOut(
            MEMCACHED_AUDIT_AUTHENTICATION_FAILED, user1Local, {}, {}, {}, {}));
    EXPECT_FALSE(filter->isFilteredOut(MEMCACHED_AUDIT_AUTHENTICATION_FAILED,
                                       user2External,
                                       {},
                                       {},
                                       {},
                                       {}));
}

TEST(AuditEventFilterTest, isFilteredOut_DisabledFilter) {
    auto filter = AuditEventFilter::create(AuditImpl::generation.load(), {});
    EXPECT_TRUE(filter->isFilteredOut(
            MEMCACHED_AUDIT_DOCUMENT_READ, {}, {}, {}, {}, {}));
}

TEST(AuditEventFilterTest, isFilteredOut_Include_List) {
    cb::rbac::UserIdent user1Local{"user1", cb::rbac::Domain::Local};
    cb::rbac::UserIdent user1External{"user1", cb::rbac::Domain::External};
    cb::rbac::UserIdent user2Local{"user2", cb::rbac::Domain::Local};
    cb::rbac::UserIdent user2External{"user2", cb::rbac::Domain::External};

    auto json =
            R"({"default":{"enabled":[20481,20488],"filter_in":{"user1/couchbase":[20488],"user2/external":[20488]}}})"_json;
    auto filter = AuditEventFilter::create(AuditImpl::generation.load(), json);
    ASSERT_TRUE(filter) << "Failed to create filter from " << json.dump(2);

    EXPECT_FALSE(filter->isFilteredOut(
            MEMCACHED_AUDIT_DOCUMENT_READ, user1Local, {}, {}, {}, {}));
    EXPECT_TRUE(filter->isFilteredOut(
            MEMCACHED_AUDIT_DOCUMENT_READ, user1External, {}, {}, {}, {}));
    EXPECT_FALSE(filter->isFilteredOut(
            MEMCACHED_AUDIT_DOCUMENT_READ, user2External, {}, {}, {}, {}));
    EXPECT_TRUE(filter->isFilteredOut(
            MEMCACHED_AUDIT_DOCUMENT_READ, user2Local, {}, {}, {}, {}));

    // Verify that we don't filter out if the id doesn't allow filtering
    EXPECT_FALSE(filter->isFilteredOut(
            MEMCACHED_AUDIT_AUTHENTICATION_FAILED, user1Local, {}, {}, {}, {}));
    EXPECT_FALSE(filter->isFilteredOut(MEMCACHED_AUDIT_AUTHENTICATION_FAILED,
                                       user2External,
                                       {},
                                       {},
                                       {},
                                       {}));
}
