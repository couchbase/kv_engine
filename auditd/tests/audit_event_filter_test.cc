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
#include <boost/filesystem/path.hpp>
#include <folly/portability/GTest.h>
#include <nlohmann/json.hpp>
#include <platform/dirutils.h>

TEST(AuditEventFilterTest, InvalidGeneration) {
    auto filter = AuditEventFilter::create(
            AuditImpl::generation.load() + 1, true, {});
    EXPECT_FALSE(filter->isValid());
}

TEST(AuditEventFilterTest, ValidGeneration) {
    auto filter =
            AuditEventFilter::create(AuditImpl::generation.load(), true, {});
    EXPECT_TRUE(filter->isValid());
}

TEST(AuditEventFilterTest, isFilteredOut) {
    cb::rbac::UserIdent user1Local{"user1", cb::rbac::Domain::Local};
    cb::rbac::UserIdent user1External{"user1", cb::rbac::Domain::External};
    cb::rbac::UserIdent user2Local{"user2", cb::rbac::Domain::Local};
    cb::rbac::UserIdent user2External{"user2", cb::rbac::Domain::External};

    auto filter =
            AuditEventFilter::create(0, true, {user1Local, user2External});

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
