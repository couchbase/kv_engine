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
    AuditEventFilter filter(AuditImpl::generation.load() + 1, true, {});
    EXPECT_FALSE(filter.isValid());
}

TEST(AuditEventFilterTest, ValidGeneration) {
    AuditEventFilter filter(AuditImpl::generation.load(), true, {});
    EXPECT_TRUE(filter.isValid());
}

TEST(AuditEventFilterTest, isIdSubjectToFilter) {
    class MockAuditEventFilter : public AuditEventFilter {
    public:
        MockAuditEventFilter()
            : AuditEventFilter(AuditImpl::generation.load(), true, {}) {
        }
        void testIsIdSubjectToFilter() {
            auto json = nlohmann::json::parse(
                    cb::io::loadFile((boost::filesystem::path{SOURCE_ROOT} /
                                      "etc" / "memcached_descriptor.json")
                                             .generic_string()));
            for (const auto& entry : json["events"]) {
                EXPECT_EQ(entry["filtering_permitted"].get<bool>(),
                          isIdSubjectToFilter(entry["id"].get<uint32_t>()))
                        << entry.dump(2);
            }

            // Verify outside the memcached range
            EXPECT_FALSE(isIdSubjectToFilter(
                    MEMCACHED_AUDIT_OPENED_DCP_CONNECTION - 1));
            EXPECT_FALSE(isIdSubjectToFilter(
                    MEMCACHED_AUDIT_TENANT_RATE_LIMITED + 1));
        }
    };

    MockAuditEventFilter filter;
    filter.testIsIdSubjectToFilter();
}

TEST(AuditEventFilterTest, isFilteredOut) {
    cb::rbac::UserIdent user1Local{"user1", cb::rbac::Domain::Local};
    cb::rbac::UserIdent user1External{"user1", cb::rbac::Domain::External};
    cb::rbac::UserIdent user2Local{"user2", cb::rbac::Domain::Local};
    cb::rbac::UserIdent user2External{"user2", cb::rbac::Domain::External};

    AuditEventFilter filter(0, true, {user1Local, user2External});

    EXPECT_TRUE(
            filter.isFilteredOut(MEMCACHED_AUDIT_DOCUMENT_READ, user1Local));
    EXPECT_FALSE(
            filter.isFilteredOut(MEMCACHED_AUDIT_DOCUMENT_READ, user1External));
    EXPECT_TRUE(
            filter.isFilteredOut(MEMCACHED_AUDIT_DOCUMENT_READ, user2External));
    EXPECT_FALSE(
            filter.isFilteredOut(MEMCACHED_AUDIT_DOCUMENT_READ, user2Local));

    // Verify that we don't filter out if the id doesn't allow filtering
    EXPECT_FALSE(filter.isFilteredOut(MEMCACHED_AUDIT_AUTHENTICATION_FAILED,
                                      user1Local));
    EXPECT_FALSE(filter.isFilteredOut(MEMCACHED_AUDIT_AUTHENTICATION_FAILED,
                                      user2External));
}
