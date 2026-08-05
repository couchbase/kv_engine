/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "disk_format_constraint.h"
#include <folly/portability/GTest.h>
#include <nlohmann/json.hpp>

using cb::snapshot::DiskFormatConstraint;

/// Validate, discarding the failure detail - for assertions which only care
/// about the outcome
static bool accepts(const DiskFormatConstraint& constraint,
                    std::string_view backend,
                    uint32_t version) {
    nlohmann::json failure;
    return constraint.validate(backend, version, failure);
}

TEST(DiskFormatConstraint, Conversion) {
    const DiskFormatConstraint constraint{"couchstore", 14};
    nlohmann::json json = constraint;
    EXPECT_EQ(R"({"backend":"couchstore","max_version":14})"_json, json);
    EXPECT_EQ(constraint, json.get<DiskFormatConstraint>());

    // max_version of 0 (unknown) isn't included and parses back as 0
    const DiskFormatConstraint unversioned{"magma", 0};
    json = unversioned;
    EXPECT_EQ(R"({"backend":"magma"})"_json, json);
    EXPECT_EQ(unversioned, json.get<DiskFormatConstraint>());
}

TEST(DiskFormatConstraint, ParseInvalid) {
    EXPECT_THROW(nlohmann::json::parse("{}").get<DiskFormatConstraint>(),
                 std::invalid_argument);
    EXPECT_THROW(R"({"backend":1})"_json.get<DiskFormatConstraint>(),
                 std::invalid_argument);
    EXPECT_THROW(R"({"backend":"couchstore","max_version":"14"})"_json
                         .get<DiskFormatConstraint>(),
                 std::invalid_argument);
}

TEST(DiskFormatConstraint, ValidateAccepts) {
    const DiskFormatConstraint constraint{"couchstore", 14};
    EXPECT_TRUE(accepts(constraint, "couchstore", 14));
    EXPECT_TRUE(accepts(constraint, "couchstore", 13));

    // The failure object isn't touched for a successful validation
    nlohmann::json failure;
    EXPECT_TRUE(constraint.validate("couchstore", 14, failure));
    EXPECT_TRUE(failure.is_null());
}

TEST(DiskFormatConstraint, ValidateBackendMismatch) {
    const DiskFormatConstraint constraint{"couchstore", 14};
    nlohmann::json failure;
    EXPECT_FALSE(constraint.validate("magma", 14, failure));
    EXPECT_EQ(R"({"reason":"backend mismatch",
                  "snapshot":"magma",
                  "constraint":"couchstore"})"_json,
              failure);

    // The backend is checked before the version
    EXPECT_FALSE(accepts(constraint, "magma", 15));
}

TEST(DiskFormatConstraint, ValidateVersionMismatch) {
    const DiskFormatConstraint constraint{"couchstore", 14};
    nlohmann::json failure;
    EXPECT_FALSE(constraint.validate("couchstore", 15, failure));
    EXPECT_EQ(R"({"reason":"version mismatch",
                  "snapshot":15,
                  "constraint":14})"_json,
              failure);
}

/// Unknown values (empty backend / version 0) on either side are not checked;
/// they occur for snapshots created before the storage information was
/// introduced and for backends which don't report a disk format version.
/// The two fields are skipped independently of each other.
TEST(DiskFormatConstraint, ValidateSkipsUnknownSnapshotFields) {
    const DiskFormatConstraint constraint{"couchstore", 14};

    // An unknown snapshot backend skips the backend check only; the version
    // is still checked
    EXPECT_TRUE(accepts(constraint, "", 14));
    EXPECT_FALSE(accepts(constraint, "", 15));

    // An unknown snapshot version skips the version check only; the backend
    // is still checked
    EXPECT_TRUE(accepts(constraint, "couchstore", 0));
    EXPECT_FALSE(accepts(constraint, "magma", 0));

    // Nothing known about the snapshot; nothing to check
    EXPECT_TRUE(accepts(constraint, "", 0));
}

TEST(DiskFormatConstraint, ValidateSkipsUnknownConstraintFields) {
    // A constraint without a version accepts any version of its backend
    const DiskFormatConstraint unknownVersion{"couchstore", 0};
    EXPECT_TRUE(accepts(unknownVersion, "couchstore", 15));
    EXPECT_FALSE(accepts(unknownVersion, "magma", 15));

    // A constraint without a backend accepts any backend within its version
    const DiskFormatConstraint unknownBackend{"", 14};
    EXPECT_TRUE(accepts(unknownBackend, "magma", 14));
    EXPECT_FALSE(accepts(unknownBackend, "magma", 15));

    // A default constructed constraint accepts anything
    const DiskFormatConstraint unknown{};
    EXPECT_TRUE(accepts(unknown, "magma", 15));
}
