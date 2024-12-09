/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "dek_file_utilities.h"
#include <folly/portability/GTest.h>
#include <platform/dirutils.h>

class DekFileUtilTest : public ::testing::Test {
protected:
    void SetUp() override {
        root = cb::io::mkdtemp("DekFileUtilTest");
    }
    void TearDown() override {
        remove_all(root);
    }

    void touch(const std::filesystem::path& path) {
        FILE* fp = fopen(path.string().c_str(), "w");
        ASSERT_NE(nullptr, fp);
        fclose(fp);
    }

    std::filesystem::path root;
};

TEST_F(DekFileUtilTest, iterateKeyFiles) {
    touch(root / "foo.key.1");
    touch(root / "foo.key.2");
    touch(root / "foo.bar");
    bool foo1 = false;
    bool foo2 = false;
    cb::dek::util::iterateKeyFiles(
            "foo", root, [&foo1, &foo2](const auto& path) {
                if (path.filename().string() == "foo.key.1") {
                    foo1 = true;
                } else if (path.filename().string() == "foo.key.2") {
                    foo2 = true;
                } else {
                    FAIL() << "Unexpected file: " << path;
                }
            });
    EXPECT_TRUE(foo1);
    EXPECT_TRUE(foo2);
}

TEST_F(DekFileUtilTest, locateNewestKeyFile) {
    touch(root / "foo.key.1");
    touch(root / "foo.key.2");
    touch(root / "foo.key.3");
    touch(root / "foo.key.4");
    touch(root / "foo.key.5");

    auto path = cb::dek::util::locateNewestKeyFile("foo", root);
    ASSERT_TRUE(path);
    EXPECT_EQ(root / "foo.key.5", *path);
}

TEST_F(DekFileUtilTest, locateNewestKeyFileNoSuchFile) {
    auto path = cb::dek::util::locateNewestKeyFile("foo", root);
    EXPECT_FALSE(path);
}

TEST_F(DekFileUtilTest, locateNewestKeyFileInvalidFile) {
    touch(root / "foo.key.1");
    touch(root / "foo.key.2");
    touch(root / "foo.key.3");
    touch(root / "foo.key.4");
    touch(root / "foo.key.5");
    touch(root / "foo.key.6");

    // add a file with an invalid extension
    touch(root / "foo.key.7.invalid");

    auto path = cb::dek::util::locateNewestKeyFile("foo", root);
    ASSERT_TRUE(path);
    EXPECT_EQ(root / "foo.key.6", *path);
}

TEST_F(DekFileUtilTest, copyKeyFile) {
    touch(root / "foo.key.1");
    touch(root / "foo.key.2");
    touch(root / "foo.key.3");
    touch(root / "foo.key.4");
    touch(root / "foo.key.5");

    std::filesystem::path target = cb::io::mkdtemp("DekFileUtilTestDest");
    auto path = cb::dek::util::copyKeyFile("foo", root, target);

    EXPECT_EQ((target / "foo.key.5").string(), path.string());
    EXPECT_TRUE(std::filesystem::exists(path));
}

TEST_F(DekFileUtilTest, copyKeyFileNoSuchFile) {
    std::filesystem::path target = cb::io::mkdtemp("DekFileUtilTestDest");
    EXPECT_THROW(cb::dek::util::copyKeyFile("foo", root, target),
                 std::runtime_error);
}
