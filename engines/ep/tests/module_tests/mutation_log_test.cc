/*
 *     Copyright 2011-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "../mock/mock_function_helper.h"

#include <folly/portability/Fcntl.h>
#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>
#include <platform/dirutils.h>
#include <platform/strerror.h>
#include <sys/stat.h>
#include <algorithm>
#include <fstream>
#include <set>
#include <stdexcept>

#include "mutation_log.h"
#include "tests/module_tests/test_helpers.h"

class MutationLogTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Generate a temporary log filename.
        tmp_log_filename = cb::io::mktemp("mlt_test");
    }

    void TearDown() override {
        std::filesystem::remove(tmp_log_filename);
    }

    // Storage for temporary log filename
    std::string tmp_log_filename;
};

TEST_F(MutationLogTest, Unconfigured) {
    MutationLog ml("");
    ml.open();
    ASSERT_FALSE(ml.isEnabled());
    ml.newItem(Vbid(3), makeStoredDocKey("somekey"));
    ml.commit1();
    ml.commit2();
    ml.flush();

    EXPECT_EQ(ml.begin(), ml.end());
}

TEST_F(MutationLogTest, SyncSet) {

    // Some basics
    static_assert((SYNC_COMMIT_1 | SYNC_COMMIT_2) == SYNC_FULL,
                  "Incorrect value for SYNC_FULL");
    static_assert((FLUSH_COMMIT_1 | FLUSH_COMMIT_2) == FLUSH_FULL,
                  "Incorrect value for FLUSH_FULL");
    // No overlap
    static_assert((FLUSH_FULL & ~SYNC_FULL) == FLUSH_FULL,
                  "FLUSH_FULL overlaps with SYNC_FULL");
    static_assert((SYNC_FULL & ~FLUSH_FULL) == SYNC_FULL,
                  "SYNC_FULL overlaps with FLUSH_FULL");

    //
    // Now the real tests.
    //
    MutationLog ml("");
    ml.open();

    EXPECT_TRUE(ml.setSyncConfig("off"));
    EXPECT_EQ(0, ml.getSyncConfig());

    EXPECT_TRUE(ml.setSyncConfig("commit1"));
    EXPECT_EQ(SYNC_COMMIT_1, ml.getSyncConfig());

    EXPECT_TRUE(ml.setSyncConfig("commit2"));
    EXPECT_EQ(SYNC_COMMIT_2, ml.getSyncConfig());

    EXPECT_TRUE(ml.setSyncConfig("full"));
    EXPECT_EQ(SYNC_COMMIT_1 | SYNC_COMMIT_2, ml.getSyncConfig());

    EXPECT_FALSE(ml.setSyncConfig("otherwise"));

    // reset
    EXPECT_TRUE(ml.setSyncConfig("off"));
    EXPECT_EQ(0, ml.getSyncConfig());

    //
    // Flush tests
    //
    EXPECT_TRUE(ml.setFlushConfig("commit1"));
    EXPECT_EQ(FLUSH_COMMIT_1, ml.getFlushConfig());

    EXPECT_TRUE(ml.setFlushConfig("commit2"));
    EXPECT_EQ(FLUSH_COMMIT_2, ml.getFlushConfig());

    EXPECT_TRUE(ml.setFlushConfig("full"));
    EXPECT_EQ(FLUSH_COMMIT_1 | FLUSH_COMMIT_2, ml.getFlushConfig());

    EXPECT_FALSE(ml.setFlushConfig("otherwise"));

    // reset
    EXPECT_TRUE(ml.setSyncConfig("off"));
    EXPECT_EQ(0, ml.getSyncConfig());
    EXPECT_TRUE(ml.setFlushConfig("off"));
    EXPECT_EQ(0, ml.getFlushConfig());

    //
    // Try both
    //

    EXPECT_TRUE(ml.setSyncConfig("commit1"));
    EXPECT_TRUE(ml.setFlushConfig("commit2"));
    EXPECT_EQ(SYNC_COMMIT_1, ml.getSyncConfig());
    EXPECT_EQ(FLUSH_COMMIT_2, ml.getFlushConfig());

    // Swap them and apply in reverse order.
    EXPECT_TRUE(ml.setFlushConfig("commit1"));
    EXPECT_TRUE(ml.setSyncConfig("commit2"));
    EXPECT_EQ(SYNC_COMMIT_2, ml.getSyncConfig());
    EXPECT_EQ(FLUSH_COMMIT_1, ml.getFlushConfig());
}

static bool loaderFun(void* arg, Vbid vb, const DocKeyView& k) {
    auto* sets = reinterpret_cast<std::set<StoredDocKey> *>(arg);
    sets[vb.get()].insert(StoredDocKey(k));
    return true;
}

TEST_F(MutationLogTest, Logging) {

    {
        MutationLog ml(tmp_log_filename);
        ml.open();

        ml.newItem(Vbid(2), makeStoredDocKey("key1"));
        ml.commit1();
        ml.commit2();
        ml.newItem(Vbid(3), makeStoredDocKey("key2"));
        ml.commit1();
        ml.commit2();
        // Remaining:   3:key2, 2:key1

        EXPECT_EQ(2, ml.itemsLogged[int(MutationLogType::New)]);
        EXPECT_EQ(2, ml.itemsLogged[int(MutationLogType::Commit1)]);
        EXPECT_EQ(2, ml.itemsLogged[int(MutationLogType::Commit2)]);
    }

    {
        MutationLog ml(tmp_log_filename);
        ml.open(true);
        MutationLogHarvester h(ml);
        h.setVBucket(Vbid(1));
        h.setVBucket(Vbid(2));
        h.setVBucket(Vbid(3));

        EXPECT_TRUE(h.load());

        EXPECT_EQ(2, h.getItemsSeen()[int(MutationLogType::New)]);
        EXPECT_EQ(2, h.getItemsSeen()[int(MutationLogType::Commit1)]);
        EXPECT_EQ(2, h.getItemsSeen()[int(MutationLogType::Commit2)]);

        // Check stat copying
        ml.resetCounts(h.getItemsSeen());

        EXPECT_EQ(2, ml.itemsLogged[int(MutationLogType::New)]);
        EXPECT_EQ(2, ml.itemsLogged[int(MutationLogType::Commit1)]);
        EXPECT_EQ(2, ml.itemsLogged[int(MutationLogType::Commit2)]);

        // See if we got what we expect.
        std::set<StoredDocKey> maps[4];
        h.apply(&maps, loaderFun);

        EXPECT_EQ(0, maps[0].size());
        EXPECT_EQ(0, maps[1].size());
        EXPECT_EQ(1, maps[2].size());
        EXPECT_EQ(1, maps[3].size());

        EXPECT_NE(maps[2].end(), maps[2].find(makeStoredDocKey("key1")));
        EXPECT_NE(maps[3].end(), maps[3].find(makeStoredDocKey("key2")));
    }
}

TEST_F(MutationLogTest, SmallerBlockSize) {
    {
        MutationLog ml(tmp_log_filename, 512);
        ml.open();

        ml.newItem(Vbid(2), makeStoredDocKey("key1"));
        ml.commit1();
        ml.commit2();
        ml.close();
    }

    {
        MutationLog ml(tmp_log_filename);
        ml.open(true);
        MutationLogHarvester h(ml);
        h.setVBucket(Vbid(2));

        EXPECT_TRUE(h.load());

        EXPECT_EQ(1, h.getItemsSeen()[int(MutationLogType::New)]);
        EXPECT_EQ(1, h.getItemsSeen()[int(MutationLogType::Commit1)]);
        EXPECT_EQ(1, h.getItemsSeen()[int(MutationLogType::Commit2)]);
    }
}

TEST_F(MutationLogTest, LoggingDirty) {

    {
        MutationLog ml(tmp_log_filename);
        ml.open();

        ml.newItem(Vbid(3), makeStoredDocKey("key1"));
        ml.newItem(Vbid(2), makeStoredDocKey("key1"));
        ml.commit1();
        ml.commit2();
        // This will be dropped from the normal loading path
        // because there's no commit.
        ml.newItem(Vbid(3), makeStoredDocKey("key2"));
        // Remaining:   3:key1, 2:key1

        EXPECT_EQ(3, ml.itemsLogged[int(MutationLogType::New)]);
        EXPECT_EQ(1, ml.itemsLogged[int(MutationLogType::Commit1)]);
        EXPECT_EQ(1, ml.itemsLogged[int(MutationLogType::Commit2)]);
    }

    {
        MutationLog ml(tmp_log_filename);
        ml.open(true);
        MutationLogHarvester h(ml);
        h.setVBucket(Vbid(1));
        h.setVBucket(Vbid(2));
        h.setVBucket(Vbid(3));

        EXPECT_FALSE(h.load());

        EXPECT_EQ(3, h.getItemsSeen()[int(MutationLogType::New)]);
        EXPECT_EQ(1, h.getItemsSeen()[int(MutationLogType::Commit1)]);
        EXPECT_EQ(1, h.getItemsSeen()[int(MutationLogType::Commit2)]);

        // Check stat copying
        ml.resetCounts(h.getItemsSeen());

        EXPECT_EQ(3, ml.itemsLogged[int(MutationLogType::New)]);
        EXPECT_EQ(1, ml.itemsLogged[int(MutationLogType::Commit1)]);
        EXPECT_EQ(1, ml.itemsLogged[int(MutationLogType::Commit2)]);

        // See if we got what we expect.
        std::set<StoredDocKey> maps[4];
        h.apply(&maps, loaderFun);

        EXPECT_EQ(0, maps[0].size());
        EXPECT_EQ(0, maps[1].size());
        EXPECT_EQ(1, maps[2].size());
        EXPECT_EQ(1, maps[3].size());

        EXPECT_NE(maps[2].end(), maps[2].find(makeStoredDocKey("key1")));
        EXPECT_NE(maps[3].end(), maps[3].find(makeStoredDocKey("key1")));
        EXPECT_EQ(maps[3].end(), maps[3].find(makeStoredDocKey("key2")));

    }
}

TEST_F(MutationLogTest, LoggingBadCRC) {

    {
        MutationLog ml(tmp_log_filename);
        ml.open();

        ml.newItem(Vbid(2), makeStoredDocKey("key1"));
        ml.commit1();
        ml.commit2();
        ml.newItem(Vbid(3), makeStoredDocKey("key2"));
        ml.commit1();
        ml.commit2();
        // Remaining:   3:key2, 2:key1

        EXPECT_EQ(2, ml.itemsLogged[int(MutationLogType::New)]);
        EXPECT_EQ(2, ml.itemsLogged[int(MutationLogType::Commit1)]);
        EXPECT_EQ(2, ml.itemsLogged[int(MutationLogType::Commit2)]);
    }

    // Break the log
    int file = open(tmp_log_filename.c_str(), O_RDWR);
    EXPECT_EQ(5000, lseek(file, 5000, SEEK_SET));
    uint8_t b;
    EXPECT_EQ(1, read(file, &b, sizeof(b)));
    EXPECT_EQ(5000, lseek(file, 5000, SEEK_SET));
    b = ~b;
    EXPECT_EQ(1, write(file, &b, sizeof(b)));
    close(file);

    {
        MutationLog ml(tmp_log_filename);
        ml.open(true);
        MutationLogHarvester h(ml);
        h.setVBucket(Vbid(1));
        h.setVBucket(Vbid(2));
        h.setVBucket(Vbid(3));

        EXPECT_THROW(h.load(), MutationLog::CRCReadException);

        EXPECT_EQ(0, h.getItemsSeen()[int(MutationLogType::New)]);
        EXPECT_EQ(0, h.getItemsSeen()[int(MutationLogType::Commit1)]);
        EXPECT_EQ(0, h.getItemsSeen()[int(MutationLogType::Commit2)]);

        // Check stat copying
        ml.resetCounts(h.getItemsSeen());

        EXPECT_EQ(0, ml.itemsLogged[int(MutationLogType::New)]);
        EXPECT_EQ(0, ml.itemsLogged[int(MutationLogType::Commit1)]);
        EXPECT_EQ(0, ml.itemsLogged[int(MutationLogType::Commit2)]);

        // See if we got what we expect.
        std::set<std::string> maps[4];
        h.apply(&maps, loaderFun);

        EXPECT_EQ(0, maps[0].size());
        EXPECT_EQ(0, maps[1].size());
        EXPECT_EQ(0, maps[2].size());
        EXPECT_EQ(0, maps[3].size());

        EXPECT_EQ(maps[2].end(), maps[2].find("key1"));
        EXPECT_EQ(maps[3].end(), maps[3].find("key2"));
    }
}

TEST_F(MutationLogTest, LoggingShortRead) {

    {
        MutationLog ml(tmp_log_filename);
        ml.open();

        ml.newItem(Vbid(2), makeStoredDocKey("key1"));
        ml.commit1();
        ml.commit2();
        ml.newItem(Vbid(3), makeStoredDocKey("key2"));
        ml.commit1();
        ml.commit2();
        // Remaining:   3:key2, 2:key1

        EXPECT_EQ(2, ml.itemsLogged[int(MutationLogType::New)]);
        EXPECT_EQ(2, ml.itemsLogged[int(MutationLogType::Commit1)]);
        EXPECT_EQ(2, ml.itemsLogged[int(MutationLogType::Commit2)]);
    }

    // Break the log
    EXPECT_EQ(0, truncate(tmp_log_filename.c_str(), 5000));

    {
        MutationLog ml(tmp_log_filename);

        ml.open(true);
        MutationLogHarvester h(ml);
        h.setVBucket(Vbid(1));
        h.setVBucket(Vbid(2));
        h.setVBucket(Vbid(3));

        EXPECT_THROW(h.load(), MutationLog::ShortReadException);
    }

    // Break the log harder (can't read even the initial block)
    // This should succeed as open() will call reset() to give us a usable
    // mutation log.
    EXPECT_EQ(0, truncate(tmp_log_filename.c_str(), 8));

    {
        MutationLog ml(tmp_log_filename);
        ml.open(true);
    }
}

TEST_F(MutationLogTest, YUNOOPEN) {
    // Make file unreadable
    std::filesystem::permissions(tmp_log_filename,
                                 std::filesystem::perms::none);
    MutationLog ml(tmp_log_filename);
    EXPECT_THROW(ml.open(), MutationLog::ReadException);
    // Restore permissions to be able to delete file.
    std::filesystem::permissions(tmp_log_filename,
                                 std::filesystem::perms::owner_read |
                                         std::filesystem::perms::owner_write);
}

class MockFileIface : public mlog::DefaultFileIface {
public:
    MOCK_METHOD(ssize_t,
                doWrite,
                (file_handle_t fd, const uint8_t* buf, size_t nbyte),
                (override));
};

// MB-55939: Test behaviour when the mutation log cannot be written to disk
// (e.g. disk full).
TEST_F(MutationLogTest, WriteFail) {
    // Setup a Mock FileIface which will return one less byte from pwrite()
    // than requested and set errno to ENOSPC.
    using namespace ::testing;
    auto mockFileIface = std::make_unique<MockFileIface>();
    EXPECT_CALL(*mockFileIface, doWrite(_, _, _))
            .WillOnce([](file_handle_t, const uint8_t*, size_t) -> ssize_t {
                throw std::system_error(
                        ENOSPC, std::system_category(), "disk full");
            });

    // Test: Create and open a MutationLog; on destruction we should not see
    // an exception thrown.
    MutationLog ml(
            tmp_log_filename, MIN_LOG_HEADER_SIZE, std::move(mockFileIface));
    ml.open();
}

// Test that the MutationLog::iterator class obeys expected iterator behaviour.
TEST_F(MutationLogTest, Iterator) {
    // Create a simple mutation log to work on.
    {
        MutationLog ml(tmp_log_filename);
        ml.open();
        ml.newItem(Vbid(0), makeStoredDocKey("key1"));
        ml.newItem(Vbid(0), makeStoredDocKey("key2"));
        ml.newItem(Vbid(0), makeStoredDocKey("key3"));
        ml.commit1();
        ml.commit2();

        EXPECT_EQ(3, ml.itemsLogged[int(MutationLogType::New)]);
        EXPECT_EQ(1, ml.itemsLogged[int(MutationLogType::Commit1)]);
        EXPECT_EQ(1, ml.itemsLogged[int(MutationLogType::Commit2)]);
    }

    // Now check the iterators.
    MutationLog ml(tmp_log_filename);
    ml.open(true);

    // Can copy-construct.
    auto iter = ml.begin();
    EXPECT_EQ(ml.begin(), iter);

    // Can copy-assign.
    iter = ml.end();
    EXPECT_EQ(ml.end(), iter);

    // Can advance the correct number.
    size_t count = 0;
    for (auto iter2 = ml.begin(); iter2 != ml.end(); ++iter2) {
        count++;
    }
    EXPECT_EQ(5, count);
}

TEST_F(MutationLogTest, BatchLoad) {

    {
        MutationLog ml(tmp_log_filename);
        ml.open();

        // Add a number of items, then check that batch load only returns
        // the requested number.
        for (size_t ii = 0; ii < 10; ii++) {
            std::string key = std::string("key") + std::to_string(ii);
            ml.newItem(Vbid(ii % 2), makeStoredDocKey(key));
        }
        ml.commit1();
        ml.commit2();

        EXPECT_EQ(10, ml.itemsLogged[int(MutationLogType::New)]);
        EXPECT_EQ(1, ml.itemsLogged[int(MutationLogType::Commit1)]);
        EXPECT_EQ(1, ml.itemsLogged[int(MutationLogType::Commit2)]);
    }

    {
        MutationLog ml(tmp_log_filename);
        ml.open(true);
        MutationLogHarvester h(ml);
        h.setVBucket(Vbid(0));
        h.setVBucket(Vbid(1));

        // Ask for 2 items, ensure we get just two. Expect return true as more
        // data can be loaded.
        EXPECT_TRUE(h.loadBatch(2));

        std::set<StoredDocKey> maps[2];
        h.apply(&maps, loaderFun);
        EXPECT_EQ(2, maps[0].size() + maps[1].size());

        // Ask for 10; should get the remainder (8).
        EXPECT_FALSE(h.loadBatch(10));

        for (auto& map : maps) {
            map.clear();
        }
        h.apply(&maps, loaderFun);
        EXPECT_EQ(8, maps[0].size() + maps[1].size());
    }
}

// @todo
//   Test Read Only log
//   Test close / open / close / open
//   Fix copy constructor bug
//

TEST_F(MutationLogTest, ReadOnly) {
    remove(tmp_log_filename.c_str());
    MutationLog ml(tmp_log_filename);
    EXPECT_THROW(ml.open(true), MutationLog::FileNotFoundException);

    MutationLog m2(tmp_log_filename);
    m2.open();
    m2.newItem(Vbid(3), makeStoredDocKey("key1"));
    m2.close();

    // We should be able to open the file now
    ml.open(true);

    // But we should not be able to add items to a read only stream
    EXPECT_THROW(ml.newItem(Vbid(4), makeStoredDocKey("key2")),
                 MutationLog::WriteException);
}
