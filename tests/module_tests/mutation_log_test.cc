/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2011 Couchbase, Inc
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

#include "config.h"

#include <algorithm>
#include <fcntl.h>
#include <gtest/gtest.h>
#include <map>
#include <platform/strerror.h>
#include <set>
#include <stdexcept>
#include <sys/stat.h>
#include <vector>

#include "makestoreddockey.h"
#include "mutation_log.h"

// Windows doesn't have a truncate() function, implement one.
#if defined(WIN32)
int truncate(const char *path, off_t length) {
    LARGE_INTEGER distance;
    distance.u.HighPart = 0;
    distance.u.LowPart = length;

    HANDLE fh = CreateFile(path, GENERIC_WRITE, 0, NULL, OPEN_EXISTING, 0, NULL);
    if (fh == INVALID_HANDLE_VALUE) {
        fprintf(stderr, "truncate: CreateFile failed with error: %s\n",
                cb_strerror().c_str());
        abort();
    }

    cb_assert(SetFilePointerEx(fh, distance, NULL, FILE_BEGIN) != 0);
    cb_assert(SetEndOfFile(fh) != 0);

    CloseHandle(fh);
    return 0;
}
#endif

// Bitfield of available file permissions.
namespace FilePerms {
    const int None = 0;
#if defined(WIN32)
    const int Read = _S_IREAD;
    const int Write = _S_IWRITE;
#else
    const int Read = S_IRUSR;
    const int Write = S_IWUSR;
#endif
}

class MutationLogTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Generate a temporary log filename.
        tmp_log_filename = "mlt_test.XXXXXX";
        ASSERT_NE(nullptr, cb_mktemp(&tmp_log_filename[0]));
    }

    void TearDown() override {
        remove(tmp_log_filename.c_str());
    }

    /**
     * Sets the read/write permissions on the tmp log file (in a
     * cross-platform way).
     */
    void set_file_perms(/*FilePerms*/int perms) {
#if defined(WIN32)
        if (_chmod(tmp_log_filename.c_str(), perms) != 0)
        {
#else
        if (chmod(tmp_log_filename.c_str(), perms) != 0)
        {
#endif
            std::cerr << "set_file_perms: chmod failed: " << cb_strerror() << std::endl;
            abort();
        }
    }

    // Storage for temporary log filename
    std::string tmp_log_filename;
};

TEST_F(MutationLogTest, Unconfigured) {
    MutationLog ml("");
    ml.open();
    ASSERT_FALSE(ml.isEnabled());
    ml.newItem(3, makeStoredDocKey("somekey"),  931);
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

static bool loaderFun(void *arg, uint16_t vb, const DocKey& k) {
    std::set<StoredDocKey>* sets = reinterpret_cast<std::set<StoredDocKey> *>(arg);
    sets[vb].insert(k);
    return true;
}

TEST_F(MutationLogTest, Logging) {
    remove(tmp_log_filename.c_str());

    {
        MutationLog ml(tmp_log_filename.c_str());
        ml.open();

        ml.newItem(2, makeStoredDocKey("key1"), 2);
        ml.commit1();
        ml.commit2();
        ml.newItem(3, makeStoredDocKey("key2"), 3);
        ml.commit1();
        ml.commit2();
        // Remaining:   3:key2, 2:key1

        EXPECT_EQ(2, ml.itemsLogged[ML_NEW]);
        EXPECT_EQ(2, ml.itemsLogged[ML_COMMIT1]);
        EXPECT_EQ(2, ml.itemsLogged[ML_COMMIT2]);
    }

    {
        MutationLog ml(tmp_log_filename.c_str());
        ml.open();
        MutationLogHarvester h(ml);
        h.setVBucket(1);
        h.setVBucket(2);
        h.setVBucket(3);

        EXPECT_TRUE(h.load());

        EXPECT_EQ(2, h.getItemsSeen()[ML_NEW]);
        EXPECT_EQ(2, h.getItemsSeen()[ML_COMMIT1]);
        EXPECT_EQ(2, h.getItemsSeen()[ML_COMMIT2]);

        // Check stat copying
        ml.resetCounts(h.getItemsSeen());

        EXPECT_EQ(2, ml.itemsLogged[ML_NEW]);
        EXPECT_EQ(2, ml.itemsLogged[ML_COMMIT1]);
        EXPECT_EQ(2, ml.itemsLogged[ML_COMMIT2]);

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

TEST_F(MutationLogTest, LoggingDirty) {
    remove(tmp_log_filename.c_str());

    {
        MutationLog ml(tmp_log_filename.c_str());
        ml.open();

        ml.newItem(3, makeStoredDocKey("key1"),  1);
        ml.newItem(2, makeStoredDocKey("key1"),  2);
        ml.commit1();
        ml.commit2();
        // This will be dropped from the normal loading path
        // because there's no commit.
        ml.newItem(3, makeStoredDocKey("key2"),  3);
        // Remaining:   3:key1, 2:key1

        EXPECT_EQ(3, ml.itemsLogged[ML_NEW]);
        EXPECT_EQ(1, ml.itemsLogged[ML_COMMIT1]);
        EXPECT_EQ(1, ml.itemsLogged[ML_COMMIT2]);
    }

    {
        MutationLog ml(tmp_log_filename.c_str());
        ml.open();
        MutationLogHarvester h(ml);
        h.setVBucket(1);
        h.setVBucket(2);
        h.setVBucket(3);

        EXPECT_FALSE(h.load());

        EXPECT_EQ(3, h.getItemsSeen()[ML_NEW]);
        EXPECT_EQ(1, h.getItemsSeen()[ML_COMMIT1]);
        EXPECT_EQ(1, h.getItemsSeen()[ML_COMMIT2]);

        // Check stat copying
        ml.resetCounts(h.getItemsSeen());

        EXPECT_EQ(3, ml.itemsLogged[ML_NEW]);
        EXPECT_EQ(1, ml.itemsLogged[ML_COMMIT1]);
        EXPECT_EQ(1, ml.itemsLogged[ML_COMMIT2]);

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
    remove(tmp_log_filename.c_str());

    {
        MutationLog ml(tmp_log_filename.c_str());
        ml.open();

        ml.newItem(2, makeStoredDocKey("key1"),  2);
        ml.commit1();
        ml.commit2();
        ml.newItem(3, makeStoredDocKey("key2"),  3);
        ml.commit1();
        ml.commit2();
        // Remaining:   3:key2, 2:key1

        EXPECT_EQ(2, ml.itemsLogged[ML_NEW]);
        EXPECT_EQ(2, ml.itemsLogged[ML_COMMIT1]);
        EXPECT_EQ(2, ml.itemsLogged[ML_COMMIT2]);
    }

    // Break the log
    int file = open(tmp_log_filename.c_str(), O_RDWR, FilePerms::Read | FilePerms::Write);
    EXPECT_EQ(5000, lseek(file, 5000, SEEK_SET));
    uint8_t b;
    EXPECT_EQ(1, read(file, &b, sizeof(b)));
    EXPECT_EQ(5000, lseek(file, 5000, SEEK_SET));
    b = ~b;
    EXPECT_EQ(1, write(file, &b, sizeof(b)));
    close(file);

    {
        MutationLog ml(tmp_log_filename.c_str());
        ml.open();
        MutationLogHarvester h(ml);
        h.setVBucket(1);
        h.setVBucket(2);
        h.setVBucket(3);

        EXPECT_THROW(h.load(), MutationLog::CRCReadException);

        EXPECT_EQ(0, h.getItemsSeen()[ML_NEW]);
        EXPECT_EQ(0, h.getItemsSeen()[ML_COMMIT1]);
        EXPECT_EQ(0, h.getItemsSeen()[ML_COMMIT2]);

        // Check stat copying
        ml.resetCounts(h.getItemsSeen());

        EXPECT_EQ(0, ml.itemsLogged[ML_NEW]);
        EXPECT_EQ(0, ml.itemsLogged[ML_COMMIT1]);
        EXPECT_EQ(0, ml.itemsLogged[ML_COMMIT2]);

        // See if we got what we expect.
        std::map<std::string, uint64_t> maps[4];
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
    remove(tmp_log_filename.c_str());

    {
        MutationLog ml(tmp_log_filename.c_str());
        ml.open();

        ml.newItem(2, makeStoredDocKey("key1"),  2);
        ml.commit1();
        ml.commit2();
        ml.newItem(3, makeStoredDocKey("key2"),  3);
        ml.commit1();
        ml.commit2();
        // Remaining:   3:key2, 2:key1

        EXPECT_EQ(2, ml.itemsLogged[ML_NEW]);
        EXPECT_EQ(2, ml.itemsLogged[ML_COMMIT1]);
        EXPECT_EQ(2, ml.itemsLogged[ML_COMMIT2]);
    }

    // Break the log
    EXPECT_EQ(0, truncate(tmp_log_filename.c_str(), 5000));

    {
        MutationLog ml(tmp_log_filename.c_str());

        ml.open();
        MutationLogHarvester h(ml);
        h.setVBucket(1);
        h.setVBucket(2);
        h.setVBucket(3);

        EXPECT_THROW(h.load(), MutationLog::ShortReadException);
    }

    // Break the log harder (can't read even the initial block)
    // This should succeed as open() will call reset() to give us a usable
    // mutation log.
    EXPECT_EQ(0, truncate(tmp_log_filename.c_str(), 4000));

    {
        MutationLog ml(tmp_log_filename.c_str());
        ml.open();
    }
}

TEST_F(MutationLogTest, YUNOOPEN) {
    // Make file unreadable
    set_file_perms(FilePerms::None);
    MutationLog ml(tmp_log_filename.c_str());
    EXPECT_THROW(ml.open(), MutationLog::ReadException);
    // Restore permissions to be able to delete file.
    set_file_perms(FilePerms::Read | FilePerms::Write);
}

// Test that the MutationLog::iterator class obeys expected iterator behaviour.
TEST_F(MutationLogTest, Iterator) {
    // Create a simple mutation log to work on.
    {
        MutationLog ml(tmp_log_filename.c_str());
        ml.open();
        ml.newItem(0, makeStoredDocKey("key1"),  0);
        ml.newItem(0, makeStoredDocKey("key2"),  1);
        ml.newItem(0, makeStoredDocKey("key3"),  2);
        ml.commit1();
        ml.commit2();

        EXPECT_EQ(3, ml.itemsLogged[ML_NEW]);
        EXPECT_EQ(1, ml.itemsLogged[ML_COMMIT1]);
        EXPECT_EQ(1, ml.itemsLogged[ML_COMMIT2]);
    }

    // Now check the iterators.
    MutationLog ml(tmp_log_filename.c_str());
    ml.open();

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
    remove(tmp_log_filename.c_str());

    {
        MutationLog ml(tmp_log_filename.c_str());
        ml.open();

        // Add a number of items, then check that batch load only returns
        // the requested number.
        for (size_t ii = 0; ii < 10; ii++) {
            std::string key = std::string("key") + std::to_string(ii);
            ml.newItem(ii % 2, makeStoredDocKey(key),  ii);
        }
        ml.commit1();
        ml.commit2();

        EXPECT_EQ(10, ml.itemsLogged[ML_NEW]);
        EXPECT_EQ(1, ml.itemsLogged[ML_COMMIT1]);
        EXPECT_EQ(1, ml.itemsLogged[ML_COMMIT2]);
    }

    {
        MutationLog ml(tmp_log_filename.c_str());
        ml.open();
        MutationLogHarvester h(ml);
        h.setVBucket(0);
        h.setVBucket(1);

        // Ask for 2 items, ensure we get just two.
        auto next_it = h.loadBatch(ml.begin(), 2);
        EXPECT_NE(next_it, ml.end());

        std::map<StoredDocKey, uint64_t> maps[2];
        h.apply(&maps, loaderFun);
        EXPECT_EQ(2, maps[0].size() + maps[1].size());

        // Ask for 10; should get the remainder (8).
        next_it = h.loadBatch(next_it, 10);
        cb_assert(next_it == ml.end());

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
    MutationLog ml(tmp_log_filename.c_str());
    EXPECT_THROW(ml.open(true), MutationLog::FileNotFoundException);

    MutationLog m2(tmp_log_filename);
    m2.open();
    m2.newItem(3, makeStoredDocKey("key1"), 1);
    m2.close();

    // We should be able to open the file now
    ml.open(true);

    // But we should not be able to add items to a read only stream
    EXPECT_THROW(ml.newItem(4, makeStoredDocKey("key2"), 1), MutationLog::WriteException);
}
