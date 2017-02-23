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

#include <fcntl.h>
#include <gtest/gtest.h>
#include <platform/strerror.h>
#include <sys/stat.h>
#include <algorithm>
#include <fstream>
#include <map>
#include <set>
#include <stdexcept>
#include <vector>

extern "C" {
#include "crc32.h"
}

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

        EXPECT_EQ(2, ml.itemsLogged[int(MutationLogType::New)]);
        EXPECT_EQ(2, ml.itemsLogged[int(MutationLogType::Commit1)]);
        EXPECT_EQ(2, ml.itemsLogged[int(MutationLogType::Commit2)]);
    }

    {
        MutationLog ml(tmp_log_filename.c_str());
        ml.open();
        MutationLogHarvester h(ml);
        h.setVBucket(1);
        h.setVBucket(2);
        h.setVBucket(3);

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

TEST_F(MutationLogTest, LoggingDirty) {

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

        EXPECT_EQ(3, ml.itemsLogged[int(MutationLogType::New)]);
        EXPECT_EQ(1, ml.itemsLogged[int(MutationLogType::Commit1)]);
        EXPECT_EQ(1, ml.itemsLogged[int(MutationLogType::Commit2)]);
    }

    {
        MutationLog ml(tmp_log_filename.c_str());
        ml.open();
        MutationLogHarvester h(ml);
        h.setVBucket(1);
        h.setVBucket(2);
        h.setVBucket(3);

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
        MutationLog ml(tmp_log_filename.c_str());
        ml.open();

        ml.newItem(2, makeStoredDocKey("key1"),  2);
        ml.commit1();
        ml.commit2();
        ml.newItem(3, makeStoredDocKey("key2"),  3);
        ml.commit1();
        ml.commit2();
        // Remaining:   3:key2, 2:key1

        EXPECT_EQ(2, ml.itemsLogged[int(MutationLogType::New)]);
        EXPECT_EQ(2, ml.itemsLogged[int(MutationLogType::Commit1)]);
        EXPECT_EQ(2, ml.itemsLogged[int(MutationLogType::Commit2)]);
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

        EXPECT_EQ(0, h.getItemsSeen()[int(MutationLogType::New)]);
        EXPECT_EQ(0, h.getItemsSeen()[int(MutationLogType::Commit1)]);
        EXPECT_EQ(0, h.getItemsSeen()[int(MutationLogType::Commit2)]);

        // Check stat copying
        ml.resetCounts(h.getItemsSeen());

        EXPECT_EQ(0, ml.itemsLogged[int(MutationLogType::New)]);
        EXPECT_EQ(0, ml.itemsLogged[int(MutationLogType::Commit1)]);
        EXPECT_EQ(0, ml.itemsLogged[int(MutationLogType::Commit2)]);

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

        EXPECT_EQ(2, ml.itemsLogged[int(MutationLogType::New)]);
        EXPECT_EQ(2, ml.itemsLogged[int(MutationLogType::Commit1)]);
        EXPECT_EQ(2, ml.itemsLogged[int(MutationLogType::Commit2)]);
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

        EXPECT_EQ(3, ml.itemsLogged[int(MutationLogType::New)]);
        EXPECT_EQ(1, ml.itemsLogged[int(MutationLogType::Commit1)]);
        EXPECT_EQ(1, ml.itemsLogged[int(MutationLogType::Commit2)]);
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

        EXPECT_EQ(10, ml.itemsLogged[int(MutationLogType::New)]);
        EXPECT_EQ(1, ml.itemsLogged[int(MutationLogType::Commit1)]);
        EXPECT_EQ(1, ml.itemsLogged[int(MutationLogType::Commit2)]);
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

class MockMutationLogEntryV1 : public MutationLogEntryV1 {
public:
    /**
     * Initialize a new entry inside the given buffer.
     *
     * @param r the rowid
     * @param t the type of log entry
     * @param vb the vbucket
     * @param k the key
     */
    static MockMutationLogEntryV1* newEntry(uint8_t* buf,
                                            uint64_t r,
                                            MutationLogType t,
                                            uint16_t vb,
                                            const std::string& k) {
        return new (buf) MockMutationLogEntryV1(r, t, vb, k);
    }

    MockMutationLogEntryV1(uint64_t r,
                           MutationLogType t,
                           uint16_t vb,
                           const std::string& k)
        : MutationLogEntryV1(r, t, vb, k) {
    }
};

TEST_F(MutationLogTest, upgrade) {
    // Craft a V1 format file
    LogHeaderBlock headerBlock(MutationLogVersion::V1);
    headerBlock.set(MIN_LOG_HEADER_SIZE);
    const auto* ptr = reinterpret_cast<uint8_t*>(&headerBlock);

    std::vector<uint8_t> toWrite(ptr, ptr + sizeof(LogHeaderBlock));
    toWrite.resize(MIN_LOG_HEADER_SIZE);

    // Make space for 2 byte CRC
    uint16_t zero = 0x2233;
    toWrite.insert(toWrite.end(),
                   reinterpret_cast<uint8_t*>(&zero),
                   reinterpret_cast<uint8_t*>(&zero) + sizeof(uint16_t));

    // Add items
    uint16_t items = 10;
    uint16_t swapped = htons(items);
    toWrite.insert(toWrite.end(),
                   reinterpret_cast<uint8_t*>(&swapped),
                   reinterpret_cast<uint8_t*>(&swapped) + sizeof(uint16_t));

    const uint16_t vbid = 3;
    std::vector<std::string> keys;
    for (int ii = 0; ii < items; ii++) {
        keys.push_back({"mykey" + std::to_string(ii)});
        std::vector<uint8_t> bytes(
                MockMutationLogEntryV1::len(keys.back().size()));
        (void)MockMutationLogEntryV1::newEntry(
                bytes.data(), 0, MutationLogType::New, vbid, keys.back());
        toWrite.insert(toWrite.end(), bytes.begin(), bytes.end());
    }

    // Now commit1 and commit2
    std::array<MutationLogType, 2> types = {
            {MutationLogType::Commit1, MutationLogType::Commit2}};
    for (auto t : types) {
        std::string key = "";
        std::vector<uint8_t> bytes(MockMutationLogEntryV1::len(key.size()));
        (void)MockMutationLogEntryV1::newEntry(bytes.data(), 0, t, vbid, key);
        toWrite.insert(toWrite.end(), bytes.begin(), bytes.end());
    }

    // Not handling multiple blocks, so check we're below 2x
    ASSERT_LT(toWrite.size(), MIN_LOG_HEADER_SIZE * 2);
    toWrite.resize(MIN_LOG_HEADER_SIZE * 2);

    // Now calc the CRC and copy into the CRC location
    // it (goes in the start of each block)
    uint32_t crc32(crc32buf(&toWrite[MIN_LOG_HEADER_SIZE + 2],
                            MIN_LOG_HEADER_SIZE - 2));
    uint16_t crc16(htons(crc32 & 0xffff));
    std::copy_n(reinterpret_cast<uint8_t*>(&crc16),
                sizeof(uint16_t),
                toWrite.begin() + MIN_LOG_HEADER_SIZE);

    // Now write the file
    {
        std::ofstream logFile(tmp_log_filename,
                              std::ios::out | std::ofstream::binary);
        std::copy(toWrite.begin(),
                  toWrite.end(),
                  std::ostreambuf_iterator<char>(logFile));
    }

    {
        MutationLog ml(tmp_log_filename.c_str());
        ml.open();
        MutationLogHarvester h(ml);
        h.setVBucket(vbid);

        // Ask for 2 items, ensure we get just two.
        auto next_it = h.loadBatch(ml.begin(), 2);
        EXPECT_NE(next_it, ml.end());

        std::map<StoredDocKey, uint64_t> maps[vbid + 1];
        h.apply(&maps, loaderFun);
        for (int i = 0; i < 2; i++) {
            EXPECT_TRUE(maps[vbid].count(makeStoredDocKey(keys[i])) == 1);
        }

        // Ask for the remainder
        next_it = h.loadBatch(next_it, items - 2);
        EXPECT_EQ(ml.end(), next_it);
        h.apply(&maps, loaderFun);
        for (int i = 2; i < items; i++) {
            EXPECT_TRUE(maps[vbid].count(makeStoredDocKey(keys[i])) == 1);
        }
    }
}
