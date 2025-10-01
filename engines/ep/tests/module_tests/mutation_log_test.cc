/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2011-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "test_file_helper.h"

#include "../mock/mock_function_helper.h"

#include <folly/portability/Fcntl.h>
#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>
#include <platform/cbassert.h>
#include <platform/dirutils.h>
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

#include "mutation_log.h"
#include "tests/module_tests/test_helpers.h"

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
        tmp_log_filename = cb::io::mktemp("mlt_test");
    }

    void TearDown() override {
        cb::io::rmrf(tmp_log_filename);
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

static bool loaderFun(void* arg, Vbid vb, const DocKey& k) {
    auto* sets = reinterpret_cast<std::set<StoredDocKey> *>(arg);
    sets[vb.get()].insert(StoredDocKey(k));
    return true;
}

TEST_F(MutationLogTest, Logging) {

    {
        MutationLog ml(tmp_log_filename.c_str());
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
        MutationLog ml(tmp_log_filename.c_str());
        ml.open();
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

TEST_F(MutationLogTest, LoggingDirty) {

    {
        MutationLog ml(tmp_log_filename.c_str());
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
        MutationLog ml(tmp_log_filename.c_str());
        ml.open();
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
        MutationLog ml(tmp_log_filename.c_str());
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
        MutationLog ml(tmp_log_filename.c_str());
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
        MutationLog ml(tmp_log_filename.c_str());

        ml.open();
        MutationLogHarvester h(ml);
        h.setVBucket(Vbid(1));
        h.setVBucket(Vbid(2));
        h.setVBucket(Vbid(3));

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

class MockFileIface : public mlog::DefaultFileIface {
public:
    MOCK_METHOD(
            ssize_t,
            pwrite,
            (file_handle_t fd, const void* buf, size_t nbyte, uint64_t offset),
            (override));
};

// MB-55939: Test behaviour when the mutation log cannot be written to disk
// (e.g. disk full).
TEST_F(MutationLogTest, WriteFail) {
    // Setup a Mock FileIface which will return one less byte from pwrite()
    // than requested and set errno to ENOSPC.
    using namespace ::testing;
    auto mockFileIface = std::make_unique<MockFileIface>();
    EXPECT_CALL(*mockFileIface, pwrite(_, _, _, _))
            .WillOnce([](file_handle_t, const void*, size_t nbytes, uint64_t) {
                errno = ENOSPC;
                return nbytes - 1;
            });

    // Test: Create and open a MutationLog; on destruction we should not see
    // an exception thrown.
    EXPECT_NO_THROW({
        MutationLog ml(tmp_log_filename,
                       MIN_LOG_HEADER_SIZE,
                       std::move(mockFileIface));
        ml.open();
    });
}

// Test that the MutationLog::iterator class obeys expected iterator behaviour.
TEST_F(MutationLogTest, Iterator) {
    // Create a simple mutation log to work on.
    {
        MutationLog ml(tmp_log_filename.c_str());
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
            ml.newItem(Vbid(ii % 2), makeStoredDocKey(key));
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
        h.setVBucket(Vbid(0));
        h.setVBucket(Vbid(1));

        // Ask for 2 items, ensure we get just two.
        auto next_it = h.loadBatch(ml.begin(), 2);
        EXPECT_NE(next_it, ml.end());

        std::set<StoredDocKey> maps[2];
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
    m2.newItem(Vbid(3), makeStoredDocKey("key1"));
    m2.close();

    // We should be able to open the file now
    ml.open(true);

    // But we should not be able to add items to a read only stream
    EXPECT_THROW(ml.newItem(Vbid(4), makeStoredDocKey("key2")),
                 MutationLog::WriteException);
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
                                            Vbid vb,
                                            const std::string& k) {
        return new (buf) MockMutationLogEntryV1(r, t, vb, k);
    }

    MockMutationLogEntryV1(uint64_t r,
                           MutationLogType t,
                           Vbid vb,
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

    const Vbid vbid = Vbid(3);
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

        std::set<StoredDocKey> maps[4]; // 4 <- vbid.get() + 1
        h.apply(&maps, loaderFun);
        for (int i = 0; i < 2; i++) {
            EXPECT_TRUE(maps[vbid.get()].count(makeStoredDocKey(keys[i])) == 1);
        }

        // Ask for the remainder
        next_it = h.loadBatch(next_it, items - 2);
        EXPECT_EQ(ml.end(), next_it);
        h.apply(&maps, loaderFun);
        for (int i = 2; i < items; i++) {
            EXPECT_TRUE(maps[vbid.get()].count(makeStoredDocKey(keys[i])) == 1);
        }
    }
}

TEST_F(MutationLogTest, upgradeV3toV4) {
    // V3 and V4 use the same format for the various entries, but use
    // a different CRC version
    class MockMutationLog : public MutationLog {
    public:
        MockMutationLog(std::string path, MutationLogVersion version)
            : MutationLog(path) {
            headerBlock.setVersion(version);
        }
    };

    const auto somekey = makeStoredDocKey("somekey");

    {
        MockMutationLog ml(tmp_log_filename, MutationLogVersion::V3);
        ml.open();
        ml.newItem(Vbid(3), somekey);
        ml.commit1();
        ml.commit2();
        ml.flush();
        ml.close();
    }

    {
        MutationLog ml(tmp_log_filename);
        ml.open(true);
        ASSERT_EQ(MutationLogVersion::V3, ml.header().version());
        auto iter = ml.begin();
        EXPECT_EQ(MutationLogType::New, (*iter)->type());
        EXPECT_EQ(static_cast<const DocKey&>(somekey), (*iter)->key());
        ++iter;
        EXPECT_EQ(MutationLogType::Commit1, (*iter)->type());
        ++iter;
        EXPECT_EQ(MutationLogType::Commit2, (*iter)->type());
    }
}

// matcher testing a DocKey against an expected key (std::string)
MATCHER_P(_key, expected, "") {
    auto key =
            std::string(reinterpret_cast<const char*>(arg.data()), arg.size());
    return key == expected;
}

// helper to call a std::function passed in arg for each mutation log entry.
bool mutationLogCB(void* arg, Vbid vbid, const DocKey& key) {
    const auto& func =
            *reinterpret_cast<std::function<bool(Vbid, const DocKey&)>*>(arg);
    return func(vbid, key);
}

TEST_F(MutationLogTest, readPreMadHatterAccessLog) {
    /* read an access log written by spock code to confirm
     * the upgrade path can correctly parse what was written.
     * File contains:
     * - 10 mutations (all for vbid 3)
     *    New: mykey0
     *    New: mykey1
     *    ...
     *    New: mykey10
     * - commit1
     * - commit2
     */

    auto vbid = Vbid(3);

    MutationLog ml(std::string(testsSourceDir) +
                   "/pre-mad-hatter_access_log.bin");
    ml.open(true);
    MutationLogHarvester h(ml);
    h.setVBucket(vbid);

    auto next_it = h.loadBatch(ml.begin(), 0);
    EXPECT_EQ(ml.end(), next_it);

    using namespace testing;
    // mutation log callback - called for each entry read.
    StrictMock<MockFunction<bool(Vbid, const DocKey&)>> cb;

    {
        // mark that the following EXPECT_CALLs must be triggered in order.
        InSequence s;

        // expect the callback to be called for every key known to be in the
        // "blessed input" in order, and not be called for anything else.
        for (int keyNum = 0; keyNum < 10; keyNum++) {
            using namespace std::literals;
            auto expected = "\0mykey"s + std::to_string(keyNum);
            EXPECT_CALL(cb, Call(vbid, _key(expected))).WillOnce(Return(true));
        }
    }

    auto mlcb = asStdFunction(cb);
    h.apply(&mlcb, mutationLogCB);
}
