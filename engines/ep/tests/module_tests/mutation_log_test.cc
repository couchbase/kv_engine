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

#include <cbcrypto/symmetric.h>
#include <folly/portability/Fcntl.h>
#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>
#include <platform/dirutils.h>
#include <sys/stat.h>
#include <algorithm>
#include <fstream>
#include <set>
#include <stdexcept>

#include "mutation_log.h"
#include "mutation_log_writer.h"
#include "tests/module_tests/test_helpers.h"

class MutationLogTest : public ::testing::Test,
                        public ::testing::WithParamInterface<bool> {
protected:
    void SetUp() override {
        // Generate a temporary log filename.
        tmp_log_filename = cb::io::mktemp("mlt_test");
        if (GetParam()) {
            encryption_key = cb::crypto::DataEncryptionKey::generate();
        }
    }

    void TearDown() override {
        std::filesystem::remove(tmp_log_filename);
    }

    // Storage for temporary log filename
    std::string tmp_log_filename;
    cb::crypto::SharedEncryptionKey encryption_key;
    std::function<cb::crypto::SharedEncryptionKey(std::string_view)>
            key_lookup_function = [this](std::string_view key) {
                if (encryption_key && key == encryption_key->getId()) {
                    return encryption_key;
                }
                return cb::crypto::SharedEncryptionKey{};
            };
};

static bool loaderFun(void* arg, Vbid vb, const DocKeyView& k) {
    auto* sets = reinterpret_cast<std::set<StoredDocKey> *>(arg);
    sets[vb.get()].insert(StoredDocKey(k));
    return true;
}

TEST_P(MutationLogTest, Logging) {
    {
        MutationLogWriter ml(
                tmp_log_filename, MIN_LOG_HEADER_SIZE, encryption_key);

        ml.newItem(Vbid(2), makeStoredDocKey("key1"));
        ml.commit1();
        ml.commit2();
        ml.newItem(Vbid(3), makeStoredDocKey("key2"));
        ml.commit1();
        ml.commit2();
        // Remaining:   3:key2, 2:key1

        EXPECT_EQ(2, ml.getItemsLogged(MutationLogType::New));
        EXPECT_EQ(2, ml.getItemsLogged(MutationLogType::Commit1));
        EXPECT_EQ(2, ml.getItemsLogged(MutationLogType::Commit2));
    }

    {
        MutationLogReader ml(tmp_log_filename, key_lookup_function);
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

        EXPECT_EQ(2, ml.getItemsLogged(MutationLogType::New));
        EXPECT_EQ(2, ml.getItemsLogged(MutationLogType::Commit1));
        EXPECT_EQ(2, ml.getItemsLogged(MutationLogType::Commit2));

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

TEST_P(MutationLogTest, SmallerBlockSize) {
    {
        MutationLogWriter ml(tmp_log_filename, 512);

        ml.newItem(Vbid(2), makeStoredDocKey("key1"));
        ml.commit1();
        ml.commit2();
        ml.close();
    }

    {
        MutationLogReader ml(tmp_log_filename);
        MutationLogHarvester h(ml);
        h.setVBucket(Vbid(2));

        EXPECT_TRUE(h.load());

        EXPECT_EQ(1, h.getItemsSeen()[int(MutationLogType::New)]);
        EXPECT_EQ(1, h.getItemsSeen()[int(MutationLogType::Commit1)]);
        EXPECT_EQ(1, h.getItemsSeen()[int(MutationLogType::Commit2)]);
    }
}

TEST_P(MutationLogTest, LoggingDirty) {
    {
        MutationLogWriter ml(
                tmp_log_filename, MIN_LOG_HEADER_SIZE, encryption_key);

        ml.newItem(Vbid(3), makeStoredDocKey("key1"));
        ml.newItem(Vbid(2), makeStoredDocKey("key1"));
        ml.commit1();
        ml.commit2();
        // This will be dropped from the normal loading path
        // because there's no commit.
        ml.newItem(Vbid(3), makeStoredDocKey("key2"));
        // Remaining:   3:key1, 2:key1

        EXPECT_EQ(3, ml.getItemsLogged(MutationLogType::New));
        EXPECT_EQ(1, ml.getItemsLogged(MutationLogType::Commit1));
        EXPECT_EQ(1, ml.getItemsLogged(MutationLogType::Commit2));
    }

    {
        MutationLogReader ml(tmp_log_filename, key_lookup_function);
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

        EXPECT_EQ(3, ml.getItemsLogged(MutationLogType::New));
        EXPECT_EQ(1, ml.getItemsLogged(MutationLogType::Commit1));
        EXPECT_EQ(1, ml.getItemsLogged(MutationLogType::Commit2));

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

TEST_P(MutationLogTest, LoggingBadCRC) {
    {
        MutationLogWriter ml(
                tmp_log_filename, MIN_LOG_HEADER_SIZE, encryption_key);

        ml.newItem(Vbid(2), makeStoredDocKey("key1"));
        ml.commit1();
        ml.commit2();
        ml.newItem(Vbid(3), makeStoredDocKey("key2"));
        ml.commit1();
        ml.commit2();
        // Remaining:   3:key2, 2:key1

        EXPECT_EQ(2, ml.getItemsLogged(MutationLogType::New));
        EXPECT_EQ(2, ml.getItemsLogged(MutationLogType::Commit1));
        EXPECT_EQ(2, ml.getItemsLogged(MutationLogType::Commit2));
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
        MutationLogReader ml(tmp_log_filename, key_lookup_function);
        MutationLogHarvester h(ml);
        h.setVBucket(Vbid(1));
        h.setVBucket(Vbid(2));
        h.setVBucket(Vbid(3));

        if (GetParam()) {
            //
            EXPECT_THROW(h.load(), cb::crypto::MacVerificationError);
        } else {
            EXPECT_THROW(h.load(), MutationLogReader::CRCReadException);
        }

        EXPECT_EQ(0, h.getItemsSeen()[int(MutationLogType::New)]);
        EXPECT_EQ(0, h.getItemsSeen()[int(MutationLogType::Commit1)]);
        EXPECT_EQ(0, h.getItemsSeen()[int(MutationLogType::Commit2)]);

        // Check stat copying
        ml.resetCounts(h.getItemsSeen());

        EXPECT_EQ(0, ml.getItemsLogged(MutationLogType::New));
        EXPECT_EQ(0, ml.getItemsLogged(MutationLogType::Commit1));
        EXPECT_EQ(0, ml.getItemsLogged(MutationLogType::Commit2));

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

TEST_P(MutationLogTest, LoggingShortRead) {
    {
        MutationLogWriter ml(
                tmp_log_filename, MIN_LOG_HEADER_SIZE, encryption_key);

        ml.newItem(Vbid(2), makeStoredDocKey("key1"));
        ml.commit1();
        ml.commit2();
        ml.newItem(Vbid(3), makeStoredDocKey("key2"));
        ml.commit1();
        ml.commit2();
        // Remaining:   3:key2, 2:key1

        EXPECT_EQ(2, ml.getItemsLogged(MutationLogType::New));
        EXPECT_EQ(2, ml.getItemsLogged(MutationLogType::Commit1));
        EXPECT_EQ(2, ml.getItemsLogged(MutationLogType::Commit2));
    }

    // Break the log
    EXPECT_EQ(0, truncate(tmp_log_filename.c_str(), 5000));

    {
        MutationLogReader ml(tmp_log_filename, key_lookup_function);
        MutationLogHarvester h(ml);
        h.setVBucket(Vbid(1));
        h.setVBucket(Vbid(2));
        h.setVBucket(Vbid(3));

        if (GetParam()) {
            EXPECT_THROW(h.load(), std::underflow_error);
        } else {
            EXPECT_THROW(h.load(), MutationLogReader::ShortReadException);
        }
    }

    // Break the log harder (can't read even the initial block)
    // This should succeed as open() will call reset() to give us a usable
    // mutation log.

    if (GetParam()) {
        EXPECT_EQ(0,
                  truncate(tmp_log_filename.c_str(),
                           sizeof(cb::crypto::EncryptedFileHeader) + 8));
        EXPECT_THROW(
                MutationLogReader ml(tmp_log_filename, key_lookup_function),
                std::underflow_error);
    } else {
        EXPECT_EQ(0, truncate(tmp_log_filename.c_str(), 8));
        EXPECT_THROW(
                MutationLogReader ml(tmp_log_filename, key_lookup_function),
                MutationLogReader::ShortReadException);
    }
}

TEST_P(MutationLogTest, YUNOOPEN) {
    // Make file unreadable
    std::filesystem::permissions(tmp_log_filename,
                                 std::filesystem::perms::none);
    try {
        MutationLogReader ml(tmp_log_filename, key_lookup_function);
        FAIL() << "Should not have access to logfile; ";
#ifdef WIN32
    } catch (const MutationLogReader::ReadException&) {
#else
    } catch (const std::system_error& error) {
        EXPECT_EQ(EACCES, error.code().value());
#endif
    }
    // Restore permissions to be able to delete file.
    std::filesystem::permissions(tmp_log_filename,
                                 std::filesystem::perms::owner_read |
                                         std::filesystem::perms::owner_write);
}

// MB-55939: Test behaviour when the mutation log cannot be written to disk
// (e.g. disk full).
TEST_P(MutationLogTest, WriteFail) {
    // Test: Create and open a MutationLogReader; on destruction we should not
    // see an exception thrown.
    bool hookCalled = false;
    {
        MutationLogWriter ml(tmp_log_filename,
                             MIN_LOG_HEADER_SIZE,
                             encryption_key,
                             cb::crypto::Compression::None,
                             [&hookCalled](auto method) {
                                 if (method == "~MutationLogWriter") {
                                     hookCalled = true;
                                     throw std::system_error(
                                             ENOSPC,
                                             std::system_category(),
                                             "~MutationLogWriter");
                                 }
                             });
    }
    EXPECT_TRUE(hookCalled);
}

// Test that the MutationLogReader::iterator class obeys expected iterator
// behaviour.
TEST_P(MutationLogTest, Iterator) {
    // Create a simple mutation log to work on.
    constexpr std::size_t num_items = 10000;
    {
        MutationLogWriter ml(tmp_log_filename,
                             MIN_LOG_HEADER_SIZE,
                             encryption_key,
                             cb::crypto::Compression::ZLIB);
        for (std::size_t ii = 0; ii < num_items; ++ii) {
            ml.newItem(Vbid(0), makeStoredDocKey(fmt::format("key{}", ii)));
        }
        ml.commit1();
        ml.commit2();

        EXPECT_EQ(num_items, ml.getItemsLogged(MutationLogType::New));
        EXPECT_EQ(1, ml.getItemsLogged(MutationLogType::Commit1));
        EXPECT_EQ(1, ml.getItemsLogged(MutationLogType::Commit2));
        ml.close();
    }

    // Now check the iterators.
    MutationLogReader ml(tmp_log_filename, key_lookup_function);

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
    EXPECT_EQ(num_items + 2, count);
}

TEST_P(MutationLogTest, BatchLoad) {
    {
        MutationLogWriter ml(
                tmp_log_filename, MIN_LOG_HEADER_SIZE, encryption_key);

        // Add a number of items, then check that batch load only returns
        // the requested number.
        for (size_t ii = 0; ii < 10; ii++) {
            std::string key = std::string("key") + std::to_string(ii);
            ml.newItem(Vbid(ii % 2), makeStoredDocKey(key));
        }
        ml.commit1();
        ml.commit2();

        EXPECT_EQ(10, ml.getItemsLogged(MutationLogType::New));
        EXPECT_EQ(1, ml.getItemsLogged(MutationLogType::Commit1));
        EXPECT_EQ(1, ml.getItemsLogged(MutationLogType::Commit2));
    }

    {
        MutationLogReader ml(tmp_log_filename, key_lookup_function);
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

INSTANTIATE_TEST_SUITE_P(EncryptionOnOff,
                         MutationLogTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());
