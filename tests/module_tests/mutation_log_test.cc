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

#include <signal.h>

#include <algorithm>
#include <map>
#include <set>
#include <stdexcept>
#include <vector>

#include "assert.h"
#include "mutation_log.h"

#define TMP_LOG_FILE "/tmp/mlt_test.log"

static void testUnconfigured() {
    MutationLog ml("");
    ml.open();
    cb_assert(!ml.isEnabled());
    ml.newItem(3, "somekey", 931);
    ml.delItem(3, "somekey");
    ml.deleteAll(3);
    ml.commit1();
    ml.commit2();
    ml.flush();

    cb_assert(ml.begin() == ml.end());
}

static void testSyncSet() {

    // Some basics
    cb_assert((SYNC_COMMIT_1 | SYNC_COMMIT_2) == SYNC_FULL);
    cb_assert((FLUSH_COMMIT_1 | FLUSH_COMMIT_2) == FLUSH_FULL);
    // No overlap
    cb_assert((FLUSH_FULL & ~SYNC_FULL) == FLUSH_FULL);
    cb_assert((SYNC_FULL & ~FLUSH_FULL) == SYNC_FULL);

    //
    // Now the real tests.
    //
    MutationLog ml("");
    ml.open();

    cb_assert(ml.setSyncConfig("off"));
    cb_assert(ml.getSyncConfig() == 0);

    cb_assert(ml.setSyncConfig("commit1"));
    cb_assert(ml.getSyncConfig() == SYNC_COMMIT_1);

    cb_assert(ml.setSyncConfig("commit2"));
    cb_assert(ml.getSyncConfig() == SYNC_COMMIT_2);

    cb_assert(ml.setSyncConfig("full"));
    cb_assert(ml.getSyncConfig() == (SYNC_COMMIT_1|SYNC_COMMIT_2));

    cb_assert(!ml.setSyncConfig("otherwise"));

    // reset
    cb_assert(ml.setSyncConfig("off"));
    cb_assert(ml.getSyncConfig() == 0);

    //
    // Flush tests
    //
    cb_assert(ml.setFlushConfig("commit1"));
    cb_assert(ml.getFlushConfig() == FLUSH_COMMIT_1);

    cb_assert(ml.setFlushConfig("commit2"));
    cb_assert(ml.getFlushConfig() == FLUSH_COMMIT_2);

    cb_assert(ml.setFlushConfig("full"));
    cb_assert(ml.getFlushConfig() == (FLUSH_COMMIT_1|FLUSH_COMMIT_2));

    cb_assert(!ml.setFlushConfig("otherwise"));

    // reset
    cb_assert(ml.setSyncConfig("off"));
    cb_assert(ml.getSyncConfig() == 0);
    cb_assert(ml.setFlushConfig("off"));
    cb_assert(ml.getFlushConfig() == 0);

    //
    // Try both
    //

    cb_assert(ml.setSyncConfig("commit1"));
    cb_assert(ml.setFlushConfig("commit2"));
    cb_assert(ml.getSyncConfig() == SYNC_COMMIT_1);
    cb_assert(ml.getFlushConfig() == FLUSH_COMMIT_2);

    // Swap them and apply in reverse order.
    cb_assert(ml.setFlushConfig("commit1"));
    cb_assert(ml.setSyncConfig("commit2"));
    cb_assert(ml.getSyncConfig() == SYNC_COMMIT_2);
    cb_assert(ml.getFlushConfig() == FLUSH_COMMIT_1);
}

static void loaderFun(void *arg, uint16_t vb,
                      const std::string &k, uint64_t rowid) {
    std::map<std::string, uint64_t> *maps = reinterpret_cast<std::map<std::string, uint64_t> *>(arg);
    maps[vb][k] = rowid;
}

static void testLogging() {
    remove(TMP_LOG_FILE);

    {
        MutationLog ml(TMP_LOG_FILE);
        ml.open();

        ml.newItem(3, "key1", 1);
        ml.newItem(2, "key1", 2);
        ml.commit1();
        ml.commit2();
        ml.newItem(3, "key2", 3);
        ml.delItem(3, "key1");
        ml.commit1();
        ml.commit2();
        // Remaining:   3:key2, 2:key1

        cb_assert(ml.itemsLogged[ML_NEW] == 3);
        cb_assert(ml.itemsLogged[ML_DEL] == 1);
        cb_assert(ml.itemsLogged[ML_COMMIT1] == 2);
        cb_assert(ml.itemsLogged[ML_COMMIT2] == 2);
    }

    {
        MutationLog ml(TMP_LOG_FILE);
        ml.open();
        MutationLogHarvester h(ml);
        h.setVBucket(1);
        h.setVBucket(2);
        h.setVBucket(3);

        cb_assert(h.load());

        cb_assert(h.getItemsSeen()[ML_NEW] == 3);
        cb_assert(h.getItemsSeen()[ML_DEL] == 1);
        cb_assert(h.getItemsSeen()[ML_COMMIT1] == 2);
        cb_assert(h.getItemsSeen()[ML_COMMIT2] == 2);

        // Check stat copying
        ml.resetCounts(h.getItemsSeen());

        cb_assert(ml.itemsLogged[ML_NEW] == 3);
        cb_assert(ml.itemsLogged[ML_DEL] == 1);
        cb_assert(ml.itemsLogged[ML_COMMIT1] == 2);
        cb_assert(ml.itemsLogged[ML_COMMIT2] == 2);

        // See if we got what we expect.
        std::map<std::string, uint64_t> maps[4];
        h.apply(&maps, loaderFun);

        cb_assert(maps[0].size() == 0);
        cb_assert(maps[1].size() == 0);
        cb_assert(maps[2].size() == 1);
        cb_assert(maps[3].size() == 1);

        cb_assert(maps[2].find("key1") != maps[2].end());
        cb_assert(maps[3].find("key2") != maps[3].end());
    }

    remove(TMP_LOG_FILE);
}

static void testDelAll() {
    remove(TMP_LOG_FILE);

    {
        MutationLog ml(TMP_LOG_FILE);
        ml.open();

        ml.newItem(3, "key1", 1);
        ml.newItem(2, "key1", 2);
        ml.commit1();
        ml.commit2();
        ml.newItem(3, "key2", 3);
        ml.deleteAll(3);
        ml.commit1();
        ml.commit2();
        // Remaining:   2:key1

        cb_assert(ml.itemsLogged[ML_NEW] == 3);
        cb_assert(ml.itemsLogged[ML_DEL] == 0);
        cb_assert(ml.itemsLogged[ML_DEL_ALL] == 1);
        cb_assert(ml.itemsLogged[ML_COMMIT1] == 2);
        cb_assert(ml.itemsLogged[ML_COMMIT2] == 2);
    }

    {
        MutationLog ml(TMP_LOG_FILE);
        ml.open();
        MutationLogHarvester h(ml);
        h.setVBucket(1);
        h.setVBucket(2);
        h.setVBucket(3);

        cb_assert(h.load());

        cb_assert(h.getItemsSeen()[ML_NEW] == 3);
        cb_assert(h.getItemsSeen()[ML_DEL_ALL] == 1);
        cb_assert(h.getItemsSeen()[ML_COMMIT1] == 2);
        cb_assert(h.getItemsSeen()[ML_COMMIT2] == 2);

        // Check stat copying
        ml.resetCounts(h.getItemsSeen());

        cb_assert(ml.itemsLogged[ML_NEW] == 3);
        cb_assert(ml.itemsLogged[ML_DEL_ALL] == 1);
        cb_assert(ml.itemsLogged[ML_COMMIT1] == 2);
        cb_assert(ml.itemsLogged[ML_COMMIT2] == 2);

        // See if we got what we expect.
        std::map<std::string, uint64_t> maps[4];
        h.apply(&maps, loaderFun);

        cb_assert(maps[0].size() == 0);
        cb_assert(maps[1].size() == 0);
        cb_assert(maps[2].size() == 1);
        cb_assert(maps[3].size() == 0);

        cb_assert(maps[2].find("key1") != maps[2].end());
    }

    remove(TMP_LOG_FILE);
}

static bool leftover_compare(mutation_log_uncommitted_t a,
                             mutation_log_uncommitted_t b) {
    if (a.vbucket != b.vbucket) {
        return a.vbucket < b.vbucket;
    }

    if (a.key != b.key) {
        return a.key < b.key;
    }

    if (a.type != b.type) {
        return a.type < b.type;
    }

    return false;
}

static void testLoggingDirty() {
    remove(TMP_LOG_FILE);

    {
        MutationLog ml(TMP_LOG_FILE);
        ml.open();

        ml.newItem(3, "key1", 1);
        ml.newItem(2, "key1", 2);
        ml.commit1();
        ml.commit2();
        // These two will be dropped from the normal loading path
        // because there's no commit.
        ml.newItem(3, "key2", 3);
        ml.delItem(3, "key1");
        // Remaining:   3:key1, 2:key1

        cb_assert(ml.itemsLogged[ML_NEW] == 3);
        cb_assert(ml.itemsLogged[ML_DEL] == 1);
        cb_assert(ml.itemsLogged[ML_COMMIT1] == 1);
        cb_assert(ml.itemsLogged[ML_COMMIT2] == 1);
    }

    {
        MutationLog ml(TMP_LOG_FILE);
        ml.open();
        MutationLogHarvester h(ml);
        h.setVBucket(1);
        h.setVBucket(2);
        h.setVBucket(3);

        cb_assert(!h.load());

        cb_assert(h.getItemsSeen()[ML_NEW] == 3);
        cb_assert(h.getItemsSeen()[ML_DEL] == 1);
        cb_assert(h.getItemsSeen()[ML_COMMIT1] == 1);
        cb_assert(h.getItemsSeen()[ML_COMMIT2] == 1);

        // Check stat copying
        ml.resetCounts(h.getItemsSeen());

        cb_assert(ml.itemsLogged[ML_NEW] == 3);
        cb_assert(ml.itemsLogged[ML_DEL] == 1);
        cb_assert(ml.itemsLogged[ML_COMMIT1] == 1);
        cb_assert(ml.itemsLogged[ML_COMMIT2] == 1);

        // See if we got what we expect.
        std::map<std::string, uint64_t> maps[4];
        h.apply(&maps, loaderFun);

        cb_assert(maps[0].size() == 0);
        cb_assert(maps[1].size() == 0);
        cb_assert(maps[2].size() == 1);
        cb_assert(maps[3].size() == 1);

        cb_assert(maps[2].find("key1") != maps[2].end());
        cb_assert(maps[3].find("key1") != maps[3].end());
        cb_assert(maps[3].find("key2") == maps[3].end());

        // Check the leftovers
        std::vector<mutation_log_uncommitted_t> leftovers;
        h.getUncommitted(leftovers);
        std::sort(leftovers.begin(), leftovers.end(), leftover_compare);
        cb_assert(leftovers.size() == 2);
        cb_assert(leftovers[0].vbucket == 3);
        cb_assert(leftovers[0].key == "key1");
        cb_assert(leftovers[0].type == ML_DEL);
        cb_assert(leftovers[1].vbucket == 3);
        cb_assert(leftovers[1].key == "key2");
        cb_assert(leftovers[1].type == ML_NEW);
        cb_assert(leftovers[1].rowid == 3);
    }

    remove(TMP_LOG_FILE);
}

static void testLoggingBadCRC() {
    remove(TMP_LOG_FILE);

    {
        MutationLog ml(TMP_LOG_FILE);
        ml.open();

        ml.newItem(3, "key1", 1);
        ml.newItem(2, "key1", 2);
        ml.commit1();
        ml.commit2();
        ml.newItem(3, "key2", 3);
        ml.delItem(3, "key1");
        ml.commit1();
        ml.commit2();
        // Remaining:   3:key2, 2:key1

        cb_assert(ml.itemsLogged[ML_NEW] == 3);
        cb_assert(ml.itemsLogged[ML_DEL] == 1);
        cb_assert(ml.itemsLogged[ML_COMMIT1] == 2);
        cb_assert(ml.itemsLogged[ML_COMMIT2] == 2);
    }

    // Break the log
    int file = open(TMP_LOG_FILE, O_RDWR, 0666);
    cb_assert(lseek(file, 5000, SEEK_SET) == 5000);
    uint8_t b;
    cb_assert(read(file, &b, sizeof(b)) == 1);
    cb_assert(lseek(file, 5000, SEEK_SET) == 5000);
    b = ~b;
    cb_assert(write(file, &b, sizeof(b)) == 1);

    {
        MutationLog ml(TMP_LOG_FILE);
        ml.open();
        MutationLogHarvester h(ml);
        h.setVBucket(1);
        h.setVBucket(2);
        h.setVBucket(3);

        try {
            h.load();
            abort();
        } catch(MutationLog::CRCReadException &e) {
            // expected
        }

        cb_assert(h.getItemsSeen()[ML_NEW] == 0);
        cb_assert(h.getItemsSeen()[ML_DEL] == 0);
        cb_assert(h.getItemsSeen()[ML_COMMIT1] == 0);
        cb_assert(h.getItemsSeen()[ML_COMMIT2] == 0);

        // Check stat copying
        ml.resetCounts(h.getItemsSeen());

        cb_assert(ml.itemsLogged[ML_NEW] == 0);
        cb_assert(ml.itemsLogged[ML_DEL] == 0);
        cb_assert(ml.itemsLogged[ML_COMMIT1] == 0);
        cb_assert(ml.itemsLogged[ML_COMMIT2] == 0);

        // See if we got what we expect.
        std::map<std::string, uint64_t> maps[4];
        h.apply(&maps, loaderFun);

        cb_assert(maps[0].size() == 0);
        cb_assert(maps[1].size() == 0);
        cb_assert(maps[2].size() == 0);
        cb_assert(maps[3].size() == 0);

        cb_assert(maps[2].find("key1") == maps[2].end());
        cb_assert(maps[3].find("key2") == maps[3].end());
    }

    remove(TMP_LOG_FILE);
}

static void testLoggingShortRead() {
    remove(TMP_LOG_FILE);

    {
        MutationLog ml(TMP_LOG_FILE);
        ml.open();

        ml.newItem(3, "key1", 1);
        ml.newItem(2, "key1", 2);
        ml.commit1();
        ml.commit2();
        ml.newItem(3, "key2", 3);
        ml.delItem(3, "key1");
        ml.commit1();
        ml.commit2();
        // Remaining:   3:key2, 2:key1

        cb_assert(ml.itemsLogged[ML_NEW] == 3);
        cb_assert(ml.itemsLogged[ML_DEL] == 1);
        cb_assert(ml.itemsLogged[ML_COMMIT1] == 2);
        cb_assert(ml.itemsLogged[ML_COMMIT2] == 2);
    }

    // Break the log
    cb_assert(truncate(TMP_LOG_FILE, 5000) == 0);

    {
        MutationLog ml(TMP_LOG_FILE);
        try {
            ml.open();
            MutationLogHarvester h(ml);
            h.setVBucket(1);
            h.setVBucket(2);
            h.setVBucket(3);

            h.load();
            abort();
        } catch(MutationLog::ShortReadException &e) {
            // expected
        }
    }

    // Break the log harder (can't read even the initial block)
    cb_assert(truncate(TMP_LOG_FILE, 4000) == 0);

    {
        MutationLog ml(TMP_LOG_FILE);
        try {
            ml.open();
            abort();
        } catch(MutationLog::ShortReadException &e) {
            // expected
        }
    }

    remove(TMP_LOG_FILE);
}

static void testYUNOOPEN() {
    int file = open(TMP_LOG_FILE, O_CREAT|O_RDWR, 0);
    cb_assert(file >= 0);
    close(file);
    MutationLog ml(TMP_LOG_FILE);
    try {
        ml.open();
        abort();
    } catch(MutationLog::ReadException &e) {
        std::string exp("Unable to open log file: Permission denied");
        // This is kind of a soft assertion.  The actual text may vary.
        if (e.what() != exp) {
            std::cerr << "Expected ``" << exp << "'', got: " << e.what() << std::endl;
        }
    }
    cb_assert(remove(TMP_LOG_FILE) == 0);
}

// @todo
//   Test Read Only log
//   Test close / open / close / open
//   Fix copy constructor bug
//

static void testReadOnly() {
    MutationLog ml(TMP_LOG_FILE);
    try {
        ml.open(true);
        abort();
    } catch (MutationLog::FileNotFoundException &e) {
    }

    MutationLog m2(TMP_LOG_FILE);
    m2.open();
    m2.newItem(3, "key1", 1);
    m2.close();

    // We should be able to open the file now
    ml.open(true);

    // But we should not be able to add items to a read only stream
    try {
        ml.newItem(4, "key2", 1);
        abort();
    } catch (MutationLog::WriteException &e) {
    }
}

int main(int, char **) {
    testReadOnly();
    testUnconfigured();
    testSyncSet();
    testLogging();
    testDelAll();
    testLoggingDirty();
    testLoggingBadCRC();
    testLoggingShortRead();
    testYUNOOPEN();

    remove(TMP_LOG_FILE);
    return 0;
}
