/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <pthread.h>
#include <signal.h>
#include <unistd.h>

#include <vector>
#include <set>
#include <map>
#include <algorithm>
#include <stdexcept>

#include "assert.h"
#include "mutation_log.hh"

#define TMP_LOG_FILE "/tmp/mlt_test.log"

static void testUnconfigured() {
    MutationLog ml("");
    ml.open();
    assert(!ml.isEnabled());
    ml.newItem(3, "somekey", 931);
    ml.delItem(3, "somekey");
    ml.deleteAll(3);
    ml.commit1();
    ml.commit2();
    ml.flush();

    assert(ml.begin() == ml.end());
}

static void testSyncSet() {

    // Some basics
    assert(SYNC_COMMIT_1 | SYNC_COMMIT_2 == SYNC_FULL);
    assert(FLUSH_COMMIT_1 | FLUSH_COMMIT_2 == FLUSH_FULL);
    // No overlap
    assert((FLUSH_FULL & ~SYNC_FULL) == FLUSH_FULL);
    assert((SYNC_FULL & ~FLUSH_FULL) == SYNC_FULL);

    //
    // Now the real tests.
    //
    MutationLog ml("");
    ml.open();

    assert(ml.setSyncConfig("off"));
    assert(ml.getSyncConfig() == 0);

    assert(ml.setSyncConfig("commit1"));
    assert(ml.getSyncConfig() == SYNC_COMMIT_1);

    assert(ml.setSyncConfig("commit2"));
    assert(ml.getSyncConfig() == SYNC_COMMIT_2);

    assert(ml.setSyncConfig("full"));
    assert(ml.getSyncConfig() == SYNC_COMMIT_1|SYNC_COMMIT_2);

    assert(!ml.setSyncConfig("otherwise"));

    // reset
    assert(ml.setSyncConfig("off"));
    assert(ml.getSyncConfig() == 0);

    //
    // Flush tests
    //
    assert(ml.setFlushConfig("commit1"));
    assert(ml.getFlushConfig() == FLUSH_COMMIT_1);

    assert(ml.setFlushConfig("commit2"));
    assert(ml.getFlushConfig() == FLUSH_COMMIT_2);

    assert(ml.setFlushConfig("full"));
    assert(ml.getFlushConfig() == FLUSH_COMMIT_1|FLUSH_COMMIT_2);

    assert(!ml.setFlushConfig("otherwise"));

    // reset
    assert(ml.setSyncConfig("off"));
    assert(ml.getSyncConfig() == 0);
    assert(ml.setFlushConfig("off"));
    assert(ml.getFlushConfig() == 0);

    //
    // Try both
    //

    assert(ml.setSyncConfig("commit1"));
    assert(ml.setFlushConfig("commit2"));
    assert(ml.getSyncConfig() == SYNC_COMMIT_1);
    assert(ml.getFlushConfig() == FLUSH_COMMIT_2);

    // Swap them and apply in reverse order.
    assert(ml.setFlushConfig("commit1"));
    assert(ml.setSyncConfig("commit2"));
    assert(ml.getSyncConfig() == SYNC_COMMIT_2);
    assert(ml.getFlushConfig() == FLUSH_COMMIT_1);
}

static void loaderFun(void *arg, uint16_t vb, uint16_t,
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

        assert(ml.itemsLogged[ML_NEW] == 3);
        assert(ml.itemsLogged[ML_DEL] == 1);
        assert(ml.itemsLogged[ML_COMMIT1] == 2);
        assert(ml.itemsLogged[ML_COMMIT2] == 2);
    }

    {
        MutationLog ml(TMP_LOG_FILE);
        ml.open();
        MutationLogHarvester h(ml);
        h.setVbVer(1, 1);
        h.setVbVer(2, 1);
        h.setVbVer(3, 1);

        assert(h.load());

        assert(h.getItemsSeen()[ML_NEW] == 3);
        assert(h.getItemsSeen()[ML_DEL] == 1);
        assert(h.getItemsSeen()[ML_COMMIT1] == 2);
        assert(h.getItemsSeen()[ML_COMMIT2] == 2);

        // Check stat copying
        ml.resetCounts(h.getItemsSeen());

        assert(ml.itemsLogged[ML_NEW] == 3);
        assert(ml.itemsLogged[ML_DEL] == 1);
        assert(ml.itemsLogged[ML_COMMIT1] == 2);
        assert(ml.itemsLogged[ML_COMMIT2] == 2);

        // See if we got what we expect.
        std::map<std::string, uint64_t> maps[4];
        h.apply(&maps, loaderFun);

        assert(maps[0].size() == 0);
        assert(maps[1].size() == 0);
        assert(maps[2].size() == 1);
        assert(maps[3].size() == 1);

        assert(maps[2].find("key1") != maps[2].end());
        assert(maps[3].find("key2") != maps[3].end());
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

        assert(ml.itemsLogged[ML_NEW] == 3);
        assert(ml.itemsLogged[ML_DEL] == 0);
        assert(ml.itemsLogged[ML_DEL_ALL] == 1);
        assert(ml.itemsLogged[ML_COMMIT1] == 2);
        assert(ml.itemsLogged[ML_COMMIT2] == 2);
    }

    {
        MutationLog ml(TMP_LOG_FILE);
        ml.open();
        MutationLogHarvester h(ml);
        h.setVbVer(1, 1);
        h.setVbVer(2, 1);
        h.setVbVer(3, 1);

        assert(h.load());

        assert(h.getItemsSeen()[ML_NEW] == 3);
        assert(h.getItemsSeen()[ML_DEL_ALL] == 1);
        assert(h.getItemsSeen()[ML_COMMIT1] == 2);
        assert(h.getItemsSeen()[ML_COMMIT2] == 2);

        // Check stat copying
        ml.resetCounts(h.getItemsSeen());

        assert(ml.itemsLogged[ML_NEW] == 3);
        assert(ml.itemsLogged[ML_DEL_ALL] == 1);
        assert(ml.itemsLogged[ML_COMMIT1] == 2);
        assert(ml.itemsLogged[ML_COMMIT2] == 2);

        // See if we got what we expect.
        std::map<std::string, uint64_t> maps[4];
        h.apply(&maps, loaderFun);

        assert(maps[0].size() == 0);
        assert(maps[1].size() == 0);
        assert(maps[2].size() == 1);
        assert(maps[3].size() == 0);

        assert(maps[2].find("key1") != maps[2].end());
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

        assert(ml.itemsLogged[ML_NEW] == 3);
        assert(ml.itemsLogged[ML_DEL] == 1);
        assert(ml.itemsLogged[ML_COMMIT1] == 1);
        assert(ml.itemsLogged[ML_COMMIT2] == 1);
    }

    {
        MutationLog ml(TMP_LOG_FILE);
        ml.open();
        MutationLogHarvester h(ml);
        h.setVbVer(1, 1);
        h.setVbVer(2, 1);
        h.setVbVer(3, 1);

        assert(!h.load());

        assert(h.getItemsSeen()[ML_NEW] == 3);
        assert(h.getItemsSeen()[ML_DEL] == 1);
        assert(h.getItemsSeen()[ML_COMMIT1] == 1);
        assert(h.getItemsSeen()[ML_COMMIT2] == 1);

        // Check stat copying
        ml.resetCounts(h.getItemsSeen());

        assert(ml.itemsLogged[ML_NEW] == 3);
        assert(ml.itemsLogged[ML_DEL] == 1);
        assert(ml.itemsLogged[ML_COMMIT1] == 1);
        assert(ml.itemsLogged[ML_COMMIT2] == 1);

        // See if we got what we expect.
        std::map<std::string, uint64_t> maps[4];
        h.apply(&maps, loaderFun);

        assert(maps[0].size() == 0);
        assert(maps[1].size() == 0);
        assert(maps[2].size() == 1);
        assert(maps[3].size() == 1);

        assert(maps[2].find("key1") != maps[2].end());
        assert(maps[3].find("key1") != maps[3].end());
        assert(maps[3].find("key2") == maps[3].end());

        // Check the leftovers
        std::vector<mutation_log_uncommitted_t> leftovers;
        h.getUncommitted(leftovers);
        std::sort(leftovers.begin(), leftovers.end(), leftover_compare);
        assert(leftovers.size() == 2);
        assert(leftovers[0].vbucket == 3);
        assert(leftovers[0].key == "key1");
        assert(leftovers[0].type == ML_DEL);
        assert(leftovers[1].vbucket == 3);
        assert(leftovers[1].key == "key2");
        assert(leftovers[1].type == ML_NEW);
        assert(leftovers[1].rowid == 3);
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

        assert(ml.itemsLogged[ML_NEW] == 3);
        assert(ml.itemsLogged[ML_DEL] == 1);
        assert(ml.itemsLogged[ML_COMMIT1] == 2);
        assert(ml.itemsLogged[ML_COMMIT2] == 2);
    }

    // Break the log
    int file = open(TMP_LOG_FILE, O_RDWR, 0666);
    assert(lseek(file, 5000, SEEK_SET) == 5000);
    uint8_t b;
    assert(read(file, &b, sizeof(b)) == 1);
    assert(lseek(file, 5000, SEEK_SET) == 5000);
    b = ~b;
    assert(write(file, &b, sizeof(b)) == 1);

    {
        MutationLog ml(TMP_LOG_FILE);
        ml.open();
        MutationLogHarvester h(ml);
        h.setVbVer(1, 1);
        h.setVbVer(2, 1);
        h.setVbVer(3, 1);

        try {
            h.load();
            abort();
        } catch(MutationLog::CRCReadException e) {
            // expected
        }

        assert(h.getItemsSeen()[ML_NEW] == 0);
        assert(h.getItemsSeen()[ML_DEL] == 0);
        assert(h.getItemsSeen()[ML_COMMIT1] == 0);
        assert(h.getItemsSeen()[ML_COMMIT2] == 0);

        // Check stat copying
        ml.resetCounts(h.getItemsSeen());

        assert(ml.itemsLogged[ML_NEW] == 0);
        assert(ml.itemsLogged[ML_DEL] == 0);
        assert(ml.itemsLogged[ML_COMMIT1] == 0);
        assert(ml.itemsLogged[ML_COMMIT2] == 0);

        // See if we got what we expect.
        std::map<std::string, uint64_t> maps[4];
        h.apply(&maps, loaderFun);

        assert(maps[0].size() == 0);
        assert(maps[1].size() == 0);
        assert(maps[2].size() == 0);
        assert(maps[3].size() == 0);

        assert(maps[2].find("key1") == maps[2].end());
        assert(maps[3].find("key2") == maps[3].end());
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

        assert(ml.itemsLogged[ML_NEW] == 3);
        assert(ml.itemsLogged[ML_DEL] == 1);
        assert(ml.itemsLogged[ML_COMMIT1] == 2);
        assert(ml.itemsLogged[ML_COMMIT2] == 2);
    }

    // Break the log
    assert(truncate(TMP_LOG_FILE, 5000) == 0);

    {
        MutationLog ml(TMP_LOG_FILE);
        try {
            ml.open();
            MutationLogHarvester h(ml);
            h.setVbVer(1, 1);
            h.setVbVer(2, 1);
            h.setVbVer(3, 1);

            h.load();
            abort();
        } catch(MutationLog::ShortReadException e) {
            // expected
        }
    }

    // Break the log harder (can't read even the initial block)
    assert(truncate(TMP_LOG_FILE, 4000) == 0);

    {
        MutationLog ml(TMP_LOG_FILE);
        try {
            ml.open();
            abort();
        } catch(MutationLog::ShortReadException e) {
            // expected
        }
    }

    remove(TMP_LOG_FILE);
}

static void testYUNOOPEN() {
    int file = open(TMP_LOG_FILE, O_CREAT|O_RDWR, 0);
    assert(file >= 0);
    close(file);
    MutationLog ml(TMP_LOG_FILE);
    try {
        ml.open();
        abort();
    } catch(MutationLog::ReadException e) {
        std::string exp("Unable to open log file: Permission denied");
        // This is kind of a soft assertion.  The actual text may vary.
        if (e.what() != exp) {
            std::cerr << "Expected ``" << exp << "'', got: " << e.what() << std::endl;
        }
    }
    assert(remove(TMP_LOG_FILE) == 0);
}

int main(int, char **) {
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
