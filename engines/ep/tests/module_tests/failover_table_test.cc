/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2014-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "failover-table.h"

#include <folly/portability/GTest.h>

#include <engines/ep/src/bucket_logger.h>
#include <limits>

typedef std::list<failover_entry_t> table_t;

//create entries in the Failover table under test with
//specified no of entries and seqno hint
static table_t generate_entries(FailoverTable& table,
                                int numentries,
                                uint64_t seqno_range){

    table_t failover_entries;

    for(int i=0;i<numentries;i++){
        table.createEntry(i*150*seqno_range);
        failover_entries.push_front(table.getLatestEntry());
    }
    return failover_entries;
}

//TESTS BELOW
TEST(FailoverTableTest, test_initial_failover_log) {
    FailoverTable table(25);

    // rollback not needed
    EXPECT_FALSE(table.needsRollback(0,
                                     0,
                                     0,
                                     0,
                                     0,
                                     0,
                                     false /*strictVbUuidMatch*/,
                                     std::nullopt));

    // rollback needed
    auto result = table.needsRollback(
            10, 0, 0, 0, 0, 0, false /*strictVbUuidMatch*/, std::nullopt);
    ASSERT_TRUE(result);
    EXPECT_EQ(0, result->rollbackSeqno);
}

TEST(FailoverTableTest, test_5_failover_log) {
    uint64_t curr_seqno;

    FailoverTable table(25);
    table_t failover_entries = generate_entries(table, 5,1);

    // rollback not needed
    EXPECT_FALSE(table.needsRollback(0,
                                     0,
                                     0,
                                     0,
                                     0,
                                     0,
                                     false /*strictVbUuidMatch*/,
                                     std::nullopt));

    curr_seqno = table.getLatestEntry().by_seqno + 100;
    EXPECT_FALSE(table.needsRollback(10,
                                     curr_seqno,
                                     table.getLatestEntry().vb_uuid,
                                     0,
                                     20,
                                     0,
                                     false /*strictVbUuidMatch*/,
                                     std::nullopt));

    // rollback needed
    auto result = table.needsRollback(
            10, 0, 0, 0, 0, 0, false /*strictVbUuidMatch*/, std::nullopt);
    ASSERT_TRUE(result);
    EXPECT_EQ(0, result->rollbackSeqno);

    curr_seqno = table.getLatestEntry().by_seqno + 100;
    result = table.needsRollback(curr_seqno - 80,
                                 curr_seqno,
                                 table.getLatestEntry().vb_uuid,
                                 0,
                                 curr_seqno + 20,
                                 0,
                                 false /*strictVbUuidMatch*/,
                                 std::nullopt);
    ASSERT_TRUE(result);
    EXPECT_EQ(0, result->rollbackSeqno);
}

/* A DCP client can have a diverging (w.r.t producer) branch at seqno 0.
   Even though the client makes a request with start_seqno == 0, it expects a
   rollback if its vb_uuid @ 0 does not match the failover table vb_uuid @ 0 */
TEST(FailoverTableTest, test_diverging_branch_at_seqno_0) {
    FailoverTable table(5);
    table_t failover_entries = generate_entries(table, 2, 1);

    /* rollback not needed as vb_uuid == 0 (don't care) */
    EXPECT_FALSE(table.needsRollback(/*start_seqno*/ 0,
                                     0,
                                     /*vb_uuid*/ 0,
                                     0,
                                     0,
                                     0,
                                     true /*strictVbUuidMatch*/,
                                     std::nullopt));

    /* rollback not needed when vb_uuid matches the failover table vb_uuid */
    EXPECT_FALSE(table.needsRollback(/*start_seqno*/ 0,
                                     0,
                                     table.getLatestEntry().vb_uuid,
                                     0,
                                     0,
                                     0,
                                     true /*strictVbUuidMatch*/,
                                     std::nullopt));

    /* rollback needed as vb_uuid does not match and 'strictVbUuidMatch' is
       demanded */
    EXPECT_TRUE(table.needsRollback(/*start_seqno*/ 0,
                                    0,
                                    /*vb_uuid*/ 0xabcd,
                                    0,
                                    0,
                                    0,
                                    true /*strictVbUuidMatch*/,
                                    std::nullopt));

    /* rollback not needed as vb_uuid does not match and 'strictVbUuidMatch' is
       not demanded */
    EXPECT_FALSE(table.needsRollback(/*start_seqno*/ 0,
                                     0,
                                     /*vb_uuid*/ 0xabcd,
                                     0,
                                     0,
                                     0,
                                     false /*strictVbUuidMatch*/,
                                     std::nullopt));
}

TEST(FailoverTableTest, test_edgetests_failover_log) {
    uint64_t start_seqno;
    uint64_t snap_start_seqno;
    uint64_t snap_end_seqno;
    uint64_t curr_seqno;

    FailoverTable table(25);
    table.createEntry(100);
    table.createEntry(200);

    table_t failover_entries = generate_entries(table, 5,1);

    //TESTS for rollback not needed
    EXPECT_FALSE(table.needsRollback(0,
                                     0,
                                     0,
                                     0,
                                     0,
                                     0,
                                     false /*strictVbUuidMatch*/,
                                     std::nullopt));

    //start_seqno == snap_start_seqno == snap_end_seqno and start_seqno < upper
    curr_seqno = 300;
    start_seqno = 200;
    snap_start_seqno = 200;
    snap_end_seqno = 200;

    EXPECT_FALSE(table.needsRollback(start_seqno,
                                     curr_seqno,
                                     table.getLatestEntry().vb_uuid,
                                     snap_start_seqno,
                                     snap_end_seqno,
                                     0,
                                     false /*strictVbUuidMatch*/,
                                     std::nullopt));

    //start_seqno == snap_start_seqno and snap_end_seqno > upper
    curr_seqno = 300;
    start_seqno = 200;
    snap_start_seqno = 200;
    snap_end_seqno = 301;

    EXPECT_FALSE(table.needsRollback(start_seqno,
                                     curr_seqno,
                                     table.getLatestEntry().vb_uuid,
                                     snap_start_seqno,
                                     snap_end_seqno,
                                     0,
                                     false /*strictVbUuidMatch*/,
                                     std::nullopt));

    //start_seqno == snap_start_seqno == upper and snap_end_seqno > upper
    curr_seqno = 300;
    start_seqno = 300;
    snap_start_seqno = 300;
    snap_end_seqno = 301;

    EXPECT_FALSE(table.needsRollback(start_seqno,
                                     curr_seqno,
                                     table.getLatestEntry().vb_uuid,
                                     snap_start_seqno,
                                     snap_end_seqno,
                                     0,
                                     false /*strictVbUuidMatch*/,
                                     std::nullopt));

    //TEST for rollback needed

    //start_seqno == snap_start_seqno == snap_end_seqno and start_seqno > upper
    curr_seqno = 300;
    start_seqno = 400;
    snap_start_seqno = 400;
    snap_end_seqno = 400;

    auto result = table.needsRollback(start_seqno,
                                      curr_seqno,
                                      table.getLatestEntry().vb_uuid,
                                      snap_start_seqno,
                                      snap_end_seqno,
                                      0,
                                      false /*strictVbUuidMatch*/,
                                      std::nullopt);
    ASSERT_TRUE(result);
    EXPECT_EQ(curr_seqno, result->rollbackSeqno);

    //start_seqno > snap_start_seqno and snap_end_seqno > upper
    curr_seqno = 300;
    start_seqno = 220;
    snap_start_seqno = 210;
    snap_end_seqno = 301;

    result = table.needsRollback(start_seqno,
                                 curr_seqno,
                                 table.getLatestEntry().vb_uuid,
                                 snap_start_seqno,
                                 snap_end_seqno,
                                 0,
                                 false /*strictVbUuidMatch*/,
                                 std::nullopt);
    ASSERT_TRUE(result);
    EXPECT_EQ(snap_start_seqno, result->rollbackSeqno);

    //start_seqno > upper and snap_end_seqno > upper
    curr_seqno = 300;
    start_seqno = 310;
    snap_start_seqno = 210;
    snap_end_seqno = 320;

    result = table.needsRollback(start_seqno,
                                 curr_seqno,
                                 table.getLatestEntry().vb_uuid,
                                 snap_start_seqno,
                                 snap_end_seqno,
                                 0,
                                 false /*strictVbUuidMatch*/,
                                 std::nullopt);
    ASSERT_TRUE(result);
    EXPECT_EQ(snap_start_seqno, result->rollbackSeqno);
}

TEST(FailoverTableTest, test_5_failover_largeseqno_log) {
    uint64_t start_seqno;
    uint64_t curr_seqno;
    uint64_t vb_uuid;

    FailoverTable table(25);
    uint64_t range = std::numeric_limits<uint64_t>::max()/(5*150);
    table_t failover_entries = generate_entries(table, 5, range);

    //TESTS for rollback not needed
    EXPECT_FALSE(table.needsRollback(0,
                                     0,
                                     0,
                                     0,
                                     0,
                                     0,
                                     false /*strictVbUuidMatch*/,
                                     std::nullopt));

    vb_uuid = table.getLatestEntry().vb_uuid;
    curr_seqno = table.getLatestEntry().by_seqno + 100;
    start_seqno = 10;
    //snapshot end seqno less than upper
    EXPECT_FALSE(table.needsRollback(start_seqno,
                                     curr_seqno,
                                     vb_uuid,
                                     0,
                                     20,
                                     0,
                                     false /*strictVbUuidMatch*/,
                                     std::nullopt));

    //TESTS for rollback needed
    auto result = table.needsRollback(
            10, 0, 0, 0, 0, 0, false /*strictVbUuidMatch*/, std::nullopt);
    ASSERT_TRUE(result);
    EXPECT_EQ(0, result->rollbackSeqno);

    //vbucket uuid sent by client not present in failover table
    result = table.needsRollback(start_seqno,
                                 curr_seqno,
                                 0,
                                 0,
                                 20,
                                 0,
                                 false /*strictVbUuidMatch*/,
                                 std::nullopt);
    ASSERT_TRUE(result);
    EXPECT_EQ(0, result->rollbackSeqno);

    vb_uuid = table.getLatestEntry().vb_uuid;
    curr_seqno = table.getLatestEntry().by_seqno + 100;
    start_seqno = curr_seqno-80;
    //snapshot sequence no is greater than upper && snapshot start sequence no
    // less than upper
    result = table.needsRollback(start_seqno,
                                 curr_seqno,
                                 vb_uuid,
                                 curr_seqno - 20,
                                 curr_seqno + 20,
                                 0,
                                 false /*strictVbUuidMatch*/,
                                 std::nullopt);
    ASSERT_TRUE(result);
    EXPECT_EQ((curr_seqno - 20), result->rollbackSeqno);
    //snapshot start seqno greate than  upper
    result = table.needsRollback(curr_seqno + 20,
                                 curr_seqno,
                                 vb_uuid,
                                 curr_seqno + 10,
                                 curr_seqno + 40,
                                 0,
                                 false /*strictVbUuidMatch*/,
                                 0);
    ASSERT_TRUE(result);
    EXPECT_EQ(curr_seqno, result->rollbackSeqno);
    //client vb uuiud is not the latest vbuuid in failover table and
    //snap_end_seqno > upper && snap_start_seqno > upper
    auto itr = failover_entries.begin();
    ++itr;
    vb_uuid = itr->vb_uuid;
    --itr;
    result = table.needsRollback(itr->by_seqno - 5,
                                 curr_seqno,
                                 vb_uuid,
                                 itr->by_seqno - 10,
                                 itr->by_seqno + 40,
                                 0,
                                 false /*strictVbUuidMatch*/,
                                 std::nullopt);
    ASSERT_TRUE(result);
    EXPECT_EQ(((itr->by_seqno) - 10), result->rollbackSeqno);
    //client vb uuiud is not the latest vbuuid in failover table and
    //snapshot start seqno greate than  upper
    result = table.needsRollback(itr->by_seqno + 20,
                                 curr_seqno,
                                 vb_uuid,
                                 itr->by_seqno + 10,
                                 itr->by_seqno + 40,
                                 0,
                                 false /*strictVbUuidMatch*/,
                                 std::nullopt);
    ASSERT_TRUE(result);
    EXPECT_EQ(itr->by_seqno, result->rollbackSeqno);
}

TEST(FailoverTableTest, test_pop_5_failover_log) {
    FailoverTable table(25);
    table_t failover_entries = generate_entries(table, 30,1);


    //Verify seq no. in latest entry
    EXPECT_EQ(29*150, table.getLatestEntry().by_seqno);
    EXPECT_EQ(failover_entries.front().by_seqno, table.getLatestEntry().by_seqno);

    // rollback not needed
    EXPECT_FALSE(table.needsRollback(0,
                                     0,
                                     0,
                                     0,
                                     0,
                                     0,
                                     false /*strictVbUuidMatch*/,
                                     std::nullopt));

    // rollback needed
    auto result = table.needsRollback(
            10, 0, 0, 0, 0, 0, false /*strictVbUuidMatch*/, std::nullopt);
    ASSERT_TRUE(result);
    EXPECT_EQ(0, result->rollbackSeqno);
}

TEST(FailoverTableTest, test_add_entry) {
    /* Capacity of max 10 entries */
    const int max_entries = 10;
    FailoverTable table(max_entries);

    /* Add 4 entries with increasing order of seqno */
    const int low_seqno = 100, step = 100;
    for (int i = 0; i < (max_entries/2); ++i) {
        table.createEntry(low_seqno + i * step);
    }

    /* We must have all the entries we added + one default entry with seqno == 0
       that was added when we created failover table */
    EXPECT_EQ((max_entries/2 + 1), table.getNumEntries());

    /* Add an entry such that low_seqno < seqno < low_seqno + step.
       Now the table must have only 3 entries: 0, low_seqno, seqno */
    table.createEntry(low_seqno + step - 1);
    EXPECT_EQ(3, table.getNumEntries());
}

TEST(FailoverTableTest, rollback_log_messages) {
    /* Doesn't actually test functionality, just allows manual confirmation of
     * the logged messages */
    FailoverTable table(25);

    table_t failover_entries = generate_entries(table, 1, 50);

    uint64_t vb_uuid = table.getLatestEntry().vb_uuid;

    EP_LOG_WARN("{}",
                table.needsRollback(10,
                                    0,
                                    0,
                                    0,
                                    0,
                                    20,
                                    false /*strictVbUuidMatch*/,
                                    std::nullopt)
                        ->rollbackReason);
    EP_LOG_WARN("{}",
                table.needsRollback(10,
                                    0,
                                    0,
                                    0,
                                    0,
                                    0,
                                    false /*strictVbUuidMatch*/,
                                    std::nullopt)
                        ->rollbackReason);
    EP_LOG_WARN("{}",
                table.needsRollback(10,
                                    0,
                                    vb_uuid,
                                    0,
                                    100,
                                    0,
                                    false /*strictVbUuidMatch*/,
                                    std::nullopt)
                        ->rollbackReason);
    EP_LOG_WARN("{}",
                table.needsRollback(10,
                                    15,
                                    vb_uuid,
                                    20,
                                    100,
                                    0,
                                    false /*strictVbUuidMatch*/,
                                    std::nullopt)
                        ->rollbackReason);
}

TEST(FailoverTableTest, test_max_capacity) {
    /* Capacity of max 5 entries */
    const int max_entries = 5;
    FailoverTable table(max_entries);

    const int low_seqno = 100, step = 100;
    for (int i = 0; i < max_entries + 2; ++i) {
        table.createEntry(low_seqno + i * step);
    }
    const int max_seqno = low_seqno + (max_entries + 1) * step;

    /* Expect to have only max num of entries */
    EXPECT_EQ(max_entries, table.getNumEntries());

    /* The table must remove entries in FIFO */
    EXPECT_EQ(max_seqno, table.getLatestEntry().by_seqno);
}

TEST(FailoverTableTest, test_sanitize_failover_table) {
    const int numErroneousEntries = 4, numCorrectEntries = 2;

    nlohmann::json failover_json = {
            {{"id", 0}, {"seq", 0}},
            {{"id", 1356861809263}, {"seq", 100}},
            {{"id", 227813077095126}, {"seq", 200}},
            {{"id", 227813077095128}, {"seq", 300}},
            {{"id", 0}, {"seq", 50}},
            {{"id", 160260368866392}, {"seq", 0}},
    };

    FailoverTable table(failover_json, 10 /* max_entries */, 0);

    EXPECT_EQ(numCorrectEntries, table.getNumEntries());
    EXPECT_EQ(numErroneousEntries, table.getNumErroneousEntriesErased());
    EXPECT_NE(failover_json, table.getJSON());
}

// Test that after removing everything, the table is still sane
TEST(FailoverTableTest, test_sanitize_failover_table_all) {
    const int numErroneousEntries = 2, numCorrectEntries = 1;
    nlohmann::json failover_json = {{{"id", 0}, {"seq", 0}},
                                    {{"id", 0}, {"seq", 50}}};

    FailoverTable table(failover_json, 10 /* max_entries */, 0);

    EXPECT_EQ(numCorrectEntries, table.getNumEntries());
    EXPECT_EQ(numErroneousEntries, table.getNumErroneousEntriesErased());
    EXPECT_NE(failover_json, table.getJSON());
}
