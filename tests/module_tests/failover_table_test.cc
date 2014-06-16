/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2014 Couchbase, Inc
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
#include <limits>
#include "failover-table.h"

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
static void test_initial_failover_log() {
    uint64_t rollback_seqno;
    FailoverTable table(25);

    // rollback not needed
    cb_assert(!table.needsRollback(0, 0, 0, 0, 0, &rollback_seqno));

    // rollback needed
    cb_assert(table.needsRollback(10, 0, 0, 0, 0, &rollback_seqno));
    cb_assert(rollback_seqno == 0);
}

static void test_5_failover_log() {
    uint64_t rollback_seqno;
    uint64_t curr_seqno;

    FailoverTable table(25);
    table_t failover_entries = generate_entries(table, 5,1);

    // rollback not needed
    cb_assert(!table.needsRollback(0, 0, 0, 0, 0, &rollback_seqno));

    curr_seqno = table.getLatestEntry().by_seqno + 100;
    cb_assert(!table.needsRollback(10, curr_seqno,
                                   table.getLatestEntry().vb_uuid,
                                   0, 20, &rollback_seqno));

    // rollback needed
    cb_assert(table.needsRollback(10, 0, 0, 0, 0, &rollback_seqno));
    cb_assert(rollback_seqno == 0);

    curr_seqno = table.getLatestEntry().by_seqno + 100;
    cb_assert(table.needsRollback(curr_seqno-80, curr_seqno,
                                  table.getLatestEntry().vb_uuid, 0,
                                  curr_seqno+20, &rollback_seqno));
    cb_assert(rollback_seqno == 0);
}

static void test_edgetests_failover_log() {
    uint64_t start_seqno;
    uint64_t snap_start_seqno;
    uint64_t snap_end_seqno;
    uint64_t rollback_seqno;
    uint64_t curr_seqno;

    FailoverTable table(25);
    table.createEntry(100);
    table.createEntry(200);

    table_t failover_entries = generate_entries(table, 5,1);

    //TESTS for rollback not needed
    cb_assert(!table.needsRollback(0, 0, 0, 0, 0, &rollback_seqno));

    //start_seqno == snap_start_seqno == snap_end_seqno and start_seqno < upper
    curr_seqno = 300;
    start_seqno = 200;
    snap_start_seqno = 200;
    snap_end_seqno = 200;

    cb_assert(!table.needsRollback(start_seqno, curr_seqno,
                                   table.getLatestEntry().vb_uuid,
                                   snap_start_seqno, snap_end_seqno,
                                   &rollback_seqno));

    //start_seqno == snap_start_seqno and snap_end_seqno > upper
    curr_seqno = 300;
    start_seqno = 200;
    snap_start_seqno = 200;
    snap_end_seqno = 301;

        cb_assert(!table.needsRollback(start_seqno, curr_seqno,
                                   table.getLatestEntry().vb_uuid,
                                   snap_start_seqno, snap_end_seqno,
                                   &rollback_seqno));

    //start_seqno == snap_start_seqno == upper and snap_end_seqno > upper
    curr_seqno = 300;
    start_seqno = 300;
    snap_start_seqno = 300;
    snap_end_seqno = 301;
    
    cb_assert(!table.needsRollback(start_seqno, curr_seqno,
                                   table.getLatestEntry().vb_uuid,
                                   snap_start_seqno, snap_end_seqno,
                                   &rollback_seqno));


    //TEST for rollback needed

    //start_seqno == snap_start_seqno == snap_end_seqno and start_seqno > upper
    curr_seqno = 300;
    start_seqno = 400;
    snap_start_seqno = 400;
    snap_end_seqno = 400;

    cb_assert(table.needsRollback(start_seqno, curr_seqno,
                                   table.getLatestEntry().vb_uuid,
                                   snap_start_seqno, snap_end_seqno,
                                   &rollback_seqno));
    cb_assert(rollback_seqno == curr_seqno);

    //start_seqno > snap_start_seqno and snap_end_seqno > upper
    curr_seqno = 300;
    start_seqno = 220;
    snap_start_seqno = 210;
    snap_end_seqno = 301;

    cb_assert(table.needsRollback(start_seqno, curr_seqno,
                                   table.getLatestEntry().vb_uuid,
                                   snap_start_seqno, snap_end_seqno,
                                   &rollback_seqno));
    cb_assert(rollback_seqno == snap_start_seqno);


    //start_seqno > upper and snap_end_seqno > upper
    curr_seqno = 300;
    start_seqno = 310;
    snap_start_seqno = 210;
    snap_end_seqno = 320;

    cb_assert(table.needsRollback(start_seqno, curr_seqno,
                                   table.getLatestEntry().vb_uuid,
                                   snap_start_seqno, snap_end_seqno,
                                   &rollback_seqno));
    cb_assert(rollback_seqno == snap_start_seqno);
}

static void test_5_failover_largeseqno_log() {
    uint64_t start_seqno;
    uint64_t rollback_seqno;
    uint64_t curr_seqno;
    uint64_t vb_uuid;

    FailoverTable table(25);
    uint64_t range = std::numeric_limits<uint64_t>::max()/(5*150);
    table_t failover_entries = generate_entries(table, 5, range);

    //TESTS for rollback not needed
    cb_assert(!table.needsRollback(0, 0, 0, 0, 0, &rollback_seqno));

    vb_uuid = table.getLatestEntry().vb_uuid;
    curr_seqno = table.getLatestEntry().by_seqno + 100;
    start_seqno = 10;
    //snapshot end seqno less than upper
    cb_assert(!table.needsRollback(start_seqno, curr_seqno, vb_uuid, 0, 20,
              &rollback_seqno));

    //TESTS for rollback needed
    cb_assert(table.needsRollback(10, 0, 0, 0, 0, &rollback_seqno));
    cb_assert(rollback_seqno == 0);

    //vbucket uuid sent by client not present in failover table
    cb_assert(table.needsRollback(start_seqno, curr_seqno, 0, 0, 20,
              &rollback_seqno));
    cb_assert(rollback_seqno == 0);

    vb_uuid = table.getLatestEntry().vb_uuid;
    curr_seqno = table.getLatestEntry().by_seqno + 100;
    start_seqno = curr_seqno-80;
    //snapshot sequence no is greater than upper && snapshot start sequence no
    // less than upper
    cb_assert(table.needsRollback(start_seqno, curr_seqno, vb_uuid,
                                  curr_seqno-20, curr_seqno+20,
                                  &rollback_seqno));
    cb_assert(rollback_seqno == (curr_seqno-20));
    //snapshot start seqno greate than  upper
    cb_assert(table.needsRollback(curr_seqno+20, curr_seqno, vb_uuid,
                                  curr_seqno+10, curr_seqno+40,
                                  &rollback_seqno));
    cb_assert(rollback_seqno == curr_seqno);
    //client vb uuiud is not the latest vbuuid in failover table and
    //snap_end_seqno > upper && snap_start_seqno > upper
    std::list<failover_entry_t>::iterator itr = failover_entries.begin();
    ++itr;
    vb_uuid = itr->vb_uuid;
    --itr;
    cb_assert(table.needsRollback(itr->by_seqno-5 ,curr_seqno, vb_uuid,
                                  itr->by_seqno-10, itr->by_seqno+40,
                                  &rollback_seqno));
    cb_assert(rollback_seqno == ((itr->by_seqno)-10));
    //client vb uuiud is not the latest vbuuid in failover table and
    //snapshot start seqno greate than  upper
    cb_assert(table.needsRollback(itr->by_seqno+20, curr_seqno, vb_uuid,
                                  itr->by_seqno+10, itr->by_seqno+40,
                                  &rollback_seqno));
    cb_assert(rollback_seqno == itr->by_seqno);
}

static void test_pop_5_failover_log() {
    uint64_t rollback_seqno;

    FailoverTable table(25);
    table_t failover_entries = generate_entries(table, 30,1);


    //Verify seq no. in latest entry
    cb_assert(table.getLatestEntry().by_seqno==29*150);
    cb_assert(table.getLatestEntry().by_seqno==failover_entries.front().by_seqno);

    // rollback not needed
    cb_assert(!table.needsRollback(0, 0, 0, 0, 0, &rollback_seqno));

    // rollback needed
    cb_assert(table.needsRollback(10, 0, 0, 0, 0, &rollback_seqno));
    cb_assert(rollback_seqno == 0);
}

int main(int argc, char **argv) {
    (void)argc;
    (void)argv;

    test_initial_failover_log();
    test_5_failover_log();
    test_pop_5_failover_log();
    test_5_failover_largeseqno_log();
    test_edgetests_failover_log();
    return 0;
}
