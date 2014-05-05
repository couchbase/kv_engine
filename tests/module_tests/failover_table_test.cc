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

#include "failover-table.h"

static void test_initial_failover_log() {
    uint64_t rollback_seqno;
    FailoverTable table(25);

    // Test shuld pass
    cb_assert(!table.needsRollback(0, 0, 0, 0, 0, &rollback_seqno));

    // Test should fail
    cb_assert(table.needsRollback(10, 0, 0, 0, 0, &rollback_seqno));
    cb_assert(rollback_seqno == 0);
}

int main(int argc, char **argv) {
    (void)argc;
    (void)argv;

    test_initial_failover_log();

    return 0;
}
