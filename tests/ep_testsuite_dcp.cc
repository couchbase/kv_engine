/* -*- MODE: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
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

/*
 * Testsuite for 'dcp' functionality in ep-engine.
 */

#include "config.h"

#include "ep_test_apis.h"
#include "ep_testsuite_common.h"
#include "mock/mock_dcp.h"

// Helper functions ///////////////////////////////////////////////////////////

static void dcp_stream(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char *name,
                       const void *cookie, uint16_t vbucket, uint32_t flags,
                       uint64_t start, uint64_t end, uint64_t vb_uuid,
                       uint64_t snap_start_seqno, uint64_t snap_end_seqno,
                       int exp_mutations, int exp_deletions, int exp_markers,
                       int extra_takeover_ops,
                       bool exp_disk_snapshot = false,
                       bool time_sync_enabled = false,
                       uint8_t exp_conflict_res = 0,
                       bool skipEstimateCheck = false,
                       uint64_t *total_bytes = NULL,
                       bool simulate_cursor_dropping = false,
                       uint64_t flow_control_buf_size = 1024,
                       bool disable_ack = false) {


    /* Reset any stale dcp data */
    clear_dcp_data();

    uint32_t opaque = 1;
    uint16_t nname = strlen(name);

    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie, ++opaque, 0, DCP_OPEN_PRODUCER, (void*)name,
                         nname),
            "Failed dcp producer open connection.");

    std::string flow_control_buf_sz(std::to_string(flow_control_buf_size));
    checkeq(ENGINE_SUCCESS,
            h1->dcp.control(h, cookie, ++opaque, "connection_buffer_size", 22,
                            flow_control_buf_sz.c_str(),
                            flow_control_buf_sz.length()),
            "Failed to establish connection buffer");
    char stats_buffer[50] = {0};
    if (flow_control_buf_size) {
        snprintf(stats_buffer, sizeof(stats_buffer),
                 "eq_dcpq:%s:max_buffer_bytes", name);
        checkeq(static_cast<int>(flow_control_buf_size),
                get_int_stat(h, h1, stats_buffer, "dcp"),
                "Buffer Size did not get set correctly");
    } else {
        snprintf(stats_buffer, sizeof(stats_buffer),
                 "eq_dcpq:%s:flow_control", name);
        std::string status = get_str_stat(h, h1, stats_buffer, "dcp");
        checkeq(0, status.compare("disabled"), "Flow control enabled!");
    }

    checkeq(ENGINE_SUCCESS,
            h1->dcp.control(h, cookie, ++opaque, "enable_ext_metadata", 19,
                            "true", 4),
            "Failed to enable xdcr extras");

    uint64_t rollback = 0;
    checkeq(ENGINE_SUCCESS,
            h1->dcp.stream_req(h, cookie, flags, opaque, vbucket, start, end,
                               vb_uuid, snap_start_seqno, snap_end_seqno,
                               &rollback, mock_dcp_add_failover_log),
            "Failed to initiate stream request");

    if (flags & DCP_ADD_STREAM_FLAG_TAKEOVER) {
        end  = -1;
    } else if (flags & DCP_ADD_STREAM_FLAG_LATEST ||
               flags & DCP_ADD_STREAM_FLAG_DISKONLY) {
        std::string high_seqno("vb_" + std::to_string(vbucket) + ":high_seqno");
        end = get_int_stat(h, h1, high_seqno.c_str(), "vbucket-seqno");
    }

    std::stringstream stats_flags;
    stats_flags << "eq_dcpq:" << name << ":stream_" << vbucket << "_flags";
    checkeq(flags,
            (uint32_t)get_int_stat(h, h1, stats_flags.str().c_str(), "dcp"),
            "Flags didn't match");

    std::stringstream stats_opaque;
    stats_opaque << "eq_dcpq:" << name << ":stream_" << vbucket << "_opaque";
    checkeq(opaque,
            (uint32_t)get_int_stat(h, h1, stats_opaque.str().c_str(), "dcp"),
            "Opaque didn't match");

    std::stringstream stats_start_seqno;
    stats_start_seqno << "eq_dcpq:" << name << ":stream_" << vbucket << "_start_seqno";
    checkeq(start,
            (uint64_t)get_ull_stat(h, h1, stats_start_seqno.str().c_str(), "dcp"),
            "Start Seqno Didn't match");

    std::stringstream stats_end_seqno;
    stats_end_seqno << "eq_dcpq:" << name << ":stream_" << vbucket << "_end_seqno";
    checkeq(end,
            (uint64_t)get_ull_stat(h, h1, stats_end_seqno.str().c_str(), "dcp"),
            "End Seqno didn't match");

    std::stringstream stats_vb_uuid;
    stats_vb_uuid << "eq_dcpq:" << name << ":stream_" << vbucket << "_vb_uuid";
    checkeq(vb_uuid,
            (uint64_t)get_ull_stat(h, h1, stats_vb_uuid.str().c_str(), "dcp"),
            "VBucket UUID didn't match");

    std::stringstream stats_snap_seqno;
    stats_snap_seqno << "eq_dcpq:" << name << ":stream_" << vbucket << "_snap_start_seqno";
    checkeq(snap_start_seqno,
            (uint64_t)get_ull_stat(h, h1, stats_snap_seqno.str().c_str(), "dcp"),
            "snap start seqno didn't match");

    std::unique_ptr<dcp_message_producers> producers(get_dcp_producers(h, h1));

    if ((flags & DCP_ADD_STREAM_FLAG_TAKEOVER) == 0 &&
        (flags & DCP_ADD_STREAM_FLAG_DISKONLY) == 0 &&
        !skipEstimateCheck) {
        int est = end - start;
        std::stringstream stats_takeover;
        stats_takeover << "dcp-vbtakeover " << vbucket << " " << name;
        wait_for_stat_to_be_lte(h, h1, "estimate", est,
                                stats_takeover.str().c_str());
    }

    bool done = false;
    bool exp_all_items_streamed = true;
    int num_mutations = 0;
    int num_deletions = 0;
    int num_snapshot_markers = 0;
    int num_set_vbucket_pending = 0;
    int num_set_vbucket_active = 0;

    ExtendedMetaData *emd = NULL;
    bool pending_marker_ack = false;
    uint64_t marker_end = 0;

    uint64_t last_by_seqno = 0;
    uint32_t bytes_read = 0;
    uint64_t all_bytes = 0;
    uint64_t total_acked_bytes = 0;
    uint64_t ack_limit = flow_control_buf_size/2;

    bool delay_buffer_acking = false;
    if (simulate_cursor_dropping) {
        /**
         * Simulates cursor dropping by slowing down the initial buffer
         * acknowledgement from the consmer.
         *
         * Note that the cursor may not be dropped if the memory usage
         * is not over the cursor_dropping_upper_threshold or if the
         * checkpoint_remover sleep time is high.
         */
        delay_buffer_acking = true;
    }

    do {
        if (!disable_ack && (bytes_read > ack_limit)) {
            if (delay_buffer_acking) {
                sleep(2);
                delay_buffer_acking = false;
            }
            h1->dcp.buffer_acknowledgement(h, cookie, ++opaque, vbucket, bytes_read);
            total_acked_bytes += bytes_read;
            bytes_read = 0;
        }
        ENGINE_ERROR_CODE err = h1->dcp.step(h, cookie, producers.get());
        if (err == ENGINE_DISCONNECT) {
            done = true;
        } else {
            switch (dcp_last_op) {
                case PROTOCOL_BINARY_CMD_DCP_MUTATION:
                    check(last_by_seqno < dcp_last_byseqno, "Expected bigger seqno");
                    last_by_seqno = dcp_last_byseqno;
                    num_mutations++;
                    bytes_read += dcp_last_packet_size;
                    all_bytes += dcp_last_packet_size;
                    if (pending_marker_ack && dcp_last_byseqno == marker_end) {
                        sendDcpAck(h, h1, cookie, PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER,
                                   PROTOCOL_BINARY_RESPONSE_SUCCESS, dcp_last_opaque);
                    }
                    if (time_sync_enabled) {
                        checkeq(static_cast<size_t>(16), dcp_last_meta.size(),
                                "Expected extended meta in mutation packet");
                    } else {
                        checkeq(static_cast<size_t>(5), dcp_last_meta.size(),
                                "Expected no extended metadata");
                    }

                    emd = new ExtendedMetaData(dcp_last_meta.c_str(),
                                               dcp_last_meta.size());
                    checkeq(exp_conflict_res, emd->getConflictResMode(),
                            "Unexpected conflict resolution mode");
                    delete emd;
                    break;
                case PROTOCOL_BINARY_CMD_DCP_DELETION:
                    check(last_by_seqno < dcp_last_byseqno, "Expected bigger seqno");
                    last_by_seqno = dcp_last_byseqno;
                    num_deletions++;
                    bytes_read += dcp_last_packet_size;
                    all_bytes += dcp_last_packet_size;
                    if (pending_marker_ack && dcp_last_byseqno == marker_end) {
                        sendDcpAck(h, h1, cookie, PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER,
                                   PROTOCOL_BINARY_RESPONSE_SUCCESS, dcp_last_opaque);
                    }
                    if (time_sync_enabled) {
                        checkeq(static_cast<size_t>(16),
                                dcp_last_meta.size(),
                                "Expected adjusted time in mutation packet");
                    } else {
                        checkeq(static_cast<size_t>(5),
                                dcp_last_meta.size(),
                                "Expected no extended metadata");
                    }

                    emd = new ExtendedMetaData(dcp_last_meta.c_str(),
                                               dcp_last_meta.size());
                    checkeq(exp_conflict_res, emd->getConflictResMode(),
                            "Unexpected conflict resolution mode");
                    delete emd;
                    break;
                case PROTOCOL_BINARY_CMD_DCP_STREAM_END:
                    done = true;
                    bytes_read += dcp_last_packet_size;
                    all_bytes += dcp_last_packet_size;
                    break;
                case PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER:
                    if (exp_disk_snapshot && num_snapshot_markers == 0) {
                        checkeq(static_cast<uint32_t>(1),
                                dcp_last_flags, "Expected disk snapshot");
                    }

                    if (dcp_last_flags & 8) {
                        pending_marker_ack = true;
                        marker_end = dcp_last_snap_end_seqno;
                    }

                    num_snapshot_markers++;
                    bytes_read += dcp_last_packet_size;
                    all_bytes += dcp_last_packet_size;
                    break;
                case PROTOCOL_BINARY_CMD_DCP_SET_VBUCKET_STATE:
                    if (dcp_last_vbucket_state == vbucket_state_pending) {
                        num_set_vbucket_pending++;
                        for (int j = 0; j < extra_takeover_ops; ++j) {
                            item *i = NULL;
                            std::stringstream ss;
                            ss << "key" << j;
                            checkeq(ENGINE_SUCCESS,
                                    store(h, h1, NULL, OPERATION_SET,
                                          ss.str().c_str(), "data", &i,
                                          0, vbucket),
                                    "Failed to store a value");
                            h1->release(h, NULL, i);
                        }
                    } else if (dcp_last_vbucket_state == vbucket_state_active) {
                        num_set_vbucket_active++;
                    }
                    bytes_read += dcp_last_packet_size;
                    all_bytes += dcp_last_packet_size;
                    sendDcpAck(h, h1, cookie, PROTOCOL_BINARY_CMD_DCP_SET_VBUCKET_STATE,
                               PROTOCOL_BINARY_RESPONSE_SUCCESS, dcp_last_opaque);
                    break;
                case 0:
                    /* No messages were ready on the last step call, so we
                     * wait till the conn is notified of new item.
                     * Note that we check for 0 because we clear the
                     * dcp_last_op value below.
                     */
                     if (disable_ack && flow_control_buf_size) {
                         /* If there is no acking and if flow control is enabled
                            we are done because producer should not send us any
                            more items. */
                         done = true;
                         exp_all_items_streamed = false;
                     }

                    testHarness.lock_cookie(cookie);
                    /* waitfor_cookie() waits on a condition variable. But the
                       api expects the cookie to be locked before calling it */
                    testHarness.waitfor_cookie(cookie);
                    testHarness.unlock_cookie(cookie);
                    break;
                default:
                    // Aborting ...
                    std::stringstream ss;
                    ss << "Unknown DCP operation: " << dcp_last_op;
                    check(false, ss.str().c_str());
            }
            if (dcp_last_op != PROTOCOL_BINARY_CMD_DCP_STREAM_END) {
                dcp_last_op = 0;
                dcp_last_nru = 0;
            }
        }
    } while (!done);

    if (total_bytes) {
        *total_bytes += all_bytes;
    }

    if (simulate_cursor_dropping) {
        if (num_snapshot_markers == 0) {
            cb_assert(num_mutations == 0 && num_deletions == 0);
        } else {
            check(num_mutations <= exp_mutations, "Invalid number of mutations");
            check(num_deletions <= exp_deletions, "Invalid number of deletes");
        }
    } else {
        // Account for cursors that may have been dropped because
        // of high memory usage
        if (get_int_stat(h, h1, "ep_cursors_dropped") > 0) {
            check(num_snapshot_markers <= exp_markers, "Invalid number of markers");
            check(num_mutations <= exp_mutations, "Invalid number of mutations");
            check(num_deletions <= exp_deletions, "Invalid number of deletions");
        } else {
            checkeq(exp_mutations, num_mutations, "Invalid number of mutations");
            checkeq(exp_deletions, num_deletions, "Invalid number of deletes");
            checkeq(exp_markers, num_snapshot_markers,
                    "Didn't receive expected number of snapshot marker");
        }
    }

    if (flags & DCP_ADD_STREAM_FLAG_TAKEOVER) {
        checkeq(1, num_set_vbucket_pending, "Didn't receive pending set state");
        checkeq(1, num_set_vbucket_active, "Didn't receive active set state");
    }

    /* Check if the readyQ size goes to zero after all items are streamed */
    if (exp_all_items_streamed) {
        std::stringstream stats_ready_queue_memory;
        stats_ready_queue_memory << "eq_dcpq:" << name << ":stream_" << vbucket
            << "_ready_queue_memory";
        checkeq(static_cast<uint64_t>(0),
                get_ull_stat(h, h1, stats_ready_queue_memory.str().c_str(), "dcp"),
                "readyQ size did not go to zero");
    }

    /* Check if the producer has updated flow control stat correctly */
    if (flow_control_buf_size) {
        memset(stats_buffer, 0, 50);
        snprintf(stats_buffer, sizeof(stats_buffer), "eq_dcpq:%s:unacked_bytes",
                 name);
        checkeq(static_cast<int>(all_bytes - total_acked_bytes),
                get_int_stat(h, h1, stats_buffer, "dcp"),
                "Buffer Size did not get set correctly");
    }
}

static void dcp_stream_req(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                           uint32_t opaque, uint16_t vbucket, uint64_t start,
                           uint64_t end, uint64_t uuid,
                           uint64_t snap_start_seqno,
                           uint64_t snap_end_seqno,
                           uint64_t exp_rollback, ENGINE_ERROR_CODE err) {
    const void *cookie = testHarness.create_cookie();
    uint32_t flags = DCP_OPEN_PRODUCER;
    const char *name = "unittest";

    // Open consumer connection
    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie, opaque, 0, flags, (void*)name, strlen(name)),
            "Failed dcp Consumer open connection.");

    uint64_t rollback = 0;
    ENGINE_ERROR_CODE rv = h1->dcp.stream_req(h, cookie, 0, 1, 0, start, end,
                                              uuid, snap_start_seqno,
                                              snap_end_seqno,
                                              &rollback, mock_dcp_add_failover_log);
    checkeq(err, rv, "Unexpected error code");
    if (err == ENGINE_ROLLBACK || err == ENGINE_KEY_ENOENT) {
        checkeq(exp_rollback, rollback, "Rollback didn't match expected value");
    }
    testHarness.destroy_cookie(cookie);
}

static void notifier_request(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                             const void* cookie, uint32_t opaque,
                             uint16_t vbucket, uint64_t start,
                             bool shouldSucceed) {

    uint32_t flags = 0;
    uint64_t rollback = 0;
    uint64_t vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
    uint64_t snap_start_seqno = get_ull_stat(h, h1, "vb_0:0:seq", "failovers");
    uint64_t snap_end_seqno = snap_start_seqno;
    ENGINE_ERROR_CODE err = h1->dcp.stream_req(h, cookie, flags, opaque,
                                               vbucket, start, 0, vb_uuid,
                                               snap_start_seqno, snap_end_seqno,
                                               &rollback,
                                               mock_dcp_add_failover_log);
    checkeq(ENGINE_SUCCESS, err, "Failed to initiate stream request");

    std::string type = get_str_stat(h, h1, "eq_dcpq:unittest:type", "dcp");
    check(type.compare("notifier") == 0, "Consumer not found");

    check((uint32_t)get_int_stat(h, h1, "eq_dcpq:unittest:stream_0_flags", "dcp")
          == flags, "Flags didn't match");
    check((uint32_t)get_int_stat(h, h1, "eq_dcpq:unittest:stream_0_opaque", "dcp")
          == opaque, "Opaque didn't match");
    check((uint64_t)get_ull_stat(h, h1, "eq_dcpq:unittest:stream_0_start_seqno", "dcp")
          == start, "Start Seqno Didn't match");
    check((uint64_t)get_ull_stat(h, h1, "eq_dcpq:unittest:stream_0_end_seqno", "dcp")
          == 0, "End Seqno didn't match");
    check((uint64_t)get_ull_stat(h, h1, "eq_dcpq:unittest:stream_0_vb_uuid", "dcp")
          == vb_uuid, "VBucket UUID didn't match");
    check((uint64_t)get_ull_stat(h, h1, "eq_dcpq:unittest:stream_0_snap_start_seqno", "dcp")
          == snap_start_seqno, "snap start seqno didn't match");
}

static void dcp_stream_to_replica(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                  const void *cookie, uint32_t opaque,
                                  uint16_t vbucket, uint32_t flags,
                                  uint64_t start, uint64_t end,
                                  uint64_t snap_start_seqno,
                                  uint64_t snap_end_seqno,
                                  uint8_t cas = 0x1, uint8_t datatype = 1,
                                  uint32_t exprtime = 0, uint32_t lockTime = 0,
                                  uint64_t revSeqno = 0)
{
    /* Send snapshot marker */
    checkeq(ENGINE_SUCCESS, h1->dcp.snapshot_marker(h, cookie, opaque, vbucket,
                                                    snap_start_seqno,
                                                    snap_end_seqno, flags),
            "Failed to send marker!");
    const std::string data("data");
    /* Send DCP mutations */
    for (uint64_t i = start; i <= end; i++) {
        std::stringstream key;
        key << "key" << i;
        checkeq(ENGINE_SUCCESS, h1->dcp.mutation(h, cookie, opaque,
                                                 key.str().c_str(),
                                                 key.str().length(),
                                                 data.c_str(), data.length(),
                                                 cas, vbucket, flags, datatype,
                                                 i, revSeqno, exprtime,
                                                 lockTime, NULL, 0, 0),
                "Failed dcp mutate.");
    }
}

struct mb16357_ctx {
    mb16357_ctx(ENGINE_HANDLE *_h, ENGINE_HANDLE_V1 *_h1, int _items) :
    h(_h), h1(_h1), items(_items) { }

    ENGINE_HANDLE *h;
    ENGINE_HANDLE_V1 *h1;
    int items;
};

//Forward declaration required for dcp_thread_func
static uint32_t add_stream_for_consumer(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                        const void* cookie, uint32_t opaque,
                                        uint16_t vbucket, uint32_t flags,
                                        protocol_binary_response_status response,
                                        uint64_t exp_snap_start = 0,
                                        uint64_t exp_snap_end = 0);

extern "C" {
    static void dcp_thread_func(void *args) {
        struct mb16357_ctx *ctx = static_cast<mb16357_ctx *>(args);

        const void *cookie = testHarness.create_cookie();
        uint32_t opaque = 0xFFFF0000;
        uint32_t flags = 0;
        std::string name = "unittest";

        while (get_int_stat(ctx->h, ctx->h1, "ep_pending_compactions") == 0);

        // Switch to replica
        check(set_vbucket_state(ctx->h, ctx->h1, 0, vbucket_state_replica),
              "Failed to set vbucket state.");

        // Open consumer connection
        checkeq(ctx->h1->dcp.open(ctx->h, cookie, opaque, 0, flags,
                                  (void*)name.c_str(), name.length()),
                ENGINE_SUCCESS, "Failed dcp Consumer open connection.");

        add_stream_for_consumer(ctx->h, ctx->h1, cookie, opaque++, 0, 0,
                                PROTOCOL_BINARY_RESPONSE_SUCCESS);


        uint32_t stream_opaque = get_int_stat(ctx->h, ctx->h1,
                                              "eq_dcpq:unittest:stream_0_opaque",
                                              "dcp");


        for (int i = 1; i <= ctx->items; i++) {
            std::stringstream ss;
            ss << "kamakeey-" << i;

            // send mutations in single mutation snapshots to race more with compaction
            ctx->h1->dcp.snapshot_marker(ctx->h, cookie,
                                         stream_opaque, 0/*vbid*/,
                                         ctx->items, ctx->items + i, 2);
            ctx->h1->dcp.mutation(ctx->h, cookie, stream_opaque,
                                  ss.str().c_str(), ss.str().length(),
                                  "value", 5, i * 3, 0, 0, 0,
                                  i + ctx->items, i + ctx->items,
                                  0, 0, "", 0, INITIAL_NRU_VALUE);
        }

        testHarness.destroy_cookie(cookie);
    }

    static void compact_thread_func(void *args) {
        struct mb16357_ctx *ctx = static_cast<mb16357_ctx *>(args);
        compact_db(ctx->h, ctx->h1, 0, 99, ctx->items, 1);
    }

    static void seqno_persistence_thread(void *arg) {
        struct handle_pair *hp = static_cast<handle_pair *>(arg);

        for (int j = 0; j < 1000; ++j) {
            std::stringstream ss;
            ss << "key" << j;
            item *i;
            checkeq(ENGINE_SUCCESS,
                    store(hp->h, hp->h1, NULL, OPERATION_SET,
                          ss.str().c_str(), ss.str().c_str(), &i, 0, 0),
                    "Failed to store a value");
            hp->h1->release(hp->h, NULL, i);
        }

        check(seqnoPersistence(hp->h, hp->h1, 0, 2003) == ENGINE_TMPFAIL,
              "Expected temp failure for seqno persistence request");
    }
}

// Testcases //////////////////////////////////////////////////////////////////

static enum test_result test_dcp_vbtakeover_no_stream(ENGINE_HANDLE *h,
                                                      ENGINE_HANDLE_V1 *h1) {
    for (auto j = 0; j < 10; ++j) {
        item *i = nullptr;
        const auto key = "key" + std::to_string(j);
        checkeq(ENGINE_SUCCESS,
                store(h, h1, nullptr, OPERATION_SET, key.c_str(), "data", &i),
                "Failed to store a value");
        h1->release(h, nullptr, i);
    }
    const auto est = get_int_stat(h, h1, "estimate", "dcp-vbtakeover 0");
    checkeq(10, est, "Invalid estimate for non-existent stream");
    checkeq(ENGINE_NOT_MY_VBUCKET,
            h1->get_stats(h, nullptr, "dcp-vbtakeover 1",
                          strlen("dcp-vbtakeover 1"), add_stats),
            "Expected not my vbucket");
    return SUCCESS;
}

static enum test_result test_dcp_notifier(ENGINE_HANDLE *h,
                                          ENGINE_HANDLE_V1 *h1) {
    for (auto j = 0; j < 10; ++j) {
        item *i = nullptr;
        const auto key = "key" + std::to_string(j);
        checkeq(ENGINE_SUCCESS, store(h, h1, nullptr, OPERATION_SET,
                                      key.c_str(), "data", &i),
                "Failed to store a value");
        h1->release(h, nullptr, i);
    }
    const auto *cookie = testHarness.create_cookie();
    const std::string name("unittest");
    const uint32_t seqno = 0;
    const uint16_t vbucket = 0;
    uint32_t opaque = 0;
    uint64_t start = 0;

    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie, opaque, seqno, DCP_OPEN_NOTIFIER,
                         (void*)name.c_str(), name.size()),
            "Failed dcp notifier open connection.");
    // Get notification for an old item
    notifier_request(h, h1, cookie, ++opaque, vbucket, start, true);
    dcp_step(h, h1, cookie);
    checkeq(static_cast<uint8_t>(PROTOCOL_BINARY_CMD_DCP_STREAM_END),
            dcp_last_op, "Expected stream end");
    // Get notification when we're slightly behind
    start += 9;
    notifier_request(h, h1, cookie, ++opaque, vbucket, start, true);
    dcp_step(h, h1, cookie);
    checkeq(static_cast<uint8_t>(PROTOCOL_BINARY_CMD_DCP_STREAM_END),
            dcp_last_op, "Expected stream end");
    // Wait for notification of a future item
    start += 11;
    notifier_request(h, h1, cookie, ++opaque, vbucket, start, true);
    dcp_step(h, h1, cookie);
    for (auto j = 0; j < 5; ++j) {
        item *i = nullptr;
        const auto key = "key" + std::to_string(j);
        checkeq(ENGINE_SUCCESS,
                store(h, h1, nullptr, OPERATION_SET, key.c_str(),
                      "data", &i),
                "Failed to store a value");
        h1->release(h, nullptr, i);
    }
    // Shouldn't get a stream end yet
    dcp_step(h, h1, cookie);
    check(dcp_last_op != PROTOCOL_BINARY_CMD_DCP_STREAM_END,
          "Wasn't expecting a stream end");
    for (auto j = 0; j < 6; ++j) {
        item *i = nullptr;
        const auto key = "key" + std::to_string(j);
        checkeq(ENGINE_SUCCESS,
                store(h, h1, nullptr, OPERATION_SET, key.c_str(),
                      "data", &i),
                "Failed to store a value");
        h1->release(h, nullptr, i);
    }
    // Should get a stream end
    dcp_step(h, h1, cookie);
    checkeq(static_cast<uint8_t>(PROTOCOL_BINARY_CMD_DCP_STREAM_END),
            dcp_last_op, "Expected stream end");
    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_dcp_consumer_open(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const auto *cookie1 = testHarness.create_cookie();
    const std::string name("unittest");
    const uint32_t opaque = 0;
    const uint32_t seqno = 0;
    const uint32_t flags = 0;
    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie1, opaque, seqno, flags,
                         (void*)name.c_str(), name.size()),
            "Failed dcp consumer open connection.");

    const auto stat_type("eq_dcpq:" + name + ":type");
    auto type = get_str_stat(h, h1, stat_type.c_str(), "dcp");
    const auto stat_created("eq_dcpq:" + name + ":created");
    const auto created = get_int_stat(h, h1, stat_created.c_str(), "dcp");
    checkeq(0, type.compare("consumer"), "Consumer not found");
    testHarness.destroy_cookie(cookie1);

    testHarness.time_travel(600);

    const auto *cookie2 = testHarness.create_cookie();
    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie2, opaque, seqno, flags,
                         (void*)name.c_str(), name.size()),
            "Failed dcp consumer open connection.");

    type = get_str_stat(h, h1, stat_type.c_str(), "dcp");
    checkeq(0, type.compare("consumer"), "Consumer not found");
    check(get_int_stat(h, h1, stat_created.c_str(), "dcp") > created,
          "New dcp stream is not newer");
    testHarness.destroy_cookie(cookie2);

    return SUCCESS;
}

static enum test_result test_dcp_consumer_flow_control_none(ENGINE_HANDLE *h,
                                                        ENGINE_HANDLE_V1 *h1) {
    const auto *cookie1 = testHarness.create_cookie();
    const std::string name("unittest");
    const uint32_t opaque = 0;
    const uint32_t seqno = 0;
    const uint32_t flags = 0;
    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie1, opaque, seqno, flags,
                         (void*)name.c_str(), name.size()),
            "Failed dcp consumer open connection.");

    const auto stat_name("eq_dcpq:" + name + ":max_buffer_bytes");
    checkeq(0, get_int_stat(h, h1, stat_name.c_str(), "dcp"),
            "Flow Control Buffer Size not zero");
    testHarness.destroy_cookie(cookie1);

    return SUCCESS;
}

static enum test_result test_dcp_consumer_flow_control_static(ENGINE_HANDLE *h,
                                                        ENGINE_HANDLE_V1 *h1) {
    const auto *cookie1 = testHarness.create_cookie();
    const std::string name("unittest");
    const uint32_t opaque = 0;
    const uint32_t seqno = 0;
    const uint32_t flags = 0;
    const auto flow_ctl_buf_def_size = 10485760;
    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie1, opaque, seqno, flags,
                         (void*)name.c_str(), name.size()),
            "Failed dcp consumer open connection.");

    const auto stat_name("eq_dcpq:" + name + ":max_buffer_bytes");
    checkeq(flow_ctl_buf_def_size,
            get_int_stat(h, h1, stat_name.c_str(), "dcp"),
            "Flow Control Buffer Size not equal to default value");
    testHarness.destroy_cookie(cookie1);

    return SUCCESS;
}

static enum test_result test_dcp_consumer_flow_control_dynamic(ENGINE_HANDLE *h,
                                                        ENGINE_HANDLE_V1 *h1) {
    const auto *cookie1 = testHarness.create_cookie();
    const std::string name("unittest");
    const uint32_t opaque = 0;
    const uint32_t seqno = 0;
    const uint32_t flags = 0;
    /* Check the min limit */
    set_param(h, h1, protocol_binary_engine_param_flush, "max_size",
              "500000000");
    checkeq(500000000, get_int_stat(h, h1, "ep_max_size"),
            "Incorrect new size.");

    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie1, opaque, seqno, flags,
                         (void*)name.c_str(), name.size()),
            "Failed dcp consumer open connection.");

    const auto stat_name("eq_dcpq:" + name + ":max_buffer_bytes");
    checkeq(10485760,
            get_int_stat(h, h1, stat_name.c_str(), "dcp"),
            "Flow Control Buffer Size not equal to min");
    testHarness.destroy_cookie(cookie1);

    /* Check the size as percentage of the bucket memory */
    const auto *cookie2 = testHarness.create_cookie();
    set_param(h, h1, protocol_binary_engine_param_flush, "max_size",
              "2000000000");
    checkeq(2000000000, get_int_stat(h, h1, "ep_max_size"),
            "Incorrect new size.");

    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie2, opaque, seqno, flags,
                         (void*)name.c_str(), name.size()),
            "Failed dcp consumer open connection.");

    checkeq(20000000,
            get_int_stat(h, h1, stat_name.c_str(), "dcp"),
            "Flow Control Buffer Size not equal to 1% of mem size");
    testHarness.destroy_cookie(cookie2);

    /* Check the case when mem used by flow control bufs hit the threshold */
    /* Create around 10 more connections to use more than 10% of the total
       memory */
    for (auto count = 0; count < 10; count++) {
        const auto *cookie = testHarness.create_cookie();
        checkeq(ENGINE_SUCCESS,
                h1->dcp.open(h, cookie, opaque, seqno, flags,
                             (void*)name.c_str(), name.size()),
                "Failed dcp consumer open connection.");
        testHarness.destroy_cookie(cookie);
    }
    /* By now mem used by flow control bufs would have crossed the threshold */
    const auto *cookie3 = testHarness.create_cookie();
    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie3, opaque, seqno, flags,
                         (void*)name.c_str(), name.size()),
            "Failed dcp consumer open connection.");

    checkeq(10485760,
            get_int_stat(h, h1, stat_name.c_str(), "dcp"),
            "Flow Control Buffer Size not equal to min after threshold is hit");
    testHarness.destroy_cookie(cookie3);

    /* Check the max limit */
    const auto *cookie4 = testHarness.create_cookie();
    set_param(h, h1, protocol_binary_engine_param_flush, "max_size",
              "7000000000");
    checkeq(static_cast<uint64_t>(7000000000),
            get_ull_stat(h, h1, "ep_max_size"), "Incorrect new size.");

    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie4, opaque, seqno, flags,
                         (void*)name.c_str(), name.size()),
            "Failed dcp consumer open connection.");

    checkeq(52428800,
            get_int_stat(h, h1, stat_name.c_str(), "dcp"),
            "Flow Control Buffer Size beyond max");
    testHarness.destroy_cookie(cookie4);

    return SUCCESS;
}

static enum test_result test_dcp_consumer_flow_control_aggressive(
                                                        ENGINE_HANDLE *h,
                                                        ENGINE_HANDLE_V1 *h1) {
    const auto max_conns = 6;
    const void *cookie[max_conns];
    const auto flow_ctl_buf_max = 52428800;
    const auto flow_ctl_buf_min = 10485760;
    const auto ep_max_size = 1200000000;
    const auto bucketMemQuotaFraction = 0.05;
    set_param(h, h1, protocol_binary_engine_param_flush, "max_size",
              std::to_string(ep_max_size).c_str());
    checkeq(ep_max_size, get_int_stat(h, h1, "ep_max_size"), "Incorrect new size.");

    /* Create first connection */
    const std::string name("unittest_");
    const auto name1(name + "0");
    const uint32_t opaque = 0;
    const uint32_t seqno = 0;
    const uint32_t flags = 0;
    cookie[0] = testHarness.create_cookie();
    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie[0], opaque, seqno, flags,
                         (void*)name1.c_str(), name1.size()),
            "Failed dcp consumer open connection.");

    /* Check the max limit */
    auto stat_name = "eq_dcpq:" + name1 + ":max_buffer_bytes";
    checkeq(flow_ctl_buf_max, get_int_stat(h, h1, stat_name.c_str(), "dcp"),
            "Flow Control Buffer Size not equal to max");

    /* Create at least 4 more connections */
    for (auto count = 1; count < max_conns - 1; count++) {
        cookie[count] = testHarness.create_cookie();
        const auto name2(name + std::to_string(count));
        checkeq(ENGINE_SUCCESS,
                h1->dcp.open(h, cookie[count], opaque, seqno, flags,
                             (void*)name2.c_str(), name2.length()),
                "Failed dcp consumer open connection.");

        for (auto i = 0; i <= count; i++) {
            /* Check if the buffer size of all connections has changed */
            const auto stat_name("eq_dcpq:" + name + std::to_string(i) +
                               ":max_buffer_bytes");
            checkeq((int)((ep_max_size * bucketMemQuotaFraction) / (count+1)),
                    get_int_stat(h, h1, stat_name.c_str(), "dcp"),
                    "Flow Control Buffer Size not correct");
        }
    }

    /* Opening another connection should set the buffer size to min value */
    cookie[max_conns - 1] = testHarness.create_cookie();
    const auto name3(name + std::to_string(max_conns - 1));
    const auto stat_name2("eq_dcpq:" + name3 + ":max_buffer_bytes");
    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie[max_conns - 1], opaque, seqno, flags,
                         (void*)name3.c_str(), name3.size()),
            "Failed dcp consumer open connection.");
    checkeq(flow_ctl_buf_min, get_int_stat(h, h1, stat_name2.c_str(), "dcp"),
            "Flow Control Buffer Size not equal to min");

    /* Disconnect connections and see if flow control
     * buffer size of existing conns increase
     */
    for (auto count = 0; count < max_conns / 2; count++) {
        testHarness.destroy_cookie(cookie[count]);
    }
    /* Wait for disconnected connections to be deleted */
    wait_for_stat_to_be(h, h1, "ep_dcp_dead_conn_count", 0, "dcp");

    /* Check if the buffer size of all connections has increased */
    const auto exp_buf_size = (int)((ep_max_size * bucketMemQuotaFraction) /
                              (max_conns - (max_conns / 2)));

    /* Also check if we get control message indicating the flow control buffer
       size change from the consumer connections */
    const auto producers(get_dcp_producers(h, h1));

    for (auto i = max_conns / 2; i < max_conns; i++) {
        /* Check if the buffer size of all connections has changed */
        const auto name4(name + std::to_string(i));
        const auto stat_name3("eq_dcpq:" + name4 + ":max_buffer_bytes");
        checkeq(exp_buf_size, get_int_stat(h, h1, stat_name3.c_str(), "dcp"),
                "Flow Control Buffer Size not correct");
        checkeq(ENGINE_WANT_MORE, h1->dcp.step(h, cookie[i], producers.get()),
                "Pending flow control buffer change not processed");
        checkeq((uint8_t)PROTOCOL_BINARY_CMD_DCP_CONTROL, dcp_last_op,
                "Flow ctl buf size change control message not received");
        checkeq(0, dcp_last_key.compare("connection_buffer_size"),
                "Flow ctl buf size change control message key error");
        checkeq(exp_buf_size, atoi(dcp_last_value.c_str()),
                "Flow ctl buf size in control message not correct");
    }
    /* Disconnect remaining connections */
    for (auto count = max_conns / 2; count < max_conns; count++) {
        testHarness.destroy_cookie(cookie[count]);
    }

    return SUCCESS;
}

static enum test_result test_dcp_producer_open(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const auto *cookie1 = testHarness.create_cookie();
    const std::string name("unittest");
    const uint32_t opaque = 0;
    const uint32_t seqno = 0;
    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie1, opaque, seqno, DCP_OPEN_PRODUCER,
                         (void*)name.c_str(), name.size()),
            "Failed dcp producer open connection.");
    const auto stat_type("eq_dcpq:" + name + ":type");
    auto type = get_str_stat(h, h1, stat_type.c_str(), "dcp");
    const auto stat_created("eq_dcpq:" + name + ":created");
    const auto created = get_int_stat(h, h1, stat_created.c_str(), "dcp");
    checkeq(0, type.compare("producer"), "Producer not found");
    testHarness.destroy_cookie(cookie1);

    testHarness.time_travel(600);

    const auto *cookie2 = testHarness.create_cookie();
    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie2, opaque, seqno, DCP_OPEN_PRODUCER,
                         (void*)name.c_str(), name.size()),
            "Failed dcp producer open connection.");
    type = get_str_stat(h, h1, stat_type.c_str(), "dcp");
    checkeq(0, type.compare("producer"), "Producer not found");
    check(get_int_stat(h, h1, stat_created.c_str(), "dcp") > created,
          "New dcp stream is not newer");
    testHarness.destroy_cookie(cookie2);

    return SUCCESS;
}

static enum test_result test_dcp_noop(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const auto *cookie = testHarness.create_cookie();
    const std::string name("unittest");
    const uint32_t seqno = 0;
    uint32_t opaque = 0;
    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie, opaque, seqno, DCP_OPEN_PRODUCER,
                         (void*)name.c_str(), name.size()),
            "Failed dcp producer open connection.");
    const std::string param1_name("connection_buffer_size");
    const std::string param1_value("1024");
    checkeq(ENGINE_SUCCESS,
            h1->dcp.control(h, cookie, ++opaque,
                            param1_name.c_str(), param1_name.size(),
                            param1_value.c_str(), param1_value.size()),
            "Failed to establish connection buffer");
    const std::string param2_name("enable_noop");
    const std::string param2_value("true");
    checkeq(ENGINE_SUCCESS,
            h1->dcp.control(h, cookie, ++opaque,
                            param2_name.c_str(), param2_name.size(),
                            param2_value.c_str(), param2_value.size()),
            "Failed to enable no-ops");

    testHarness.time_travel(201);

    const auto producers(get_dcp_producers(h, h1));
    auto done = false;
    while (!done) {
        if (h1->dcp.step(h, cookie, producers.get()) == ENGINE_DISCONNECT) {
            done = true;
        } else if (dcp_last_op == PROTOCOL_BINARY_CMD_DCP_NOOP) {
            done = true;
            // Producer opaques are hard coded to start from 10M
            checkeq(10000001, (int)dcp_last_opaque,
                    "dcp_last_opaque != 10,000,001");
            const auto stat_name("eq_dcpq:" + name + ":noop_wait");
            checkeq(1, get_int_stat(h, h1, stat_name.c_str(), "dcp"),
                    "Didn't send noop");
            sendDcpAck(h, h1, cookie, PROTOCOL_BINARY_CMD_DCP_NOOP,
                       PROTOCOL_BINARY_RESPONSE_SUCCESS, dcp_last_opaque);
            checkeq(0, get_int_stat(h, h1, stat_name.c_str(), "dcp"),
                    "Didn't ack noop");
        } else if (dcp_last_op != 0) {
            abort();
        }
        dcp_last_op = 0;
    }
    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_noop_fail(ENGINE_HANDLE *h,
                                           ENGINE_HANDLE_V1 *h1) {
    const auto *cookie = testHarness.create_cookie();
    const std::string name("unittest");
    const uint32_t seqno = 0;
    uint32_t opaque = 0;
    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie, opaque, seqno, DCP_OPEN_PRODUCER,
                         (void*)name.c_str(), name.size()),
                         "Failed dcp producer open connection.");
    const std::string param1_name("connection_buffer_size");
    const std::string param1_value("1024");
    checkeq(ENGINE_SUCCESS,
            h1->dcp.control(h, cookie, ++opaque,
                            param1_name.c_str(), param1_name.size(),
                            param1_value.c_str(), param1_value.size()),
            "Failed to establish connection buffer");
    const std::string param2_name("enable_noop");
    const std::string param2_value("true");
    checkeq(ENGINE_SUCCESS,
            h1->dcp.control(h, cookie, ++opaque,
                            param2_name.c_str(), param2_name.size(),
                            param2_value.c_str(), param2_value.size()),
            "Failed to enable no-ops");

    testHarness.time_travel(201);

    const auto producers(get_dcp_producers(h, h1));
    while (h1->dcp.step(h, cookie, producers.get()) != ENGINE_DISCONNECT) {
        if (dcp_last_op == PROTOCOL_BINARY_CMD_DCP_NOOP) {
            // Producer opaques are hard coded to start from 10M
            checkeq(10000001, (int)dcp_last_opaque,
                    "dcp_last_opaque != 10,000,001");
            const auto stat_name("eq_dcpq:" + name + ":noop_wait");
            checkeq(1, get_int_stat(h, h1, stat_name.c_str(), "dcp"),
                    "Didn't send noop");
            testHarness.time_travel(201);
        } else if (dcp_last_op != 0) {
            abort();
        }
    }
    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_consumer_noop(ENGINE_HANDLE *h,
                                               ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state.");
    const auto *cookie = testHarness.create_cookie();
    const std::string name("unittest");
    const uint32_t seqno = 0;
    const uint32_t flags = 0;
    const uint16_t vbucket = 0;
    uint32_t opaque = 0;
    // Open consumer connection
    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie, opaque, seqno, flags,
                         (void*)name.c_str(), name.size()),
            "Failed dcp Consumer open connection.");
    add_stream_for_consumer(h, h1, cookie, opaque, vbucket, flags,
                            PROTOCOL_BINARY_RESPONSE_SUCCESS);
    testHarness.time_travel(201);
    // No-op not recieved for 201 seconds. Should be ok.
    const auto producers(get_dcp_producers(h, h1));
    checkeq(ENGINE_SUCCESS, h1->dcp.step(h, cookie, producers.get()),
            "Expected engine success");

    testHarness.time_travel(200);

    // Message not recieved for over 400 seconds. Should disconnect.
    checkeq(ENGINE_DISCONNECT, h1->dcp.step(h, cookie, producers.get()),
            "Expected engine disconnect");
    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_producer_stream_req_partial(ENGINE_HANDLE *h,
                                                             ENGINE_HANDLE_V1 *h1) {
    int num_items = 200;
    for (int j = 0; j < num_items; ++j) {
        item *i = NULL;
        std::stringstream ss;
        ss << "key" << j;
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET, ss.str().c_str(), "data", &i),
                "Failed to store a value");
        h1->release(h, NULL, i);
    }

    wait_for_flusher_to_settle(h, h1);
    stop_persistence(h, h1);

    for (int j = 0; j < (num_items / 2); ++j) {
        std::stringstream ss;
        ss << "key" << j;
        checkeq(ENGINE_SUCCESS,
                del(h, h1, ss.str().c_str(), 0, 0),
                "Expected delete to succeed");
    }

    wait_for_stat_to_be(h, h1, "vb_0:num_checkpoints", 2, "checkpoint");

    uint64_t vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");

    const void *cookie = testHarness.create_cookie();

    dcp_stream(h, h1, "unittest", cookie, 0, 0, 95, 209, vb_uuid, 95, 95, 105,
               100, 2, 0);

    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_producer_stream_req_partial_with_time_sync(
                                                             ENGINE_HANDLE *h,
                                                             ENGINE_HANDLE_V1 *h1) {

    set_drift_counter_state(h, h1, 1000, 0x01);

    int num_items = 200;
    for (int j = 0; j < num_items; ++j) {
        item *i = NULL;
        std::stringstream ss;
        ss << "key" << j;
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET, ss.str().c_str(), "data", &i),
                "Failed to store a value");
        h1->release(h, NULL, i);
    }

    wait_for_flusher_to_settle(h, h1);
    stop_persistence(h, h1);

    for (int j = 0; j < (num_items / 2); ++j) {
        std::stringstream ss;
        ss << "key" << j;
        checkeq(ENGINE_SUCCESS,
                del(h, h1, ss.str().c_str(), 0, 0),
                "Expected delete to succeed");
    }

    wait_for_stat_to_be(h, h1, "vb_0:num_checkpoints", 2, "checkpoint");

    uint64_t vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");

    const void *cookie = testHarness.create_cookie();

    dcp_stream(h, h1, "unittest", cookie, 0, 0, 95, 209, vb_uuid, 95, 95, 105,
               100, 2, 0, false, true, 1);

    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_producer_stream_req_full(ENGINE_HANDLE *h,
                                                          ENGINE_HANDLE_V1 *h1) {
    int num_items = 300;
    for (int j = 0; j < num_items; ++j) {
        if (j % 100 == 0) {
            wait_for_flusher_to_settle(h, h1);
        }
        item *i = NULL;
        std::stringstream ss;
        ss << "key" << j;
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET, ss.str().c_str(), "data", &i),
                "Failed to store a value");
        h1->release(h, NULL, i);
    }

    wait_for_flusher_to_settle(h, h1);
    verify_curr_items(h, h1, num_items, "Wrong amount of items");
    wait_for_stat_to_be(h, h1, "vb_0:num_checkpoints", 1, "checkpoint");

    uint64_t end = get_int_stat(h, h1, "vb_0:high_seqno", "vbucket-seqno");
    uint64_t vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");

    const void *cookie = testHarness.create_cookie();

    dcp_stream(h, h1, "unittest", cookie, 0, 0, 0, end, vb_uuid, 0, 0,
               num_items, 0, 1, 0);

    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_producer_stream_req_disk(ENGINE_HANDLE *h,
                                                          ENGINE_HANDLE_V1 *h1) {
    int num_items = 400;
    for (int j = 0; j < num_items; ++j) {
        if (j == 200) {
            wait_for_flusher_to_settle(h, h1);
            wait_for_stat_to_be(h, h1, "ep_items_rm_from_checkpoints", 200);
            stop_persistence(h, h1);
        }
        item *i = NULL;
        std::stringstream ss;
        ss << "key" << j;
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET, ss.str().c_str(), "data", &i),
                "Failed to store a value");
        h1->release(h, NULL, i);
    }

    verify_curr_items(h, h1, num_items, "Wrong amount of items");
    wait_for_stat_to_be_gte(h, h1, "vb_0:num_checkpoints", 2, "checkpoint");

    uint64_t vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");

    const void *cookie = testHarness.create_cookie();

    dcp_stream(h, h1,"unittest", cookie, 0, 0, 0, 200, vb_uuid, 0, 0, 200, 0, 1,
               0);

    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_producer_stream_req_diskonly(ENGINE_HANDLE *h,
                                                              ENGINE_HANDLE_V1 *h1) {
    int num_items = 300;
    for (int j = 0; j < num_items; ++j) {
        if (j % 100 == 0) {
            wait_for_flusher_to_settle(h, h1);
        }
        item *i = NULL;
        std::stringstream ss;
        ss << "key" << j;
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET, ss.str().c_str(), "data", &i),
                "Failed to store a value");
        h1->release(h, NULL, i);
    }

    wait_for_flusher_to_settle(h, h1);
    verify_curr_items(h, h1, num_items, "Wrong amount of items");
    wait_for_stat_to_be(h, h1, "vb_0:num_checkpoints", 1, "checkpoint");

    uint64_t vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
    uint32_t flags = DCP_ADD_STREAM_FLAG_DISKONLY;

    const void *cookie = testHarness.create_cookie();

    dcp_stream(h, h1, "unittest", cookie, 0, flags, 0, -1, vb_uuid, 0, 0, 300,
               0, 1, 0);

    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_producer_stream_req_mem(ENGINE_HANDLE *h,
                                                         ENGINE_HANDLE_V1 *h1) {
    int num_items = 300;
    for (int j = 0; j < num_items; ++j) {
        if (j % 100 == 0) {
            wait_for_flusher_to_settle(h, h1);
        }
        item *i = NULL;
        std::stringstream ss;
        ss << "key" << j;
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET, ss.str().c_str(), "data", &i),
                "Failed to store a value");
        h1->release(h, NULL, i);
    }

    wait_for_flusher_to_settle(h, h1);
    verify_curr_items(h, h1, num_items, "Wrong amount of items");

    uint64_t vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");

    const void *cookie = testHarness.create_cookie();

    dcp_stream(h, h1, "unittest", cookie, 0, 0, 200, 300, vb_uuid, 200, 200,
               100, 0, 1, 0);

    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_producer_stream_req_dgm(ENGINE_HANDLE *h,
                                                         ENGINE_HANDLE_V1 *h1) {
    int i = 0;  // Item count
    while (true) {
        // Gathering stats on every store is expensive, just check every 100 iterations
        if ((i % 100) == 0) {
            if (get_int_stat(h, h1, "vb_active_perc_mem_resident") < 50) {
                break;
            }
        }

        item *itm = NULL;
        std::stringstream ss;
        ss << "key" << i;
        ENGINE_ERROR_CODE ret = store(h, h1, NULL, OPERATION_SET,
                                      ss.str().c_str(), "somevalue", &itm);
        if (ret == ENGINE_SUCCESS) {
            i++;
        }
        h1->release(h, NULL, itm);
    }

    wait_for_flusher_to_settle(h, h1);
    verify_curr_items(h, h1, i, "Wrong number of items");
    int num_non_resident = get_int_stat(h, h1, "vb_active_num_non_resident");
    cb_assert(num_non_resident >= ((float)(50/100) * i));

    uint64_t end = get_int_stat(h, h1, "vb_0:high_seqno", "vbucket-seqno");
    uint64_t vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");

    set_param(h, h1, protocol_binary_engine_param_flush, "max_size", "5242880");
    cb_assert(get_int_stat(h, h1, "vb_active_perc_mem_resident") < 50);

    const void *cookie = testHarness.create_cookie();

    dcp_stream(h, h1,"unittest", cookie, 0, 0, 0, end, vb_uuid, 0, 0, i, 0, 1,
               0);

    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_producer_stream_latest(ENGINE_HANDLE *h,
                                                        ENGINE_HANDLE_V1 *h1) {
    int num_items = 300;
    for (int j = 0; j < num_items; ++j) {
        if (j % 100 == 0) {
            wait_for_flusher_to_settle(h, h1);
        }
        item *i = NULL;
        std::stringstream ss;
        ss << "key" << j;
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET, ss.str().c_str(), "data", &i),
                "Failed to store a value");
        h1->release(h, NULL, i);
    }

    wait_for_flusher_to_settle(h, h1);
    verify_curr_items(h, h1, num_items, "Wrong amount of items");

    uint64_t vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");

    const void *cookie = testHarness.create_cookie();

    uint32_t flags = DCP_ADD_STREAM_FLAG_LATEST;
    dcp_stream(h, h1, "unittest", cookie, 0, flags, 200, 205, vb_uuid, 200, 200,
               100, 0, 1, 0);

    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static test_result test_dcp_producer_stream_req_nmvb(ENGINE_HANDLE *h,
                                                     ENGINE_HANDLE_V1 *h1) {
    const void *cookie1 = testHarness.create_cookie();
    uint32_t opaque = 0;
    uint32_t seqno = 0;
    uint32_t flags = DCP_OPEN_PRODUCER;
    const char *name = "unittest";
    uint16_t nname = strlen(name);

    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie1, opaque, seqno, flags, (void*)name, nname),
          "Failed dcp producer open connection.");

    uint32_t req_vbucket = 1;
    uint64_t rollback = 0;

    checkeq(ENGINE_NOT_MY_VBUCKET,
            h1->dcp.stream_req(h, cookie1, 0, 0, req_vbucket, 0, 0, 0, 0,
                               0, &rollback, mock_dcp_add_failover_log),
            "Expected not my vbucket");
    testHarness.destroy_cookie(cookie1);

    return SUCCESS;
}

static test_result test_dcp_agg_stats(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    int num_items = 300;
    for (int j = 0; j < num_items; ++j) {
        if (j % 100 == 0) {
            wait_for_flusher_to_settle(h, h1);
        }
        item *i = NULL;
        std::stringstream ss;
        ss << "key" << j;
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET, ss.str().c_str(), "data", &i),
                "Failed to store a value");
        h1->release(h, NULL, i);
    }

    wait_for_flusher_to_settle(h, h1);
    verify_curr_items(h, h1, num_items, "Wrong amount of items");

    uint64_t vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");

    const void *cookie[5];

    uint64_t total_bytes = 0;
    for (int j = 0; j < 5; ++j) {
        char name[12];
        snprintf(name, sizeof(name), "unittest_%d", j);
        cookie[j] = testHarness.create_cookie();
        dcp_stream(h, h1, name, cookie[j], 0, 0, 200, 300, vb_uuid, 200, 200,
                   100, 0, 1, 0, false, false, 0, false, &total_bytes);
    }

    checkeq(5, get_int_stat(h, h1, "unittest:producer_count", "dcpagg _"),
            "producer count mismatch");
    checkeq((int)total_bytes,
            get_int_stat(h, h1, "unittest:total_bytes", "dcpagg _"),
            "aggregate total bytes sent mismatch");
    checkeq(500, get_int_stat(h, h1, "unittest:items_sent", "dcpagg _"),
            "aggregate total items sent mismatch");
    checkeq(0, get_int_stat(h, h1, "unittest:items_remaining", "dcpagg _"),
            "aggregate total items remaining mismatch");

    for (int j = 0; j < 5; ++j) {
        testHarness.destroy_cookie(cookie[j]);
    }

    return SUCCESS;
}

static test_result test_dcp_cursor_dropping(ENGINE_HANDLE *h,
                                            ENGINE_HANDLE_V1 *h1) {
    int i = 0;  // Item count
    int maxSize = get_int_stat(h, h1, "ep_max_size", "memory");
    stop_persistence(h, h1);
    while(1) {
        // Load items into server until 90% of the mem quota
        // is used.
        int memUsed = get_int_stat(h, h1, "mem_used", "memory");
        if ((float)memUsed <= ((float)(maxSize) * 0.90)) {
            item *itm = NULL;
            std::stringstream ss;
            ss << "key" << i;
            ENGINE_ERROR_CODE ret = store(h, h1, NULL, OPERATION_SET,
                    ss.str().c_str(), "somevalue", &itm);
            if (ret == ENGINE_SUCCESS) {
                i++;
            }
            h1->release(h, NULL, itm);
        } else {
            break;
        }
    }

    uint64_t end = get_int_stat(h, h1, "vb_0:high_seqno", "vbucket-seqno");
    uint64_t vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
    const void *cookie = testHarness.create_cookie();

    start_persistence(h, h1);
    std::string name("unittest");

    dcp_stream(h, h1, name.c_str(), cookie, 0, 0, 0, end, vb_uuid, 0, 0, i,
               0, 1, 0, false, false, 0, true, NULL, true);

    checkeq(static_cast<uint8_t>(PROTOCOL_BINARY_CMD_DCP_STREAM_END),
            dcp_last_op,
            "Last DCP op wasn't a stream END");

    if (get_int_stat(h, h1, "ep_cursors_dropped") > 0) {
        checkeq(static_cast<uint32_t>(4),
                dcp_last_flags, "Last DCP flag not END_STREAM_SLOW");
        // Also ensure the status of the active stream for the vbucket
        // shows as "temporarily_disconnected", in vbtakeover stats.
        std::string stats_takeover("dcp-vbtakeover 0 " + name);
        std::string status = get_str_stat(h, h1, "status",
                                          stats_takeover.c_str());
        checkeq(status.compare("temporarily_disconnected"), 0,
                "Unexpected status");
    } else {
        checkeq(static_cast<uint32_t>(0), dcp_last_flags,
                "Last DCP flag not END_STREAM_OK");
    }

    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static test_result test_dcp_value_compression(ENGINE_HANDLE *h,
                                              ENGINE_HANDLE_V1 *h1) {

    checkeq((float)0.85,
            get_float_stat(h, h1, "ep_dcp_min_compression_ratio"),
            "Unexpected dcp_min_compression_ratio");

    // Set dcp_min_compression_ratio to infinite, which means
    // a DCP producer would ship the doc no matter what the
    // achieved compressed length is.
    set_param(h, h1, protocol_binary_engine_param_flush,
              "dcp_min_compression_ratio",
              std::to_string(std::numeric_limits<float>::max()).c_str());

    item *i = NULL;
    std::string originalValue("{\"FOO\":\"BAR\"}");

    checkeq(storeCasVb11(h, h1, NULL, OPERATION_SET, "key",
                         originalValue.c_str(), originalValue.length(),
                         0, &i, 0, 0, 3600,
                         PROTOCOL_BINARY_DATATYPE_JSON),
            ENGINE_SUCCESS, "Failed to store an item.");
    h1->release(h, NULL, i);
    wait_for_flusher_to_settle(h, h1);

    uint64_t end = get_int_stat(h, h1, "vb_0:high_seqno", "vbucket-seqno");
    uint64_t vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
    const void *cookie = testHarness.create_cookie();
    const char *name = "unittest";
    uint16_t nname = strlen(name);
    uint32_t opaque = 1;

    checkeq(h1->dcp.open(h, cookie, ++opaque, 0, DCP_OPEN_PRODUCER,
                         (void*)name, nname),
            ENGINE_SUCCESS,
            "Failed dcp producer open connection.");

    checkeq(h1->dcp.control(h, cookie, ++opaque, "connection_buffer_size",
                            strlen("connection_buffer_size"), "1024", 4),
            ENGINE_SUCCESS,
            "Failed to establish connection buffer");

    checkeq(h1->dcp.control(h, cookie, ++opaque, "enable_value_compression",
                            strlen("enable_value_compression"), "true", 4),
            ENGINE_SUCCESS,
            "Failed to enable value compression");

    uint64_t rollback = 0;
    checkeq(h1->dcp.stream_req(h, cookie, 0, opaque, 0, 0, end,
                               vb_uuid, 0, 0, &rollback,
                               mock_dcp_add_failover_log),
            ENGINE_SUCCESS,
            "Failed to initiate stream request");

    std::unique_ptr<dcp_message_producers> producers(get_dcp_producers(h, h1));

    bool done = false;
    uint32_t bytes_read = 0;
    bool pending_marker_ack = false;
    uint64_t marker_end = 0;

    std::string last_mutation_val;

    do {
        if (bytes_read > 512) {
            h1->dcp.buffer_acknowledgement(h, cookie, ++opaque, 0, bytes_read);
            bytes_read = 0;
        }
        ENGINE_ERROR_CODE err = h1->dcp.step(h, cookie, producers.get());
        if (err == ENGINE_DISCONNECT) {
            done = true;
        } else {
            switch (dcp_last_op) {
                case PROTOCOL_BINARY_CMD_DCP_MUTATION:
                    bytes_read += dcp_last_packet_size;
                    if (pending_marker_ack && dcp_last_byseqno == marker_end) {
                        sendDcpAck(h, h1, cookie, PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER,
                               PROTOCOL_BINARY_RESPONSE_SUCCESS, dcp_last_opaque);
                    }
                    last_mutation_val.assign(dcp_last_value);
                    break;
                case PROTOCOL_BINARY_CMD_DCP_STREAM_END:
                    done = true;
                    bytes_read += dcp_last_packet_size;
                    break;
                case PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER:
                    if (dcp_last_flags & 8) {
                        pending_marker_ack = true;
                        marker_end = dcp_last_snap_end_seqno;
                    }

                    bytes_read += dcp_last_packet_size;
                    break;
                case 0:
                    break;
                default:
                     // Aborting ...
                    std::stringstream ss;
                    ss << "Unexpected DCP operation: " << dcp_last_op;
                    check(false, ss.str().c_str());
            }
            dcp_last_op = 0;
        }
    } while (!done);

    cb_assert(!last_mutation_val.empty());

    snap_buf output;
    doSnappyUncompress(last_mutation_val.c_str(),
                       last_mutation_val.length(),
                       output);
    std::string received(output.buf.get(), output.len);

    checkeq(originalValue.compare(received), 0,
            "Value received is not what is expected");

    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_producer_stream_backfill_no_value(
                                                        ENGINE_HANDLE *h,
                                                        ENGINE_HANDLE_V1 *h1) {
    uint64_t num_items = 0, total_bytes = 0, est_bytes = 0;
    const int rr_thresh = 80;
    const std::string value("somevalue");
    /* We want a DGM case to test both in memory backfill and disk backfill */
    while (true) {
        /* Gathering stats on every store is expensive, just check every 100
         iterations */
        if ((num_items % 10) == 0) {
            if (get_int_stat(h, h1, "vb_active_perc_mem_resident") <
                rr_thresh) {
                break;
            }
        }

        item *itm = NULL;
        std::string key("key" + std::to_string(num_items));
        ENGINE_ERROR_CODE ret = store(h, h1, NULL, OPERATION_SET, key.c_str(),
                                      value.c_str(), &itm);
        if (ret == ENGINE_SUCCESS) {
            num_items++;
            est_bytes += key.length();
        }
        h1->release(h, NULL, itm);
    }

    wait_for_flusher_to_settle(h, h1);
    verify_curr_items(h, h1, num_items, "Wrong number of items");
    int num_non_resident = get_int_stat(h, h1, "vb_active_num_non_resident");
    cb_assert(num_non_resident >= (((float)(100 - rr_thresh)/100) * num_items));

    uint64_t end = get_int_stat(h, h1, "vb_0:high_seqno", "vbucket-seqno");
    uint64_t vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");

    set_param(h, h1, protocol_binary_engine_param_flush, "max_size",
              "52428800");
    cb_assert(get_int_stat(h, h1, "vb_active_perc_mem_resident") < rr_thresh);

    const void *cookie = testHarness.create_cookie();

    /* Stream mutations with "no_value" flag set */
    dcp_stream(h, h1, "unittest", cookie, 0, DCP_ADD_STREAM_FLAG_NO_VALUE, 0,
               end, vb_uuid, 0, 0, num_items, 0, 1, 0, false, false, 0, false,
               &total_bytes);

    /* basebytes mutation + nmeta (when no ext_meta is expected) */
    const int packet_fixed_size = dcp_mutation_base_msg_bytes +
                                  dcp_meta_size_none;
    est_bytes += (num_items * packet_fixed_size);
    /* Add DCP_SNAPSHOT_MARKER bytes and DCP_STREAM_END bytes */
    est_bytes += (dcp_snapshot_marker_base_msg_bytes +
                  dcp_stream_end_resp_base_msg_bytes);
    checkeq(est_bytes, total_bytes, "Maybe values streamed in stream no_value");
    testHarness.destroy_cookie(cookie);

    /* Stream without NO_VALUE flag (default) and expect both key and value for
       all mutations */
    total_bytes = 0;
    const void *cookie1 = testHarness.create_cookie();

    dcp_last_op = 0;
    dcp_last_nru = 0;

    dcp_stream(h, h1, "unittest1", cookie1, 0, 0, 0, end, vb_uuid, 0, 0,
               num_items, 0, 1, 0, false, false, 0, false, &total_bytes);

    /* Just add total value size to estimated bytes */
    est_bytes += (value.length() * num_items);
    checkeq(est_bytes, total_bytes, "Maybe key values are not streamed");
    testHarness.destroy_cookie(cookie1);

    return SUCCESS;
}

static enum test_result test_dcp_producer_stream_mem_no_value(
                                                        ENGINE_HANDLE *h,
                                                        ENGINE_HANDLE_V1 *h1) {
    uint64_t num_items = 300, total_bytes = 0, est_bytes = 0;
    const uint64_t start = 200, end = 300;
    const std::string value("data");

    for (uint64_t j = 0; j < num_items; ++j) {
        if (j % 100 == 0) {
            wait_for_flusher_to_settle(h, h1);
        }
        item *i = NULL;
        std::string key("key" + std::to_string(j));
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET, key.c_str(), value.c_str(), &i),
                "Failed to store a value");
        h1->release(h, NULL, i);
    }

    wait_for_flusher_to_settle(h, h1);
    verify_curr_items(h, h1, num_items, "Wrong amount of items");

    uint64_t vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");

    const void *cookie = testHarness.create_cookie();

    /* Stream mutations with "no_value" flag set */
    dcp_stream(h, h1, "unittest", cookie, 0, DCP_ADD_STREAM_FLAG_NO_VALUE,
               start, end, vb_uuid, 200, 200, (end-start), 0, 1, 0, false,
               false, 0, false, &total_bytes);

    /* basebytes mutation + nmeta (when no ext_meta is expected) */
    const int packet_fixed_size = dcp_mutation_base_msg_bytes +
                                  dcp_meta_size_none;
    est_bytes += ((end-start) * packet_fixed_size);
    /* Add DCP_SNAPSHOT_MARKER bytes and DCP_STREAM_END bytes */
    est_bytes += (dcp_snapshot_marker_base_msg_bytes +
                  dcp_stream_end_resp_base_msg_bytes);
    /* Add key size (keys from "key201" till "key300") */
    est_bytes += (6 * 100);
    checkeq(est_bytes, total_bytes, "Maybe values streamed in stream no_value");
    testHarness.destroy_cookie(cookie);

    /* Stream without NO_VALUE flag (default) and expect both key and value for
     all mutations */
    total_bytes = 0;
    const void *cookie1 = testHarness.create_cookie();

    dcp_stream(h, h1, "unittest1", cookie1, 0, 0, start, end, vb_uuid, 200, 200,
               (end-start), 0, 1, 0, false, false, 0, false, &total_bytes);

    /* Just add total value size to estimated bytes */
    est_bytes += (value.length() * (end-start));
    checkeq(est_bytes, total_bytes, "Maybe key values are not streamed");
    testHarness.destroy_cookie(cookie1);

    return SUCCESS;
}

static test_result test_dcp_takeover(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    int num_items = 10;
    for (int j = 0; j < num_items; ++j) {
        item *i = NULL;
        std::stringstream ss;
        ss << "key" << j;
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET, ss.str().c_str(), "data", &i),
                "Failed to store a value");
        h1->release(h, NULL, i);
    }

    const void *cookie = testHarness.create_cookie();

    uint32_t flags = DCP_ADD_STREAM_FLAG_TAKEOVER;
    uint64_t vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");

    dcp_stream(h, h1, "unittest", cookie, 0, flags, 0, 1000, vb_uuid, 0, 0, 20,
               0, 2, 10);

    check(verify_vbucket_state(h, h1, 0, vbucket_state_dead), "Wrong vb state");

    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static test_result test_dcp_takeover_no_items(ENGINE_HANDLE *h,
                                              ENGINE_HANDLE_V1 *h1) {
    int num_items = 10;
    for (int j = 0; j < num_items; ++j) {
        item *i = NULL;
        std::stringstream ss;
        ss << "key" << j;
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET, ss.str().c_str(), "data", &i),
                "Failed to store a value");
        h1->release(h, NULL, i);
    }

    const void *cookie = testHarness.create_cookie();
    const char *name = "unittest";
    uint32_t opaque = 1;

    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie, ++opaque, 0, DCP_OPEN_PRODUCER, (void*)name,
                         strlen(name)),
            "Failed dcp producer open connection.");

    uint16_t vbucket = 0;
    uint32_t flags = DCP_ADD_STREAM_FLAG_TAKEOVER;
    uint64_t start_seqno = 10;
    uint64_t end_seqno = std::numeric_limits<uint64_t>::max();
    uint64_t vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
    uint64_t snap_start_seqno = 10;
    uint64_t snap_end_seqno = 10;

    uint64_t rollback = 0;
    checkeq(ENGINE_SUCCESS,
            h1->dcp.stream_req(h, cookie, flags, ++opaque, vbucket, start_seqno,
                               end_seqno, vb_uuid, snap_start_seqno,
                               snap_end_seqno, &rollback,
                               mock_dcp_add_failover_log),
            "Failed to initiate stream request");

    std::unique_ptr<dcp_message_producers> producers(get_dcp_producers(h, h1));

    bool done = false;
    int num_snapshot_markers = 0;
    int num_set_vbucket_pending = 0;
    int num_set_vbucket_active = 0;

    do {
        ENGINE_ERROR_CODE err = h1->dcp.step(h, cookie, producers.get());
        if (err == ENGINE_DISCONNECT) {
            done = true;
        } else {
            switch (dcp_last_op) {
                case PROTOCOL_BINARY_CMD_DCP_STREAM_END:
                    done = true;
                    break;
                case PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER:
                    num_snapshot_markers++;
                    break;
                case PROTOCOL_BINARY_CMD_DCP_SET_VBUCKET_STATE:
                    if (dcp_last_vbucket_state == vbucket_state_pending) {
                        num_set_vbucket_pending++;
                    } else if (dcp_last_vbucket_state == vbucket_state_active) {
                        num_set_vbucket_active++;
                    }
                    sendDcpAck(h, h1, cookie, PROTOCOL_BINARY_CMD_DCP_SET_VBUCKET_STATE,
                               PROTOCOL_BINARY_RESPONSE_SUCCESS, dcp_last_opaque);
                    break;
                case 0:
                     break;
                default:
                    break;
                    abort();
            }
            dcp_last_op = 0;
        }
    } while (!done);

    checkeq(0, num_snapshot_markers, "Invalid number of snapshot marker");
    checkeq(1, num_set_vbucket_pending, "Didn't receive pending set state");
    checkeq(1, num_set_vbucket_active, "Didn't receive active set state");

    check(verify_vbucket_state(h, h1, 0, vbucket_state_dead), "Wrong vb state");
    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static uint32_t add_stream_for_consumer(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                        const void* cookie, uint32_t opaque,
                                        uint16_t vbucket, uint32_t flags,
                                        protocol_binary_response_status response,
                                        uint64_t exp_snap_start,
                                        uint64_t exp_snap_end) {

    dcp_step(h, h1, cookie);
    cb_assert(dcp_last_op == PROTOCOL_BINARY_CMD_DCP_CONTROL);
    cb_assert(dcp_last_key.compare("connection_buffer_size") == 0);
    cb_assert(dcp_last_opaque != opaque);

    if (get_bool_stat(h, h1, "ep_dcp_enable_noop")) {
        // Check that the enable noop message is sent
        dcp_step(h, h1, cookie);
        cb_assert(dcp_last_op == PROTOCOL_BINARY_CMD_DCP_CONTROL);
        cb_assert(dcp_last_key.compare("enable_noop") == 0);
        cb_assert(dcp_last_opaque != opaque);

        // Check that the set noop interval message is sent
        dcp_step(h, h1, cookie);
        cb_assert(dcp_last_op == PROTOCOL_BINARY_CMD_DCP_CONTROL);
        cb_assert(dcp_last_key.compare("set_noop_interval") == 0);
        cb_assert(dcp_last_opaque != opaque);
    }

    dcp_step(h, h1, cookie);
    cb_assert(dcp_last_op == PROTOCOL_BINARY_CMD_DCP_CONTROL);
    cb_assert(dcp_last_key.compare("set_priority") == 0);
    cb_assert(dcp_last_opaque != opaque);

    dcp_step(h, h1, cookie);
    cb_assert(dcp_last_op == PROTOCOL_BINARY_CMD_DCP_CONTROL);
    cb_assert(dcp_last_key.compare("enable_ext_metadata") == 0);
    cb_assert(dcp_last_opaque != opaque);

    if (get_bool_stat(h, h1, "ep_dcp_value_compression_enabled")) {
        dcp_step(h, h1, cookie);
        cb_assert(dcp_last_op == PROTOCOL_BINARY_CMD_DCP_CONTROL);
        cb_assert(dcp_last_key.compare("enable_value_compression") == 0);
        cb_assert(dcp_last_opaque != opaque);
    }

    dcp_step(h, h1, cookie);
    cb_assert(dcp_last_op = PROTOCOL_BINARY_CMD_DCP_CONTROL);
    cb_assert(dcp_last_key.compare("supports_cursor_dropping") == 0);
    cb_assert(dcp_last_opaque != opaque);

    checkeq(ENGINE_SUCCESS,
            h1->dcp.add_stream(h, cookie, opaque, vbucket, flags),
            "Add stream request failed");

    dcp_step(h, h1, cookie);
    uint32_t stream_opaque = dcp_last_opaque;
    cb_assert(dcp_last_op == PROTOCOL_BINARY_CMD_DCP_STREAM_REQ);
    cb_assert(dcp_last_opaque != opaque);

    if (exp_snap_start != 0) {
        cb_assert(exp_snap_start == dcp_last_snap_start_seqno);
    }

    if (exp_snap_end != 0) {
        cb_assert(exp_snap_end == dcp_last_snap_end_seqno);
    }

    size_t bodylen = 0;
    if (response == PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        bodylen = 16;
    } else if (response == PROTOCOL_BINARY_RESPONSE_ROLLBACK) {
        bodylen = 8;
    }

    size_t headerlen = sizeof(protocol_binary_response_header);
    size_t pkt_len = headerlen + bodylen;

    protocol_binary_response_header* pkt =
        (protocol_binary_response_header*)malloc(pkt_len);
    memset(pkt->bytes, '\0', pkt_len);
    pkt->response.magic = PROTOCOL_BINARY_RES;
    pkt->response.opcode = PROTOCOL_BINARY_CMD_DCP_STREAM_REQ;
    pkt->response.status = htons(response);
    pkt->response.opaque = dcp_last_opaque;

    if (response == PROTOCOL_BINARY_RESPONSE_ROLLBACK) {
        bodylen = sizeof(uint64_t);
        uint64_t rollbackSeqno = 0;
        memcpy(pkt->bytes + headerlen, &rollbackSeqno, bodylen);
    }

    pkt->response.bodylen = htonl(bodylen);

    if (response == PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        uint64_t vb_uuid = htonll(123456789);
        uint64_t by_seqno = 0;
        memcpy(pkt->bytes + headerlen, &vb_uuid, sizeof(uint64_t));
        memcpy(pkt->bytes + headerlen + 8, &by_seqno, sizeof(uint64_t));
    }

    checkeq(ENGINE_SUCCESS,
            h1->dcp.response_handler(h, cookie, pkt),
            "Expected success");
    dcp_step(h, h1, cookie);
    free (pkt);

    if (response == PROTOCOL_BINARY_RESPONSE_ROLLBACK) {
        return stream_opaque;
    }

    if (dcp_last_op == PROTOCOL_BINARY_CMD_DCP_STREAM_REQ) {
        cb_assert(dcp_last_opaque != opaque);
        verify_curr_items(h, h1, 0, "Wrong amount of items");

        protocol_binary_response_header* pkt =
            (protocol_binary_response_header*)malloc(pkt_len);
        memset(pkt->bytes, '\0', 40);
        pkt->response.magic = PROTOCOL_BINARY_RES;
        pkt->response.opcode = PROTOCOL_BINARY_CMD_DCP_STREAM_REQ;
        pkt->response.status = htons(PROTOCOL_BINARY_RESPONSE_SUCCESS);
        pkt->response.opaque = dcp_last_opaque;
        pkt->response.bodylen = htonl(16);

        uint64_t vb_uuid = htonll(123456789);
        uint64_t by_seqno = 0;
        memcpy(pkt->bytes + headerlen, &vb_uuid, sizeof(uint64_t));
        memcpy(pkt->bytes + headerlen + 8, &by_seqno, sizeof(uint64_t));

        checkeq(ENGINE_SUCCESS,
                h1->dcp.response_handler(h, cookie, pkt),
                "Expected success");
        dcp_step(h, h1, cookie);

        cb_assert(dcp_last_op == PROTOCOL_BINARY_CMD_DCP_ADD_STREAM);
        cb_assert(dcp_last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS);
        cb_assert(dcp_last_stream_opaque == stream_opaque);
        free(pkt);
    } else {
        cb_assert(dcp_last_op == PROTOCOL_BINARY_CMD_DCP_ADD_STREAM);
        cb_assert(dcp_last_status == response);
        cb_assert(dcp_last_stream_opaque == stream_opaque);
    }

    if (response == PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        uint64_t uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
        uint64_t seq = get_ull_stat(h, h1, "vb_0:0:seq", "failovers");
        cb_assert(uuid == 123456789);
        cb_assert(seq == 0);
    }

    return stream_opaque;
}

static enum test_result test_dcp_reconnect(ENGINE_HANDLE *h,
                                           ENGINE_HANDLE_V1 *h1,
                                           bool full, bool restart) {
    // Test reconnect when we were disconnected after receiving a full snapshot
    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state.");

    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    const char *name = "unittest";
    uint16_t nname = strlen(name);
    int items = full ? 10 : 5;

    // Open consumer connection
    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie, opaque, 0, flags, (void*)name, nname),
            "Failed dcp Consumer open connection.");

    add_stream_for_consumer(h, h1, cookie, opaque++, 0, 0,
                            PROTOCOL_BINARY_RESPONSE_SUCCESS);

    uint32_t stream_opaque =
        get_int_stat(h, h1, "eq_dcpq:unittest:stream_0_opaque", "dcp");
    checkeq(ENGINE_SUCCESS,
            h1->dcp.snapshot_marker(h, cookie, stream_opaque, 0, 0, 10, 2),
            "Failed to send snapshot marker");

    for (int i = 1; i <= items; i++) {
        std::stringstream ss;
        ss << "key" << i;
        checkeq(ENGINE_SUCCESS,
                h1->dcp.mutation(h, cookie, stream_opaque, ss.str().c_str(),
                                 ss.str().length(), "value", 5, i * 3, 0, 0, 0, i,
                                 0, 0, 0, "", 0, INITIAL_NRU_VALUE),
                "Failed to send dcp mutation");
    }

    wait_for_flusher_to_settle(h, h1);
    wait_for_stat_to_be(h, h1, "vb_replica_curr_items", items);

    testHarness.destroy_cookie(cookie);

    if (restart) {
        testHarness.reload_engine(&h, &h1, testHarness.engine_path,
                                  testHarness.get_current_testcase()->cfg,
                                  true, true);
        wait_for_warmup_complete(h, h1);
    }

    cookie = testHarness.create_cookie();

    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie, opaque, 0, flags, (void*)name, nname),
            "Failed dcp Consumer open connection.");

    uint64_t snap_start = full ? 10 : 0;
    uint64_t snap_end = 10;
    add_stream_for_consumer(h, h1, cookie, opaque++, 0, 0,
                            PROTOCOL_BINARY_RESPONSE_SUCCESS, snap_start,
                            snap_end);

    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_dcp_reconnect_full(ENGINE_HANDLE *h,
                                                ENGINE_HANDLE_V1 *h1) {
    // Test reconnect after a dropped connection with a full snapshot
    return test_dcp_reconnect(h, h1, true, false);
}

static enum test_result test_dcp_reconnect_partial(ENGINE_HANDLE *h,
                                                   ENGINE_HANDLE_V1 *h1) {
    // Test reconnect after a dropped connection with a partial snapshot
    return test_dcp_reconnect(h, h1, false, false);
}

static enum test_result test_dcp_crash_reconnect_full(ENGINE_HANDLE *h,
                                                      ENGINE_HANDLE_V1 *h1) {
    // Test reconnect after we crash with a full snapshot
    return test_dcp_reconnect(h, h1, true, true);
}

static enum test_result test_dcp_crash_reconnect_partial(ENGINE_HANDLE *h,
                                                         ENGINE_HANDLE_V1 *h1) {
    // Test reconnect after we crash with a partial snapshot
    return test_dcp_reconnect(h, h1, false, true);
}

static enum test_result test_dcp_consumer_takeover(ENGINE_HANDLE *h,
                                                   ENGINE_HANDLE_V1 *h1) {
    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    const char *name = "unittest";
    uint16_t nname = strlen(name);

    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state.");

    // Open consumer connection
    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie, opaque, 0, flags, (void*)name, nname),
            "Failed dcp Consumer open connection.");

    add_stream_for_consumer(h, h1, cookie, opaque++, 0,
                            DCP_ADD_STREAM_FLAG_TAKEOVER,
                            PROTOCOL_BINARY_RESPONSE_SUCCESS);

    uint32_t stream_opaque =
        get_int_stat(h, h1, "eq_dcpq:unittest:stream_0_opaque", "dcp");

    h1->dcp.snapshot_marker(h, cookie, stream_opaque, 0, 1, 5, 10);
    for (int i = 1; i <= 5; i++) {
        std::stringstream ss;
        ss << "key" << i;
        checkeq(ENGINE_SUCCESS,
                h1->dcp.mutation(h, cookie, stream_opaque, ss.str().c_str(),
                                 ss.str().length(), "value", 5, i * 3, 0, 0, 0, i,
                                 0, 0, 0, "", 0, INITIAL_NRU_VALUE),
                "Failed to send dcp mutation");
    }

    h1->dcp.snapshot_marker(h, cookie, stream_opaque, 0, 6, 10, 10);
    for (int i = 6; i <= 10; i++) {
        std::stringstream ss;
        ss << "key" << i;
        checkeq(ENGINE_SUCCESS,
                h1->dcp.mutation(h, cookie, stream_opaque, ss.str().c_str(),
                                 ss.str().length(), "value", 5, i * 3, 0, 0, 0, i,
                                 0, 0, 0, "", 0, INITIAL_NRU_VALUE),
                "Failed to send dcp mutation");
    }

    wait_for_stat_to_be(h, h1, "eq_dcpq:unittest:stream_0_buffer_items", 0, "dcp");

    dcp_step(h, h1, cookie);
    cb_assert(dcp_last_op == PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER);
    cb_assert(dcp_last_status == ENGINE_SUCCESS);
    cb_assert(dcp_last_opaque != opaque);

    dcp_step(h, h1, cookie);
    cb_assert(dcp_last_op == PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER);
    cb_assert(dcp_last_status == ENGINE_SUCCESS);
    cb_assert(dcp_last_opaque != opaque);

    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_failover_scenario_one_with_dcp(ENGINE_HANDLE *h,
                                                            ENGINE_HANDLE_V1 *h1) {

    int num_items = 50;
    for (int j = 0; j < num_items; ++j) {
        item *i = NULL;
        std::stringstream ss;
        ss << "key" << j;
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET, ss.str().c_str(), "data", &i),
                "Failed to store a value");
        h1->release(h, NULL, i);
        if (j % 10 == 0) {
            wait_for_flusher_to_settle(h, h1);
            createCheckpoint(h, h1);
        }
    }

    createCheckpoint(h, h1);
    wait_for_flusher_to_settle(h, h1);

    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    const char *name = "unittest";
    uint16_t nname = strlen(name);

    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state.");

    // Open consumer connection
    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie, opaque, 0, flags, (void*)name, nname),
            "Failed dcp Consumer open connection.");

    add_stream_for_consumer(h, h1, cookie, opaque++, 0,
                            DCP_ADD_STREAM_FLAG_TAKEOVER,
                            PROTOCOL_BINARY_RESPONSE_SUCCESS);

    uint32_t stream_opaque =
        get_int_stat(h, h1, "eq_dcpq:unittest:stream_0_opaque", "dcp");

    checkeq(ENGINE_SUCCESS,
            h1->dcp.snapshot_marker(h, cookie, stream_opaque, 0, 200, 300, 300),
            "Failed to send snapshot marker");

    wait_for_stat_to_be(h, h1, "eq_dcpq:unittest:stream_0_buffer_items", 0, "dcp");

    checkeq(ENGINE_SUCCESS,
            h1->dcp.close_stream(h, cookie, stream_opaque, 0),
            "Expected success");

    // Simulating a failover scenario, where the replica vbucket will
    // be marked as active.
    check(set_vbucket_state(h, h1, 0, vbucket_state_active),
            "Failed to set vbucket state.");

    item *i = NULL;
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i),
            "Error in SET operation.");

    h1->release(h, NULL, i);

    wait_for_flusher_to_settle(h, h1);
    checkeq(0, get_int_stat(h, h1, "ep_diskqueue_items"),
            "Unexpected diskqueue");

    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_failover_scenario_two_with_dcp(ENGINE_HANDLE *h,
                                                            ENGINE_HANDLE_V1 *h1) {

    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state.");
    wait_for_flusher_to_settle(h, h1);

    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    const char *name = "unittest";
    uint16_t nname = strlen(name);

    // Open consumer connection
    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie, opaque, 0, 0, (void*)name, nname),
            "Failed dcp Consumer open connection.");

    // Set up a passive stream
    add_stream_for_consumer(h, h1, cookie, opaque++, 0, 0,
                            PROTOCOL_BINARY_RESPONSE_SUCCESS);

    uint32_t stream_opaque =
        get_int_stat(h, h1, "eq_dcpq:unittest:stream_0_opaque", "dcp");

    // Snapshot marker indicating 5 mutations will follow
    checkeq(ENGINE_SUCCESS, h1->dcp.snapshot_marker(h, cookie, stream_opaque,
                                                    0, 0, 5, 0),
            "Failed to send marker!");

    // Send 4 mutations
    uint64_t i;
    for (i = 1; i <= 4; i++) {
        std::string key("key" + std::to_string(i));
        checkeq(ENGINE_SUCCESS, h1->dcp.mutation(h, cookie, stream_opaque,
                                                 key.c_str(), key.length(),
                                                 "value", 5,
                                                 i * 3, 0, 0,
                                                 PROTOCOL_BINARY_RAW_BYTES,
                                                 i, 0, 0, 0, "", 0,
                                                 INITIAL_NRU_VALUE),
                "Failed dcp mutate.");
    }

    // Simulate failover
    check(set_vbucket_state(h, h1, 0, vbucket_state_active),
          "Failed to set vbucket state.");
    wait_for_flusher_to_settle(h, h1);

    int openCheckpointId = get_int_stat(h, h1, "vb_0:open_checkpoint_id",
                                        "checkpoint");

    // Front-end operations (sets)
    for (int j = 1; j <= 2; ++j) {
        item *i = NULL;
        std::string key("key_" + std::to_string(j));
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET, key.c_str(), "data", &i),
                "Failed to store a value");
        h1->release(h, NULL, i);
    }

    // Wait for a new open checkpoint
    wait_for_stat_to_be(h, h1, "vb_0:open_checkpoint_id", openCheckpointId + 1,
                        "checkpoint");

    // Consumer processes 5th mutation
    std::string key("key" + std::to_string(i));
    checkeq(ENGINE_KEY_ENOENT,
            h1->dcp.mutation(h, cookie, stream_opaque, key.c_str(), key.length(),
                             "value", 5, i * 3, 0, 0,
                             PROTOCOL_BINARY_RAW_BYTES,
                             i, 0, 0, 0, "", 0, INITIAL_NRU_VALUE),
            "Unexpected response for the mutation!");

    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_dcp_add_stream(ENGINE_HANDLE *h,
                                            ENGINE_HANDLE_V1 *h1) {
    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    const char *name = "unittest";
    uint16_t nname = strlen(name);

    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state.");

    // Open consumer connection
    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie, opaque, 0, flags, (void*)name, nname),
            "Failed dcp Consumer open connection.");

    add_stream_for_consumer(h, h1, cookie, opaque++, 0, 0,
                            PROTOCOL_BINARY_RESPONSE_SUCCESS);

    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_consumer_backoff_stat(ENGINE_HANDLE *h,
                                                   ENGINE_HANDLE_V1 *h1) {
    set_param(h, h1, protocol_binary_engine_param_tap,
              "replication_throttle_queue_cap", "10");
    checkeq(10, get_int_stat(h, h1, "ep_replication_throttle_queue_cap"),
            "Incorrect tap_keepalive value.");

    stop_persistence(h, h1);

    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    const char *name = "unittest";
    uint16_t nname = strlen(name);

    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state.");

    // Open consumer connection
    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie, opaque, 0, flags, (void*)name, nname),
            "Failed dcp Consumer open connection.");

    add_stream_for_consumer(h, h1, cookie, opaque++, 0, 0,
                            PROTOCOL_BINARY_RESPONSE_SUCCESS);

    testHarness.time_travel(30);
    checkeq(0, get_int_stat(h, h1, "eq_dcpq:unittest:total_backoffs", "dcp"),
            "Expected backoffs to be 0");

    uint32_t stream_opaque =
        get_int_stat(h, h1, "eq_dcpq:unittest:stream_0_opaque", "dcp");
    checkeq(h1->dcp.snapshot_marker(h, cookie, stream_opaque, 0, 0, 20, 1),
            ENGINE_SUCCESS, "Failed to send snapshot marker");

    for (int i = 1; i <= 20; i++) {
        std::stringstream ss;
        ss << "key" << i;
        checkeq(ENGINE_SUCCESS,
                h1->dcp.mutation(h, cookie, stream_opaque, ss.str().c_str(),
                                 ss.str().length(), "value", 5, i * 3, 0, 0, 0, i,
                                 0, 0, 0, "", 0, INITIAL_NRU_VALUE),
                "Failed to send dcp mutation");
    }

    wait_for_stat_change(h, h1, "eq_dcpq:unittest:total_backoffs", 0, "dcp");
    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_rollback_to_zero(ENGINE_HANDLE *h,
                                              ENGINE_HANDLE_V1 *h1) {
    int num_items = 10;
    for (int j = 0; j < num_items; ++j) {
        item *i = NULL;
        std::stringstream ss;
        ss << "key" << j;
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET, ss.str().c_str(), "data", &i),
                "Failed to store a value");
        h1->release(h, NULL, i);
    }

    wait_for_flusher_to_settle(h, h1);
    verify_curr_items(h, h1, num_items, "Wrong amount of items");

    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state.");

    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    const char *name = "unittest";
    uint16_t nname = strlen(name);

    // Open consumer connection
    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie, opaque, 0, flags, (void*)name, nname),
            "Failed dcp Consumer open connection.");

    add_stream_for_consumer(h, h1, cookie, opaque++, 0, 0,
                            PROTOCOL_BINARY_RESPONSE_ROLLBACK);

    wait_for_flusher_to_settle(h, h1);
    wait_for_rollback_to_finish(h, h1);

    checkeq(0, get_int_stat(h, h1, "curr_items"),
            "All items should be rolled back");
    checkeq(num_items, get_int_stat(h, h1, "vb_replica_rollback_item_count"),
            "Replica rollback count does not match");
    checkeq(num_items, get_int_stat(h, h1, "rollback_item_count"),
            "Aggr rollback count does not match");

    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_chk_manager_rollback(ENGINE_HANDLE *h,
                                                  ENGINE_HANDLE_V1 *h1) {
    uint16_t vbid = 0;
    int num_items = 40;
    stop_persistence(h, h1);
    for (int j = 0; j < num_items; ++j) {
        item *i = NULL;
        std::stringstream ss;
        ss << "key" << j;
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET, ss.str().c_str(), "data", &i),
                "Failed to store a value");
        h1->release(h, NULL, i);
    }

    start_persistence(h, h1);
    wait_for_flusher_to_settle(h, h1);
    verify_curr_items(h, h1, num_items, "Wrong amount of items");

    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);

    wait_for_warmup_complete(h, h1);
    stop_persistence(h, h1);

    for (int j = 0; j < num_items / 2; ++j) {
        item *i = NULL;
        std::stringstream ss;
        ss << "key" << (j + num_items);
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET, ss.str().c_str(), "data", &i),
                "Failed to store a value");
        h1->release(h, NULL, i);
    }

    start_persistence(h, h1);
    wait_for_flusher_to_settle(h, h1);
    verify_curr_items(h, h1, 60, "Wrong amount of items");
    set_vbucket_state(h, h1, vbid, vbucket_state_replica);

    // Create rollback stream
    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    const char *name = "unittest";
    uint16_t nname = strlen(name);

    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie, opaque, 0, flags, (void*)name, nname),
            "Failed dcp Consumer open connection.");

    do {
        dcp_step(h, h1, cookie);
    } while (dcp_last_op == PROTOCOL_BINARY_CMD_DCP_CONTROL);

    checkeq(ENGINE_SUCCESS,
            h1->dcp.add_stream(h, cookie, ++opaque, vbid, 0),
            "Add stream request failed");

    dcp_step(h, h1, cookie);
    uint32_t stream_opaque = dcp_last_opaque;
    cb_assert(dcp_last_op == PROTOCOL_BINARY_CMD_DCP_STREAM_REQ);
    cb_assert(dcp_last_opaque != opaque);

    uint64_t rollbackSeqno = htonll(40);
    protocol_binary_response_header* pkt =
        (protocol_binary_response_header*)malloc(32);
    memset(pkt->bytes, '\0', 32);
    pkt->response.magic = PROTOCOL_BINARY_RES;
    pkt->response.opcode = PROTOCOL_BINARY_CMD_DCP_STREAM_REQ;
    pkt->response.status = htons(PROTOCOL_BINARY_RESPONSE_ROLLBACK);
    pkt->response.opaque = stream_opaque;
    pkt->response.bodylen = htonl(8);
    memcpy(pkt->bytes + 24, &rollbackSeqno, sizeof(uint64_t));

    checkeq(ENGINE_SUCCESS,
            h1->dcp.response_handler(h, cookie, pkt),
            "Expected success");

    do {
        dcp_step(h, h1, cookie);
        usleep(100);
    } while (dcp_last_op != PROTOCOL_BINARY_CMD_DCP_STREAM_REQ);

    stream_opaque = dcp_last_opaque;
    free(pkt);

    // Send success

    uint64_t vb_uuid = htonll(123456789);
    uint64_t by_seqno = 0;
    pkt = (protocol_binary_response_header*)malloc(40);
    memset(pkt->bytes, '\0', 40);
    pkt->response.magic = PROTOCOL_BINARY_RES;
    pkt->response.opcode = PROTOCOL_BINARY_CMD_DCP_STREAM_REQ;
    pkt->response.status = htons(PROTOCOL_BINARY_RESPONSE_SUCCESS);
    pkt->response.opaque = stream_opaque;
    pkt->response.bodylen = htonl(16);
    memcpy(pkt->bytes + 24, &vb_uuid, sizeof(uint64_t));
    memcpy(pkt->bytes + 22, &by_seqno, sizeof(uint64_t));

    checkeq(ENGINE_SUCCESS,
            h1->dcp.response_handler(h, cookie, pkt),
            "Expected success");
    dcp_step(h, h1, cookie);
    free(pkt);

    int items = get_int_stat(h, h1, "curr_items_tot");
    int seqno = get_int_stat(h, h1, "vb_0:high_seqno", "vbucket-seqno");
    int chk = get_int_stat(h, h1, "vb_0:num_checkpoint_items", "checkpoint");

    checkeq(40, items, "Got invalid amount of items");
    checkeq(40, seqno, "Seqno should be 40 after rollback");
    checkeq(1, chk, "There should only be one checkpoint item");
    checkeq(num_items/2, get_int_stat(h, h1, "vb_replica_rollback_item_count"),
            "Replica rollback count does not match");
    checkeq(num_items/2, get_int_stat(h, h1, "rollback_item_count"),
            "Aggr rollback count does not match");

    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_fullrollback_for_consumer(ENGINE_HANDLE *h,
                                                       ENGINE_HANDLE_V1 *h1) {
    const int num_items = 10;
    std::vector<std::string> keys;
    for (int i = 0; i < num_items; ++i) {
        std::stringstream ss;
        ss << "key" << i;
        std::string key(ss.str());
        keys.push_back(key);
    }
    std::vector<std::string>::iterator it;
    for (it = keys.begin(); it != keys.end(); ++it) {
        item *itm;
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET, it->c_str(), it->c_str(),
                      &itm, 0, 0),
                "Failed to store a value");
        h1->release(h, NULL, itm);
    }
    wait_for_flusher_to_settle(h, h1);
    checkeq(num_items,
            get_int_stat(h, h1, "curr_items"),
            "Item count should've been 10");

    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    const char *name = "unittest";
    uint16_t nname = strlen(name);

    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state.");

    // Open consumer connection
    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie, opaque, 0, flags, (void*)name, nname),
            "Failed dcp Consumer open connection.");

    do {
        dcp_step(h, h1, cookie);
    } while (dcp_last_op == PROTOCOL_BINARY_CMD_DCP_CONTROL);

    checkeq(ENGINE_SUCCESS,
            h1->dcp.add_stream(h, cookie, opaque, 0, 0),
            "Add stream request failed");

    dcp_step(h, h1, cookie);
    cb_assert(dcp_last_op == PROTOCOL_BINARY_CMD_DCP_STREAM_REQ);
    cb_assert(dcp_last_opaque != opaque);

    uint32_t headerlen = sizeof(protocol_binary_response_header);
    uint32_t bodylen = sizeof(uint64_t);
    uint64_t rollbackSeqno = htonll(5);
    protocol_binary_response_header *pkt1 =
        (protocol_binary_response_header*)malloc(headerlen + bodylen);
    memset(pkt1->bytes, '\0', headerlen + bodylen);
    pkt1->response.magic = PROTOCOL_BINARY_RES;
    pkt1->response.opcode = PROTOCOL_BINARY_CMD_DCP_STREAM_REQ;
    pkt1->response.status = htons(PROTOCOL_BINARY_RESPONSE_ROLLBACK);
    pkt1->response.bodylen = htonl(bodylen);
    pkt1->response.opaque = dcp_last_opaque;
    memcpy(pkt1->bytes + headerlen, &rollbackSeqno, bodylen);

    checkeq(ENGINE_SUCCESS,
            h1->dcp.response_handler(h, cookie, pkt1),
            "Expected Success after Rollback");
    wait_for_stat_to_be(h, h1, "ep_rollback_count", 1);
    dcp_step(h, h1, cookie);

    opaque++;

    cb_assert(dcp_last_opaque != opaque);

    bodylen = 2 *sizeof(uint64_t);
    protocol_binary_response_header* pkt2 =
        (protocol_binary_response_header*)malloc(headerlen + bodylen);
    memset(pkt2->bytes, '\0', headerlen + bodylen);
    pkt2->response.magic = PROTOCOL_BINARY_RES;
    pkt2->response.opcode = PROTOCOL_BINARY_CMD_DCP_STREAM_REQ;
    pkt2->response.status = htons(PROTOCOL_BINARY_RESPONSE_SUCCESS);
    pkt2->response.opaque = dcp_last_opaque;
    pkt2->response.bodylen = htonl(bodylen);
    uint64_t vb_uuid = htonll(123456789);
    uint64_t by_seqno = 0;
    memcpy(pkt2->bytes + headerlen, &vb_uuid, sizeof(uint64_t));
    memcpy(pkt2->bytes + headerlen + 8, &by_seqno, sizeof(uint64_t));

    checkeq(ENGINE_SUCCESS,
            h1->dcp.response_handler(h, cookie, pkt2),
            "Expected success");

    dcp_step(h, h1, cookie);
    cb_assert(dcp_last_op == PROTOCOL_BINARY_CMD_DCP_ADD_STREAM);

    free(pkt1);
    free(pkt2);

    //Verify that all items have been removed from consumer
    wait_for_flusher_to_settle(h, h1);
    checkeq(0, get_int_stat(h, h1, "vb_replica_curr_items"),
            "Item count should've been 0");
    checkeq(1, get_int_stat(h, h1, "ep_rollback_count"),
            "Rollback count expected to be 1");
    checkeq(num_items, get_int_stat(h, h1, "vb_replica_rollback_item_count"),
            "Replica rollback count does not match");
    checkeq(num_items, get_int_stat(h, h1, "rollback_item_count"),
            "Aggr rollback count does not match");

    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_partialrollback_for_consumer(ENGINE_HANDLE *h,
                                                          ENGINE_HANDLE_V1 *h1) {

    stop_persistence(h, h1);
    std::vector<std::string> keys;
    for (int i = 0; i < 100; ++i) {
        std::stringstream ss;
        ss << "key_" << i;
        std::string key(ss.str());
        keys.push_back(key);
    }
    std::vector<std::string>::iterator it;
    for (it = keys.begin(); it != keys.end(); ++it) {
        item *itm;
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET, it->c_str(), it->c_str(),
                      &itm, 0, 0),
                "Failed to store a value");
        h1->release(h, NULL, itm);
    }
    start_persistence(h, h1);
    wait_for_flusher_to_settle(h, h1);
    checkeq(100, get_int_stat(h, h1, "curr_items"),
            "Item count should've been 100");

    stop_persistence(h, h1);
    keys.clear();
    for (int i = 90; i < 110; ++i) {
        std::stringstream ss;
        ss << "key_" << i;
        std::string key(ss.str());
        keys.push_back(key);
    }
    for (it = keys.begin(); it != keys.end(); ++it) {
        item *itm;
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET, it->c_str(), it->c_str(),
                      &itm, 0, 0),
                "Failed to store a value");
        h1->release(h, NULL, itm);
    }
    start_persistence(h, h1);
    wait_for_flusher_to_settle(h, h1);
    checkeq(110, get_int_stat(h, h1, "curr_items"),
            "Item count should've been 110");

    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state.");

    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    const char *name = "unittest";
    uint16_t nname = strlen(name);

    // Open consumer connection
    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie, opaque, 0, flags, (void*)name, nname),
            "Failed dcp Consumer open connection.");

    do {
        dcp_step(h, h1, cookie);
    } while (dcp_last_op == PROTOCOL_BINARY_CMD_DCP_CONTROL);

    checkeq(ENGINE_SUCCESS,
            h1->dcp.add_stream(h, cookie, opaque, 0, 0),
            "Add stream request failed");

    dcp_step(h, h1, cookie);
    cb_assert(dcp_last_op == PROTOCOL_BINARY_CMD_DCP_STREAM_REQ);
    cb_assert(dcp_last_opaque != opaque);

    uint32_t headerlen = sizeof(protocol_binary_response_header);
    uint32_t bodylen = sizeof(uint64_t);
    uint64_t rollbackSeqno = htonll(100);
    protocol_binary_response_header *pkt1 =
        (protocol_binary_response_header*)malloc(headerlen + bodylen);
    memset(pkt1->bytes, '\0', headerlen + bodylen);
    pkt1->response.magic = PROTOCOL_BINARY_RES;
    pkt1->response.opcode = PROTOCOL_BINARY_CMD_DCP_STREAM_REQ;
    pkt1->response.status = htons(PROTOCOL_BINARY_RESPONSE_ROLLBACK);
    pkt1->response.bodylen = htonl(bodylen);
    pkt1->response.opaque = dcp_last_opaque;
    memcpy(pkt1->bytes + headerlen, &rollbackSeqno, bodylen);

    checkeq(ENGINE_SUCCESS,
            h1->dcp.response_handler(h, cookie, pkt1),
            "Expected Success after Rollback");
    wait_for_stat_to_be(h, h1, "ep_rollback_count", 1);
    dcp_step(h, h1, cookie);
    opaque++;

    bodylen = 2 * sizeof(uint64_t);
    protocol_binary_response_header* pkt2 =
        (protocol_binary_response_header*)malloc(headerlen + bodylen);
    memset(pkt2->bytes, '\0', headerlen + bodylen);
    pkt2->response.magic = PROTOCOL_BINARY_RES;
    pkt2->response.opcode = PROTOCOL_BINARY_CMD_DCP_STREAM_REQ;
    pkt2->response.status = htons(PROTOCOL_BINARY_RESPONSE_SUCCESS);
    pkt2->response.opaque = dcp_last_opaque;
    pkt2->response.bodylen = htonl(bodylen);
    uint64_t vb_uuid = htonll(123456789);
    uint64_t by_seqno = 0;
    memcpy(pkt2->bytes + headerlen, &vb_uuid, sizeof(uint64_t));
    memcpy(pkt2->bytes + headerlen + 8, &by_seqno, sizeof(uint64_t));

    checkeq(ENGINE_SUCCESS,
            h1->dcp.response_handler(h, cookie, pkt2),
            "Expected success");
    dcp_step(h, h1, cookie);

    free(pkt1);
    free(pkt2);

    //?Verify that 10 items plus 10 updates have been removed from consumer
    wait_for_flusher_to_settle(h, h1);
    checkeq(100, get_int_stat(h, h1, "vb_replica_curr_items"),
            "Item count should've been 100");
    checkeq(1, get_int_stat(h, h1, "ep_rollback_count"),
            "Rollback count expected to be 1");
    checkeq(20, get_int_stat(h, h1, "vb_replica_rollback_item_count"),
            "Replica rollback count does not match");
    checkeq(20, get_int_stat(h, h1, "rollback_item_count"),
            "Aggr rollback count does not match");

    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_buffer_log_size(ENGINE_HANDLE *h,
                                                  ENGINE_HANDLE_V1 *h1) {
    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = DCP_OPEN_PRODUCER;
    const char *name = "unittest";
    uint16_t nname = strlen(name);
    char stats_buffer[50];
    char status_buffer[50];

    // Open consumer connection
    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie, opaque, 0, flags, (void*)name, nname),
            "Failed dcp Consumer open connection.");

    checkeq(ENGINE_SUCCESS,
            h1->dcp.control(h, cookie, ++opaque, "connection_buffer_size", 22,
                            "0", 1),
            "Failed to establish connection buffer");
    snprintf(status_buffer, sizeof(status_buffer),
             "eq_dcpq:%s:flow_control", name);
    std::string status = get_str_stat(h, h1, status_buffer, "dcp");
    checkeq(0, status.compare("disabled"), "Flow control enabled!");

    checkeq(ENGINE_SUCCESS,
            h1->dcp.control(h, cookie, ++opaque, "connection_buffer_size", 22,
                            "512", 4),
            "Failed to establish connection buffer");

    snprintf(stats_buffer, sizeof(stats_buffer),
             "eq_dcpq:%s:max_buffer_bytes", name);

    checkeq(512, get_int_stat(h, h1, stats_buffer, "dcp"),
            "Buffer Size did not get set");

    checkeq(ENGINE_SUCCESS,
            h1->dcp.control(h, cookie, ++opaque, "connection_buffer_size", 22,
                            "1024", 4),
            "Failed to establish connection buffer");

    checkeq(1024, get_int_stat(h, h1, stats_buffer, "dcp"),
            "Buffer Size did not get reset");

    /* Set flow control buffer size to zero which implies disable it */
    checkeq(ENGINE_SUCCESS,
            h1->dcp.control(h, cookie, ++opaque, "connection_buffer_size", 22,
                            "0", 1),
            "Failed to establish connection buffer");
    status = get_str_stat(h, h1, status_buffer, "dcp");
    checkeq(0, status.compare("disabled"), "Flow control enabled!");

    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_producer_flow_control(ENGINE_HANDLE *h,
                                                       ENGINE_HANDLE_V1 *h1) {
    /* Write 10 items */
    const int num_items = 10;
    for (int j = 0; j < num_items; ++j) {
        item *i = NULL;
        std::stringstream key;
        key << "key" << j;
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET, key.str().c_str(),
                      "123456789", &i),
                "Failed to store a value");
        h1->release(h, NULL, i);
    }
    wait_for_flusher_to_settle(h, h1);
    verify_curr_items(h, h1, num_items, "Wrong amount of items");
    uint64_t vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");

    /* Disable flow control and stream all items. The producer should stream all
     items even when we do not send acks */
    std::string name("unittest");
    const void *cookie = testHarness.create_cookie();
    dcp_stream(h, h1, name.c_str(), cookie, 0, 0, 0, num_items, vb_uuid, 0,
               0, num_items, 0, 1, 0, false, false, 0, false, NULL, false,
               0 /* do not enable flow control */,
               true /* do not ack */);

    /* Set flow control buffer to a very low value such that producer is not
     expected to send more than 1 item when we do not send acks */
    std::string name1("unittest1");
    const void *cookie1 = testHarness.create_cookie();
    dcp_stream(h, h1, name1.c_str(), cookie1, 0, 0, 0, num_items, vb_uuid,
               0, 0, 1, 0, 1, 0, false, false, 0, false, NULL, false,
               100 /* flow control buf to low value */,
               true /* do not ack */);

    testHarness.destroy_cookie(cookie);
    testHarness.destroy_cookie(cookie1);

    return SUCCESS;
}

static enum test_result test_dcp_get_failover_log(ENGINE_HANDLE *h,
                                                  ENGINE_HANDLE_V1 *h1) {
    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = DCP_OPEN_PRODUCER;
    const char *name = "unittest";
    uint16_t nname = strlen(name);

    // Open consumer connection
    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie, opaque, 0, flags, (void*)name, nname),
            "Failed dcp Consumer open connection.");

    checkeq(ENGINE_SUCCESS,
            h1->dcp.get_failover_log(h, cookie, opaque, 0,
                                     mock_dcp_add_failover_log),
            "Failed to retrieve failover log");

    testHarness.destroy_cookie(cookie);

    checkeq(ENGINE_SUCCESS,
            h1->get_stats(h, NULL, "failovers", 9, add_stats),
            "Failed to get stats.");

    size_t i = 0;
    for (i = 0; i < dcp_failover_log.size(); i++) {
        std::string itr;
        std::ostringstream ss;
        ss << i;
        itr = ss.str();
        std::string uuid = "vb_0:" + itr + ":id";
        std::string seqno = "vb_0:" + itr + ":seq";
        check(dcp_failover_log[i].first ==
                strtoull((vals[uuid]).c_str(), NULL, 10),
                "UUID mismatch in failover stats");
        check(dcp_failover_log[i].second ==
                strtoull((vals[seqno]).c_str(), NULL, 10),
                "SEQNO mismatch in failover stats");
    }

    vals.clear();
    return SUCCESS;
}

static enum test_result test_dcp_add_stream_exists(ENGINE_HANDLE *h,
                                                   ENGINE_HANDLE_V1 *h1) {
    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    const char *name = "unittest";
    uint16_t nname = strlen(name);
    uint16_t vbucket = 0;

    check(set_vbucket_state(h, h1, vbucket, vbucket_state_replica),
          "Failed to set vbucket state.");

    /* Open consumer connection */
    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie, opaque, 0, flags, (void*)name, nname),
            "Failed dcp consumer open connection.");

    /* Send add stream to consumer */
    checkeq(ENGINE_SUCCESS,
            h1->dcp.add_stream(h, cookie, ++opaque, vbucket, 0),
            "Add stream request failed");

    /* Send add stream to consumer twice and expect failure */
    checkeq(ENGINE_KEY_EEXISTS,
            h1->dcp.add_stream(h, cookie, ++opaque, 0, 0),
            "Stream exists for this vbucket");

    /* Try adding another stream for the vbucket in another consumer conn */
    /* Open another consumer connection */
    const void *cookie1 = testHarness.create_cookie();
    uint32_t opaque1 = 0xFFFF0000;
    std::string name1("unittest1");
    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie1, opaque1, 0, flags, (void*)name1.c_str(),
                         name1.length()),
            "Failed dcp consumer open connection.");

    /* Send add stream */
    checkeq(ENGINE_KEY_EEXISTS,
            h1->dcp.add_stream(h, cookie1, ++opaque1, vbucket, 0),
            "Stream exists for this vbucket");

    /* Just check that we can add passive stream for another vbucket in this
       conn*/
    checkeq(true, set_vbucket_state(h, h1, vbucket + 1, vbucket_state_replica),
            "Failed to set vbucket state.");
    checkeq(ENGINE_SUCCESS,
            h1->dcp.add_stream(h, cookie1, ++opaque1, vbucket + 1, 0),
            "Add stream request failed in the second conn");
    testHarness.destroy_cookie(cookie);
    testHarness.destroy_cookie(cookie1);
    return SUCCESS;
}

static enum test_result test_dcp_add_stream_nmvb(ENGINE_HANDLE *h,
                                                 ENGINE_HANDLE_V1 *h1) {
    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    const char *name = "unittest";
    uint16_t nname = strlen(name);

    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state.");

    // Open consumer connection
    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie, opaque, 0, flags, (void*)name, nname),
            "Failed dcp consumer open connection.");

    // Send add stream to consumer for vbucket that doesn't exist
    opaque++;
    checkeq(ENGINE_NOT_MY_VBUCKET,
            h1->dcp.add_stream(h, cookie, opaque, 1, 0),
            "Add stream expected not my vbucket");
    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_add_stream_prod_exists(ENGINE_HANDLE*h,
                                                        ENGINE_HANDLE_V1 *h1) {
    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    const char *name = "unittest";
    uint16_t nname = strlen(name);

    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state.");

    // Open consumer connection
    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie, opaque, 0, flags, (void*)name, nname),
            "Failed dcp consumer open connection.");

    add_stream_for_consumer(h, h1, cookie, opaque++, 0, 0,
                            PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS);
    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_dcp_add_stream_prod_nmvb(ENGINE_HANDLE*h,
                                                      ENGINE_HANDLE_V1 *h1) {
    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    const char *name = "unittest";
    uint16_t nname = strlen(name);

    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state.");

    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie, opaque, 0, flags, (void*)name, nname),
            "Failed dcp producer open connection.");

    add_stream_for_consumer(h, h1, cookie, opaque++, 0, 0,
                            PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET);
    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_dcp_close_stream_no_stream(ENGINE_HANDLE *h,
                                                        ENGINE_HANDLE_V1 *h1) {
    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    const char *name = "unittest";
    uint16_t nname = strlen(name);

    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie, opaque, 0, flags, (void*)name, nname),
            "Failed dcp producer open connection.");

    checkeq(ENGINE_KEY_ENOENT,
            h1->dcp.close_stream(h, cookie, opaque + 1, 0),
           "Expected stream doesn't exist");

    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_dcp_close_stream(ENGINE_HANDLE *h,
                                              ENGINE_HANDLE_V1 *h1) {
    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    const char *name = "unittest";
    uint16_t nname = strlen(name);

    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state.");

    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie, opaque, 0, flags, (void*)name, nname),
            "Failed dcp producer open connection.");

    add_stream_for_consumer(h, h1, cookie, opaque++, 0, 0,
                            PROTOCOL_BINARY_RESPONSE_SUCCESS);

    uint32_t stream_opaque =
        get_int_stat(h, h1, "eq_dcpq:unittest:stream_0_opaque", "dcp");
    std::string state =
        get_str_stat(h, h1, "eq_dcpq:unittest:stream_0_state", "dcp");
    checkeq(0, state.compare("reading"), "Expected stream in reading state");

    checkeq(ENGINE_SUCCESS,
            h1->dcp.close_stream(h, cookie, stream_opaque, 0),
            "Expected success");

    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_dcp_consumer_end_stream(ENGINE_HANDLE *h,
                                                     ENGINE_HANDLE_V1 *h1) {
    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    uint16_t vbucket = 0;
    uint32_t end_flag = 0;
    const char *name = "unittest";
    uint16_t nname = strlen(name);

    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state.");

    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie, opaque, 0, flags, (void*)name, nname),
            "Failed dcp producer open connection.");

    add_stream_for_consumer(h, h1, cookie, opaque++, vbucket, 0,
                            PROTOCOL_BINARY_RESPONSE_SUCCESS);

    uint32_t stream_opaque =
        get_int_stat(h, h1, "eq_dcpq:unittest:stream_0_opaque", "dcp");
    std::string state =
        get_str_stat(h, h1, "eq_dcpq:unittest:stream_0_state", "dcp");
    checkeq(0, state.compare("reading"), "Expected stream in reading state");

    checkeq(ENGINE_SUCCESS,
            h1->dcp.stream_end(h, cookie, stream_opaque, vbucket, end_flag),
            "Expected success");

    wait_for_str_stat_to_be(h, h1, "eq_dcpq:unittest:stream_0_state", "dead",
                            "dcp");

    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_dcp_consumer_mutate(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state.");

    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t seqno = 0;
    uint32_t flags = 0;
    const char *name = "unittest";
    uint16_t nname = strlen(name);

    // Open an DCP connection
    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie, opaque, seqno, flags, (void*)name, nname),
            "Failed dcp producer open connection.");

    std::string type = get_str_stat(h, h1, "eq_dcpq:unittest:type", "dcp");
    checkeq(0, type.compare("consumer"), "Consumer not found");

    opaque = add_stream_for_consumer(h, h1, cookie, opaque, 0, 0,
                                     PROTOCOL_BINARY_RESPONSE_SUCCESS);

    uint32_t dataLen = 100;
    char *data = static_cast<char *>(malloc(dataLen));
    memset(data, 'x', dataLen);

    uint8_t cas = 0x1;
    uint16_t vbucket = 0;
    uint8_t datatype = 1;
    uint64_t bySeqno = 10;
    uint64_t revSeqno = 0;
    uint32_t exprtime = 0;
    uint32_t lockTime = 0;

    checkeq(ENGINE_SUCCESS,
            h1->dcp.snapshot_marker(h, cookie, opaque, 0, 10, 10, 1),
            "Failed to send snapshot marker");

    // Ensure that we don't accept invalid opaque values
    checkeq(ENGINE_KEY_ENOENT,
            h1->dcp.mutation(h, cookie, opaque + 1, "key", 3, data, dataLen, cas,
                             vbucket, flags, datatype,
                             bySeqno, revSeqno, exprtime,
                             lockTime, NULL, 0, 0),
          "Failed to detect invalid DCP opaque value");

    // Send snapshot marker
    checkeq(ENGINE_SUCCESS,
            h1->dcp.snapshot_marker(h, cookie, opaque, 0, 10, 15, 300),
            "Failed to send marker!");

    // Consume an DCP mutation
    checkeq(ENGINE_SUCCESS,
            h1->dcp.mutation(h, cookie, opaque, "key", 3, data, dataLen, cas,
                           vbucket, flags, datatype,
                           bySeqno, revSeqno, exprtime,
                           lockTime, NULL, 0, 0),
            "Failed dcp mutate.");

    wait_for_stat_to_be(h, h1, "eq_dcpq:unittest:stream_0_buffer_items", 0,
                        "dcp");

    check(set_vbucket_state(h, h1, 0, vbucket_state_active),
          "Failed to set vbucket state.");

    check_key_value(h, h1, "key", data, dataLen);

    testHarness.destroy_cookie(cookie);
    free(data);

    return SUCCESS;
}

static enum test_result test_dcp_consumer_mutate_with_time_sync(
                                                        ENGINE_HANDLE *h,
                                                        ENGINE_HANDLE_V1 *h1) {

    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state.");

    set_drift_counter_state(h, h1, 1000, 0x01);

    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t seqno = 0;
    uint32_t flags = 0;
    const char *name = "unittest";
    uint16_t nname = strlen(name);

    // Open an DCP connection
    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie, opaque, seqno, flags, (void*)name, nname),
            "Failed dcp producer open connection.");

    std::string type = get_str_stat(h, h1, "eq_dcpq:unittest:type", "dcp");
    checkeq(0, type.compare("consumer"), "Consumer not found");

    opaque = add_stream_for_consumer(h, h1, cookie, opaque, 0, 0,
                                     PROTOCOL_BINARY_RESPONSE_SUCCESS);

    uint32_t dataLen = 100;
    char *data = static_cast<char *>(malloc(dataLen));
    memset(data, 'x', dataLen);

    uint8_t cas = 0x1;
    uint16_t vbucket = 0;
    uint8_t datatype = 1;
    uint64_t bySeqno = 10;
    uint64_t revSeqno = 0;
    uint32_t exprtime = 0;
    uint32_t lockTime = 0;

    checkeq(ENGINE_SUCCESS,
            h1->dcp.snapshot_marker(h, cookie, opaque, 0, 10, 10, 1),
            "Failed to send snapshot marker");

    // Consume a DCP mutation with extended meta
    int64_t adjusted_time1 = gethrtime() * 2;
    ExtendedMetaData *emd = new ExtendedMetaData(adjusted_time1, false);
    cb_assert(emd && emd->getStatus() == ENGINE_SUCCESS);
    std::pair<const char*, uint16_t> meta = emd->getExtMeta();
    checkeq(ENGINE_SUCCESS,
            h1->dcp.mutation(h, cookie, opaque, "key", 3, data, dataLen, cas,
                           vbucket, flags, datatype,
                           bySeqno, revSeqno, exprtime,
                           lockTime, meta.first, meta.second, 0),
            "Failed dcp mutate.");
    delete emd;

    wait_for_stat_to_be(h, h1, "eq_dcpq:unittest:stream_0_buffer_items", 0, "dcp");

    checkeq(ENGINE_SUCCESS,
            h1->dcp.close_stream(h, cookie, opaque, 0),
            "Expected success");

    check(set_vbucket_state(h, h1, 0, vbucket_state_active),
          "Failed to set vbucket state.");
    wait_for_flusher_to_settle(h, h1);

    check_key_value(h, h1, "key", data, dataLen);

    testHarness.destroy_cookie(cookie);
    free(data);

    protocol_binary_request_header *request;
    int64_t adjusted_time2;
    request = createPacket(PROTOCOL_BINARY_CMD_GET_ADJUSTED_TIME, 0, 0, NULL, 0,
                           NULL, 0, NULL, 0);
    h1->unknown_command(h, NULL, request, add_response);
    free(request);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
            "Expected Success");
    checkeq(last_body.size(), sizeof(int64_t),
            "Bodylen didn't match expected value");
    memcpy(&adjusted_time2, last_body.data(), last_body.size());
    adjusted_time2 = ntohll(adjusted_time2);

    /**
     * Check that adjusted_time2 is marginally greater than
     * adjusted_time1.
     */
    check(adjusted_time2 >= adjusted_time1,
            "Adjusted time after mutation: Not what is expected");

    return SUCCESS;
}

static enum test_result test_dcp_consumer_delete(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    // Store an item
    item *i = NULL;
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_ADD,"key", "value", &i),
            "Failed to fail to store an item.");
    h1->release(h, NULL, i);
    verify_curr_items(h, h1, 1, "one item stored");

    wait_for_flusher_to_settle(h, h1);

    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state.");

    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0;
    uint8_t cas = 0x1;
    uint16_t vbucket = 0;
    uint32_t flags = 0;
    uint64_t bySeqno = 10;
    uint64_t revSeqno = 0;
    const char *name = "unittest";
    uint16_t nname = strlen(name);
    uint32_t seqno = 0;

    // Open an DCP connection
    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie, opaque, seqno, flags, (void*)name, nname),
            "Failed dcp producer open connection.");

    std::string type = get_str_stat(h, h1, "eq_dcpq:unittest:type", "dcp");
    checkeq(0, type.compare("consumer"), "Consumer not found");

    opaque = add_stream_for_consumer(h, h1, cookie, opaque, 0, 0,
                                     PROTOCOL_BINARY_RESPONSE_SUCCESS);

    checkeq(ENGINE_SUCCESS,
            h1->dcp.snapshot_marker(h, cookie, opaque, 0, 10, 10, 1),
            "Failed to send snapshot marker");

    // verify that we don't accept invalid opaque id's
    checkeq(ENGINE_KEY_ENOENT,
            h1->dcp.deletion(h, cookie, opaque + 1, "key", 3, cas, vbucket,
                             bySeqno, revSeqno, NULL, 0),
            "Failed to detect invalid DCP opaque value.");

    // Consume an DCP deletion
    checkeq(ENGINE_SUCCESS,
            h1->dcp.deletion(h, cookie, opaque, "key", 3, cas, vbucket,
                             bySeqno, revSeqno, NULL, 0),
            "Failed dcp delete.");

    wait_for_stat_to_be(h, h1, "eq_dcpq:unittest:stream_0_buffer_items", 0,
                        "dcp");

    wait_for_stat_change(h, h1, "curr_items", 1);
    verify_curr_items(h, h1, 0, "one item deleted");
    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_consumer_delete_with_time_sync(
                                                        ENGINE_HANDLE *h,
                                                        ENGINE_HANDLE_V1 *h1) {

    set_drift_counter_state(h, h1, 1000, 0x01);

    // Store an item
    item *i = NULL;
    checkeq(ENGINE_SUCCESS,
            store(h, h1, NULL, OPERATION_ADD,"key", "value", &i),
            "Failed to fail to store an item.");
    h1->release(h, NULL, i);
    verify_curr_items(h, h1, 1, "one item stored");

    wait_for_flusher_to_settle(h, h1);

    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state.");

    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0;
    uint8_t cas = 0x1;
    uint16_t vbucket = 0;
    uint32_t flags = 0;
    uint64_t bySeqno = 10;
    uint64_t revSeqno = 0;
    const char *name = "unittest";
    uint16_t nname = strlen(name);
    uint32_t seqno = 0;

    // Open an DCP connection
    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie, opaque, seqno, flags, (void*)name, nname),
            "Failed dcp producer open connection.");

    std::string type = get_str_stat(h, h1, "eq_dcpq:unittest:type", "dcp");
    checkeq(0, type.compare("consumer"), "Consumer not found");

    opaque = add_stream_for_consumer(h, h1, cookie, opaque, 0, 0,
                                     PROTOCOL_BINARY_RESPONSE_SUCCESS);

    checkeq(ENGINE_SUCCESS,
            h1->dcp.snapshot_marker(h, cookie, opaque, 0, 10, 10, 1),
            "Failed to send snapshot marker");

    // Consume an DCP deletion
    int64_t adjusted_time1 = gethrtime() * 2;
    ExtendedMetaData *emd = new ExtendedMetaData(adjusted_time1, false);
    cb_assert(emd && emd->getStatus() == ENGINE_SUCCESS);
    std::pair<const char*, uint16_t> meta = emd->getExtMeta();
    checkeq(ENGINE_SUCCESS,
            h1->dcp.deletion(h, cookie, opaque, "key", 3, cas, vbucket,
                             bySeqno, revSeqno, meta.first, meta.second),
            "Failed dcp delete.");
    delete emd;

    wait_for_stat_to_be(h, h1, "eq_dcpq:unittest:stream_0_buffer_items", 0,
                        "dcp");

    wait_for_stat_change(h, h1, "curr_items", 1);
    verify_curr_items(h, h1, 0, "one item deleted");
    testHarness.destroy_cookie(cookie);

    protocol_binary_request_header *request;
    int64_t adjusted_time2;
    request = createPacket(PROTOCOL_BINARY_CMD_GET_ADJUSTED_TIME, 0, 0, NULL, 0,
                           NULL, 0, NULL, 0);
    h1->unknown_command(h, NULL, request, add_response);
    free(request);
    checkeq(PROTOCOL_BINARY_RESPONSE_SUCCESS, last_status.load(),
            "Expected Success");
    checkeq(sizeof(int64_t), last_body.size(),
            "Bodylen didn't match expected value");
    memcpy(&adjusted_time2, last_body.data(), last_body.size());
    adjusted_time2 = ntohll(adjusted_time2);

    /**
     * Check that adjusted_time2 is marginally greater than
     * adjusted_time1.
     */
    check(adjusted_time2 >= adjusted_time1,
            "Adjusted time after deletion: Not what is expected");

    return SUCCESS;
}


static enum test_result test_dcp_replica_stream_backfill(ENGINE_HANDLE *h,
                                                         ENGINE_HANDLE_V1 *h1)
{
    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state.");

    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t seqno = 0;
    uint32_t flags = 0;
    const int num_items = 100;
    const char *name = "unittest";
    uint16_t nname = strlen(name);

    /* Open an DCP consumer connection */
    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie, opaque, seqno, flags, (void*)name, nname),
            "Failed dcp producer open connection.");

    std::string type = get_str_stat(h, h1, "eq_dcpq:unittest:type", "dcp");
    checkeq(0, type.compare("consumer"), "Consumer not found");

    opaque = add_stream_for_consumer(h, h1, cookie, opaque, 0, 0,
                                     PROTOCOL_BINARY_RESPONSE_SUCCESS);

    /* Write backfill elements on to replica, flag (0x02) */
    dcp_stream_to_replica(h, h1, cookie, opaque, 0, 0x02, 1, num_items, 0,
                          num_items);

    /* Stream in mutations from replica */
    wait_for_flusher_to_settle(h, h1);
    wait_for_stat_to_be(h, h1, "vb_0:high_seqno", num_items,
                        "vbucket-seqno");
    uint64_t vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");

    const void *cookie1 = testHarness.create_cookie();
    dcp_stream(h, h1, "unittest1", cookie1, 0, 0, 0, num_items, vb_uuid, 0, 0,
               num_items, 0, 1, 0);

    testHarness.destroy_cookie(cookie1);
    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_dcp_replica_stream_in_memory(ENGINE_HANDLE *h,
                                                          ENGINE_HANDLE_V1 *h1)
{
    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state.");

    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t seqno = 0;
    uint32_t flags = 0;
    const int num_items = 100;
    const char *name = "unittest";
    uint16_t nname = strlen(name);

    /* Open an DCP consumer connection */
    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie, opaque, seqno, flags, (void*)name, nname),
            "Failed dcp producer open connection.");

    std::string type = get_str_stat(h, h1, "eq_dcpq:unittest:type", "dcp");
    checkeq(0, type.compare("consumer"), "Consumer not found");

    opaque = add_stream_for_consumer(h, h1, cookie, opaque, 0, 0,
                                     PROTOCOL_BINARY_RESPONSE_SUCCESS);

    /* Send DCP mutations with in memory flag (0x01) */
    dcp_stream_to_replica(h, h1, cookie, opaque, 0, 0x01, 1, num_items, 0,
                          num_items);

    /* Stream in memory mutations from replica */
    wait_for_flusher_to_settle(h, h1);
    wait_for_stat_to_be(h, h1, "vb_0:high_seqno", num_items,
                        "vbucket-seqno");
    uint64_t vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");

    const void *cookie1 = testHarness.create_cookie();
    dcp_stream(h, h1, "unittest1", cookie1, 0, 0, 0, num_items, vb_uuid, 0, 0,
               num_items, 0, 1, 0);

    testHarness.destroy_cookie(cookie1);
    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_dcp_replica_stream_all(ENGINE_HANDLE *h,
                                                    ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state.");

    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t seqno = 0;
    uint32_t flags = 0;
    const int num_items = 100;
    const char *name = "unittest";
    uint16_t nname = strlen(name);

    /* Open an DCP consumer connection */
    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie, opaque, seqno, flags, (void*)name, nname),
            "Failed dcp producer open connection.");

    std::string type = get_str_stat(h, h1, "eq_dcpq:unittest:type", "dcp");
    checkeq(0, type.compare("consumer"), "Consumer not found");

    opaque = add_stream_for_consumer(h, h1, cookie, opaque, 0, 0,
                                     PROTOCOL_BINARY_RESPONSE_SUCCESS);

    /* Send DCP mutations with in memory flag (0x01) */
    dcp_stream_to_replica(h, h1, cookie, opaque, 0, 0x01, 1, num_items, 0,
                          num_items);

    /* Send 100 more DCP mutations with checkpoint creation flag (0x04) */
    uint64_t start = num_items;
    dcp_stream_to_replica(h, h1, cookie, opaque, 0, 0x04, start + 1,
                          start + 100, start, start + 100);

    wait_for_flusher_to_settle(h, h1);
    stop_persistence(h, h1);
    checkeq(2 * num_items, get_int_stat(h, h1, "vb_replica_curr_items"),
            "wrong number of items in replica vbucket");

    /* Add 100 more items to the replica node on a new checkpoint */
    /* Send with flag (0x04) indicating checkpoint creation */
    start = 2 * num_items;
    dcp_stream_to_replica(h, h1, cookie, opaque, 0, 0x04, start + 1,
                          start + 100, start, start + 100);

    /* Disk backfill + in memory stream from replica */
    /* Wait for a checkpoint to be removed */
    wait_for_stat_to_be(h, h1, "vb_0:num_checkpoints", 2, "checkpoint");

    uint64_t end = get_int_stat(h, h1, "vb_0:high_seqno", "vbucket-seqno");
    uint64_t vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
    const void *cookie1 = testHarness.create_cookie();
    dcp_stream(h, h1, "unittest1", cookie1, 0, 0, 0, end, vb_uuid, 0, 0, 300, 0,
               1, 0);

    testHarness.destroy_cookie(cookie1);
    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_persistence_seqno(ENGINE_HANDLE *h,
                                                   ENGINE_HANDLE_V1 *h1) {
    const int  n_threads = 2;
    cb_thread_t threads[n_threads];
    struct handle_pair hp = {h, h1};

    for (int i = 0; i < n_threads; ++i) {
        int r = cb_create_thread(&threads[i], seqno_persistence_thread, &hp, 0);
        cb_assert(r == 0);
    }

    for (int i = 0; i < n_threads; ++i) {
        int r = cb_join_thread(threads[i]);
        cb_assert(r == 0);
    }

    wait_for_flusher_to_settle(h, h1);

    check(seqnoPersistence(h, h1, 0, 2000) == ENGINE_SUCCESS,
          "Expected success for seqno persistence request");

    return SUCCESS;
}

static enum test_result test_dcp_last_items_purged(ENGINE_HANDLE *h,
                                                   ENGINE_HANDLE_V1 *h1) {
    item_info info;
    mutation_descr_t mut_info;
    uint64_t vb_uuid = 0;
    uint64_t cas = 0;
    uint32_t high_seqno = 0;
    const int num_items = 3;
    char key[][3] = {"k1", "k2", "k3"};

    memset(&info, 0, sizeof(info));

    vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
    /* Set 3 items */
    for (int count = 0; count < num_items; count++){
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET, key[count], "somevalue", NULL,
                      0, 0, 0),
                "Error setting.");
    }

    memset(&mut_info, 0, sizeof(mut_info));

    /* Delete last 2 items */
    for (int count = 1; count < num_items; count++){
        checkeq(ENGINE_SUCCESS,
                h1->remove(h, NULL, key[count], strlen(key[count]), &cas, 0,
                           &mut_info),
                "Failed remove with value.");
        cas = 0;
    }

    high_seqno = get_ull_stat(h, h1, "vb_0:high_seqno", "vbucket-seqno");

    /* wait for flusher to settle */
    wait_for_flusher_to_settle(h, h1);

    /* Run compaction */
    compact_db(h, h1, 0, 2, high_seqno, 1);
    wait_for_stat_to_be(h, h1, "ep_pending_compactions", 0);
    check(get_int_stat(h, h1, "vb_0:purge_seqno", "vbucket-seqno") ==
            static_cast<int>(high_seqno - 1),
          "purge_seqno didn't match expected value");

    wait_for_stat_to_be(h, h1, "vb_0:open_checkpoint_id", 3, "checkpoint");
    wait_for_stat_to_be(h, h1, "vb_0:num_checkpoints", 1, "checkpoint");

    /* Create a DCP stream */
    const void *cookie = testHarness.create_cookie();
    high_seqno = get_ull_stat(h, h1, "vb_0:high_seqno", "vbucket-seqno");
    dcp_stream(h, h1, "unittest", cookie, 0, 0, 0, high_seqno, vb_uuid, 0, 0,
               1, 1, 1, 0, false, false, 0, true);

    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_dcp_rollback_after_purge(ENGINE_HANDLE *h,
                                                      ENGINE_HANDLE_V1 *h1) {
    item_info info;
    mutation_descr_t mut_info;
    uint64_t vb_uuid = 0;
    uint64_t cas = 0;
    uint32_t high_seqno = 0;
    const int num_items = 3;
    char key[][3] = {"k1", "k2", "k3"};

    memset(&info, 0, sizeof(info));

    vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");

    /* Set 3 items */
    for (int count = 0; count < num_items; count++){
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET, key[count], "somevalue", NULL,
                      0, 0, 0),
                "Error setting.");
    }
    high_seqno = get_ull_stat(h, h1, "vb_0:high_seqno", "vbucket-seqno");
    /* wait for flusher to settle */
    wait_for_flusher_to_settle(h, h1);

    /* Create a DCP stream to send 3 items to the replica */
    const void *cookie = testHarness.create_cookie();
    dcp_stream(h, h1, "unittest", cookie, 0, 0, 0, high_seqno, vb_uuid, 0, 0,
               3, 0, 1, 0, false, false, 0, true);

    testHarness.destroy_cookie(cookie);

    memset(&mut_info, 0, sizeof(mut_info));
    /* Delete last 2 items */
    for (int count = 1; count < num_items; count++){
        checkeq(ENGINE_SUCCESS,
                h1->remove(h, NULL, key[count], strlen(key[count]), &cas, 0,
                           &mut_info),
                "Failed remove with value.");
        cas = 0;
    }
    high_seqno = get_ull_stat(h, h1, "vb_0:high_seqno", "vbucket-seqno");
    /* wait for flusher to settle */
    wait_for_flusher_to_settle(h, h1);

    /* Run compaction */
    compact_db(h, h1, 0, 2, high_seqno, 1);
    wait_for_stat_to_be(h, h1, "ep_pending_compactions", 0);
    check(get_int_stat(h, h1, "vb_0:purge_seqno", "vbucket-seqno") ==
            static_cast<int>(high_seqno - 1),
          "purge_seqno didn't match expected value");

    wait_for_stat_to_be(h, h1, "vb_0:open_checkpoint_id", 3, "checkpoint");
    wait_for_stat_to_be(h, h1, "vb_0:num_checkpoints", 1, "checkpoint");

    /* DCP stream, expect a rollback to seq 0 */
    dcp_stream_req(h, h1, 1, 0, 3, high_seqno, vb_uuid,
                   3, high_seqno, 0, ENGINE_ROLLBACK);

    /* Do not expect rollback when you already have all items in the snapshot
       (that is, start == snap_end_seqno)*/
    dcp_stream_req(h, h1, 1, 0, high_seqno, high_seqno + 10, vb_uuid,
                   0, high_seqno, 0, ENGINE_SUCCESS);

    return SUCCESS;
}

static enum test_result test_dcp_erroneous_mutations(ENGINE_HANDLE *h,
                                                     ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state");
    wait_for_flusher_to_settle(h, h1);

    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    std::string name("err_mutations");

    checkeq(h1->dcp.open(h, cookie, opaque, 0, flags, (void*)(name.c_str()),
                         name.size()),
            ENGINE_SUCCESS,
            "Failed to open DCP consumer connection!");
    add_stream_for_consumer(h, h1, cookie, opaque++, 0, 0,
                            PROTOCOL_BINARY_RESPONSE_SUCCESS);

    std::string opaqueStr("eq_dcpq:" + name + ":stream_0_opaque");
    uint32_t  stream_opaque = get_int_stat(h, h1, opaqueStr.c_str(), "dcp");

    checkeq(h1->dcp.snapshot_marker(h, cookie, stream_opaque, 0, 5, 10, 300),
            ENGINE_SUCCESS,
            "Failed to send snapshot marker!");
    for (int i = 5; i <= 10; i++) {
        std::string key("key" + std::to_string(i));
        checkeq(h1->dcp.mutation(h, cookie, stream_opaque, key.c_str(),
                                 key.length(), "value", 5, i * 3, 0, 0, 0,
                                 i, 0, 0, 0, "", 0, INITIAL_NRU_VALUE),
                ENGINE_SUCCESS,
                "Unexpected return code for mutation!");
    }

    // Send a mutation and a deletion both out-of-sequence
    checkeq(h1->dcp.mutation(h, cookie, stream_opaque, "key", 3, "val", 3,
                             35, 0, 0, 0, 2, 0, 0, 0, "", 0,
                             INITIAL_NRU_VALUE),
            ENGINE_ERANGE,
            "Mutation should've returned ERANGE!");
    checkeq(h1->dcp.deletion(h, cookie, stream_opaque, "key5", 4, 40,
                             0, 3, 0, "", 0),
            ENGINE_ERANGE,
            "Deletion should've returned ERANGE!");

    std::string bufferItemsStr("eq_dcpq:" + name + ":stream_0_buffer_items");

    int buffered_items = get_int_stat(h, h1, bufferItemsStr.c_str(), "dcp");

    ENGINE_ERROR_CODE err = h1->dcp.mutation(h, cookie, stream_opaque,
                                             "key20", 5, "val", 3,
                                             45, 0, 0, 0, 20, 0, 0, 0, "",
                                             0, INITIAL_NRU_VALUE);

    if (buffered_items == 0) {
        checkeq(err, ENGINE_ERANGE, "Mutation shouldn't have been accepted!");
    } else {
        checkeq(err, ENGINE_SUCCESS, "Mutation should have been buffered!");
    }

    wait_for_stat_to_be(h, h1, bufferItemsStr.c_str(), 0, "dcp");

    checkeq(get_int_stat(h, h1, "vb_0:num_items", "vbucket-details 0"),
            6, "The last mutation should've been dropped!");

    checkeq(h1->dcp.close_stream(h, cookie, stream_opaque, 0),
            ENGINE_SUCCESS,
            "Expected to close stream!");
    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_erroneous_marker(ENGINE_HANDLE *h,
                                                  ENGINE_HANDLE_V1 *h1) {

    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state");
    wait_for_flusher_to_settle(h, h1);

    const void *cookie1 = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    std::string name("first_marker");

    checkeq(h1->dcp.open(h, cookie1, opaque, 0, flags, (void*)(name.c_str()),
                         name.size()),
            ENGINE_SUCCESS,
            "Failed to open DCP consumer connection!");
    add_stream_for_consumer(h, h1, cookie1, opaque++, 0, 0,
                            PROTOCOL_BINARY_RESPONSE_SUCCESS);

    std::string opaqueStr("eq_dcpq:" + name + ":stream_0_opaque");
    uint32_t stream_opaque = get_int_stat(h, h1, opaqueStr.c_str(), "dcp");

    checkeq(h1->dcp.snapshot_marker(h, cookie1, stream_opaque, 0, 1, 10, 300),
            ENGINE_SUCCESS,
            "Failed to send snapshot marker!");
    for (int i = 1; i <= 10; i++) {
        std::string key("key" + std::to_string(i));
        checkeq(h1->dcp.mutation(h, cookie1, stream_opaque, key.c_str(),
                                 key.length(), "value", 5, i * 3, 0, 0, 0,
                                 i, 0, 0, 0, "", 0, INITIAL_NRU_VALUE),
                ENGINE_SUCCESS,
                "Unexpected return code for mutation!");
    }

    std::string bufferItemsStr("eq_dcpq:" + name + ":stream_0_buffer_items");
    wait_for_stat_to_be(h, h1, bufferItemsStr.c_str(), 0, "dcp");

    checkeq(h1->dcp.close_stream(h, cookie1, stream_opaque, 0),
            ENGINE_SUCCESS,
            "Expected to close stream1!");
    testHarness.destroy_cookie(cookie1);

    const void *cookie2 = testHarness.create_cookie();
    opaque = 0xFFFFF000;
    name.assign("second_marker");

    checkeq(h1->dcp.open(h, cookie2, opaque, 0 ,flags, (void*)(name.c_str()),
                         name.size()),
            ENGINE_SUCCESS,
            "Failed to open DCP consumer connection!");
    add_stream_for_consumer(h, h1, cookie2, opaque++, 0, 0,
                            PROTOCOL_BINARY_RESPONSE_SUCCESS);

    opaqueStr.assign("eq_dcpq:" + name + ":stream_0_opaque");
    stream_opaque = get_int_stat(h , h1, opaqueStr.c_str(), "dcp");

    // Send a snapshot marker that would be rejected
    checkeq(h1->dcp.snapshot_marker(h, cookie2, stream_opaque, 0, 5, 10, 1),
            ENGINE_ERANGE,
            "Snapshot marker should have been dropped!");

    // Send a snapshot marker that would be accepted, but a few of
    // the mutations that are part of this snapshot will be dropped
    checkeq(h1->dcp.snapshot_marker(h, cookie2, stream_opaque, 0, 5, 15, 1),
            ENGINE_SUCCESS,
            "Failed to send snapshot marker!");
    for (int i = 5; i <= 15; i++) {
        std::string key("key_" + std::to_string(i));
        ENGINE_ERROR_CODE err = h1->dcp.mutation(h, cookie2, stream_opaque,
                                                 key.c_str(), key.length(),
                                                 "val", 3, i * 3, 0, 0, 0, i,
                                                 0, 0, 0, "", 0, INITIAL_NRU_VALUE);
        if (i <= 10) {
            checkeq(err, ENGINE_ERANGE, "Mutation should have been dropped!");
        } else {
            checkeq(err, ENGINE_SUCCESS, "Failed to send mutation!");
        }
    }

    checkeq(h1->dcp.close_stream(h, cookie2, stream_opaque, 0),
            ENGINE_SUCCESS,
            "Expected to close stream2!");
    testHarness.destroy_cookie(cookie2);

    return SUCCESS;
}

static enum test_result test_dcp_invalid_mutation_deletion(ENGINE_HANDLE* h,
                                                              ENGINE_HANDLE_V1* h1) {

    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state");
    wait_for_flusher_to_settle(h, h1);

    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    std::string name("err_mutations");

    checkeq(h1->dcp.open(h, cookie, opaque, 0, flags, (void*)(name.c_str()),
                         name.size()),
            ENGINE_SUCCESS,
            "Failed to open DCP consumer connection!");
    add_stream_for_consumer(h, h1, cookie, opaque++, 0, 0,
                            PROTOCOL_BINARY_RESPONSE_SUCCESS);

    std::string opaqueStr("eq_dcpq:" + name + ":stream_0_opaque");
    uint32_t  stream_opaque = get_int_stat(h, h1, opaqueStr.c_str(), "dcp");

    // Mutation(s) or deletion(s) with seqno 0 are invalid!
    std::string key("key");
    std::string val("value");
    checkeq(h1->dcp.mutation(h, cookie, stream_opaque, key.c_str(),
                             key.length(), val.c_str(), val.length(), 10, 0, 0,
                             0, /*seqno*/ 0, 0, 0, 0, "", 0, INITIAL_NRU_VALUE),
            ENGINE_EINVAL,
            "Mutation should have returned EINVAL!");

    checkeq(h1->dcp.deletion(h, cookie, stream_opaque, key.c_str(),
                             key.length(), 10, 0, /*seqno*/ 0, 0, "", 0),
            ENGINE_EINVAL,
            "Deletion should have returned EINVAL!");

    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_invalid_snapshot_marker(ENGINE_HANDLE* h,
                                                         ENGINE_HANDLE_V1* h1) {
    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state");
    wait_for_flusher_to_settle(h, h1);

    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    std::string name("unittest");

    checkeq(h1->dcp.open(h, cookie, opaque, 0, flags, (void*)(name.c_str()),
                         name.size()),
            ENGINE_SUCCESS,
            "Failed to open DCP consumer connection!");
    add_stream_for_consumer(h, h1, cookie, opaque++, 0, 0,
                            PROTOCOL_BINARY_RESPONSE_SUCCESS);

    std::string opaqueStr("eq_dcpq:" + name + ":stream_0_opaque");
    uint32_t stream_opaque = get_int_stat(h, h1, opaqueStr.c_str(), "dcp");

    checkeq(h1->dcp.snapshot_marker(h, cookie, stream_opaque, 0, 1, 10, 300),
            ENGINE_SUCCESS,
            "Failed to send snapshot marker!");
    for (int i = 1; i <= 10; i++) {
        std::string key("key" + std::to_string(i));
        checkeq(h1->dcp.mutation(h, cookie, stream_opaque, key.c_str(),
                                 key.length(), "value", 5, i * 3, 0, 0, 0,
                                 i, 0, 0, 0, "", 0, INITIAL_NRU_VALUE),
                ENGINE_SUCCESS,
                "Unexpected return code for mutation!");
    }

    std::string bufferItemsStr("eq_dcpq:" + name + ":stream_0_buffer_items");
    wait_for_stat_to_be(h, h1, bufferItemsStr.c_str(), 0, "dcp");

    // Invalid snapshot marker with end <= start
    checkeq(h1->dcp.snapshot_marker(h, cookie, stream_opaque, 0, 11, 8, 300),
            ENGINE_EINVAL,
            "Failed to send snapshot marker!");

    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

/*
 * Test that destroying a DCP producer before it ends
 * works. MB-16915 reveals itself via valgrind.
 */
static enum test_result test_dcp_early_termination(ENGINE_HANDLE* h,
                                                   ENGINE_HANDLE_V1* h1) {


    // create enough streams that some backfill tasks should overlap
    // with the connection deletion task.
    const int streams = 100;

    // 1 item so that we will at least allow backfill to be scheduled
    const int num_items = 1;
    uint64_t vbuuid[streams];
    for (int i = 0; i < streams; i++) {

        check(set_vbucket_state(h, h1, i, vbucket_state_active),
            "Failed to set vbucket state");
        std::stringstream statkey;
        statkey << "vb_" << i <<  ":0:id";
        vbuuid[i] = get_ull_stat(h, h1, statkey.str().c_str(), "failovers");

        /* Set n items */

        for (int count = 0; count < num_items; count++) {
            std::stringstream key;
            key << "KEY" << i << count;
            check(ENGINE_SUCCESS ==
                  store(h, h1, NULL, OPERATION_SET, key.str().c_str(),
                        "somevalue", NULL, 0, i, 0), "Error storing.");
        }
    }
    wait_for_flusher_to_settle(h, h1);

    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 1;
    check(h1->dcp.open(h, cookie, ++opaque, 0, DCP_OPEN_PRODUCER,
                       (void*)"unittest", strlen("unittest")) == ENGINE_SUCCESS,
          "Failed dcp producer open connection.");

    check(h1->dcp.control(h, cookie, ++opaque, "connection_buffer_size",
                          strlen("connection_buffer_size"),
                          "1024", 4) == ENGINE_SUCCESS,
          "Failed to establish connection buffer");

    std::unique_ptr<dcp_message_producers> producers(get_dcp_producers(h, h1));
    for (int i = 0; i < streams; i++) {
        uint64_t rollback = 0;
        check(h1->dcp.stream_req(h, cookie, DCP_ADD_STREAM_FLAG_DISKONLY,
                                 ++opaque, i, 0, num_items,
                                 vbuuid[i], 0, num_items, &rollback,
                                 mock_dcp_add_failover_log)
                    == ENGINE_SUCCESS,
              "Failed to initiate stream request");
        h1->dcp.step(h, cookie, producers.get());
    }

    // Destroy the connection
    testHarness.destroy_cookie(cookie);

    // Let all backfills finish
    wait_for_stat_to_be(h, h1, "ep_dcp_num_running_backfills", 0, "dcp");

    return SUCCESS;
}

static enum test_result test_failover_log_dcp(ENGINE_HANDLE *h,
                                              ENGINE_HANDLE_V1 *h1) {
    const int num_items = 50, num_testcases = 12;
    uint64_t end_seqno = num_items + 1000;
    uint32_t high_seqno = 0;

    for (int j = 0; j < num_items; ++j) {
        std::string key("key" + std::to_string(j));
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET, key.c_str(), "data", NULL),
                "Failed to store a value");
    }

    wait_for_flusher_to_settle(h, h1);
    wait_for_stat_to_be(h, h1, "curr_items", num_items);

    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, true);
    wait_for_warmup_complete(h, h1);

    wait_for_stat_to_be(h, h1, "curr_items", num_items);

    high_seqno = get_ull_stat(h, h1, "vb_0:high_seqno", "vbucket-seqno");
    uint64_t uuid = get_ull_stat(h, h1, "vb_0:1:id", "failovers");

    typedef struct dcp_params {
        uint64_t vb_uuid;
        uint64_t start_seqno;
        uint64_t snap_start_seqno;
        uint64_t snap_end_seqno;
        uint64_t exp_rollback;
        ENGINE_ERROR_CODE exp_err_code;
    } dcp_params_t;

    dcp_params_t params[num_testcases] =
    {   /* Do not expect rollback when start_seqno is 0 */
        {uuid, 0, 0, 0, 0, ENGINE_SUCCESS},
        /* Do not expect rollback when start_seqno is 0 and vb_uuid mismatch */
        {0xBAD, 0, 0, 0, 0, ENGINE_SUCCESS},
        /* Don't expect rollback when you already have all items in the snapshot
           (that is, start == snap_end) and upper >= snap_end */
        {uuid, high_seqno, 0, high_seqno, 0, ENGINE_SUCCESS},
        {uuid, high_seqno - 1, 0, high_seqno - 1, 0, ENGINE_SUCCESS},
        /* Do not expect rollback when you have no items in the snapshot
         (that is, start == snap_start) and upper >= snap_end */
        {uuid, high_seqno - 10, high_seqno - 10, high_seqno, 0, ENGINE_SUCCESS},
        {uuid, high_seqno - 10, high_seqno - 10, high_seqno - 1, 0,
         ENGINE_SUCCESS},
        /* Do not expect rollback when you are in middle of a snapshot (that is,
           snap_start < start < snap_end) and upper >= snap_end */
        {uuid, 10, 0, high_seqno, 0, ENGINE_SUCCESS},
        {uuid, 10, 0, high_seqno - 1, 0, ENGINE_SUCCESS},
        /* Expect rollback when you are in middle of a snapshot (that is,
           snap_start < start < snap_end) and upper < snap_end. Rollback to
           snap_start if snap_start < upper */
        {uuid, 20, 10, high_seqno + 1, 10, ENGINE_ROLLBACK},
        /* Expect rollback when upper < snap_start_seqno. Rollback to upper */
        {uuid, high_seqno + 20, high_seqno + 10, high_seqno + 30, high_seqno,
         ENGINE_ROLLBACK},
        {uuid, high_seqno + 10, high_seqno + 10, high_seqno + 10, high_seqno,
         ENGINE_ROLLBACK},
        /* vb_uuid not found in failover table, rollback to zero */
        {0xBAD, 10, 0, high_seqno, 0, ENGINE_ROLLBACK},
        /* Add new test case here */
    };

    for (int i = 0; i < num_testcases; i++)
    {
        dcp_stream_req(h, h1, 1, 0, params[i].start_seqno, end_seqno,
                       params[i].vb_uuid, params[i].snap_start_seqno,
                       params[i].snap_end_seqno, params[i].exp_rollback,
                       params[i].exp_err_code);
    }
    return SUCCESS;
}

static enum test_result test_mb16357(ENGINE_HANDLE *h,
                                     ENGINE_HANDLE_V1 *h1) {

    // Load up vb0 with n items, expire in 1 second
    int num_items = 1000;

    for (int j = 0; j < num_items; ++j) {
        item *i = NULL;
        std::stringstream ss;
        ss << "key-" << j;
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET,
                      ss.str().c_str(), "data", &i, 0, 0, 1/*expire*/, 0),
                "Failed to store a value"); //expire in 1 second

        h1->release(h, NULL, i);
    }

    wait_for_flusher_to_settle(h, h1);
    testHarness.time_travel(3617); // force expiry pushing time forward.

    struct mb16357_ctx ctx(h, h1, num_items);
    cb_thread_t cp_thread, dcp_thread;

    cb_assert(cb_create_thread(&cp_thread,
                               compact_thread_func,
                               &ctx, 0) == 0);
    cb_assert(cb_create_thread(&dcp_thread,
                               dcp_thread_func,
                               &ctx, 0) == 0);

    cb_assert(cb_join_thread(cp_thread) == 0);
    cb_assert(cb_join_thread(dcp_thread) == 0);

    return SUCCESS;
}

// Check that an incoming DCP mutation which has an invalid CAS is fixed up
// by the engine.
static enum test_result test_mb17517_cas_minus_1_dcp(ENGINE_HANDLE *h,
                                                     ENGINE_HANDLE_V1 *h1) {
    // Attempt to insert a item with CAS of -1 via DCP.
    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    std::string name = "test_mb17517_cas_minus_1";

    // Switch vb 0 to replica (to accept DCP mutaitons).
    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state to replica.");

    // Open consumer connection
    checkeq(h1->dcp.open(h, cookie, opaque, 0, flags, (void*)name.c_str(),
                         name.size()),
            ENGINE_SUCCESS, "Failed DCP Consumer open connection.");

    add_stream_for_consumer(h, h1, cookie, opaque++, 0, 0,
                            PROTOCOL_BINARY_RESPONSE_SUCCESS);

    uint32_t stream_opaque = get_int_stat(h, h1,
                                          ("eq_dcpq:" + name + ":stream_0_opaque").c_str(),
                                          "dcp");

    h1->dcp.snapshot_marker(h, cookie,  stream_opaque, /*vbid*/0,
                            /*start*/0, /*end*/3, /*flags*/2);

    // Create two items via a DCP mutation.
    const std::string prefix{"bad_CAS_DCP"};
    std::string value{"value"};
    for (unsigned int ii = 0; ii < 2; ii++) {
        std::string key{prefix + std::to_string(ii)};
        checkeq(ENGINE_SUCCESS,
                h1->dcp.mutation(h, cookie, stream_opaque, key.c_str(), key.size(),
                                 value.c_str(), value.size(), /*cas*/-1,
                                 /*vbucket*/0,
                                 /*flags*/0, PROTOCOL_BINARY_RAW_BYTES,
                                 /*by_seqno*/ii + 1, /*rev_seqno*/1,
                                 /*expiration*/0, /*lock_time*/0,
                                 /*meta*/nullptr, /*nmeta*/0, INITIAL_NRU_VALUE),
                                 "Expected DCP mutation with CAS:-1 to succeed");
    }

    // Ensure we have processed the mutations.
    wait_for_stat_to_be(h, h1, "vb_replica_curr_items", 2);

    // Delete one of them (to allow us to test DCP deletion).
    std::string delete_key{prefix + "0"};
    checkeq(ENGINE_SUCCESS,
            h1->dcp.deletion(h, cookie, stream_opaque, delete_key.c_str(),
                             delete_key.size(), /*cas*/-1, /*vbucket*/0,
                             /*by_seqno*/3, /*rev_seqno*/2,
                             /*meta*/nullptr, /*nmeta*/0),
                             "Expected DCP deletion with CAS:-1 to succeed");

    // Ensure we have processed the deletion.
    wait_for_stat_to_be(h, h1, "vb_replica_curr_items", 1);

    // Flip vBucket to active so we can access the documents in it.
    check(set_vbucket_state(h, h1, 0, vbucket_state_active),
          "Failed to set vbucket state to active.");

    // Check that a valid CAS was regenerated for the (non-deleted) mutation.
    std::string key{prefix + "1"};
    auto cas = get_CAS(h, h1, key);
    checkne(~uint64_t(0), cas, "CAS via get() is still -1");

    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

// Check that an incoming TAP mutation which has an invalid CAS is fixed up
// by the engine.
static enum test_result test_mb17517_cas_minus_1_tap(ENGINE_HANDLE *h,
                                                     ENGINE_HANDLE_V1 *h1) {
    const uint16_t vbucket = 0;
    // Need a replica vBucket to send mutations into.
    check(set_vbucket_state(h, h1, vbucket, vbucket_state_replica),
          "Failed to set vbucket state.");

    char eng_specific[9];
    memset(eng_specific, 0, sizeof(eng_specific));

    // Create two items via TAP.
    std::string prefix{"bad_CAS_TAP"};
    std::string value{"value"};
    for (unsigned int ii = 0; ii < 2; ii++) {
        std::string key{prefix + std::to_string(ii)};
        checkeq(ENGINE_SUCCESS,
                h1->tap_notify(h, NULL, eng_specific, sizeof(eng_specific),
                               /*TTL*/1, /*tap_flags*/0, TAP_MUTATION,
                               /*tap_seqno*/ii + 1,
                               key.c_str(), key.size(), /*flags*/0, /*exptime*/0,
                               /*CAS*/-1, PROTOCOL_BINARY_RAW_BYTES,
                           value.c_str(), value.size(), vbucket),
            "Expected tap_notify to succeed.");
    }

    // Ensure we have processed the mutations.
    wait_for_stat_to_be(h, h1, "vb_replica_curr_items", 2);

    // Delete one of the items.
    std::string delete_key{prefix + "0"};
    checkeq(ENGINE_SUCCESS,
            h1->tap_notify(h, NULL, eng_specific, sizeof(eng_specific),
                           /*TTL*/1, /*tap_flags*/0, TAP_DELETION,
                           /*tap_seqno*/2, delete_key.c_str(),
                           delete_key.size(), /*flags*/0, /*exptime*/0,
                           /*CAS*/-1, PROTOCOL_BINARY_RAW_BYTES,
                       value.c_str(), value.size(), vbucket),
        "Expected tap_notify to succeed.");

    // Ensure we have processed the deletion.
    wait_for_stat_to_be(h, h1, "vb_replica_curr_items", 1);

    // Flip vBucket to active so we can access the documents in it.
    check(set_vbucket_state(h, h1, 0, vbucket_state_active),
          "Failed to set vbucket state to active.");

    // Check that a valid CAS was regenerated for the (non-deleted) mutation.
    std::string key{prefix + "1"};
    auto cas = get_CAS(h, h1, key);
    checkne(~uint64_t(0), cas, "CAS via get() is still -1");

    return SUCCESS;
}


// Test manifest //////////////////////////////////////////////////////////////

const char *default_dbname = "./ep_testsuite_dcp";

BaseTestCase testsuite_testcases[] = {
        TestCase("test dcp vbtakeover stat no stream",
                 test_dcp_vbtakeover_no_stream, test_setup, teardown, nullptr,
                 prepare, cleanup),
        TestCase("test dcp notifier", test_dcp_notifier, test_setup, teardown,
                 nullptr, prepare, cleanup),
        TestCase("test open consumer", test_dcp_consumer_open,
                 test_setup, teardown, nullptr, prepare, cleanup),
        TestCase("test dcp consumer flow control none",
                 test_dcp_consumer_flow_control_none,
                 test_setup, teardown, "dcp_flow_control_policy=none",
                 prepare, cleanup),
        TestCase("test dcp consumer flow control static",
                 test_dcp_consumer_flow_control_static,
                 test_setup, teardown, "dcp_flow_control_policy=static",
                 prepare, cleanup),
        TestCase("test dcp consumer flow control dynamic",
                 test_dcp_consumer_flow_control_dynamic,
                 test_setup, teardown, "dcp_flow_control_policy=dynamic",
                 prepare, cleanup),
        TestCase("test dcp consumer flow control aggressive",
                 test_dcp_consumer_flow_control_aggressive,
                 test_setup, teardown, "dcp_flow_control_policy=aggressive",
                 prepare, cleanup),
        TestCase("test open producer", test_dcp_producer_open,
                 test_setup, teardown, nullptr, prepare, cleanup),
        TestCase("test dcp noop", test_dcp_noop, test_setup, teardown, nullptr,
                 prepare, cleanup),
        TestCase("test dcp noop failure", test_dcp_noop_fail, test_setup,
                 teardown, nullptr, prepare, cleanup),
        TestCase("test dcp consumer noop", test_dcp_consumer_noop, test_setup,
                 teardown, nullptr, prepare, cleanup),
        TestCase("test dcp replica stream backfill",
                 test_dcp_replica_stream_backfill, test_setup, teardown,
                 "chk_remover_stime=1;max_checkpoints=2", prepare, cleanup),
        TestCase("test dcp replica stream in-memory",
                 test_dcp_replica_stream_in_memory, test_setup, teardown,
                 "chk_remover_stime=1;max_checkpoints=2", prepare, cleanup),
        TestCase("test dcp replica stream all", test_dcp_replica_stream_all,
                 test_setup, teardown, "chk_remover_stime=1;max_checkpoints=2",
                 prepare, cleanup),
        TestCase("test producer stream request (partial)",
                 test_dcp_producer_stream_req_partial, test_setup, teardown,
                 "chk_remover_stime=1;chk_max_items=100", prepare, cleanup),
        TestCase("test producer stream request with time sync (partial)",
                 test_dcp_producer_stream_req_partial_with_time_sync,
                 test_setup, teardown,
                 "chk_remover_stime=1;chk_max_items=100", prepare, cleanup),
        TestCase("test producer stream request (full)",
                 test_dcp_producer_stream_req_full, test_setup, teardown,
                 "chk_remover_stime=1;chk_max_items=100", prepare, cleanup),
        TestCase("test producer stream request (disk)",
                 test_dcp_producer_stream_req_disk, test_setup, teardown,
                 "chk_remover_stime=1;chk_max_items=100", prepare, cleanup),
        TestCase("test producer stream request (disk only)",
                 test_dcp_producer_stream_req_diskonly, test_setup, teardown,
                 "chk_remover_stime=1;chk_max_items=100", prepare, cleanup),
        TestCase("test producer stream request (memory only)",
                 test_dcp_producer_stream_req_mem, test_setup, teardown,
                 "chk_remover_stime=1;chk_max_items=100", prepare, cleanup),
        TestCase("test producer stream request (DGM)",
                 test_dcp_producer_stream_req_dgm, test_setup, teardown,
                 "chk_remover_stime=1;max_size=6291456", prepare, cleanup),
        TestCase("test producer stream request (latest flag)",
                 test_dcp_producer_stream_latest, test_setup, teardown, NULL,
                 prepare, cleanup),
        TestCase("test producer stream request nmvb",
                 test_dcp_producer_stream_req_nmvb, test_setup, teardown, NULL,
                 prepare, cleanup),
        TestCase("test dcp agg stats",
                 test_dcp_agg_stats, test_setup, teardown, "chk_max_items=100",
                 prepare, cleanup),
        TestCase("test dcp cursor dropping",
                 test_dcp_cursor_dropping, test_setup, teardown,
                 "cursor_dropping_lower_mark=60;cursor_dropping_upper_mark=70;"
                 "chk_remover_stime=1;max_size=26214400", prepare, cleanup),
        TestCase("test dcp value compression",
                 test_dcp_value_compression, test_setup, teardown,
                 "dcp_value_compression_enabled=true",
                 prepare, cleanup),
        TestCase("test producer stream request backfill no value",
                 test_dcp_producer_stream_backfill_no_value, test_setup,
                 teardown, "chk_remover_stime=1;max_size=6291456", prepare,
                 cleanup),
        TestCase("test producer stream request mem no value",
                 test_dcp_producer_stream_mem_no_value, test_setup, teardown,
                 "chk_remover_stime=1;max_size=6291456", prepare, cleanup),
        TestCase("test dcp stream takeover", test_dcp_takeover, test_setup,
                teardown, "chk_remover_stime=1", prepare, cleanup),
        TestCase("test dcp stream takeover no items", test_dcp_takeover_no_items,
                 test_setup, teardown, "chk_remover_stime=1", prepare, cleanup),
        TestCase("test dcp consumer takeover", test_dcp_consumer_takeover,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("test failover scenario one with dcp",
                 test_failover_scenario_one_with_dcp, test_setup, teardown,
                 NULL, prepare, cleanup),
        TestCase("test failover scenario two with dcp",
                 test_failover_scenario_two_with_dcp, test_setup, teardown,
                 NULL, prepare, cleanup),
        TestCase("test add stream", test_dcp_add_stream, test_setup, teardown,
                 "dcp_enable_noop=false", prepare, cleanup),
        TestCase("test consumer backoff stat", test_consumer_backoff_stat,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("test dcp reconnect full snapshot", test_dcp_reconnect_full,
                 test_setup, teardown, "dcp_enable_noop=false", prepare,
                 cleanup),
        TestCase("test reconnect partial snapshot", test_dcp_reconnect_partial,
                 test_setup, teardown, "dcp_enable_noop=false", prepare,
                 cleanup),
        TestCase("test crash full snapshot", test_dcp_crash_reconnect_full,
                 test_setup, teardown, "dcp_enable_noop=false", prepare,
                 cleanup),
        TestCase("test crash partial snapshot",
                 test_dcp_crash_reconnect_partial, test_setup, teardown,
                 "dcp_enable_noop=false", prepare, cleanup),
        TestCase("test rollback to zero on consumer", test_rollback_to_zero,
                test_setup, teardown, "dcp_enable_noop=false", prepare,
                cleanup),
        TestCase("test chk manager rollback", test_chk_manager_rollback,
                test_setup, teardown,
                 "dcp_flow_control_policy=none;dcp_enable_noop=false", prepare,
                cleanup),
        TestCase("test full rollback on consumer", test_fullrollback_for_consumer,
                test_setup, teardown,
                "dcp_enable_noop=false", prepare,
                cleanup),
        TestCase("test partial rollback on consumer",
                test_partialrollback_for_consumer, test_setup, teardown,
                "dcp_enable_noop=false", prepare, cleanup),
        TestCase("test change dcp buffer log size", test_dcp_buffer_log_size,
                test_setup, teardown, NULL, prepare, cleanup),
        TestCase("test dcp producer flow control",
                 test_dcp_producer_flow_control, test_setup, teardown, NULL,
                 prepare, cleanup),
        TestCase("test get failover log", test_dcp_get_failover_log,
                test_setup, teardown, NULL, prepare, cleanup),
        TestCase("test add stream exists", test_dcp_add_stream_exists,
                 test_setup, teardown, "dcp_enable_noop=false", prepare,
                 cleanup),
        TestCase("test add stream nmvb", test_dcp_add_stream_nmvb, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("test add stream prod exists", test_dcp_add_stream_prod_exists,
                 test_setup, teardown, "dcp_enable_noop=false", prepare,
                 cleanup),
        TestCase("test add stream prod nmvb", test_dcp_add_stream_prod_nmvb,
                 test_setup, teardown, "dcp_enable_noop=false", prepare,
                 cleanup),
        TestCase("test close stream (no stream)",
                 test_dcp_close_stream_no_stream, test_setup, teardown, NULL,
                 prepare, cleanup),
        TestCase("test close stream", test_dcp_close_stream,
                 test_setup, teardown, "dcp_enable_noop=false", prepare,
                 cleanup),
        TestCase("test dcp consumer end stream", test_dcp_consumer_end_stream,
                 test_setup, teardown, "dcp_enable_noop=false", prepare,
                 cleanup),
        TestCase("dcp consumer mutate", test_dcp_consumer_mutate, test_setup,
                 teardown, "dcp_enable_noop=false", prepare, cleanup),
        TestCase("dcp consumer mutate with time sync",
                 test_dcp_consumer_mutate_with_time_sync, test_setup,
                 teardown, "dcp_enable_noop=false", prepare, cleanup),
        TestCase("dcp consumer delete", test_dcp_consumer_delete, test_setup,
                 teardown, "dcp_enable_noop=false", prepare, cleanup),
        TestCase("dcp consumer delete with time sync",
                 test_dcp_consumer_delete_with_time_sync, test_setup,
                 teardown, "dcp_enable_noop=false", prepare, cleanup),
        TestCase("dcp failover log", test_failover_log_dcp, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("dcp persistence seqno", test_dcp_persistence_seqno, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("dcp last items purged", test_dcp_last_items_purged, test_setup,
                 teardown, NULL, prepare, cleanup),
        TestCase("dcp rollback after purge", test_dcp_rollback_after_purge,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("dcp erroneous mutations scenario", test_dcp_erroneous_mutations,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("dcp erroneous snapshot marker scenario", test_dcp_erroneous_marker,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("dcp invalid mutation(s)/deletion(s)",
                 test_dcp_invalid_mutation_deletion,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("dcp invalid snapshot marker",
                 test_dcp_invalid_snapshot_marker,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("test MB-16357", test_mb16357,
                 test_setup, teardown, "compaction_exp_mem_threshold=85",
                 prepare, cleanup),
        TestCase("test dcp early termination", test_dcp_early_termination,
                 test_setup, teardown, NULL, prepare, cleanup),

        TestCase("test MB-17517 CAS -1 DCP", test_mb17517_cas_minus_1_dcp,
                 test_setup, teardown, NULL, prepare, cleanup),

        TestCase("test MB-17517 CAS -1 TAP", test_mb17517_cas_minus_1_tap,
                 test_setup, teardown, NULL, prepare, cleanup),

        TestCase(NULL, NULL, NULL, NULL, NULL, prepare, cleanup)
};
