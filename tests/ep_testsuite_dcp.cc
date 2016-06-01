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
#include "programs/engine_testapp/mock_server.h"

// Helper functions ///////////////////////////////////////////////////////////

void dcp_step(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const void* cookie) {
    std::unique_ptr<dcp_message_producers> producers = get_dcp_producers(h, h1);
    ENGINE_ERROR_CODE err = h1->dcp.step(h, cookie, producers.get());
    check(err == ENGINE_SUCCESS || err == ENGINE_WANT_MORE,
            "Expected success or engine_want_more");
    if (err == ENGINE_SUCCESS) {
        clear_dcp_data();
    }
}

/* This is a flag that indicates the continuous dcp thread to come out of
   loop that keeps calling dcp->step().
   To be set only by the (parent) thread spawning the continuous_dcp_thread.
   To be checked by continuous_dcp_thread.
   (This approach seems safer than calling pthread_cancel()) */
static std::atomic<bool> stop_continuous_dcp_thread(false);

struct SeqnoRange {
    uint64_t start;
    uint64_t end;
};

class DcpStreamCtx {
/**
 * This class represents all attributes required for
 * a stream. Objects of this class type are to be fed
 * to TestDcpConsumer.
 */
public:
    DcpStreamCtx()
        : vbucket(0),
          flags(0),
          vb_uuid(0),
          exp_mutations(0),
          exp_deletions(0),
          exp_markers(0),
          extra_takeover_ops(0),
          exp_disk_snapshot(false),
          time_sync_enabled(false),
          exp_conflict_res(0),
          skip_estimate_check(false),
          live_frontend_client(false),
          skip_verification(false),
          exp_err(ENGINE_SUCCESS),
          exp_rollback(0)
    {
        seqno = {0, static_cast<uint64_t>(~0)};
        snapshot = {0, static_cast<uint64_t>(~0)};
    }

    /* Vbucket Id */
    uint16_t vbucket;
    /* Stream flags */
    uint32_t flags;
    /* Vbucket UUID */
    uint64_t vb_uuid;
    /* Sequence number range */
    SeqnoRange seqno;
    /* Snapshot range */
    SeqnoRange snapshot;
    /* Number of mutations expected (for verification) */
    size_t exp_mutations;
    /* Number of deletions expected (for verification) */
    size_t exp_deletions;
    /* Number of snapshot markers expected (for verification) */
    size_t exp_markers;
    /* Extra front end mutations as part of takeover */
    size_t extra_takeover_ops;
    /* Flag - expect disk snapshot or not */
    bool exp_disk_snapshot;
    /* Flag - indicating time sync status */
    bool time_sync_enabled;
    /* Expected conflict resolution flag */
    uint8_t exp_conflict_res;
    /* Skip estimate check during takeover */
    bool skip_estimate_check;
    /*
       live_frontend_client to be set to true when streaming is done in parallel
       with a client issuing writes to the vbucket. In this scenario, predicting
       the number of snapshot markers received is difficult.
    */
    bool live_frontend_client;
    /*
       skip_verification to be set to true if verification of mutation count,
       deletion count, marker count etc. is to be skipped at the end of
       streaming.
     */
    bool skip_verification;
    /* Expected error code on stream creation. We need this because rollback is
       a valid operation and returns ENGINE_ROLLBACK (not ENGINE_SUCCESS) */
    ENGINE_ERROR_CODE exp_err;
    /* Expected rollback seqno */
    uint64_t exp_rollback;
};

class TestDcpConsumer {
/**
 * This class represents a DcpConsumer which is responsible
 * for spawning a DcpProducer at the server and receiving
 * messages from it.
 */
public:
    TestDcpConsumer(const std::string &_name, const void *_cookie)
        : name(_name),
          cookie(_cookie),
          opaque(0),
          total_bytes(0),
          simulate_cursor_dropping(false),
          flow_control_buf_size(1024),
          disable_ack(false) { }

    uint64_t getTotalBytes() {
        return total_bytes;
    }

    void simulateCursorDropping() {
        simulate_cursor_dropping = true;
    }

    void setFlowControlBufSize(uint64_t to) {
        flow_control_buf_size = to;
    }

    void disableAcking() {
        disable_ack = true;
    }

    void addStreamCtx(DcpStreamCtx &ctx) {
        stream_ctxs.push_back(ctx);
    }

    void run(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1);

    /* This method just opens a DCP connection. Note it does not open a stream
       and does not call the dcp step function to get all the items from the
       producer */
    void openConnection(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1);

    /* This method opens a stream on an existing DCP connection.
       This does not call the dcp step function to get all the items from the
       producer */
    ENGINE_ERROR_CODE openStreams(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1);

private:
    /* Vbucket-level stream stats used in test */
    struct VBStats {
        VBStats()
            : num_mutations(0),
              num_deletions(0),
              num_snapshot_markers(0),
              num_set_vbucket_pending(0),
              num_set_vbucket_active(0),
              pending_marker_ack(false),
              marker_end(0),
              last_by_seqno(0),
              extra_takeover_ops(0),
              exp_disk_snapshot(false),
              time_sync_enabled(false),
              exp_conflict_res(0) { }

        size_t num_mutations;
        size_t num_deletions;
        size_t num_snapshot_markers;
        size_t num_set_vbucket_pending;
        size_t num_set_vbucket_active;
        bool pending_marker_ack;
        uint64_t marker_end;
        uint64_t last_by_seqno;
        size_t extra_takeover_ops;
        bool exp_disk_snapshot;
        bool time_sync_enabled;
        uint8_t exp_conflict_res;
    };

    /* Connection name */
    const std::string name;
    /* Connection cookie */
    const void *cookie;
    /* Vector containing information of streams */
    std::vector<DcpStreamCtx> stream_ctxs;
    /* Opaque value in the connection */
    uint32_t opaque;
    /* Total bytes received */
    uint64_t total_bytes;
    /* Flag to simulate cursor dropping */
    bool simulate_cursor_dropping;
    /* Flow control buffer size */
    uint64_t flow_control_buf_size;
    /* Flag to disable acking */
    bool disable_ack;
    /* map of vbstats */
    std::map<uint16_t, VBStats> vb_stats;

};

void TestDcpConsumer::run(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(stream_ctxs.size() >= 1, "No dcp_stream arguments provided!");

    /* Open the connection with the DCP producer */
    openConnection(h, h1);

    /* Open streams in the above open connection */
    openStreams(h, h1);

    std::unique_ptr<dcp_message_producers> producers(get_dcp_producers(h, h1));

    bool done = false;
    ExtendedMetaData *emd = NULL;
    bool exp_all_items_streamed = true;
    size_t num_stream_ends_received = 0;
    uint32_t bytes_read = 0;
    uint64_t all_bytes = 0;
    uint64_t total_acked_bytes = 0;
    uint64_t ack_limit = flow_control_buf_size / 2;

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
            h1->dcp.buffer_acknowledgement(h, cookie, ++opaque, 0, bytes_read);
            total_acked_bytes += bytes_read;
            bytes_read = 0;
        }
        ENGINE_ERROR_CODE err = h1->dcp.step(h, cookie, producers.get());
        if ((err == ENGINE_DISCONNECT) ||
            (stop_continuous_dcp_thread.load(std::memory_order_relaxed))) {
            done = true;
        } else {
            const uint16_t vbid = dcp_last_vbucket;
            auto &stats = vb_stats[vbid];
            switch (dcp_last_op) {
                case PROTOCOL_BINARY_CMD_DCP_MUTATION:
                    cb_assert(vbid != static_cast<uint16_t>(-1));
                    check(stats.last_by_seqno < dcp_last_byseqno,
                          "Expected bigger seqno");
                    stats.last_by_seqno = dcp_last_byseqno;
                    stats.num_mutations++;
                    bytes_read += dcp_last_packet_size;
                    all_bytes += dcp_last_packet_size;
                    if (stats.pending_marker_ack &&
                        dcp_last_byseqno == stats.marker_end) {
                        sendDcpAck(h, h1, cookie,
                                   PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER,
                                   PROTOCOL_BINARY_RESPONSE_SUCCESS,
                                   dcp_last_opaque);
                    }
                    if (stats.time_sync_enabled) {
                        checkeq(static_cast<size_t>(16), dcp_last_meta.size(),
                                "Expected extended meta in mutation packet");
                    } else {
                        checkeq(static_cast<size_t>(5), dcp_last_meta.size(),
                                "Expected no extended metadata");
                    }

                    emd = new ExtendedMetaData(dcp_last_meta.c_str(),
                                               dcp_last_meta.size());
                    checkeq(stats.exp_conflict_res,
                            emd->getConflictResMode(),
                            "Unexpected conflict resolution mode");
                    delete emd;
                    break;
                case PROTOCOL_BINARY_CMD_DCP_DELETION:
                    cb_assert(vbid != static_cast<uint16_t>(-1));
                    check(stats.last_by_seqno < dcp_last_byseqno,
                          "Expected bigger seqno");
                    stats.last_by_seqno = dcp_last_byseqno;
                    stats.num_deletions++;
                    bytes_read += dcp_last_packet_size;
                    all_bytes += dcp_last_packet_size;
                    if (stats.pending_marker_ack &&
                        dcp_last_byseqno == stats.marker_end) {
                        sendDcpAck(h, h1, cookie,
                                   PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER,
                                   PROTOCOL_BINARY_RESPONSE_SUCCESS,
                                   dcp_last_opaque);
                    }
                    if (stats.time_sync_enabled) {
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
                    checkeq(stats.exp_conflict_res,
                            emd->getConflictResMode(),
                            "Unexpected conflict resolution mode");
                    delete emd;
                    break;
                case PROTOCOL_BINARY_CMD_DCP_STREAM_END:
                    cb_assert(vbid != static_cast<uint16_t>(-1));
                    if (++num_stream_ends_received == stream_ctxs.size()) {
                        done = true;
                    }
                    bytes_read += dcp_last_packet_size;
                    all_bytes += dcp_last_packet_size;
                    break;
                case PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER:
                    cb_assert(vbid != static_cast<uint16_t>(-1));
                    if (stats.exp_disk_snapshot &&
                        stats.num_snapshot_markers == 0) {
                        checkeq(static_cast<uint32_t>(1),
                                dcp_last_flags, "Expected disk snapshot");
                    }

                    if (dcp_last_flags & 8) {
                        stats.pending_marker_ack = true;
                        stats.marker_end = dcp_last_snap_end_seqno;
                    }

                    stats.num_snapshot_markers++;
                    bytes_read += dcp_last_packet_size;
                    all_bytes += dcp_last_packet_size;
                    break;
                case PROTOCOL_BINARY_CMD_DCP_SET_VBUCKET_STATE:
                    cb_assert(vbid != static_cast<uint16_t>(-1));
                    if (dcp_last_vbucket_state == vbucket_state_pending) {
                        stats.num_set_vbucket_pending++;
                        for (size_t j = 0; j < stats.extra_takeover_ops; ++j) {
                            item *i = NULL;
                            std::string key("key" + std::to_string(j));
                            checkeq(ENGINE_SUCCESS,
                                    store(h, h1, NULL, OPERATION_SET,
                                          key.c_str(), "data", &i, 0, vbid),
                                    "Failed to store a value");
                            h1->release(h, NULL, i);
                        }
                    } else if (dcp_last_vbucket_state == vbucket_state_active) {
                        stats.num_set_vbucket_active++;
                    }
                    bytes_read += dcp_last_packet_size;
                    all_bytes += dcp_last_packet_size;
                    sendDcpAck(h, h1, cookie,
                               PROTOCOL_BINARY_CMD_DCP_SET_VBUCKET_STATE,
                               PROTOCOL_BINARY_RESPONSE_SUCCESS,
                               dcp_last_opaque);
                    break;
                case 0:
                    if (disable_ack && flow_control_buf_size &&
                        (bytes_read >= flow_control_buf_size)) {
                        /* If there is no acking and if flow control is enabled
                           we are done because producer should not send us any
                           more items. We need this to test that producer stops
                           sending items correctly when there are no acks while
                           flow control is enabled */
                        done = true;
                        exp_all_items_streamed = false;
                    } else {
                        /* No messages were ready on the last step call, so we
                         * wait till the conn is notified of new item.
                         * Note that we check for 0 because we clear the
                         * dcp_last_op value below.
                         */
                        testHarness.lock_cookie(cookie);
                        /* waitfor_cookie() waits on a condition variable. But
                           the api expects the cookie to be locked before
                           calling it */
                        testHarness.waitfor_cookie(cookie);
                        testHarness.unlock_cookie(cookie);
                    }
                    break;
                default:
                    // Aborting ...
                    std::stringstream ss;
                    ss << "Unknown DCP operation: " << dcp_last_op;
                    check(false, ss.str().c_str());
            }
            dcp_last_op = 0;
            dcp_last_nru = 0;
            dcp_last_vbucket = -1;
        }
    } while (!done);

    total_bytes += all_bytes;

    for (const auto& ctx : stream_ctxs) {
        if (!ctx.skip_verification) {
            auto &stats = vb_stats[ctx.vbucket];
            if (simulate_cursor_dropping) {
                if (stats.num_snapshot_markers == 0) {
                    cb_assert(stats.num_mutations == 0 &&
                              stats.num_deletions == 0);
                } else {
                    check(stats.num_mutations <= ctx.exp_mutations,
                          "Invalid number of mutations");
                    check(stats.num_deletions <= ctx.exp_deletions,
                          "Invalid number of deletes");
                }
            } else {
                // Account for cursors that may have been dropped because
                // of high memory usage
                if (get_int_stat(h, h1, "ep_cursors_dropped") > 0) {
                    // Hard to predict exact number of markers to be received
                    // if in case of a live parallel front end load
                    if (!ctx.live_frontend_client) {
                        check(stats.num_snapshot_markers <= ctx.exp_markers,
                              "Invalid number of markers");
                    }
                    check(stats.num_mutations <= ctx.exp_mutations,
                          "Invalid number of mutations");
                    check(stats.num_deletions <= ctx.exp_deletions,
                          "Invalid number of deletions");
                } else {
                    checkeq(ctx.exp_mutations, stats.num_mutations,
                            "Invalid number of mutations");
                    checkeq(ctx.exp_deletions, stats.num_deletions,
                            "Invalid number of deletes");
                    if (ctx.live_frontend_client) {
                        // Hard to predict exact number of markers to be received
                        // if in case of a live parallel front end load
                        if (ctx.exp_mutations > 0 || ctx.exp_deletions > 0) {
                            check(stats.num_snapshot_markers >= 1,
                                  "Snapshot marker count can't be zero");
                        }
                    } else {
                        checkeq(ctx.exp_markers, stats.num_snapshot_markers,
                                "Unexpected number of snapshot markers");
                    }
                }
            }

            if (ctx.flags & DCP_ADD_STREAM_FLAG_TAKEOVER) {
                checkeq(static_cast<size_t>(1), stats.num_set_vbucket_pending,
                        "Didn't receive pending set state");
                checkeq(static_cast<size_t>(1), stats.num_set_vbucket_active,
                        "Didn't receive active set state");
            }

            /* Check if the readyQ size goes to zero after all items are streamed */
            if (exp_all_items_streamed) {
                std::stringstream stats_ready_queue_memory;
                stats_ready_queue_memory << "eq_dcpq:" << name.c_str()
                                         << ":stream_" << ctx.vbucket
                                         << "_ready_queue_memory";
                checkeq(static_cast<uint64_t>(0),
                        get_ull_stat(h, h1, stats_ready_queue_memory.str().c_str(), "dcp"),
                        "readyQ size did not go to zero");
            }
        }
    }

    /* Check if the producer has updated flow control stat correctly */
    if (flow_control_buf_size) {
        char stats_buffer[50] = {0};
        snprintf(stats_buffer, sizeof(stats_buffer), "eq_dcpq:%s:unacked_bytes",
                 name.c_str());
        checkeq((all_bytes - total_acked_bytes),
                get_ull_stat(h, h1, stats_buffer, "dcp"),
                "Buffer Size did not get set correctly");
    }
}

void TestDcpConsumer::openConnection(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    /* Reset any stale dcp data */
    clear_dcp_data();

    opaque = 1;

    /* Set up Producer at server */
    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie, ++opaque, 0, DCP_OPEN_PRODUCER,
                         (void*)name.c_str(), name.length()),
            "Failed dcp producer open connection.");

    /* Set flow control buffer size */
    std::string flow_control_buf_sz(std::to_string(flow_control_buf_size));
    checkeq(ENGINE_SUCCESS,
            h1->dcp.control(h, cookie, ++opaque, "connection_buffer_size", 22,
                            flow_control_buf_sz.c_str(),
                            flow_control_buf_sz.length()),
            "Failed to establish connection buffer");
    char stats_buffer[50] = {0};
    if (flow_control_buf_size) {
        snprintf(stats_buffer, sizeof(stats_buffer),
                 "eq_dcpq:%s:max_buffer_bytes", name.c_str());
        checkeq(static_cast<int>(flow_control_buf_size),
                get_int_stat(h, h1, stats_buffer, "dcp"),
                "Buffer Size did not get set correctly");
    } else {
        snprintf(stats_buffer, sizeof(stats_buffer),
                 "eq_dcpq:%s:flow_control", name.c_str());
        std::string status = get_str_stat(h, h1, stats_buffer, "dcp");
        checkeq(status, std::string("disabled"), "Flow control enabled!");
    }

    checkeq(ENGINE_SUCCESS,
            h1->dcp.control(h, cookie, ++opaque, "enable_ext_metadata", 19,
                            "true", 4),
            "Failed to enable xdcr extras");
}

ENGINE_ERROR_CODE TestDcpConsumer::openStreams(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    for (auto& ctx : stream_ctxs) {
        /* Different opaque for every stream created */
        ++opaque;

        /* Initiate stream request */
        uint64_t rollback = 0;
        ENGINE_ERROR_CODE rv = h1->dcp.stream_req(h, cookie, ctx.flags, opaque,
                                                  ctx.vbucket, ctx.seqno.start,
                                                  ctx.seqno.end, ctx.vb_uuid,
                                                  ctx.snapshot.start,
                                                  ctx.snapshot.end,
                                                  &rollback,
                                                  mock_dcp_add_failover_log);

        checkeq(ctx.exp_err, rv, "Failed to initiate stream request");

        if (rv == ENGINE_NOT_MY_VBUCKET) {
            return rv;
        }

        if (rv == ENGINE_ROLLBACK || rv == ENGINE_KEY_ENOENT) {
            checkeq(ctx.exp_rollback, rollback,
                    "Rollback didn't match expected value");
            return rv;
        }

        if (ctx.flags & DCP_ADD_STREAM_FLAG_TAKEOVER) {
            ctx.seqno.end  = std::numeric_limits<uint64_t>::max();
        } else if (ctx.flags & DCP_ADD_STREAM_FLAG_LATEST ||
                   ctx.flags & DCP_ADD_STREAM_FLAG_DISKONLY) {
            std::string high_seqno("vb_" + std::to_string(ctx.vbucket) + ":high_seqno");
            ctx.seqno.end = get_ull_stat(h, h1, high_seqno.c_str(), "vbucket-seqno");
        }

        std::stringstream stats_flags;
        stats_flags << "eq_dcpq:" << name.c_str() << ":stream_"
        << ctx.vbucket << "_flags";
        checkeq(ctx.flags,
                (uint32_t)get_int_stat(h, h1, stats_flags.str().c_str(), "dcp"),
                "Flags didn't match");

        std::stringstream stats_opaque;
        stats_opaque << "eq_dcpq:" << name.c_str() << ":stream_"
        << ctx.vbucket << "_opaque";
        checkeq(opaque,
                (uint32_t)get_int_stat(h, h1, stats_opaque.str().c_str(), "dcp"),
                "Opaque didn't match");

        std::stringstream stats_start_seqno;
        stats_start_seqno << "eq_dcpq:" << name.c_str() << ":stream_"
        << ctx.vbucket << "_start_seqno";
        checkeq(ctx.seqno.start,
                (uint64_t)get_ull_stat(h, h1, stats_start_seqno.str().c_str(), "dcp"),
                "Start Seqno Didn't match");

        std::stringstream stats_end_seqno;
        stats_end_seqno << "eq_dcpq:" << name.c_str() << ":stream_"
        << ctx.vbucket << "_end_seqno";
        checkeq(ctx.seqno.end,
                (uint64_t)get_ull_stat(h, h1, stats_end_seqno.str().c_str(), "dcp"),
                "End Seqno didn't match");

        std::stringstream stats_vb_uuid;
        stats_vb_uuid << "eq_dcpq:" << name.c_str() << ":stream_"
        << ctx.vbucket << "_vb_uuid";
        checkeq(ctx.vb_uuid,
                (uint64_t)get_ull_stat(h, h1, stats_vb_uuid.str().c_str(), "dcp"),
                "VBucket UUID didn't match");

        std::stringstream stats_snap_seqno;
        stats_snap_seqno << "eq_dcpq:" << name.c_str() << ":stream_"
        << ctx.vbucket << "_snap_start_seqno";
        checkeq(ctx.snapshot.start,
                (uint64_t)get_ull_stat(h, h1, stats_snap_seqno.str().c_str(), "dcp"),
                "snap start seqno didn't match");

        if ((ctx.flags & DCP_ADD_STREAM_FLAG_TAKEOVER) &&
            !ctx.skip_estimate_check) {
            std::string high_seqno_str("vb_" + std::to_string(ctx.vbucket) +
                                       ":high_seqno");
            uint64_t vb_high_seqno = get_ull_stat(h, h1, high_seqno_str.c_str(),
                                                  "vbucket-seqno");
            uint64_t est = vb_high_seqno - ctx.seqno.start;
            std::stringstream stats_takeover;
            stats_takeover << "dcp-vbtakeover " << ctx.vbucket
            << " " << name.c_str();
            wait_for_stat_to_be_lte(h, h1, "estimate", static_cast<int>(est),
                                    stats_takeover.str().c_str());
        }

        if (ctx.flags & DCP_ADD_STREAM_FLAG_DISKONLY) {
            /* Wait for backfill to start */
            std::string stats_backfill_read_bytes("eq_dcpq:" + name +
                                                  ":backfill_buffer_bytes_read");
            wait_for_stat_to_be_gte(h, h1, stats_backfill_read_bytes.c_str(), 0,
                                    "dcp");
            /* Verify that we have no dcp cursors in the checkpoint. (There will
             just be one persistence cursor) */
            std::string stats_num_conn_cursors("vb_" +
                                               std::to_string(ctx.vbucket) +
                                               ":num_conn_cursors");
            checkeq(1, get_int_stat(h, h1, stats_num_conn_cursors.c_str(),
                                    "checkpoint"),
                    "DCP cursors not expected to be registered");
        }

        // Init stats used in test
        VBStats stats;
        stats.extra_takeover_ops = ctx.extra_takeover_ops;
        stats.exp_disk_snapshot = ctx.exp_disk_snapshot;
        stats.time_sync_enabled = ctx.time_sync_enabled;
        stats.exp_conflict_res = ctx.exp_conflict_res;

        vb_stats[ctx.vbucket] = stats;
    }
    return ENGINE_SUCCESS;
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

struct writer_thread_ctx {
    ENGINE_HANDLE *h;
    ENGINE_HANDLE_V1 *h1;
    int items;
    uint16_t vbid;
};

struct continuous_dcp_ctx {
    ENGINE_HANDLE *h;
    ENGINE_HANDLE_V1 *h1;
    const void *cookie;
    uint16_t vbid;
    const std::string &name;
    uint64_t start_seqno;
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

    static void writer_thread(void *args) {
        struct writer_thread_ctx *wtc = static_cast<writer_thread_ctx *>(args);

        for (int i = 0; i < wtc->items; ++i) {
            std::string key("key_" + std::to_string(i));
            item *itm;
            checkeq(ENGINE_SUCCESS,
                    store(wtc->h, wtc->h1, nullptr, OPERATION_SET,
                          key.c_str(), "somevalue", &itm, 0, wtc->vbid),
                    "Failed to store value");
            wtc->h1->release(wtc->h, nullptr, itm);
        }
    }

    static void continuous_dcp_thread(void *args) {
        struct continuous_dcp_ctx *cdc = static_cast<continuous_dcp_ctx *>(args);

        DcpStreamCtx ctx;
        ctx.vbucket = cdc->vbid;
        std::string vbuuid_entry("vb_" + std::to_string(cdc->vbid) + ":0:id");
        ctx.vb_uuid = get_ull_stat(cdc->h, cdc->h1,
                                   vbuuid_entry.c_str(), "failovers");
        ctx.seqno = {cdc->start_seqno, std::numeric_limits<uint64_t>::max()};
        ctx.snapshot = {cdc->start_seqno, cdc->start_seqno};
        ctx.skip_verification = true;

        TestDcpConsumer tdc(cdc->name, cdc->cookie);
        tdc.addStreamCtx(ctx);
        tdc.run(cdc->h, cdc->h1);
    }
}

// Testcases //////////////////////////////////////////////////////////////////

static enum test_result test_dcp_vbtakeover_no_stream(ENGINE_HANDLE *h,
                                                      ENGINE_HANDLE_V1 *h1) {
    write_items(h, h1, 10);
    const auto est = get_int_stat(h, h1, "estimate", "dcp-vbtakeover 0");
    checkeq(10, est, "Invalid estimate for non-existent stream");
    checkeq(ENGINE_NOT_MY_VBUCKET,
            h1->get_stats(h, nullptr, "dcp-vbtakeover 1",
                          strlen("dcp-vbtakeover 1"), add_stats),
            "Expected not my vbucket");

    return SUCCESS;
}

/*
 * The following test is similar to the test_dcp_consumer_open test and
 * test_dcp_producer_open test, in that it opens a connections, then
 * immediately closes it.
 * It then moves time forward and repeats the creation of a connection and
 * checks that the new connection was created after the previous connection.
 */

static enum test_result test_dcp_notifier_open(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const auto *cookie1 = testHarness.create_cookie();
    const std::string name("unittest");
    const uint32_t seqno = 0;
    uint32_t opaque = 0;
    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie1, opaque, seqno, DCP_OPEN_NOTIFIER,
                         (void*)name.c_str(), name.size()),
            "Failed dcp consumer open connection.");

    const std::string stat_type("eq_dcpq:" + name + ":type");
    auto type = get_str_stat(h, h1, stat_type.c_str(), "dcp");
    const std::string stat_created("eq_dcpq:" + name + ":created");
    const auto created = get_int_stat(h, h1, stat_created.c_str(), "dcp");
    checkeq(0, type.compare("notifier"), "Notifier not found");
    testHarness.destroy_cookie(cookie1);

    testHarness.time_travel(600);

    const auto *cookie2 = testHarness.create_cookie();
    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie2, opaque, seqno, DCP_OPEN_NOTIFIER,
                         (void*)name.c_str(), name.size()),
            "Failed dcp consumer open connection.");

    type = get_str_stat(h, h1, stat_type.c_str(), "dcp");
    checkeq(0, type.compare("notifier"), "Notifier not found");
    check(get_int_stat(h, h1, stat_created.c_str(), "dcp") >= created + 600,
          "New dcp stream is not newer");
    testHarness.destroy_cookie(cookie2);

    return SUCCESS;
}

static enum test_result test_dcp_notifier(ENGINE_HANDLE *h,
                                          ENGINE_HANDLE_V1 *h1) {
    write_items(h, h1, 10);
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

/*
 * The following test is similar to the previous test
 * (test_dcp_notifier) , whereby a notifier connection is opened and
 * notifier_requests are made checking for the occurance of stream
 * end commands.
 *
 * In the following test we make a notifier_request equal to
 * the number of operations performed (in this case one).  The test is
 * to ensure that a stream end is not received.  A second operation is
 * then performed and this time we check for the occurance of a
 * stream end command.
 */

static enum test_result test_dcp_notifier_equal_to_number_of_items(
                        ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *ii = nullptr;
    const std::string key("key0");
    checkeq(ENGINE_SUCCESS,
            store(h, h1, nullptr, OPERATION_SET, key.c_str(), "data", &ii),
            "Failed to store a value");
    h1->release(h, nullptr, ii);
    const auto *cookie = testHarness.create_cookie();
    const std::string name("unittest");
    const uint32_t seqno = 0;
    const uint16_t vbucket = 0;
    const uint64_t start = 1;
    uint32_t opaque = 0;
    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie, opaque, seqno, DCP_OPEN_NOTIFIER,
                         (void*)name.c_str(), name.size()),
            "Failed dcp notifier open connection.");
    // Should not get a stream end
    notifier_request(h, h1, cookie, ++opaque, vbucket, start, true);
    dcp_step(h, h1, cookie);
    check(dcp_last_op != PROTOCOL_BINARY_CMD_DCP_STREAM_END,
          "Wasn't expecting a stream end");
    checkeq(ENGINE_SUCCESS,
            store(h, h1, nullptr, OPERATION_SET, "key0", "data", &ii),
            "Failed to store a value");
    h1->release(h, nullptr, ii);
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

static enum test_result test_dcp_producer_open_same_cookie(ENGINE_HANDLE *h,
                                                           ENGINE_HANDLE_V1 *h1) {
    const auto *cookie = testHarness.create_cookie();
    const std::string name("unittest");
    uint32_t opaque = 0;
    const uint32_t seqno = 0;
    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie, opaque, seqno, DCP_OPEN_PRODUCER,
                         (void*)name.c_str(), name.size()),
            "Failed dcp producer open connection.");

    const auto stat_type("eq_dcpq:" + name + ":type");
    auto type = get_str_stat(h, h1, stat_type.c_str(), "dcp");
    checkeq(0, type.compare("producer"), "Producer not found");
    /*
     * Number of references is 2 (as opposed to 1) because a
     * mock_connstuct is initialised to having 1 reference
     * to represent a client being connected to it.
     */
    checkeq(2, testHarness.get_number_of_mock_cookie_references(cookie),
            "Number of cookie references is not two");
    /*
     * engine_data needs to be reset so that it passes the check that
     * a connection does not already exist on the same socket.
     */
    testHarness.store_engine_specific(cookie, nullptr);

    checkeq(ENGINE_DISCONNECT,
            h1->dcp.open(h, cookie, opaque++, seqno, DCP_OPEN_PRODUCER,
                         (void*)name.c_str(), name.size()),
            "Failed to return ENGINE_DISCONNECT");

    checkeq(2, testHarness.get_number_of_mock_cookie_references(cookie),
            "Number of cookie references is not two");

    testHarness.destroy_cookie(cookie);

    checkeq(1, testHarness.get_number_of_mock_cookie_references(cookie),
            "Number of cookie references is not one");

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
    const int num_items = 200;
    write_items(h, h1, num_items);

    wait_for_flusher_to_settle(h, h1);
    stop_persistence(h, h1);

    // Ensure all 200 items are removed from the checkpoint queues
    // to avoid any de-duplication with the delete ops that follow
    wait_for_stat_to_be(h, h1, "ep_items_rm_from_checkpoints", 200);

    for (int j = 0; j < (num_items / 2); ++j) {
        std::stringstream ss;
        ss << "key" << j;
        checkeq(ENGINE_SUCCESS,
                del(h, h1, ss.str().c_str(), 0, 0),
                "Expected delete to succeed");
    }

    wait_for_stat_to_be(h, h1, "vb_0:num_checkpoints", 2, "checkpoint");

    const void *cookie = testHarness.create_cookie();

    DcpStreamCtx ctx;
    ctx.vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
    ctx.seqno = {95, 209};
    ctx.snapshot = {95, 95};
    // Note that more than the expected number of items (mutations +
    // deletions) will be sent, because of current design.
    ctx.exp_mutations = 105;
    ctx.exp_deletions = 100;
    ctx.exp_markers = 2;

    TestDcpConsumer tdc("unittest", cookie);
    tdc.addStreamCtx(ctx);
    tdc.run(h, h1);

    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_producer_stream_req_partial_with_time_sync(
                                                             ENGINE_HANDLE *h,
                                                             ENGINE_HANDLE_V1 *h1) {
    /*
     * temporarily skip this testcase to prevent CV regr run failures
     * till fix for it will be implemented and committed (MB-18669)
     */
    return SKIPPED;

    set_drift_counter_state(h, h1, /* initial drift */1000);

    const int num_items = 200;
    write_items(h, h1, num_items);

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

    const void *cookie = testHarness.create_cookie();

    DcpStreamCtx ctx;
    ctx.vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
    ctx.seqno = {95, 209};
    ctx.snapshot = {95, 95};
    ctx.exp_mutations = 105;
    ctx.exp_deletions = 100;
    ctx.exp_markers = 2;
    ctx.time_sync_enabled = true;
    ctx.exp_conflict_res = 1;

    TestDcpConsumer tdc("unittest", cookie);
    tdc.addStreamCtx(ctx);
    tdc.run(h, h1);

    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_producer_stream_req_full(ENGINE_HANDLE *h,
                                                          ENGINE_HANDLE_V1 *h1) {
    const int num_items = 300, batch_items = 100;
    for (int start_seqno = 0; start_seqno < num_items;
         start_seqno += batch_items) {
        wait_for_flusher_to_settle(h, h1);
        write_items(h, h1, batch_items, start_seqno);
    }

    wait_for_flusher_to_settle(h, h1);
    verify_curr_items(h, h1, num_items, "Wrong amount of items");
    wait_for_stat_to_be(h, h1, "vb_0:num_checkpoints", 1, "checkpoint");

    const void *cookie = testHarness.create_cookie();

    DcpStreamCtx ctx;
    ctx.vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
    ctx.seqno = {0, get_ull_stat(h, h1, "vb_0:high_seqno", "vbucket-seqno")};
    ctx.exp_mutations = num_items;
    ctx.exp_markers = 1;

    TestDcpConsumer tdc("unittest", cookie);
    tdc.addStreamCtx(ctx);
    tdc.run(h, h1);

    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_producer_stream_req_disk(ENGINE_HANDLE *h,
                                                          ENGINE_HANDLE_V1 *h1) {
    const int num_items = 400, batch_items = 200;
    for (int start_seqno = 0; start_seqno < num_items;
         start_seqno += batch_items) {
        if (200 == start_seqno) {
            wait_for_flusher_to_settle(h, h1);
            wait_for_stat_to_be(h, h1, "ep_items_rm_from_checkpoints", 200);
            stop_persistence(h, h1);
        }
        write_items(h, h1, batch_items, start_seqno);
    }

    verify_curr_items(h, h1, num_items, "Wrong amount of items");
    wait_for_stat_to_be_gte(h, h1, "vb_0:num_checkpoints", 2, "checkpoint");

    const void *cookie = testHarness.create_cookie();

    DcpStreamCtx ctx;
    ctx.vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
    ctx.seqno = {0, 200};
    ctx.exp_mutations = 200;
    ctx.exp_markers = 1;

    TestDcpConsumer tdc("unittest", cookie);
    tdc.addStreamCtx(ctx);
    tdc.run(h, h1);

    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_producer_stream_req_diskonly(ENGINE_HANDLE *h,
                                                              ENGINE_HANDLE_V1 *h1) {
    const int num_items = 300, batch_items = 100;
    for (int start_seqno = 0; start_seqno < num_items;
         start_seqno += batch_items) {
        wait_for_flusher_to_settle(h, h1);
        write_items(h, h1, batch_items, start_seqno);
    }

    wait_for_flusher_to_settle(h, h1);
    verify_curr_items(h, h1, num_items, "Wrong amount of items");
    wait_for_stat_to_be(h, h1, "vb_0:num_checkpoints", 1, "checkpoint");

    const void *cookie = testHarness.create_cookie();

    DcpStreamCtx ctx;
    ctx.flags = DCP_ADD_STREAM_FLAG_DISKONLY;
    ctx.vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
    ctx.seqno = {0, static_cast<uint64_t>(-1)};
    ctx.exp_mutations = 300;
    ctx.exp_markers = 1;

    TestDcpConsumer tdc("unittest", cookie);
    tdc.addStreamCtx(ctx);
    tdc.run(h, h1);

    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_producer_backfill_limits(ENGINE_HANDLE *h,
                                                          ENGINE_HANDLE_V1 *h1)
{
    const int num_items = 3;
    write_items(h, h1, num_items);

    wait_for_flusher_to_settle(h, h1);
    verify_curr_items(h, h1, num_items, "Wrong amount of items");
    wait_for_stat_to_be(h, h1, "vb_0:num_checkpoints", 1, "checkpoint");

    const void *cookie = testHarness.create_cookie();

    DcpStreamCtx ctx;
    ctx.flags = DCP_ADD_STREAM_FLAG_DISKONLY;
    ctx.vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
    ctx.seqno = {0, static_cast<uint64_t>(-1)};
    ctx.exp_mutations = 3;
    ctx.exp_markers = 1;

    TestDcpConsumer tdc("unittest", cookie);
    tdc.addStreamCtx(ctx);
    tdc.run(h, h1);

    /* Backfill task runs are expected as below:
       once for backfill_state_init + once for backfill_state_completing +
       once for backfill_state_done + once post all backfills are run finished.
       Here since we have dcp_scan_byte_limit = 100, we expect the backfill task
       to run additional 'num_items' during backfill_state_scanning state. */
    uint64_t exp_backfill_task_runs = 4 + num_items;
    checkeq(exp_backfill_task_runs,
            get_histo_stat(h, h1, "backfill_tasks", "runtimes",
                           Histo_stat_info::TOTAL_COUNT),
            "backfill_tasks did not run expected number of times");

    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_producer_stream_req_mem(ENGINE_HANDLE *h,
                                                         ENGINE_HANDLE_V1 *h1) {
    const int num_items = 300, batch_items = 100;
    for (int start_seqno = 0; start_seqno < num_items;
         start_seqno += batch_items) {
        wait_for_flusher_to_settle(h, h1);
        write_items(h, h1, batch_items, start_seqno);
    }

    wait_for_flusher_to_settle(h, h1);
    verify_curr_items(h, h1, num_items, "Wrong amount of items");

    const void *cookie = testHarness.create_cookie();

    DcpStreamCtx ctx;
    ctx.vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
    ctx.seqno = {200, 300};
    ctx.snapshot = {200, 200};
    ctx.exp_mutations = 100;
    ctx.exp_markers = 1;

    TestDcpConsumer tdc("unittest", cookie);
    tdc.addStreamCtx(ctx);
    tdc.run(h, h1);

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

    // Sanity check - ensure we have enough vBucket quota (max_size)
    // such that we have 1000 items - enough to give us 0.1%
    // granuarity in any residency calculations. */
    if (i < 1000) {
        std::cerr << "Error: test_dcp_producer_stream_backfill_no_value: "
            "expected at least 1000 items after filling vbucket, "
            "but only have " << i << ". "
            "Check max_size setting for test." << std::endl;
        return FAIL;
    }

    wait_for_flusher_to_settle(h, h1);
    verify_curr_items(h, h1, i, "Wrong number of items");
    int num_non_resident = get_int_stat(h, h1, "vb_active_num_non_resident");
    cb_assert(num_non_resident >= ((float)(50/100) * i));

    // Reduce max_size from 2,000,000 to 1,800,000
    set_param(h, h1, protocol_binary_engine_param_flush, "max_size", "1800000");
    cb_assert(get_int_stat(h, h1, "vb_active_perc_mem_resident") < 50);

    const void *cookie = testHarness.create_cookie();

    DcpStreamCtx ctx;
    ctx.vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
    ctx.seqno = {0, get_ull_stat(h, h1, "vb_0:high_seqno", "vbucket-seqno")};
    ctx.exp_mutations = i;
    ctx.exp_markers = 1;

    TestDcpConsumer tdc("unittest", cookie);
    tdc.addStreamCtx(ctx);
    tdc.run(h, h1);

    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_producer_stream_latest(ENGINE_HANDLE *h,
                                                        ENGINE_HANDLE_V1 *h1) {
    const int num_items = 300, batch_items = 100;
    for (int start_seqno = 0; start_seqno < num_items;
         start_seqno += batch_items) {
        wait_for_flusher_to_settle(h, h1);
        write_items(h, h1, batch_items, start_seqno);
    }

    wait_for_flusher_to_settle(h, h1);
    verify_curr_items(h, h1, num_items, "Wrong amount of items");


    const void *cookie = testHarness.create_cookie();

    DcpStreamCtx ctx;
    ctx.flags = DCP_ADD_STREAM_FLAG_LATEST;
    ctx.vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
    ctx.seqno = {200, 205};
    ctx.snapshot = {200, 200};
    ctx.exp_mutations = 100;
    ctx.exp_markers = 1;

    TestDcpConsumer tdc("unittest", cookie);
    tdc.addStreamCtx(ctx);
    tdc.run(h, h1);

    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_producer_keep_stream_open(ENGINE_HANDLE *h,
                                                           ENGINE_HANDLE_V1 *h1)
{
    const std::string conn_name("unittest");
    const int num_items = 2, vb = 0;

    write_items(h, h1, num_items);

    wait_for_flusher_to_settle(h, h1);
    verify_curr_items(h, h1, num_items, "Wrong amount of items");

    const void *cookie = testHarness.create_cookie();

    /* We want to stream items till end and keep the stream open. Then we want
       to verify the stream is still open */
    stop_continuous_dcp_thread.store(false, std::memory_order_relaxed);
    struct continuous_dcp_ctx cdc = {h, h1, cookie, 0, conn_name.c_str(), 0};
    cb_thread_t dcp_thread;
    cb_assert(cb_create_thread(&dcp_thread, continuous_dcp_thread, &cdc, 0)
              == 0);

    /* Wait for producer to be created */
    wait_for_stat_to_be(h, h1, "ep_dcp_producer_count", 1, "dcp");

    /* Wait for an active stream to be created */
    const std::string stat_stream_count("eq_dcpq:" + conn_name +
                                        ":num_streams");
    wait_for_stat_to_be(h, h1, stat_stream_count.c_str(), 1, "dcp");

    /* Wait for the dcp stream to send upto highest seqno we have */
    std::string stat_stream_last_sent_seqno("eq_dcpq:" + conn_name + ":stream_"
                                            + std::to_string(vb) +
                                            "_last_sent_seqno");
    wait_for_stat_to_be(h, h1, stat_stream_last_sent_seqno.c_str(), num_items,
                        "dcp");

    /* Check if the stream is still open after sending out latest items */
    std::string stat_stream_state("eq_dcpq:" + conn_name + ":stream_" +
                             std::to_string(vb) + "_state");
    std::string state = get_str_stat(h, h1, stat_stream_state.c_str(), "dcp");
    checkeq(state.compare("in-memory"), 0, "Stream is not open");

    /* Before closing the connection stop the thread that continuously polls
       for dcp data */
    stop_continuous_dcp_thread.store(true, std::memory_order_relaxed);
    testHarness.notify_io_complete(cookie, ENGINE_SUCCESS);
    cb_assert(cb_join_thread(dcp_thread) == 0);
    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_producer_keep_stream_open_replica(
                                                        ENGINE_HANDLE *h,
                                                        ENGINE_HANDLE_V1 *h1)
{
    /* This test case validates if a replica vbucket correctly sends items
       and snapshot end seqno when a stream requests for items till end of time
       (end_seqno in req is 2^64 - 1).
       The test has 2 parts.
       (i) Set up replica vbucket such that it has items to be streamed from
           backfill and memory.
       (ii) Open a stream (in a DCP conn) and see if all the items are received
            correctly */

    /* Part (i):  Set up replica vbucket such that it has items to be streamed
                  from backfill and memory. */
    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state.");

    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t seqno = 0;
    uint32_t flags = 0;
    const int num_items = 10;
    const std::string conn_name("unittest");
    int vb = 0;

    /* Open an DCP consumer connection */
    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie, opaque, seqno, flags,
                         (void*)conn_name.c_str(), conn_name.length()),
            "Failed dcp producer open connection.");

    std::string type = get_str_stat(h, h1, "eq_dcpq:unittest:type", "dcp");
    checkeq(0, type.compare("consumer"), "Consumer not found");

    opaque = add_stream_for_consumer(h, h1, cookie, opaque, 0, 0,
                                     PROTOCOL_BINARY_RESPONSE_SUCCESS);

    /* Send DCP mutations with in memory flag (0x01) */
    dcp_stream_to_replica(h, h1, cookie, opaque, 0, 0x01, 1, num_items, 0,
                          num_items);

    /* Send 10 more DCP mutations with checkpoint creation flag (0x04) */
    uint64_t start = num_items;
    dcp_stream_to_replica(h, h1, cookie, opaque, 0, 0x04, start + 1,
                          start + 10, start, start + 10);

    wait_for_flusher_to_settle(h, h1);
    stop_persistence(h, h1);
    checkeq(2 * num_items, get_int_stat(h, h1, "vb_replica_curr_items"),
            "wrong number of items in replica vbucket");

    /* Add 10 more items to the replica node on a new checkpoint.
       Send with flag (0x04) indicating checkpoint creation */
    start = 2 * num_items;
    dcp_stream_to_replica(h, h1, cookie, opaque, 0, 0x04, start + 1,
                          start + 10, start, start + 10);

    /* Expecting for Disk backfill + in memory snapshot merge.
       Wait for a checkpoint to be removed */
    wait_for_stat_to_be(h, h1, "vb_0:num_checkpoints", 2, "checkpoint");

    /* Part (ii): Open a stream (in a DCP conn) and see if all the items are
                  received correctly */
    /* We want to stream items till end and keep the stream open. Then we want
       to verify the stream is still open */
    const void *cookie1 = testHarness.create_cookie();
    const std::string conn_name1("unittest1");
    stop_continuous_dcp_thread.store(false, std::memory_order_relaxed);
    struct continuous_dcp_ctx cdc = {h, h1, cookie1, 0, conn_name1.c_str(), 0};
    cb_thread_t dcp_thread;
    cb_assert(cb_create_thread(&dcp_thread, continuous_dcp_thread, &cdc, 0)
              == 0);

    /* Wait for producer to be created */
    wait_for_stat_to_be(h, h1, "ep_dcp_producer_count", 1, "dcp");

    /* Wait for an active stream to be created */
    const std::string stat_stream_count("eq_dcpq:" + conn_name1 +
                                        ":num_streams");
    wait_for_stat_to_be(h, h1, stat_stream_count.c_str(), 1, "dcp");

    /* Wait for the dcp stream to send upto highest seqno we have */
    std::string stat_stream_last_sent_seqno("eq_dcpq:" + conn_name1 + ":stream_"
                                            + std::to_string(vb) +
                                            "_last_sent_seqno");
    wait_for_stat_to_be(h, h1, stat_stream_last_sent_seqno.c_str(),
                        3 * num_items, "dcp");

    /* Check if correct snap end seqno is sent */
    std::string stat_stream_last_sent_snap_end_seqno("eq_dcpq:" + conn_name1 +
                                                     ":stream_" +
                                                     std::to_string(vb) +
                                                     "_last_sent_snap_end_seqno");
    wait_for_stat_to_be(h, h1, stat_stream_last_sent_snap_end_seqno.c_str(),
                        3 * num_items, "dcp");

    /* Check if the stream is still open after sending out latest items */
    std::string stat_stream_state("eq_dcpq:" + conn_name1 + ":stream_" +
                                  std::to_string(vb) + "_state");
    std::string state = get_str_stat(h, h1, stat_stream_state.c_str(), "dcp");
    checkeq(state.compare("in-memory"), 0, "Stream is not open");

    /* Before closing the connection stop the thread that continuously polls
       for dcp data */
    stop_continuous_dcp_thread.store(true, std::memory_order_relaxed);
    testHarness.notify_io_complete(cookie1, ENGINE_SUCCESS);
    cb_assert(cb_join_thread(dcp_thread) == 0);

    testHarness.destroy_cookie(cookie);
    testHarness.destroy_cookie(cookie1);

    return SUCCESS;
}

static enum test_result test_dcp_producer_stream_cursor_movement(
                                                        ENGINE_HANDLE *h,
                                                        ENGINE_HANDLE_V1 *h1) {
    const std::string conn_name("unittest");
    const int num_items = 30, vb = 0;
    uint64_t curr_chkpt_id = 0;
    for (int j = 0; j < num_items; ++j) {
        if (j % 10 == 0) {
            wait_for_flusher_to_settle(h, h1);
        }
        if (j == (num_items - 1)) {
            /* Since checkpoint max items is set to 10 and we are going to
               write 30th item, a new checkpoint could be added after
               writing and persisting the 30th item. I mean, if the checkpoint
               id is got outside the while loop, there could be an error due to
               race (flusher and checkpoint remover could run before getting
               this stat) */
            curr_chkpt_id = get_ull_stat(h, h1, "vb_0:open_checkpoint_id",
                                         "checkpoint");
        }
        item *i = NULL;
        std::string key("key" + std::to_string(j));
        checkeq(ENGINE_SUCCESS,
                store(h, h1, NULL, OPERATION_SET, key.c_str(), "data", &i),
                "Failed to store a value");
        h1->release(h, NULL, i);
    }

    wait_for_flusher_to_settle(h, h1);
    verify_curr_items(h, h1, num_items, "Wrong amount of items");

    const void *cookie = testHarness.create_cookie();

    /* We want to stream items till end and keep the stream open. We want to
       verify if the DCP cursor has moved to new open checkpoint */
    stop_continuous_dcp_thread.store(false, std::memory_order_relaxed);
    struct continuous_dcp_ctx cdc = {h, h1, cookie, 0, conn_name.c_str(), 20};
    cb_thread_t dcp_thread;
    cb_assert(cb_create_thread(&dcp_thread, continuous_dcp_thread, &cdc, 0)
              == 0);

    /* Wait for producer to be created */
    wait_for_stat_to_be(h, h1, "ep_dcp_producer_count", 1, "dcp");

    /* Wait for an active stream to be created */
    const std::string stat_stream_count("eq_dcpq:" + conn_name +
                                        ":num_streams");
    wait_for_stat_to_be(h, h1, stat_stream_count.c_str(), 1, "dcp");

    /* Wait for the dcp stream to send upto highest seqno we have */
    std::string stat_stream_last_sent_seqno("eq_dcpq:" + conn_name + ":stream_"
                                            + std::to_string(vb) +
                                            "_last_sent_seqno");
    wait_for_stat_to_be(h, h1, stat_stream_last_sent_seqno.c_str(), num_items,
                        "dcp");

    /* Wait for new open (empty) checkpoint to be added */
    wait_for_stat_to_be(h, h1, "vb_0:open_checkpoint_id", curr_chkpt_id + 1,
                        "checkpoint");

    /* We want to make sure that no cursors are lingering on any of the previous
       checkpoints. For that we wait for checkpoint remover to remove all but
       the latest open checkpoint cursor */
    wait_for_stat_to_be(h, h1, "vb_0:num_checkpoints", 1, "checkpoint");

    /* Before closing the connection stop the thread that continuously polls
       for dcp data */
    stop_continuous_dcp_thread.store(true, std::memory_order_relaxed);
    testHarness.notify_io_complete(cookie, ENGINE_SUCCESS);
    cb_assert(cb_join_thread(dcp_thread) == 0);
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
    const int num_items = 300, batch_items = 100;
    for (int start_seqno = 0; start_seqno < num_items;
         start_seqno += batch_items) {
        wait_for_flusher_to_settle(h, h1);
        write_items(h, h1, batch_items, start_seqno);
    }

    wait_for_flusher_to_settle(h, h1);
    verify_curr_items(h, h1, num_items, "Wrong amount of items");

    const void *cookie[5];

    uint64_t total_bytes = 0;
    for (int j = 0; j < 5; ++j) {
        std::string name("unittest_" + std::to_string(j));
        cookie[j] = testHarness.create_cookie();

        DcpStreamCtx ctx;
        ctx.vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
        ctx.seqno = {200, 300};
        ctx.snapshot = {200, 200};
        ctx.exp_mutations = 100;
        ctx.exp_markers = 1;

        TestDcpConsumer tdc(name, cookie[j]);
        tdc.addStreamCtx(ctx);
        tdc.run(h, h1);
        total_bytes += tdc.getTotalBytes();
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
    stop_persistence(h, h1);
    size_t num_items = write_items_upto_mem_perc(h, h1, 90);

    // Sanity check - ensure we have enough vBucket quota (max_size)
    // such that we have 1000 items - enough to give us 0.1%
    // granuarity in any residency calculations. */
    if (num_items < 1000) {
        std::cerr << "Error: test_dcp_cursor_dropping: "
            "expected at least 1000 items after filling vbucket, "
            "but only have " << num_items << ". "
            "Check max_size setting for test." << std::endl;
        return FAIL;
    }

    const void *cookie = testHarness.create_cookie();

    start_persistence(h, h1);
    std::string name("unittest");

    DcpStreamCtx ctx;
    ctx.vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
    ctx.seqno = {0, get_ull_stat(h, h1, "vb_0:high_seqno", "vbucket-seqno")};
    ctx.exp_mutations = num_items;
    ctx.exp_markers = 1;
    ctx.skip_estimate_check = true;

    TestDcpConsumer tdc(name, cookie);
    tdc.simulateCursorDropping();
    tdc.addStreamCtx(ctx);
    tdc.run(h, h1);

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
            /* vbucket set at 0 here, as it isn't used. */
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
        if ((num_items % 100) == 0) {
            if (get_int_stat(h, h1, "vb_active_perc_mem_resident") <
                rr_thresh) {
                break;
            }
        }

        item *itm = NULL;
        std::string key("key" + std::to_string(num_items));
        ENGINE_ERROR_CODE ret = store(h, h1, NULL, OPERATION_SET, key.c_str(),
                                      value.c_str(), &itm);
        h1->release(h, NULL, itm);

        switch (ret) {
        case ENGINE_SUCCESS:
            num_items++;
            est_bytes += key.length();
            break;

        case ENGINE_TMPFAIL:
            // TMPFAIL means we getting below 100%; retry.
            break;

        default:
            check(false, ("Unexpected response from store(): " +
                          std::to_string(ret)).c_str());
            break;
        }
    }

    // Sanity check - ensure we have enough vBucket quota (max_size)
    // such that we have 1000 items - enough to give us 0.1%
    // granuarity in any residency calculations. */
    if (num_items < 1000) {
        std::cerr << "Error: test_dcp_producer_stream_backfill_no_value: "
            "expected at least 1000 items after filling vbucket, "
            "but only have " << num_items << ". "
            "Check max_size setting for test." << std::endl;
        return FAIL;
    }

    wait_for_flusher_to_settle(h, h1);
    verify_curr_items(h, h1, num_items, "Wrong number of items");
    int num_non_resident = get_int_stat(h, h1, "vb_active_num_non_resident");
    cb_assert(num_non_resident >= (((float)(100 - rr_thresh)/100) * num_items));


    // Increase max_size from ~2.5M to ~25MB
    set_param(h, h1, protocol_binary_engine_param_flush, "max_size",
              "25000000");
    cb_assert(get_int_stat(h, h1, "vb_active_perc_mem_resident") < rr_thresh);

    const void *cookie = testHarness.create_cookie();

    DcpStreamCtx ctx1;
    ctx1.flags = DCP_ADD_STREAM_FLAG_NO_VALUE;
    ctx1.vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
    ctx1.seqno = {0, get_ull_stat(h, h1, "vb_0:high_seqno", "vbucket-seqno")};
    ctx1.exp_mutations = num_items;
    ctx1.exp_markers = 1;

    /* Stream mutations with "no_value" flag set */
    TestDcpConsumer tdc1("unittest", cookie);
    tdc1.addStreamCtx(ctx1);
    tdc1.run(h, h1);
    total_bytes = tdc1.getTotalBytes();

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
    const void *cookie1 = testHarness.create_cookie();

    dcp_last_op = 0;
    dcp_last_nru = 0;

    DcpStreamCtx ctx2;
    ctx2.vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
    ctx2.seqno = {0, get_ull_stat(h, h1, "vb_0:high_seqno", "vbucket-seqno")};
    ctx2.exp_mutations = num_items;
    ctx2.exp_markers = 1;

    TestDcpConsumer tdc2("unittest1", cookie1);
    tdc2.addStreamCtx(ctx2);
    tdc2.run(h, h1);
    total_bytes = tdc2.getTotalBytes();

    /* Just add total value size to estimated bytes */
    est_bytes += (value.length() * num_items);
    checkeq(est_bytes, total_bytes, "Maybe key values are not streamed");
    testHarness.destroy_cookie(cookie1);

    return SUCCESS;
}

static enum test_result test_dcp_producer_stream_mem_no_value(
                                                        ENGINE_HANDLE *h,
                                                        ENGINE_HANDLE_V1 *h1) {
    uint64_t total_bytes = 0, est_bytes = 0;
    const uint64_t start = 200, end = 300;
    const int num_items = 300, batch_items = 100;
    const std::string value("data");

    for (int start_seqno = 0; start_seqno < num_items;
         start_seqno += batch_items) {
        wait_for_flusher_to_settle(h, h1);
        write_items(h, h1, batch_items, start_seqno);
    }

    wait_for_flusher_to_settle(h, h1);
    verify_curr_items(h, h1, num_items, "Wrong amount of items");

    const void *cookie = testHarness.create_cookie();

    DcpStreamCtx ctx1;
    ctx1.flags = DCP_ADD_STREAM_FLAG_NO_VALUE;
    ctx1.vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
    ctx1.seqno = {start, end};
    ctx1.snapshot = {start, start};
    ctx1.exp_mutations = end - start;
    ctx1.exp_markers = 1;

    /* Stream mutations with "no_value" flag set */
    TestDcpConsumer tdc1("unittest", cookie);
    tdc1.addStreamCtx(ctx1);
    tdc1.run(h, h1);
    total_bytes = tdc1.getTotalBytes();

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
    const void *cookie1 = testHarness.create_cookie();

    DcpStreamCtx ctx2;
    ctx2.vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
    ctx2.seqno = {start, end};
    ctx2.snapshot = {start, start};
    ctx2.exp_mutations = end - start;
    ctx2.exp_markers = 1;

    TestDcpConsumer tdc2("unittest1", cookie1);
    tdc2.addStreamCtx(ctx2);
    tdc2.run(h, h1);
    total_bytes = tdc2.getTotalBytes();

    /* Just add total value size to estimated bytes */
    est_bytes += (value.length() * (end-start));
    checkeq(est_bytes, total_bytes, "Maybe key values are not streamed");
    testHarness.destroy_cookie(cookie1);

    return SUCCESS;
}

static test_result test_dcp_takeover(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const int num_items = 10;
    write_items(h, h1, num_items);

    const void *cookie = testHarness.create_cookie();

    DcpStreamCtx ctx;
    ctx.flags = DCP_ADD_STREAM_FLAG_TAKEOVER;
    ctx.vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
    ctx.seqno = {0, 1000};
    ctx.exp_mutations = 20;
    ctx.exp_markers = 2;
    ctx.extra_takeover_ops = 10;

    TestDcpConsumer tdc("unittest", cookie);
    tdc.addStreamCtx(ctx);
    tdc.run(h, h1);

    check(verify_vbucket_state(h, h1, 0, vbucket_state_dead), "Wrong vb state");

    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static test_result test_dcp_takeover_no_items(ENGINE_HANDLE *h,
                                              ENGINE_HANDLE_V1 *h1) {
    const int num_items = 10;
    write_items(h, h1, num_items);

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

#if 0 /* MB-18256: Disabling cursor droppping temporarily */
    dcp_step(h, h1, cookie);
    cb_assert(dcp_last_op = PROTOCOL_BINARY_CMD_DCP_CONTROL);
    cb_assert(dcp_last_key.compare("supports_cursor_dropping") == 0);
    cb_assert(dcp_last_opaque != opaque);
#endif

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

static enum test_result test_failover_scenario_one_with_dcp(
                                                        ENGINE_HANDLE *h,
                                                        ENGINE_HANDLE_V1 *h1) {

    const int num_items = 50, batch_items = 10;
    for (int start_seqno = 0; start_seqno < num_items;
         start_seqno += batch_items) {
        write_items(h, h1, batch_items, start_seqno);
        wait_for_flusher_to_settle(h, h1);
        createCheckpoint(h, h1);
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
    write_items(h, h1, 2, 1, "key_");

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
    std::string name("unittest");

    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state.");

    // Open consumer connection
    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie, opaque, 0, flags, (void*)(name.c_str()),
                         name.length()),
            "Failed dcp Consumer open connection.");

    std::string flow_ctl_stat_buf("eq_dcpq:" + name + ":unacked_bytes");
    checkeq(0, get_int_stat(h, h1, flow_ctl_stat_buf.c_str(), "dcp"),
            "Consumer flow ctl unacked bytes not starting from 0");

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
    const int num_items = 10;
    write_items(h, h1, num_items);

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
    const int num_items = 40;
    stop_persistence(h, h1);
    write_items(h, h1, num_items);

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
    write_items(h, h1, num_items);

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

    write_items(h, h1, 100, 0, "key_");

    start_persistence(h, h1);
    wait_for_flusher_to_settle(h, h1);
    checkeq(100, get_int_stat(h, h1, "curr_items"),
            "Item count should've been 100");

    stop_persistence(h, h1);

    /* Write items from 90 to 109 */
    write_items(h, h1, 20, 90, "key_");
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
    write_items(h, h1, 10, 0, "key", "123456789");

    wait_for_flusher_to_settle(h, h1);
    verify_curr_items(h, h1, num_items, "Wrong amount of items");

    /* Disable flow control and stream all items. The producer should stream all
     items even when we do not send acks */
    std::string name("unittest");

    DcpStreamCtx ctx1;
    ctx1.vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
    ctx1.seqno = {0, num_items};
    ctx1.exp_mutations = num_items;
    ctx1.exp_markers = 1;

    const void *cookie = testHarness.create_cookie();
    TestDcpConsumer tdc1(name, cookie);
    tdc1.setFlowControlBufSize(0);  // Disabling flow control
    tdc1.disableAcking();           // Do not ack
    tdc1.addStreamCtx(ctx1);
    tdc1.run(h, h1);

    /* Set flow control buffer to a very low value such that producer is not
     expected to send more than 1 item when we do not send acks */
    std::string name1("unittest1");

    DcpStreamCtx ctx2;
    ctx2.vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
    ctx2.seqno = {0, num_items};
    ctx2.exp_mutations = 1;
    ctx2.exp_markers = 1;

    const void *cookie1 = testHarness.create_cookie();
    TestDcpConsumer tdc2(name1, cookie1);
    tdc2.setFlowControlBufSize(100);    // Flow control buf set to low value
    tdc2.disableAcking();               // Do not ack
    tdc2.addStreamCtx(ctx2);
    tdc2.run(h, h1);

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

static enum test_result test_dcp_consumer_mutate(ENGINE_HANDLE *h,
                                                 ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state.");

    const void *cookie = testHarness.create_cookie();
    uint32_t opaque = 0xFFFF0000;
    uint32_t seqno = 0;
    uint32_t flags = 0;
    std::string name("unittest");

    // Open an DCP connection
    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie, opaque, seqno, flags, (void*)(name.c_str()),
                         name.length()),
            "Failed dcp producer open connection.");

    std::string type = get_str_stat(h, h1, "eq_dcpq:unittest:type", "dcp");
    checkeq(0, type.compare("consumer"), "Consumer not found");

    int exp_unacked_bytes = 0;
    std::string flow_ctl_stat_buf("eq_dcpq:" + name + ":unacked_bytes");
    checkeq(exp_unacked_bytes,
            get_int_stat(h, h1, flow_ctl_stat_buf.c_str(), "dcp"),
            "Consumer flow ctl unacked bytes not starting from 0");

    opaque = add_stream_for_consumer(h, h1, cookie, opaque, 0, 0,
                                     PROTOCOL_BINARY_RESPONSE_SUCCESS);

    std::string key("key");
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

    /* Add snapshot marker bytes to unacked bytes. Since we are shipping out
       acks by calling dcp.step(), the unacked bytes will increase */
    exp_unacked_bytes += dcp_snapshot_marker_base_msg_bytes;
    checkeq(exp_unacked_bytes,
            get_int_stat(h, h1, flow_ctl_stat_buf.c_str(), "dcp"),
            "Consumer flow ctl snapshot marker bytes not accounted correctly");

    // Ensure that we don't accept invalid opaque values
    checkeq(ENGINE_KEY_ENOENT,
            h1->dcp.mutation(h, cookie, opaque + 1, key.c_str(), key.length(),
                             data, dataLen, cas, vbucket, flags, datatype,
                             bySeqno, revSeqno, exprtime, lockTime, NULL, 0, 0),
          "Failed to detect invalid DCP opaque value");

    /* Add mutation bytes to unacked bytes. Since we are shipping out
       acks by calling dcp.step(), the unacked bytes will increase */
    exp_unacked_bytes += (dcp_mutation_base_msg_bytes + key.length() + dataLen);
    checkeq(exp_unacked_bytes,
            get_int_stat(h, h1, flow_ctl_stat_buf.c_str(), "dcp"),
            "Consumer flow ctl mutation bytes not accounted correctly");

    // Send snapshot marker
    checkeq(ENGINE_SUCCESS,
            h1->dcp.snapshot_marker(h, cookie, opaque, 0, 10, 15, 300),
            "Failed to send marker!");

    exp_unacked_bytes += dcp_snapshot_marker_base_msg_bytes;
    checkeq(exp_unacked_bytes,
            get_int_stat(h, h1, flow_ctl_stat_buf.c_str(), "dcp"),
            "Consumer flow ctl snapshot marker bytes not accounted correctly");

    // Consume an DCP mutation
    checkeq(ENGINE_SUCCESS,
            h1->dcp.mutation(h, cookie, opaque, key.c_str(), key.length(), data,
                             dataLen, cas, vbucket, flags, datatype, bySeqno,
                             revSeqno, exprtime, lockTime, NULL, 0, 0),
            "Failed dcp mutate.");

    exp_unacked_bytes += (dcp_mutation_base_msg_bytes + key.length() + dataLen);
    checkeq(exp_unacked_bytes,
            get_int_stat(h, h1, flow_ctl_stat_buf.c_str(), "dcp"),
            "Consumer flow ctl mutation bytes not accounted correctly");

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

    set_drift_counter_state(h, h1, /* initial_drift */1000);

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

    //Set drift value
    set_drift_counter_state(h, h1, /* initial drift */1000);

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

    DcpStreamCtx ctx;
    ctx.vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
    ctx.seqno = {0, num_items};
    ctx.exp_mutations = num_items;
    ctx.exp_markers = 1;

    const void *cookie1 = testHarness.create_cookie();
    TestDcpConsumer tdc("unittest1", cookie1);
    tdc.addStreamCtx(ctx);
    tdc.run(h, h1);

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

    DcpStreamCtx ctx;
    ctx.vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
    ctx.seqno = {0, num_items};
    ctx.exp_mutations = num_items;
    ctx.exp_markers = 1;

    const void *cookie1 = testHarness.create_cookie();
    TestDcpConsumer tdc("unittest1", cookie1);
    tdc.addStreamCtx(ctx);
    tdc.run(h, h1);

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

    DcpStreamCtx ctx;
    ctx.vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
    ctx.seqno = {0, get_ull_stat(h, h1, "vb_0:high_seqno", "vbucket-seqno")};
    ctx.exp_mutations = 300;
    ctx.exp_markers = 1;

    const void *cookie1 = testHarness.create_cookie();
    TestDcpConsumer tdc("unittest1", cookie1);
    tdc.addStreamCtx(ctx);
    tdc.run(h, h1);

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
    uint64_t cas = 0;
    uint32_t high_seqno = 0;
    const int num_items = 3;
    char key[][3] = {"k1", "k2", "k3"};

    memset(&info, 0, sizeof(info));

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
    DcpStreamCtx ctx;
    ctx.vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
    ctx.seqno = {0, get_ull_stat(h, h1, "vb_0:high_seqno", "vbucket-seqno")};
    ctx.exp_mutations = 1;
    ctx.exp_deletions = 1;
    ctx.exp_markers = 1;
    ctx.skip_estimate_check = true;

    const void *cookie = testHarness.create_cookie();
    TestDcpConsumer tdc("unittest", cookie);
    tdc.addStreamCtx(ctx);
    tdc.run(h, h1);

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
    DcpStreamCtx ctx;
    ctx.vb_uuid = vb_uuid;
    ctx.seqno = {0, high_seqno};
    ctx.exp_mutations = 3;
    ctx.exp_markers = 1;
    ctx.skip_estimate_check = true;

    const void *cookie = testHarness.create_cookie();
    TestDcpConsumer tdc("unittest", cookie);
    tdc.addStreamCtx(ctx);
    tdc.run(h, h1);

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
    DcpStreamCtx ctx1;
    ctx1.vb_uuid = vb_uuid;
    ctx1.seqno = {3, high_seqno};
    ctx1.snapshot = {3, high_seqno};
    ctx1.exp_err = ENGINE_ROLLBACK;
    ctx1.exp_rollback = 0;

    const void *cookie1 = testHarness.create_cookie();
    TestDcpConsumer tdc1("unittest1", cookie1);
    tdc1.addStreamCtx(ctx1);

    tdc1.openConnection(h, h1);
    tdc1.openStreams(h, h1);

    testHarness.destroy_cookie(cookie1);

    /* Do not expect rollback when you already have all items in the snapshot
       (that is, start == snap_end_seqno)*/
    DcpStreamCtx ctx2;
    ctx2.vb_uuid = vb_uuid;
    ctx2.seqno = {high_seqno, high_seqno + 10};
    ctx2.snapshot = {0, high_seqno};
    ctx2.exp_err = ENGINE_SUCCESS;

    const void *cookie2 = testHarness.create_cookie();
    TestDcpConsumer tdc2("unittest2", cookie2);
    tdc2.addStreamCtx(ctx2);

    tdc2.openConnection(h, h1);
    tdc2.openStreams(h, h1);

    testHarness.destroy_cookie(cookie2);

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
        write_items(h, h1, num_items, 0, "KEY", "somevalue");
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

    write_items(h, h1, num_items);

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
        DcpStreamCtx ctx;
        ctx.vb_uuid = params[i].vb_uuid;
        ctx.seqno = {params[i].start_seqno, end_seqno};
        ctx.snapshot = {params[i].snap_start_seqno, params[i].snap_end_seqno};
        ctx.exp_err = params[i].exp_err_code;
        ctx.exp_rollback = params[i].exp_rollback;

        const void *cookie = testHarness.create_cookie();
        std::string conn_name("unittest" + std::to_string(i));
        TestDcpConsumer tdc(conn_name.c_str(), cookie);
        tdc.addStreamCtx(ctx);

        tdc.openConnection(h, h1);
        tdc.openStreams(h, h1);

        testHarness.destroy_cookie(cookie);
    }
    return SUCCESS;
}

static enum test_result test_mb16357(ENGINE_HANDLE *h,
                                     ENGINE_HANDLE_V1 *h1) {

    // Load up vb0 with n items, expire in 1 second
    const int num_items = 1000;

    write_items(h, h1, num_items, 0, "key-");

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

/*
 * This test case creates and test multiple streams
 * between a single producer and consumer.
 */
static enum test_result test_dcp_multiple_streams(ENGINE_HANDLE *h,
                                                  ENGINE_HANDLE_V1 *h1) {

    check(set_vbucket_state(h, h1, 1, vbucket_state_active),
          "Failed set vbucket state on 1");
    check(set_vbucket_state(h, h1, 2, vbucket_state_active),
          "Failed set vbucket state on 2");
    wait_for_flusher_to_settle(h, h1);

    int num_items = 100;
    for (int i = 0; i < num_items; ++i) {
        std::string key("key_1_" + std::to_string(i));
        item *itm;
        checkeq(ENGINE_SUCCESS,
                store(h, h1, nullptr, OPERATION_SET, key.c_str(), "data",
                      &itm, 0, 1),
                "Failed store on vb 0");
        h1->release(h, nullptr, itm);
        key = "key_2_" + std::to_string(i);
        checkeq(ENGINE_SUCCESS,
                store(h, h1, nullptr, OPERATION_SET, key.c_str(), "data",
                      &itm, 0, 2),
                "Failed store on vb 1");
        h1->release(h, nullptr, itm);
    }

    std::string name("unittest");
    const void *cookie = testHarness.create_cookie();

    DcpStreamCtx ctx1, ctx2;

    int extra_items = 100;

    ctx1.vbucket = 1;
    ctx1.vb_uuid = get_ull_stat(h, h1, "vb_0:0:id", "failovers");
    ctx1.seqno = {0, static_cast<uint64_t>(num_items + extra_items)};
    ctx1.exp_mutations = num_items + extra_items;
    ctx1.live_frontend_client = true;

    ctx2.vbucket = 2;
    ctx2.vb_uuid = get_ull_stat(h, h1, "vb_1:0:id", "failovers");
    ctx2.seqno = {0, static_cast<uint64_t>(num_items + extra_items)};
    ctx2.exp_mutations = num_items + extra_items;
    ctx2.live_frontend_client = true;

    TestDcpConsumer tdc("unittest", cookie);
    tdc.addStreamCtx(ctx1);
    tdc.addStreamCtx(ctx2);

    cb_thread_t thread1, thread2;
    struct writer_thread_ctx t1 = {h, h1, extra_items, /*vbid*/1};
    struct writer_thread_ctx t2 = {h, h1, extra_items, /*vbid*/2};
    cb_assert(cb_create_thread(&thread1, writer_thread, &t1, 0) == 0);
    cb_assert(cb_create_thread(&thread2, writer_thread, &t2, 0) == 0);

    tdc.run(h, h1);

    cb_assert(cb_join_thread(thread1) == 0);
    cb_assert(cb_join_thread(thread2) == 0);

    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_dcp_on_vbucket_state_change(ENGINE_HANDLE *h,
                                                         ENGINE_HANDLE_V1 *h1) {

    const void *cookie = testHarness.create_cookie();

    // Set up a DcpTestConsumer that would remain in in-memory mode
    stop_continuous_dcp_thread.store(false, std::memory_order_relaxed);
    struct continuous_dcp_ctx cdc = {h, h1, cookie, 0, "unittest", 0};
    cb_thread_t dcp_thread;
    cb_assert(cb_create_thread(&dcp_thread, continuous_dcp_thread, &cdc, 0) == 0);

    // Wait for producer to be created
    wait_for_stat_to_be(h, h1, "ep_dcp_producer_count", 1, "dcp");

    // Write a mutation
    item *i;
    checkeq(ENGINE_SUCCESS,
            store(h, h1, nullptr, OPERATION_SET, "key", "value", &i),
            "Failed to store a value");
    h1->release(h, NULL, i);

    // Wait for producer to stream that item
    wait_for_stat_to_be(h, h1, "eq_dcpq:unittest:items_sent", 1, "dcp");

    // Change vbucket state to pending
    check(set_vbucket_state(h, h1, 0, vbucket_state_pending),
          "Failed set vbucket state on 1");

    // Expect DcpTestConsumer to close
    cb_assert(cb_join_thread(dcp_thread) == 0);

    // Expect dcp_last_flags to carry END_STREAM_STATE as reason
    // for stream closure
    checkeq(static_cast<uint32_t>(2),
            dcp_last_flags, "Last DCP flag not END_STREAM_STATE");

    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_consumer_processer_behavior(ENGINE_HANDLE *h,
                                                             ENGINE_HANDLE_V1 *h1) {

    check(set_vbucket_state(h, h1, 0, vbucket_state_replica),
          "Failed to set vbucket state.");
    wait_for_flusher_to_settle(h, h1);

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
                            PROTOCOL_BINARY_RESPONSE_SUCCESS);

    uint32_t stream_opaque =
        get_int_stat(h, h1, "eq_dcpq:unittest:stream_0_opaque", "dcp");

    int i = 1;
    while (true) {
        // Stats lookup is costly; only perform check every 100
        // iterations (we only need to be greater than 1.25 *
        // ep_max_size, not exactly at that point).
        if ((i % 100) == 0) {
            if (get_int_stat(h, h1, "mem_used") >=
                1.25 * get_int_stat(h, h1, "ep_max_size")) {
                break;
            }
        }

        if (i % 20) {
            checkeq(ENGINE_SUCCESS,
                    h1->dcp.snapshot_marker(h, cookie, stream_opaque,
                                            0, i, i + 20, 0x01),
                    "Failed to send snapshot marker");
        }
        std::stringstream ss;
        ss << "key" << i;
        checkeq(ENGINE_SUCCESS,
                h1->dcp.mutation(h, cookie, stream_opaque, ss.str().c_str(),
                                 ss.str().length(), "value", 5, i * 3, 0, 0, 0, i,
                                 0, 0, 0, "", 0, INITIAL_NRU_VALUE),
                "Failed to send dcp mutation");
        ++i;
    }

    // Expect buffered items and the processer's task state to be
    // CANNOT_PROCESS, because of numerous backoffs.
    check(get_int_stat(h, h1, "eq_dcpq:unittest:stream_0_buffer_items", "dcp") > 0,
          "Expected buffered items for the stream");
    wait_for_stat_to_be_gte(h, h1, "eq_dcpq:unittest:total_backoffs", 1, "dcp");
    checkne(std::string("ALL_PROCESSED"),
            get_str_stat(h, h1, "eq_dcpq:unittest:processer_task_state", "dcp"),
            "Expected Processer's task state not to be ALL_PROCESSED!");

    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_get_all_vb_seqnos(ENGINE_HANDLE *h,
                                               ENGINE_HANDLE_V1 *h1) {
    const void *cookie = testHarness.create_cookie();

    const int num_vbuckets = 10;

    /* Replica vbucket 0; snapshot 0 to 10, but write just 1 item */
    const int rep_vb_num = 0;
    check(set_vbucket_state(h, h1, rep_vb_num, vbucket_state_replica),
          "Failed to set vbucket state");
    wait_for_flusher_to_settle(h, h1);

    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    std::string name("unittest");
    uint8_t cas = 0;
    uint8_t datatype = 1;
    uint64_t bySeqno = 10;
    uint64_t revSeqno = 0;
    uint32_t exprtime = 0;
    uint32_t lockTime = 0;

    checkeq(ENGINE_SUCCESS,
            h1->dcp.open(h, cookie, opaque, 0, flags, (void*)(name.c_str()),
                         name.size()),
            "Failed to open DCP consumer connection!");
    add_stream_for_consumer(h, h1, cookie, opaque++, rep_vb_num, 0,
                            PROTOCOL_BINARY_RESPONSE_SUCCESS);

    std::string opaqueStr("eq_dcpq:" + name + ":stream_0_opaque");
    uint32_t stream_opaque = get_int_stat(h, h1, opaqueStr.c_str(), "dcp");

    checkeq(ENGINE_SUCCESS,
            h1->dcp.snapshot_marker(h, cookie, stream_opaque, rep_vb_num, 0, 10,
                                    1),
            "Failed to send snapshot marker!");

    check(h1->dcp.mutation(h, cookie, stream_opaque, "key", 3, "value", 5,
                           cas, rep_vb_num, flags, datatype,
                           bySeqno, revSeqno, exprtime,
                           lockTime, NULL, 0, 0) == ENGINE_SUCCESS,
          "Failed dcp mutate.");

    /* Create active vbuckets */
    for (int i = 1; i < num_vbuckets; i++) {
        /* Active vbuckets */
        check(set_vbucket_state(h, h1, i, vbucket_state_active),
              "Failed to set vbucket state.");
        for (int j= 0; j < i; j++) {
            std::string key("key" + std::to_string(i));
            check(store(h, h1, NULL, OPERATION_SET, key.c_str(),
                        "value", NULL, 0, i)
                  == ENGINE_SUCCESS, "Failed to store an item.");
        }
    }

    /* Create request to get vb seqno of all vbuckets */
    get_all_vb_seqnos(h, h1, static_cast<vbucket_state_t>(0), cookie);

    /* Check if the response received is correct */
    verify_all_vb_seqnos(h, h1, 0, num_vbuckets - 1);

    /* Create request to get vb seqno of active vbuckets */
    get_all_vb_seqnos(h, h1, vbucket_state_active, cookie);

    /* Check if the response received is correct */
    verify_all_vb_seqnos(h, h1, 1, num_vbuckets - 1);

    /* Create request to get vb seqno of replica vbuckets */
    get_all_vb_seqnos(h, h1, vbucket_state_replica, cookie);

    /* Check if the response received is correct */
    verify_all_vb_seqnos(h, h1, 0, 0);

    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

// Test manifest //////////////////////////////////////////////////////////////

const char *default_dbname = "./ep_testsuite_dcp";

BaseTestCase testsuite_testcases[] = {
        TestCase("test dcp vbtakeover stat no stream",
                 test_dcp_vbtakeover_no_stream, test_setup, teardown, nullptr,
                 prepare, cleanup),
        TestCase("test dcp notifier open", test_dcp_notifier_open, test_setup,
                 teardown, nullptr, prepare, cleanup),
        TestCase("test dcp notifier", test_dcp_notifier, test_setup, teardown,
                 nullptr, prepare, cleanup),
        TestCase("test dcp notifier equal number of items",
                 test_dcp_notifier_equal_to_number_of_items,
                 test_setup, teardown, nullptr, prepare, cleanup),
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
        TestCase("test open producer same cookie", test_dcp_producer_open_same_cookie,
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
                 test_setup, teardown, "chk_remover_stime=1;chk_max_items=100;"
                 "time_synchronization=enabled_with_drift", prepare, cleanup),
        TestCase("test producer stream request (full)",
                 test_dcp_producer_stream_req_full, test_setup, teardown,
                 "chk_remover_stime=1;chk_max_items=100", prepare, cleanup),
        TestCase("test producer stream request (disk)",
                 test_dcp_producer_stream_req_disk, test_setup, teardown,
                 "chk_remover_stime=1;chk_max_items=100", prepare, cleanup),
        TestCase("test producer stream request (disk only)",
                 test_dcp_producer_stream_req_diskonly, test_setup, teardown,
                 "chk_remover_stime=1;chk_max_items=100", prepare, cleanup),
        TestCase("test producer backfill limits",
                 test_dcp_producer_backfill_limits, test_setup, teardown,
                 "dcp_scan_item_limit=100;dcp_scan_byte_limit=100", prepare,
                 cleanup),
        TestCase("test producer stream request (memory only)",
                 test_dcp_producer_stream_req_mem, test_setup, teardown,
                 "chk_remover_stime=1;chk_max_items=100", prepare, cleanup),
        TestCase("test producer stream request (DGM)",
                 test_dcp_producer_stream_req_dgm, test_setup, teardown,
                 "chk_remover_stime=1;max_size=2000000", prepare, cleanup),
        TestCase("test producer stream request (latest flag)",
                 test_dcp_producer_stream_latest, test_setup, teardown, NULL,
                 prepare, cleanup),
        TestCase("test producer keep stream open",
                 test_dcp_producer_keep_stream_open, test_setup, teardown,
                 "chk_remover_stime=1;chk_max_items=100", prepare, cleanup),
        TestCase("test producer keep stream open replica",
                 test_dcp_producer_keep_stream_open_replica, test_setup,
                 teardown, "chk_remover_stime=1;chk_max_items=100", prepare,
                 cleanup),
        TestCase("test producer stream cursor movement",
                 test_dcp_producer_stream_cursor_movement, test_setup, teardown,
                 "chk_remover_stime=1;chk_max_items=10", prepare, cleanup),
        TestCase("test producer stream request nmvb",
                 test_dcp_producer_stream_req_nmvb, test_setup, teardown, NULL,
                 prepare, cleanup),
        TestCase("test dcp agg stats",
                 test_dcp_agg_stats, test_setup, teardown, "chk_max_items=100",
                 prepare, cleanup),
        TestCase("test dcp cursor dropping",
                 test_dcp_cursor_dropping, test_setup, teardown,
                 /* max_size set so that it's big enough that we can
                    create at least 1000 items when our residency
                    ratio gets to 90%. See test body for more details. */
                 "cursor_dropping_lower_mark=60;cursor_dropping_upper_mark=70;"
                 "chk_remover_stime=1;max_size=2000000", prepare, cleanup),
        TestCase("test dcp value compression",
                 test_dcp_value_compression, test_setup, teardown,
                 "dcp_value_compression_enabled=true",
                 prepare, cleanup),
        TestCase("test producer stream request backfill no value",
                 test_dcp_producer_stream_backfill_no_value, test_setup,
                 /* max_size set so that it's big enough that we can
                    create at least 1000 items when our residency
                    ratio gets to 80%. See test body for more details. */
                 teardown, "chk_remover_stime=1;max_size=2500000", prepare,
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
                 test_dcp_consumer_mutate_with_time_sync, test_setup, teardown,
                 "dcp_enable_noop=false;time_synchronization=enabled_with_drift",
                 prepare, cleanup),
        TestCase("dcp consumer delete", test_dcp_consumer_delete, test_setup,
                 teardown, "dcp_enable_noop=false", prepare, cleanup),
        TestCase("dcp consumer delete with time sync",
                 test_dcp_consumer_delete_with_time_sync, test_setup, teardown,
                 "dcp_enable_noop=false;time_synchronization=enabled_with_drift",
                 prepare, cleanup),
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
        TestCase("test dcp multiple streams", test_dcp_multiple_streams,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("test dcp on vbucket state change",
                 test_dcp_on_vbucket_state_change,
                 test_setup, teardown, NULL, prepare, cleanup),
        TestCase("test dcp consumer's processer task behavior",
                 test_dcp_consumer_processer_behavior,
                 test_setup, teardown, "max_size=1048576",
                 prepare, cleanup),
        TestCase("test get all vb seqnos", test_get_all_vb_seqnos, test_setup,
                 teardown, NULL, prepare, cleanup),

        TestCase(NULL, NULL, NULL, NULL, NULL, prepare, cleanup)
};
