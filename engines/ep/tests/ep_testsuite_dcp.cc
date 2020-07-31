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
#include "ep_test_apis.h"
#include "ep_testsuite_common.h"
#include "mock/mock_dcp.h"
#include "programs/engine_testapp/mock_server.h"

#include <platform/cb_malloc.h>
#include <platform/cbassert.h>
#include <platform/compress.h>
#include <platform/platform_thread.h>
#include <condition_variable>
#include <thread>
#include <utility>

using namespace std::string_literals;
using namespace std::string_view_literals;

// Helper functions ///////////////////////////////////////////////////////////

/**
 * Converts the given engine to a DcpIface*. If engine doesn't implement
 * DcpIface then throws.
 * @returns non-null ptr to DcpIface.
 */
static gsl::not_null<DcpIface*> requireDcpIface(EngineIface* engine) {
    return dynamic_cast<DcpIface*>(engine);
}

static void dcp_step(EngineIface* h,
                     const void* cookie,
                     MockDcpMessageProducers& producers) {
    auto dcp = requireDcpIface(h);
    ENGINE_ERROR_CODE err = dcp->step(cookie, &producers);
    check(err == ENGINE_SUCCESS || err == ENGINE_EWOULDBLOCK,
          "Expected success or engine_ewouldblock");
    if (err == ENGINE_EWOULDBLOCK) {
        producers.clear_dcp_data();
    }
}

static void dcpHandleResponse(EngineIface* h,
                              const void* cookie,
                              protocol_binary_response_header* response,
                              MockDcpMessageProducers& producers) {
    auto dcp = requireDcpIface(h);
    auto erroCode = dcp->response_handler(cookie, response);
    check(erroCode == ENGINE_SUCCESS || erroCode == ENGINE_EWOULDBLOCK,
          "Expected 'success' or 'engine_ewouldblock'");
    if (erroCode == ENGINE_EWOULDBLOCK) {
        producers.clear_dcp_data();
    }
}

static bool wait_started(false);

struct SeqnoRange {
    uint64_t start;
    uint64_t end;
};

/**
 * DeletionOpcode is used to determine whether or not to perform the deletion
 * path, or the expiration path.
 */
enum class DeletionOpcode : bool {
    Deletion,
    Expiration,
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
          exp_expirations(0),
          exp_markers(0),
          extra_takeover_ops(0),
          exp_disk_snapshot(false),
          exp_conflict_res(0),
          skip_estimate_check(false),
          live_frontend_client(false),
          skip_verification(false),
          exp_err(ENGINE_SUCCESS),
          exp_rollback(0),
          expected_values(0),
          opaque(0),
          exp_seqno_advanced(0),
          exp_system_events(0) {
        seqno = {0, static_cast<uint64_t>(~0)};
        snapshot = {0, static_cast<uint64_t>(~0)};
    }

    /* Vbucket Id */
    Vbid vbucket;
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
    /* Number of expiries expected (for verification) */
    size_t exp_expirations;
    /* Number of snapshot markers expected (for verification) */
    size_t exp_markers;
    /* Extra front end mutations as part of takeover */
    size_t extra_takeover_ops;
    /* Flag - expect disk snapshot or not */
    bool exp_disk_snapshot;
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
    /* Expected number of values (from mutations or deleted_values) */
    size_t expected_values;
    /* stream opaque */
    uint32_t opaque;
    /* Expected number of SeqnoAdvanced ops*/
    size_t exp_seqno_advanced;
    /* Expected number of SystemEvent ops*/
    size_t exp_system_events;
    /* Expected sequence of Collection IDs from SystemEvents */
    std::vector<CollectionID> exp_collection_ids{};
};

class TestDcpConsumer {
/**
 * This class represents a DcpConsumer which is responsible
 * for spawning a DcpProducer at the server and receiving
 * messages from it.
 */
public:
    TestDcpConsumer(std::string _name, const void* _cookie, EngineIface* h)
        : producers(h),
          name(std::move(_name)),
          cookie(_cookie),
          opaque(0),
          total_bytes(0),
          simulate_cursor_dropping(false),
          flow_control_buf_size(1024),
          disable_ack(false),
          h(h),
          dcp(requireDcpIface(h)),
          nruCounter(2) {
    }

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

    void run(bool openConn = true);

    // Stop the thread if it is running. This is safe to be called from
    // a different thread to the thread calling run().
    void stop();

    /**
     * This method just opens a DCP connection. Note it does not open a stream
     * and does not call the dcp step function to get all the items from the
     * producer.
     * @param flags Flags to pass to DCP_OPEN.
     */
    void openConnection(
            uint32_t flags = cb::mcbp::request::DcpOpenPayload::Producer);

    /* This method opens a stream on an existing DCP connection.
       This does not call the dcp step function to get all the items from the
       producer */
    ENGINE_ERROR_CODE openStreams();

    /* if clear is true, it will also clear the stream vector */
    ENGINE_ERROR_CODE closeStreams(bool fClear = false);

    ENGINE_ERROR_CODE sendControlMessage(const std::string& name,
                                         const std::string& value);

    const std::vector<int>& getNruCounters() const {
        return nruCounter;
    }

    MockDcpMessageProducers producers;

    void setCollectionsFilter(
            std::optional<std::string_view> filter = std::nullopt) {
        if (filter) {
            collectionFilter = filter;
            producers.isCollectionsSupported = true;
        }
    }

private:
    /* Vbucket-level stream stats used in test */
    struct VBStats {
        VBStats()
            : num_mutations(0),
              num_deletions(0),
              num_expirations(0),
              num_snapshot_markers(0),
              num_set_vbucket_pending(0),
              num_set_vbucket_active(0),
              pending_marker_ack(false),
              marker_end(0),
              last_by_seqno(0),
              extra_takeover_ops(0),
              exp_disk_snapshot(false),
              exp_conflict_res(0),
              num_values(0),
              number_of_seqno_advanced(0),
              number_of_system_events(0) {
        }

        size_t num_mutations;
        size_t num_deletions;
        size_t num_expirations;
        size_t num_snapshot_markers;
        size_t num_set_vbucket_pending;
        size_t num_set_vbucket_active;
        bool pending_marker_ack;
        uint64_t marker_end;
        uint64_t last_by_seqno;
        size_t extra_takeover_ops;
        bool exp_disk_snapshot;
        uint8_t exp_conflict_res;
        size_t num_values;
        size_t number_of_seqno_advanced;
        size_t number_of_system_events;
        std::vector<CollectionID> collections{};
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
    std::map<Vbid, VBStats> vb_stats;
    EngineIface* h;
    gsl::not_null<DcpIface*> dcp;
    std::vector<int> nruCounter;
    std::optional<std::string_view> collectionFilter;

    // Flag used by run() to check if it should continue to execute.
    std::atomic<bool> done{false};

    /**
     * Helper function to perform the very similar resolution of a deletion
     * and an expiry, triggered inside the run() case switch where one of these
     * operations is returned as the last_op.
     * @param stats The vbstats that will be updated by this function.
     * @param bytes_read The current no of bytes read which will be updated by
     *                   this function.
     * @param all_bytes The total no of bytes read which will be updated by
     *                  this function.
     * @param vbid The vBucket ID.
     * @param delOrExpire Determines whether to take the deletion case or the
     *                    expiration case.
     */
    void deleteOrExpireCase(TestDcpConsumer::VBStats& stats,
                            uint32_t& bytes_read,
                            uint64_t& all_bytes,
                            Vbid vbid,
                            DeletionOpcode delOrExpire);
};

ENGINE_ERROR_CODE TestDcpConsumer::sendControlMessage(
        const std::string& name, const std::string& value) {
    return dcp->control(cookie, ++opaque, name, value);
}

void TestDcpConsumer::deleteOrExpireCase(TestDcpConsumer::VBStats& stats,
                                         uint32_t& bytes_read,
                                         uint64_t& all_bytes,
                                         Vbid vbid,
                                         DeletionOpcode delOrExpire) {
    cb_assert(vbid != static_cast<Vbid>(-1));
    checklt(stats.last_by_seqno,
            producers.last_byseqno.load(),
            "Expected bigger seqno");
    stats.last_by_seqno = producers.last_byseqno;
    if (delOrExpire == DeletionOpcode::Deletion) {
        stats.num_deletions++;
    } else {
        stats.num_expirations++;
    }
    bytes_read += producers.last_packet_size;
    all_bytes += producers.last_packet_size;
    if (stats.pending_marker_ack &&
        producers.last_byseqno == stats.marker_end) {
        sendDcpAck(h,
                   cookie,
                   cb::mcbp::ClientOpcode::DcpSnapshotMarker,
                   cb::mcbp::Status::Success,
                   producers.last_opaque);
    }

    if (!producers.last_value.empty()) {
        stats.num_values++;
    }
    return;
}

void TestDcpConsumer::run(bool openConn) {
    checkle(size_t{1}, stream_ctxs.size(), "No dcp_stream arguments provided!");

    /* Open the connection with the DCP producer */
    if (openConn) {
        openConnection();
    }

    if (collectionFilter) {
        // Enable noop ops needed for collections
        dcp->control(cookie, opaque, "enable_noop", "true");
    }
    /* Open streams in the above open connection */
    openStreams();

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
                std::this_thread::sleep_for(std::chrono::seconds(2));
                delay_buffer_acking = false;
            }
            dcp->buffer_acknowledgement(cookie, ++opaque, Vbid(0), bytes_read);
            total_acked_bytes += bytes_read;
            bytes_read = 0;
        }
        ENGINE_ERROR_CODE err = dcp->step(cookie, &producers);
        if (err == ENGINE_DISCONNECT) {
            done = true;
        } else {
            const Vbid vbid = producers.last_vbucket;
            auto& stats = vb_stats[vbid];
            switch (producers.last_op) {
            case cb::mcbp::ClientOpcode::DcpMutation:
                cb_assert(vbid != static_cast<Vbid>(-1));
                checklt(stats.last_by_seqno,
                        producers.last_byseqno.load(),
                        "Expected bigger seqno");
                stats.last_by_seqno = producers.last_byseqno;
                stats.num_mutations++;
                bytes_read += producers.last_packet_size;
                all_bytes += producers.last_packet_size;
                if (stats.pending_marker_ack &&
                    producers.last_byseqno == stats.marker_end) {
                    sendDcpAck(h,
                               cookie,
                               cb::mcbp::ClientOpcode::DcpSnapshotMarker,
                               cb::mcbp::Status::Success,
                               producers.last_opaque);
                }

                if (producers.last_nru > 0) {
                    nruCounter[1]++;
                } else {
                    nruCounter[0]++;
                }
                if (!producers.last_value.empty()) {
                    stats.num_values++;
                }

                break;
            case cb::mcbp::ClientOpcode::DcpDeletion:
                deleteOrExpireCase(stats,
                                   bytes_read,
                                   all_bytes,
                                   vbid,
                                   DeletionOpcode::Deletion);
                break;
            case cb::mcbp::ClientOpcode::DcpExpiration:
                deleteOrExpireCase(stats,
                                   bytes_read,
                                   all_bytes,
                                   vbid,
                                   DeletionOpcode::Expiration);
                break;
            case cb::mcbp::ClientOpcode::DcpStreamEnd:
                cb_assert(vbid != static_cast<Vbid>(-1));
                if (++num_stream_ends_received == stream_ctxs.size()) {
                    done = true;
                }
                bytes_read += producers.last_packet_size;
                all_bytes += producers.last_packet_size;
                break;
            case cb::mcbp::ClientOpcode::DcpSnapshotMarker:
                cb_assert(vbid != static_cast<Vbid>(-1));
                if (stats.exp_disk_snapshot &&
                    stats.num_snapshot_markers == 0) {
                    checkeq(uint32_t{1},
                            producers.last_flags,
                            "Expected disk snapshot");
                }

                if (producers.last_flags & 8) {
                    stats.pending_marker_ack = true;
                    stats.marker_end = producers.last_snap_end_seqno;
                }

                stats.num_snapshot_markers++;
                bytes_read += producers.last_packet_size;
                all_bytes += producers.last_packet_size;
                break;
            case cb::mcbp::ClientOpcode::DcpSetVbucketState:
                cb_assert(vbid != static_cast<Vbid>(-1));
                if (producers.last_vbucket_state == vbucket_state_pending) {
                    stats.num_set_vbucket_pending++;
                    for (size_t j = 0; j < stats.extra_takeover_ops; ++j) {
                        std::string key("key" + std::to_string(j));
                        checkeq(ENGINE_SUCCESS,
                                store(h,
                                      nullptr,
                                      OPERATION_SET,
                                      key.c_str(),
                                      "data",
                                      nullptr,
                                      0,
                                      vbid),
                                "Failed to store a value");
                    }
                } else if (producers.last_vbucket_state ==
                           vbucket_state_active) {
                    stats.num_set_vbucket_active++;
                }
                bytes_read += producers.last_packet_size;
                all_bytes += producers.last_packet_size;
                sendDcpAck(h,
                           cookie,
                           cb::mcbp::ClientOpcode::DcpSetVbucketState,
                           cb::mcbp::Status::Success,
                           producers.last_opaque);
                break;
            case cb::mcbp::ClientOpcode::Invalid:
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
                     * producers.last_op value below.
                     */
                    testHarness->lock_cookie(cookie);
                    /* waitfor_cookie() waits on a condition variable. But
                       the api expects the cookie to be locked before
                       calling it */
                    testHarness->waitfor_cookie(cookie);
                    testHarness->unlock_cookie(cookie);
                }
                break;
            case cb::mcbp::ClientOpcode::DcpNoop:
                bytes_read += producers.last_packet_size;
                all_bytes += producers.last_packet_size;
                break;
            case cb::mcbp::ClientOpcode::DcpSeqnoAdvanced:
                checkeq(stream_ctxs[vbid.get()].exp_mutations,
                        stats.num_mutations,
                        "Seqno is not at end of Snapshot");
                checklt(stats.last_by_seqno,
                        producers.last_byseqno.load(),
                        "Check correct seqno");
                stats.number_of_seqno_advanced++;
                stats.last_by_seqno = producers.last_byseqno;
                bytes_read += producers.last_packet_size;
                all_bytes += producers.last_packet_size;
                break;
            case cb::mcbp::ClientOpcode::DcpSystemEvent:
                bytes_read += producers.last_packet_size;
                all_bytes += producers.last_packet_size;
                stats.number_of_system_events++;
                stats.collections.push_back(producers.last_collection_id);
                break;
            default:
                // Aborting ...
                std::stringstream ss;
                ss << "Unknown DCP operation: " << to_string(producers.last_op);
                check(false, ss.str().c_str());
            }
            producers.last_op = cb::mcbp::ClientOpcode::Invalid;
            producers.last_nru = 0;
            producers.last_vbucket = Vbid(-1);
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
                    checkge(ctx.exp_mutations, stats.num_mutations,
                          "Invalid number of mutations");
                    checkge(ctx.exp_deletions, stats.num_deletions,
                          "Invalid number of deletes");
                    checkge(ctx.exp_expirations,
                            stats.num_expirations,
                            "Invalid number of expirations");
                }
            } else {
                // Account for cursors that may have been dropped because
                // of high memory usage
                if (get_int_stat(h, "ep_cursors_dropped") > 0) {
                    // Hard to predict exact number of markers to be received
                    // if in case of a live parallel front end load
                    if (!ctx.live_frontend_client) {
                        checkle(stats.num_snapshot_markers, ctx.exp_markers,
                                "Invalid number of markers");
                    }
                    checkle(stats.num_mutations, ctx.exp_mutations,
                            "Invalid number of mutations");
                    checkle(stats.num_deletions, ctx.exp_deletions,
                            "Invalid number of deletions");
                    checkle(stats.num_expirations,
                            ctx.exp_expirations,
                            "Invalid number of expirations");
                } else {
                    checkeq(ctx.exp_mutations, stats.num_mutations,
                            "Invalid number of mutations");
                    checkeq(ctx.exp_deletions, stats.num_deletions,
                            "Invalid number of deletes");
                    checkeq(ctx.exp_expirations,
                            stats.num_expirations,
                            "Invalid number of expirations");
                    if (ctx.live_frontend_client) {
                        // Hard to predict exact number of markers to be received
                        // if in case of a live parallel front end load
                        if (ctx.exp_mutations > 0 || ctx.exp_deletions > 0 ||
                            ctx.exp_expirations > 0) {
                            checkle(size_t{1},
                                    stats.num_snapshot_markers,
                                    "Snapshot marker count can't be zero");
                        }
                    } else {
                        checkeq(ctx.exp_markers, stats.num_snapshot_markers,
                                "Unexpected number of snapshot markers");
                    }
                }
            }

            if (ctx.flags & DCP_ADD_STREAM_FLAG_TAKEOVER) {
                checkeq(size_t{1},
                        stats.num_set_vbucket_pending,
                        "Didn't receive pending set state");
                checkeq(size_t{1},
                        stats.num_set_vbucket_active,
                        "Didn't receive active set state");
            }

            /* Check if the readyQ size goes to zero after all items are streamed */
            if (exp_all_items_streamed) {
                std::stringstream stats_ready_queue_memory;
                stats_ready_queue_memory << "eq_dcpq:" << name.c_str()
                                         << ":stream_" << ctx.vbucket.get()
                                         << "_ready_queue_memory";
                checkeq(uint64_t{0},
                        get_ull_stat(h,
                                     stats_ready_queue_memory.str().c_str(),
                                     "dcp"),
                        "readyQ size did not go to zero");

                std::string stats_backfill_buffer_items(
                        "eq_dcpq:" + name + ":stream_" +
                        std::to_string(ctx.vbucket.get()) +
                        "_backfill_buffer_items");
                checkeq(uint64_t{0},
                        get_ull_stat(
                                h, stats_backfill_buffer_items.c_str(), "dcp"),
                        "backfill buffer items did not go to zero");
            }
            if (ctx.expected_values) {
                checkeq(ctx.expected_values,
                        stats.num_values,
                        "Expected values didn't match");
            }
            checkeq(ctx.exp_seqno_advanced,
                    stats.number_of_seqno_advanced,
                    "Number of expected SeqnoAdvanced ops send to the consumer "
                    "is incorrect");
            checkeq(ctx.exp_system_events,
                    stats.number_of_system_events,
                    "Number of expected SystemEvent ops sent to the consumer "
                    "is incorrect");
            if (!ctx.exp_collection_ids.empty()) {
                check(ctx.exp_collection_ids == stats.collections,
                      "Expected collections IDs does not match the ones seen "
                      "from SystemEvents");
            }
        }
    }

    /* Check if the producer has updated flow control stat correctly */
    if (flow_control_buf_size) {
        char stats_buffer[50] = {0};
        snprintf(stats_buffer, sizeof(stats_buffer), "eq_dcpq:%s:unacked_bytes",
                 name.c_str());
        checkeq((all_bytes - total_acked_bytes),
                get_ull_stat(h, stats_buffer, "dcp"),
                "Buffer Size did not get set correctly");
    }
}

void TestDcpConsumer::stop() {
    this->done = true;
}

void TestDcpConsumer::openConnection(uint32_t flags) {
    /* Reset any stale dcp data */
    producers.clear_dcp_data();

    opaque = 1;

    /* Set up Producer at server */
    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie,
                      ++opaque,
                      0,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp producer open connection.");

    /* Set flow control buffer size */
    std::string flow_control_buf_sz(std::to_string(flow_control_buf_size));
    checkeq(ENGINE_SUCCESS,
            dcp->control(cookie,
                         ++opaque,
                         "connection_buffer_size",
                         flow_control_buf_sz),
            "Failed to establish connection buffer");
    char stats_buffer[50] = {0};
    if (flow_control_buf_size) {
        snprintf(stats_buffer, sizeof(stats_buffer),
                 "eq_dcpq:%s:max_buffer_bytes", name.c_str());
        checkeq(static_cast<int>(flow_control_buf_size),
                get_int_stat(h, stats_buffer, "dcp"),
                "TestDcpConsumer::openConnection() : "
                "Buffer Size did not get set correctly");
    } else {
        snprintf(stats_buffer, sizeof(stats_buffer),
                 "eq_dcpq:%s:flow_control", name.c_str());
        std::string status = get_str_stat(h, stats_buffer, "dcp");
        checkeq(status, std::string("disabled"), "Flow control enabled!");
    }

    checkeq(ENGINE_SUCCESS,
            dcp->control(cookie, ++opaque, "enable_ext_metadata", "true"),
            "Failed to enable xdcr extras");
}

ENGINE_ERROR_CODE TestDcpConsumer::openStreams() {
    for (auto& ctx : stream_ctxs) {
        /* Different opaque for every stream created */
        ++opaque;

        /* Initiate stream request */
        uint64_t rollback = 0;
        ENGINE_ERROR_CODE rv = dcp->stream_req(cookie,
                                               ctx.flags,
                                               opaque,
                                               ctx.vbucket,
                                               ctx.seqno.start,
                                               ctx.seqno.end,
                                               ctx.vb_uuid,
                                               ctx.snapshot.start,
                                               ctx.snapshot.end,
                                               &rollback,
                                               mock_dcp_add_failover_log,
                                               collectionFilter);

        checkeq(ctx.exp_err, rv, "Failed to initiate stream request");

        if (rv == ENGINE_NOT_MY_VBUCKET || rv == ENGINE_ENOTSUP) {
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
            std::string high_seqno("vb_" + std::to_string(ctx.vbucket.get()) +
                                   ":high_seqno");
            ctx.seqno.end =
                    get_ull_stat(h, high_seqno.c_str(), "vbucket-seqno");
        }

        std::stringstream stats_flags;
        stats_flags << "eq_dcpq:" << name.c_str() << ":stream_"
                    << ctx.vbucket.get() << "_flags";
        checkeq(ctx.flags,
                (uint32_t)get_int_stat(h, stats_flags.str().c_str(), "dcp"),
                "Flags didn't match");

        std::stringstream stats_opaque;
        stats_opaque << "eq_dcpq:" << name.c_str() << ":stream_"
                     << ctx.vbucket.get() << "_opaque";
        checkeq(opaque,
                (uint32_t)get_int_stat(h, stats_opaque.str().c_str(), "dcp"),
                "Opaque didn't match");
        ctx.opaque = opaque;

        std::stringstream stats_start_seqno;
        stats_start_seqno << "eq_dcpq:" << name.c_str() << ":stream_"
                          << ctx.vbucket.get() << "_start_seqno";
        checkeq(ctx.seqno.start,
                (uint64_t)get_ull_stat(
                        h, stats_start_seqno.str().c_str(), "dcp"),
                "Start Seqno Didn't match");

        std::stringstream stats_end_seqno;
        stats_end_seqno << "eq_dcpq:" << name.c_str() << ":stream_"
                        << ctx.vbucket.get() << "_end_seqno";
        checkeq(ctx.seqno.end,
                (uint64_t)get_ull_stat(h, stats_end_seqno.str().c_str(), "dcp"),
                "End Seqno didn't match");

        std::stringstream stats_vb_uuid;
        stats_vb_uuid << "eq_dcpq:" << name.c_str() << ":stream_"
                      << ctx.vbucket.get() << "_vb_uuid";
        checkeq(ctx.vb_uuid,
                (uint64_t)get_ull_stat(h, stats_vb_uuid.str().c_str(), "dcp"),
                "VBucket UUID didn't match");

        std::stringstream stats_snap_seqno;
        stats_snap_seqno << "eq_dcpq:" << name.c_str() << ":stream_"
                         << ctx.vbucket.get() << "_snap_start_seqno";
        checkeq(ctx.snapshot.start,
                (uint64_t)get_ull_stat(
                        h, stats_snap_seqno.str().c_str(), "dcp"),
                "snap start seqno didn't match");

        if ((ctx.flags & DCP_ADD_STREAM_FLAG_TAKEOVER) &&
            !ctx.skip_estimate_check) {
            std::string high_seqno_str(
                    "vb_" + std::to_string(ctx.vbucket.get()) + ":high_seqno");
            uint64_t vb_high_seqno =
                    get_ull_stat(h, high_seqno_str.c_str(), "vbucket-seqno");
            uint64_t est = vb_high_seqno - ctx.seqno.start;
            std::stringstream stats_takeover;
            stats_takeover << "dcp-vbtakeover " << ctx.vbucket.get() << " "
                           << name.c_str();
            wait_for_stat_to_be_lte(
                    h, "estimate", est, stats_takeover.str().c_str());
        }

        if (ctx.flags & DCP_ADD_STREAM_FLAG_DISKONLY) {
            /* Wait for backfill to start */
            std::string stats_backfill_read_bytes("eq_dcpq:" + name +
                                                  ":backfill_buffer_bytes_read");
            wait_for_stat_to_be_gte(
                    h, stats_backfill_read_bytes.c_str(), 0, "dcp");
            /* Verify that we have no dcp cursors in the checkpoint. (There will
             just be one persistence cursor) */
            std::string stats_num_conn_cursors(
                    "vb_" + std::to_string(ctx.vbucket.get()) +
                    ":num_conn_cursors");
            /* In case of persistent buckets there will be 1 persistent cursor,
               in case of ephemeral buckets there will be no cursor */
            checkge(1,
                    get_int_stat(h, stats_num_conn_cursors.c_str(),
                            "checkpoint"),
                    "DCP cursors not expected to be registered");
        }

        // Init stats used in test
        VBStats stats;
        stats.extra_takeover_ops = ctx.extra_takeover_ops;
        stats.exp_disk_snapshot = ctx.exp_disk_snapshot;
        stats.exp_conflict_res = ctx.exp_conflict_res;

        vb_stats[ctx.vbucket] = stats;
    }
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE TestDcpConsumer::closeStreams(bool fClear) {
    ENGINE_ERROR_CODE err = ENGINE_SUCCESS;
    for (auto& ctx : stream_ctxs) {
        if (ctx.opaque > 0) {
            err = dcp->close_stream(cookie, ctx.opaque, Vbid(0), {});
            if (ENGINE_SUCCESS != err) {
                break;
            }
        }
    }

    if (fClear) {
        stream_ctxs.clear();
    }
    return err;
}

static void notifier_request(EngineIface* h,
                             const void* cookie,
                             uint32_t opaque,
                             Vbid vbucket,
                             uint64_t start,
                             bool shouldSucceed) {
    uint32_t flags = 0;
    uint64_t rollback = 0;
    uint64_t vb_uuid = get_ull_stat(h, "vb_0:0:id", "failovers");
    uint64_t snap_start_seqno = get_ull_stat(h, "vb_0:0:seq", "failovers");
    uint64_t snap_end_seqno = snap_start_seqno;
    auto dcp = requireDcpIface(h);

    ENGINE_ERROR_CODE err = dcp->stream_req(cookie,
                                            flags,
                                            opaque,
                                            vbucket,
                                            start,
                                            0,
                                            vb_uuid,
                                            snap_start_seqno,
                                            snap_end_seqno,
                                            &rollback,
                                            mock_dcp_add_failover_log,
                                            {});
    checkeq(ENGINE_SUCCESS, err, "Failed to initiate stream request");

    std::string type = get_str_stat(h, "eq_dcpq:unittest:type", "dcp");
    checkeq("notifier"s, type, "Consumer not found");

    checkeq(flags,
            static_cast<uint32_t>(
                    get_int_stat(h, "eq_dcpq:unittest:stream_0_flags", "dcp")),
            "Flags didn't match");
    checkeq(opaque,
            static_cast<uint32_t>(
                    get_int_stat(h, "eq_dcpq:unittest:stream_0_opaque", "dcp")),
            "Opaque didn't match");
    checkeq(start,
            get_ull_stat(h, "eq_dcpq:unittest:stream_0_start_seqno", "dcp"),
            "Start Seqno Didn't match");
    checkeq(uint64_t{0},
            get_ull_stat(h, "eq_dcpq:unittest:stream_0_end_seqno", "dcp"),
            "End Seqno didn't match");
    checkeq(vb_uuid,
            get_ull_stat(h, "eq_dcpq:unittest:stream_0_vb_uuid", "dcp"),
            "VBucket UUID didn't match");
    checkeq(snap_start_seqno,
            get_ull_stat(h,"eq_dcpq:unittest:stream_0_snap_start_seqno", "dcp"),
            "snap start seqno didn't match");
}

static void dcp_stream_to_replica(EngineIface* h,
                                  const void* cookie,
                                  uint32_t opaque,
                                  Vbid vbucket,
                                  uint32_t flags,
                                  uint64_t start,
                                  uint64_t end,
                                  uint64_t snap_start_seqno,
                                  uint64_t snap_end_seqno,
                                  uint8_t cas = 0x1,
                                  uint8_t datatype = PROTOCOL_BINARY_RAW_BYTES,
                                  uint32_t exprtime = 0,
                                  uint32_t lockTime = 0,
                                  uint64_t revSeqno = 0,
                                  CollectionID cid = CollectionID::Default) {
    /* Send snapshot marker */
    auto dcp = requireDcpIface(h);
    checkeq(ENGINE_SUCCESS,
            dcp->snapshot_marker(cookie,
                                 opaque,
                                 vbucket,
                                 snap_start_seqno,
                                 snap_end_seqno,
                                 flags,
                                 0 /*HCS*/,
                                 {} /*maxVisibleSeqno*/),
            "Failed to send marker!");
    const std::string data("data");
    /* Send DCP mutations */
    for (uint64_t i = start; i <= end; i++) {
        const std::string key{"key" + std::to_string(i)};
        const StoredDocKey docKey(key, cid);
        const cb::const_byte_buffer value{(uint8_t*)data.data(), data.size()};
        checkeq(ENGINE_SUCCESS,
                dcp->mutation(cookie,
                              opaque,
                              docKey,
                              value,
                              0, // priv bytes
                              datatype,
                              cas,
                              vbucket,
                              flags,
                              i, // by seqno
                              revSeqno,
                              exprtime,
                              lockTime,
                              {},
                              INITIAL_NRU_VALUE),
                "Failed dcp mutate.");
    }
}

/* This is a helper function to read items from an existing DCP Producer. It
   reads items from start to end on the connection. (Note: this can work
   correctly only in case there is one vbucket)
   Currently this supports only streaming mutations, but can be extend to stream
   deletion etc */
static void dcp_stream_from_producer_conn(EngineIface* h,
                                          const void* cookie,
                                          uint32_t opaque,
                                          uint64_t start,
                                          uint64_t end,
                                          uint64_t expSnapStart,
                                          MockDcpMessageProducers& producers) {
    bool done = false;
    size_t bytes_read = 0;
    bool pending_marker_ack = false;
    uint64_t marker_end = 0;
    uint64_t num_mutations = 0;
    uint64_t last_snap_start_seqno = 0;
    auto dcp = requireDcpIface(h);

    do {
        if (bytes_read > 512) {
            checkeq(ENGINE_SUCCESS,
                    dcp->buffer_acknowledgement(
                            cookie, ++opaque, Vbid(0), bytes_read),
                    "Failed to get dcp buffer ack");
            bytes_read = 0;
        }
        ENGINE_ERROR_CODE err = dcp->step(cookie, &producers);
        if (err == ENGINE_DISCONNECT) {
            done = true;
        } else {
            switch (producers.last_op) {
            case cb::mcbp::ClientOpcode::DcpMutation:
                bytes_read += producers.last_packet_size;
                if (pending_marker_ack &&
                    producers.last_byseqno == marker_end) {
                    sendDcpAck(h,
                               cookie,
                               cb::mcbp::ClientOpcode::DcpSnapshotMarker,
                               cb::mcbp::Status::Success,
                               producers.last_opaque);
                }
                num_mutations++;
                break;
            case cb::mcbp::ClientOpcode::DcpStreamEnd:
                done = true;
                bytes_read += producers.last_packet_size;
                break;
            case cb::mcbp::ClientOpcode::DcpSnapshotMarker:
                if (producers.last_flags & 8) {
                    pending_marker_ack = true;
                    marker_end = producers.last_snap_end_seqno;
                }
                bytes_read += producers.last_packet_size;
                last_snap_start_seqno = producers.last_snap_start_seqno;
                break;
            case cb::mcbp::ClientOpcode::Invalid:
                break;
            default:
                // Aborting ...
                std::string err_string(
                        "Unexpected DCP operation: " +
                        to_string(producers.last_op) + " last_byseqno: " +
                        std::to_string(producers.last_byseqno.load()) +
                        " last_key: " + producers.last_key + " last_value: " +
                        producers.last_value + " last_flags: " +
                        std::to_string(producers.last_flags));
                check(false, err_string.c_str());
            }
            if (producers.last_byseqno >= end) {
                done = true;
            }
            producers.last_op = cb::mcbp::ClientOpcode::Invalid;
        }
    } while (!done);

    /* Do buffer ack of the outstanding bytes */
    dcp->buffer_acknowledgement(cookie, ++opaque, Vbid(0), bytes_read);
    checkeq((end - start + 1), num_mutations, "Invalid number of mutations");
    if (expSnapStart) {
        checkge(last_snap_start_seqno,
                expSnapStart,
                "Incorrect snap start seqno");
    }
}

static void dcp_stream_expiries_to_replica(EngineIface* h,
                                           const void* cookie,
                                           uint32_t opaque,
                                           Vbid vbucket,
                                           uint32_t flags,
                                           uint64_t start,
                                           uint64_t end,
                                           uint64_t snap_start_seqno,
                                           uint64_t snap_end_seqno,
                                           uint32_t delTime,
                                           uint64_t revSeqno = 0,
                                           uint8_t cas = 0x1) {
    auto dcp = requireDcpIface(h);
    checkeq(ENGINE_SUCCESS,
            dcp->snapshot_marker(cookie,
                                 opaque,
                                 vbucket,
                                 snap_start_seqno,
                                 snap_end_seqno,
                                 flags,
                                 0 /*HCS*/,
                                 {} /*maxVisibleSeqno*/),
            "Failed to send marker!");
    const std::string data("data");
    /* Stream Expiries */
    for (uint64_t i = start; i <= end; i++) {
        const std::string key{"key" + std::to_string(i)};
        const DocKey docKey{key, DocKeyEncodesCollectionId::No};
        checkeq(ENGINE_SUCCESS,
                dcp->expiration(cookie,
                                opaque,
                                docKey,
                                {},
                                0, // priv bytes
                                PROTOCOL_BINARY_RAW_BYTES,
                                cas,
                                vbucket,
                                i,
                                revSeqno,
                                delTime),
                "Failed dcp expiry");
    }
}

struct mb16357_ctx {
    mb16357_ctx(EngineIface* _h, int _items)
        : h(_h), dcp(requireDcpIface(h)), items(_items) {
    }

    EngineIface* h;
    gsl::not_null<DcpIface*> dcp;
    int items;
    std::mutex mutex;
    std::condition_variable cv;
    bool compactor_waiting{false};
    bool compaction_start{false};
};

struct writer_thread_ctx {
    EngineIface* h;
    int items;
    Vbid vbid;
};

struct continuous_dcp_ctx {
    EngineIface* h;
    const void *cookie;
    Vbid vbid;
    const std::string &name;
    uint64_t start_seqno;
    std::unique_ptr<TestDcpConsumer> dcpConsumer;
};

//Forward declaration required for dcp_thread_func
static uint32_t add_stream_for_consumer(EngineIface* h,
                                        const void* cookie,
                                        uint32_t opaque,
                                        Vbid vbucket,
                                        uint32_t flags,
                                        cb::mcbp::Status response,
                                        uint64_t exp_snap_start = 0,
                                        uint64_t exp_snap_end = 0);

extern "C" {
    static void dcp_thread_func(void *args) {
        auto *ctx = static_cast<mb16357_ctx *>(args);

        const void* cookie = testHarness->create_cookie(ctx->h);
        uint32_t opaque = 0xFFFF0000;
        uint32_t flags = 0;
        std::string name = "unittest";

        // Wait for compaction thread to to ready (and waiting on cv) - as
        // we don't want the nofify_one() to be lost.
        for (;;) {
            std::lock_guard<std::mutex> lh(ctx->mutex);
            if (ctx->compactor_waiting) {
                break;
            }
        };
        // Now compactor is waiting to run (and we are just about to start DCP
        // stream, activate compaction.
        {
            std::lock_guard<std::mutex> lh(ctx->mutex);
            ctx->compaction_start = true;
        }
        ctx->cv.notify_one();

        // Switch to replica
        check(set_vbucket_state(ctx->h, Vbid(0), vbucket_state_replica),
              "Failed to set vbucket state.");

        // Open consumer connection
        checkeq(ctx->dcp->open(cookie,
                               opaque,
                               0,
                               flags,
                               name,
                               R"({"consumer_name":"replica1"})"),
                ENGINE_SUCCESS,
                "Failed dcp Consumer open connection.");

        add_stream_for_consumer(ctx->h,
                                cookie,
                                opaque++,
                                Vbid(0),
                                0,
                                cb::mcbp::Status::Success);

        uint32_t stream_opaque =
                get_int_stat(ctx->h, "eq_dcpq:unittest:stream_0_opaque", "dcp");

        for (int i = 1; i <= ctx->items; i++) {
            std::stringstream ss;
            ss << "kamakeey-" << i;

            // send mutations in single mutation snapshots to race more with compaction
            ctx->dcp->snapshot_marker(cookie,
                                      stream_opaque,
                                      Vbid(0),
                                      ctx->items,
                                      ctx->items + i,
                                      2,
                                      0 /*HCS*/,
                                      {} /*maxVisibleSeqno*/);

            const std::string key = ss.str();
            const DocKey docKey{key, DocKeyEncodesCollectionId::No};
            ctx->dcp->mutation(cookie,
                               stream_opaque,
                               docKey,
                               {(const uint8_t*)"value", 5},
                               0, // priv bytes
                               PROTOCOL_BINARY_RAW_BYTES,
                               i * 3, // cas
                               Vbid(0),
                               0, // flags
                               i + ctx->items, // by_seqno
                               i + ctx->items, // rev_seqno
                               0, // exptime
                               0, // locktime
                               {}, // meta
                               INITIAL_NRU_VALUE);
        }

        testHarness->destroy_cookie(cookie);
    }

    static void compact_thread_func(void *args) {
        auto *ctx = static_cast<mb16357_ctx *>(args);
        std::unique_lock<std::mutex> lk(ctx->mutex);
        ctx->compactor_waiting = true;
        ctx->cv.wait(lk, [ctx]{return ctx->compaction_start;});
        compact_db(ctx->h, Vbid(0), Vbid(0), 99, ctx->items, 1);
    }

    static void writer_thread(void *args) {
        auto *wtc = static_cast<writer_thread_ctx *>(args);

        for (int i = 0; i < wtc->items; ++i) {
            std::string key("key_" + std::to_string(i));
            checkeq(ENGINE_SUCCESS,
                    store(wtc->h,
                          nullptr,
                          OPERATION_SET,
                          key.c_str(),
                          "somevalue",
                          nullptr,
                          0,
                          wtc->vbid),
                    "Failed to store value");
        }
    }

    static void continuous_dcp_thread(void *args) {
        auto *cdc = static_cast<continuous_dcp_ctx *>(args);

        DcpStreamCtx ctx;
        ctx.vbucket = cdc->vbid;
        std::string vbuuid_entry("vb_" + std::to_string(cdc->vbid.get()) +
                                 ":0:id");
        ctx.vb_uuid = get_ull_stat(cdc->h, vbuuid_entry.c_str(), "failovers");
        ctx.seqno = {cdc->start_seqno, std::numeric_limits<uint64_t>::max()};
        ctx.snapshot = {cdc->start_seqno, cdc->start_seqno};
        ctx.skip_verification = true;

        cdc->dcpConsumer->addStreamCtx(ctx);
        cdc->dcpConsumer->run();
    }
}

/* DCP step thread that keeps running till it reads upto 'exp_mutations'.
   Note: the exp_mutations is cumulative across all streams in the DCP
         connection */
static void dcp_waiting_step(EngineIface* h,
                             const void* cookie,
                             uint32_t opaque,
                             uint64_t exp_mutations,
                             MockDcpMessageProducers& producers) {
    bool done = false;
    size_t bytes_read = 0;
    bool pending_marker_ack = false;
    uint64_t marker_end = 0;
    uint64_t num_mutations = 0;

    auto dcp = requireDcpIface(h);

    do {
        if (bytes_read > 512) {
            checkeq(ENGINE_SUCCESS,
                    dcp->buffer_acknowledgement(
                            cookie, ++opaque, Vbid(0), bytes_read),
                    "Failed to get dcp buffer ack");
            bytes_read = 0;
        }
        ENGINE_ERROR_CODE err = dcp->step(cookie, &producers);
        if (err == ENGINE_DISCONNECT) {
            done = true;
        } else {
            switch (producers.last_op) {
            case cb::mcbp::ClientOpcode::DcpMutation:
                bytes_read += producers.last_packet_size;
                if (pending_marker_ack &&
                    producers.last_byseqno == marker_end) {
                    sendDcpAck(h,
                               cookie,
                               cb::mcbp::ClientOpcode::DcpSnapshotMarker,
                               cb::mcbp::Status::Success,
                               producers.last_opaque);
                }
                ++num_mutations;
                break;
            case cb::mcbp::ClientOpcode::DcpStreamEnd:
                done = true;
                bytes_read += producers.last_packet_size;
                break;
            case cb::mcbp::ClientOpcode::DcpSnapshotMarker:
                if (producers.last_flags & 8) {
                    pending_marker_ack = true;
                    marker_end = producers.last_snap_end_seqno;
                }
                bytes_read += producers.last_packet_size;
                break;
            case cb::mcbp::ClientOpcode::Invalid:
                /* No messages were ready on the last step call, so we
                 * wait till the conn is notified of new item.
                 * Note that we check for 0 because we clear the
                 * producers.last_op value below.
                 */
                testHarness->lock_cookie(cookie);
                /* waitfor_cookie() waits on a condition variable. But
                   the api expects the cookie to be locked before
                   calling it */
                wait_started = true;
                testHarness->waitfor_cookie(cookie);
                testHarness->unlock_cookie(cookie);
                break;
            default:
                // Aborting ...
                std::string err_string("Unexpected DCP operation: " +
                                       to_string(producers.last_op));
                check(false, err_string.c_str());
            }
            if (num_mutations >= exp_mutations) {
                done = true;
            }
            producers.last_op = cb::mcbp::ClientOpcode::Invalid;
        }
    } while (!done);

    /* Do buffer ack of the outstanding bytes */
    dcp->buffer_acknowledgement(cookie, ++opaque, Vbid(0), bytes_read);
}

// Testcases //////////////////////////////////////////////////////////////////

static enum test_result test_dcp_vbtakeover_no_stream(EngineIface* h) {
    write_items(h, 10);
    if (isPersistentBucket(h) && is_full_eviction(h)) {
        // MB-21646: FE mode - curr_items (which is part of "estimate") is
        // updated as part of flush, and thus if the writes are flushed in
        // blocks < 10 we may see an estimate < 10
        wait_for_flusher_to_settle(h);
    }

    const auto est = get_int_stat(h, "estimate", "dcp-vbtakeover 0");
    checkeq(10, est, "Invalid estimate for non-existent stream");
    checkeq(ENGINE_NOT_MY_VBUCKET,
            get_stats(h, "dcp-vbtakeover 1"sv, {}, add_stats),
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

static enum test_result test_dcp_notifier_open(EngineIface* h) {
    const auto* cookie1 = testHarness->create_cookie(h);
    const std::string name("unittest");
    const uint32_t seqno = 0;
    uint32_t opaque = 0;
    auto dcp = requireDcpIface(h);

    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie1,
                      opaque,
                      seqno,
                      cb::mcbp::request::DcpOpenPayload::Notifier,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp consumer open connection.");

    const std::string stat_type("eq_dcpq:" + name + ":type");
    auto type = get_str_stat(h, stat_type.c_str(), "dcp");
    const std::string stat_created("eq_dcpq:" + name + ":created");
    const auto created = get_int_stat(h, stat_created.c_str(), "dcp");
    checkeq(0, type.compare("notifier"), "Notifier not found");
    testHarness->destroy_cookie(cookie1);

    testHarness->time_travel(600);

    const auto* cookie2 = testHarness->create_cookie(h);
    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie2,
                      opaque,
                      seqno,
                      cb::mcbp::request::DcpOpenPayload::Notifier,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp consumer open connection.");

    type = get_str_stat(h, stat_type.c_str(), "dcp");
    checkeq(0, type.compare("notifier"), "Notifier not found");
    checkle((created + 600), get_int_stat(h, stat_created.c_str(), "dcp"),
            "New dcp stream is not newer");
    testHarness->destroy_cookie(cookie2);

    return SUCCESS;
}

static enum test_result test_dcp_notifier(EngineIface* h) {
    write_items(h, 10);
    const auto* cookie = testHarness->create_cookie(h);
    const std::string name("unittest");
    const uint32_t seqno = 0;
    const Vbid vbucket = Vbid(0);
    uint32_t opaque = 0;
    uint64_t start = 0;
    auto dcp = requireDcpIface(h);
    MockDcpMessageProducers producers(h);

    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie,
                      opaque,
                      seqno,
                      cb::mcbp::request::DcpOpenPayload::Notifier,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp notifier open connection.");
    // Get notification for an old item
    notifier_request(h, cookie, ++opaque, vbucket, start, true);
    dcp_step(h, cookie, producers);
    checkeq(cb::mcbp::ClientOpcode::DcpStreamEnd,
            producers.last_op,
            "Expected stream end");
    // Get notification when we're slightly behind
    start += 9;
    notifier_request(h, cookie, ++opaque, vbucket, start, true);
    dcp_step(h, cookie, producers);
    checkeq(cb::mcbp::ClientOpcode::DcpStreamEnd,
            producers.last_op,
            "Expected stream end");
    // Wait for notification of a future item
    start += 11;
    notifier_request(h, cookie, ++opaque, vbucket, start, true);
    dcp_step(h, cookie, producers);
    for (auto j = 0; j < 5; ++j) {
        const auto key = "key" + std::to_string(j);
        checkeq(ENGINE_SUCCESS,
                store(h,
                      nullptr,
                      OPERATION_SET,
                      key.c_str(),
                      "data",
                      nullptr),
                "Failed to store a value");
    }
    // Shouldn't get a stream end yet
    dcp_step(h, cookie, producers);
    checkne(cb::mcbp::ClientOpcode::DcpStreamEnd,
            producers.last_op,
            "Wasn't expecting a stream end");
    for (auto j = 0; j < 6; ++j) {
        const auto key = "key" + std::to_string(j);
        checkeq(ENGINE_SUCCESS,
                store(h,
                      nullptr,
                      OPERATION_SET,
                      key.c_str(),
                      "data",
                      nullptr),
                "Failed to store a value");
    }
    // Should get a stream end
    dcp_step(h, cookie, producers);
    checkeq(cb::mcbp::ClientOpcode::DcpStreamEnd,
            producers.last_op,
            "Expected stream end");
    testHarness->destroy_cookie(cookie);

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
        EngineIface* h) {
    const std::string key("key0");
    checkeq(ENGINE_SUCCESS,
            store(h, nullptr, OPERATION_SET, key.c_str(), "data", nullptr),
            "Failed to store a value");

    const auto* cookie = testHarness->create_cookie(h);
    const std::string name("unittest");
    const uint32_t seqno = 0;
    const Vbid vbucket = Vbid(0);
    const uint64_t start = 1;
    uint32_t opaque = 0;
    auto dcp = requireDcpIface(h);
    MockDcpMessageProducers producers(h);

    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie,
                      opaque,
                      seqno,
                      cb::mcbp::request::DcpOpenPayload::Notifier,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp notifier open connection.");
    // Should not get a stream end
    notifier_request(h, cookie, ++opaque, vbucket, start, true);
    dcp_step(h, cookie, producers);
    checkne(cb::mcbp::ClientOpcode::DcpStreamEnd,
            producers.last_op,
            "Wasn't expecting a stream end");
    checkeq(ENGINE_SUCCESS,
            store(h, nullptr, OPERATION_SET, "key0", "data"),
            "Failed to store a value");
    dcp_step(h, cookie, producers);
    checkeq(cb::mcbp::ClientOpcode::DcpStreamEnd,
            producers.last_op,
            "Expected stream end");
    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_consumer_open(EngineIface* h) {
    const auto* cookie1 = testHarness->create_cookie(h);
    const std::string name("unittest");
    const uint32_t opaque = 0;
    const uint32_t seqno = 0;
    const uint32_t flags = 0;
    auto dcp = requireDcpIface(h);

    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie1,
                      opaque,
                      seqno,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp consumer open connection.");

    const auto stat_type("eq_dcpq:" + name + ":type");
    auto type = get_str_stat(h, stat_type.c_str(), "dcp");
    const auto stat_created("eq_dcpq:" + name + ":created");
    const auto created = get_int_stat(h, stat_created.c_str(), "dcp");
    checkeq(0, type.compare("consumer"), "Consumer not found");
    testHarness->destroy_cookie(cookie1);

    testHarness->time_travel(600);

    const auto* cookie2 = testHarness->create_cookie(h);
    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie2,
                      opaque,
                      seqno,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp consumer open connection.");

    type = get_str_stat(h, stat_type.c_str(), "dcp");
    checkeq(0, type.compare("consumer"), "Consumer not found");
    checklt(created, get_int_stat(h, stat_created.c_str(), "dcp"),
            "New dcp stream is not newer");
    testHarness->destroy_cookie(cookie2);

    return SUCCESS;
}

static enum test_result test_dcp_consumer_flow_control_none(EngineIface* h) {
    const auto* cookie1 = testHarness->create_cookie(h);
    const std::string name("unittest");
    const uint32_t opaque = 0;
    const uint32_t seqno = 0;
    const uint32_t flags = 0;
    auto dcp = requireDcpIface(h);

    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie1,
                      opaque,
                      seqno,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp consumer open connection.");

    const auto stat_name("eq_dcpq:" + name + ":max_buffer_bytes");
    checkeq(0,
            get_int_stat(h, stat_name.c_str(), "dcp"),
            "Flow Control Buffer Size not zero");
    testHarness->destroy_cookie(cookie1);

    return SUCCESS;
}

static enum test_result test_dcp_consumer_flow_control_static(EngineIface* h) {
    const auto* cookie1 = testHarness->create_cookie(h);
    const std::string name("unittest");
    const uint32_t opaque = 0;
    const uint32_t seqno = 0;
    const uint32_t flags = 0;
    const auto flow_ctl_buf_def_size = 10485760;
    auto dcp = requireDcpIface(h);

    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie1,
                      opaque,
                      seqno,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp consumer open connection.");

    const auto stat_name("eq_dcpq:" + name + ":max_buffer_bytes");
    checkeq(flow_ctl_buf_def_size,
            get_int_stat(h, stat_name.c_str(), "dcp"),
            "Flow Control Buffer Size not equal to default value");
    testHarness->destroy_cookie(cookie1);

    return SUCCESS;
}

static enum test_result test_dcp_consumer_flow_control_dynamic(EngineIface* h) {
    const auto* cookie1 = testHarness->create_cookie(h);
    const std::string name("unittest");
    const uint32_t opaque = 0;
    const uint32_t seqno = 0;
    const uint32_t flags = 0;
    /* Check the min limit */
    set_param(h,
              cb::mcbp::request::SetParamPayload::Type::Flush,
              "max_size",
              "500000000");
    checkeq(500000000, get_int_stat(h, "ep_max_size"), "Incorrect new size.");

    auto dcp = requireDcpIface(h);
    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie1,
                      opaque,
                      seqno,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp consumer open connection.");

    const auto stat_name("eq_dcpq:" + name + ":max_buffer_bytes");
    checkeq(10485760,
            get_int_stat(h, stat_name.c_str(), "dcp"),
            "Flow Control Buffer Size not equal to min");
    testHarness->destroy_cookie(cookie1);

    /* Check the size as percentage of the bucket memory */
    const auto* cookie2 = testHarness->create_cookie(h);
    set_param(h,
              cb::mcbp::request::SetParamPayload::Type::Flush,
              "max_size",
              "2000000000");
    checkeq(2000000000, get_int_stat(h, "ep_max_size"), "Incorrect new size.");

    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie2,
                      opaque,
                      seqno,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp consumer open connection.");

    checkeq(20000000,
            get_int_stat(h, stat_name.c_str(), "dcp"),
            "Flow Control Buffer Size not equal to 1% of mem size");
    testHarness->destroy_cookie(cookie2);

    /* Check the case when mem used by flow control bufs hit the threshold */
    /* Create around 10 more connections to use more than 10% of the total
       memory */
    for (auto count = 0; count < 10; count++) {
        const auto* cookie = testHarness->create_cookie(h);
        checkeq(ENGINE_SUCCESS,
                dcp->open(cookie,
                          opaque,
                          seqno,
                          flags,
                          name,
                          R"({"consumer_name":"replica1"})"),
                "Failed dcp consumer open connection.");
        testHarness->destroy_cookie(cookie);
    }
    /* By now mem used by flow control bufs would have crossed the threshold */
    const auto* cookie3 = testHarness->create_cookie(h);
    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie3,
                      opaque,
                      seqno,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp consumer open connection.");

    checkeq(10485760,
            get_int_stat(h, stat_name.c_str(), "dcp"),
            "Flow Control Buffer Size not equal to min after threshold is hit");
    testHarness->destroy_cookie(cookie3);

    /* Check the max limit */
    const auto* cookie4 = testHarness->create_cookie(h);
    set_param(h,
              cb::mcbp::request::SetParamPayload::Type::Flush,
              "max_size",
              "7000000000");
    checkeq(static_cast<uint64_t>(7000000000),
            get_ull_stat(h, "ep_max_size"),
            "Incorrect new size.");

    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie4,
                      opaque,
                      seqno,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp consumer open connection.");

    checkeq(52428800,
            get_int_stat(h, stat_name.c_str(), "dcp"),
            "Flow Control Buffer Size beyond max");
    testHarness->destroy_cookie(cookie4);

    return SUCCESS;
}

static enum test_result test_dcp_consumer_flow_control_aggressive(
        EngineIface* h) {
    const auto max_conns = 6;
    const void *cookie[max_conns];
    const auto flow_ctl_buf_max = 52428800;
    const auto flow_ctl_buf_min = 10485760;
    const auto ep_max_size = 1200000000;
    const auto bucketMemQuotaFraction = 0.05;
    set_param(h,
              cb::mcbp::request::SetParamPayload::Type::Flush,
              "max_size",
              std::to_string(ep_max_size).c_str());
    checkeq(ep_max_size, get_int_stat(h, "ep_max_size"), "Incorrect new size.");

    std::vector<Vbid> vbuckets = {
            {Vbid{1}, Vbid{2}, Vbid{3}, Vbid{4}, Vbid{5}, Vbid{6}}};
    checkeq(std::size_t(max_conns),
            vbuckets.size(),
            "I need one vbucket per cookie");
    for (const auto& vb : vbuckets) {
        check(set_vbucket_state(h, vb, vbucket_state_replica),
              "Failed to set VBucket state.");
    }

    /* Create first connection */
    const std::string name("unittest_");
    const auto name1(name + "0");
    const uint32_t opaque = 0;
    const uint32_t seqno = 0;
    const uint32_t flags = 0;
    cookie[0] = testHarness->create_cookie(h);
    auto dcp = requireDcpIface(h);

    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie[0],
                      opaque,
                      seqno,
                      flags,
                      name1,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp consumer open connection.");

    checkeq(ENGINE_SUCCESS,
            dcp->add_stream(cookie[0], 0, vbuckets[0], 0),
            "Failed to set up stream");

    /* Check the max limit */
    auto stat_name = "eq_dcpq:" + name1 + ":max_buffer_bytes";
    checkeq(flow_ctl_buf_max,
            get_int_stat(h, stat_name.c_str(), "dcp"),
            "Flow Control Buffer Size not equal to max");

    /* Create at least 4 more connections */
    for (auto count = 1; count < max_conns - 1; count++) {
        cookie[count] = testHarness->create_cookie(h);
        const auto name2(name + std::to_string(count));
        checkeq(ENGINE_SUCCESS,
                dcp->open(cookie[count],
                          opaque,
                          seqno,
                          flags,
                          name2,
                          R"({"consumer_name":"replica1"})"),
                "Failed dcp consumer open connection.");

        checkeq(ENGINE_SUCCESS,
                dcp->add_stream(cookie[count], 0, vbuckets[count], 0),
                "Failed to set up stream");

        for (auto i = 0; i <= count; i++) {
            /* Check if the buffer size of all connections has changed */
            const auto stat_name("eq_dcpq:" + name + std::to_string(i) +
                               ":max_buffer_bytes");
            checkeq((int)((ep_max_size * bucketMemQuotaFraction) / (count + 1)),
                    get_int_stat(h, stat_name.c_str(), "dcp"),
                    "Flow Control Buffer Size not correct");
        }
    }

    /* Opening another connection should set the buffer size to min value */
    cookie[max_conns - 1] = testHarness->create_cookie(h);
    const auto name3(name + std::to_string(max_conns - 1));
    const auto stat_name2("eq_dcpq:" + name3 + ":max_buffer_bytes");
    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie[max_conns - 1],
                      opaque,
                      seqno,
                      flags,
                      name3,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp consumer open connection.");

    checkeq(ENGINE_SUCCESS,
            dcp->add_stream(
                    cookie[max_conns - 1], 0, vbuckets[max_conns - 1], 0),
            "Failed to set up stream");

    checkeq(flow_ctl_buf_min,
            get_int_stat(h, stat_name2.c_str(), "dcp"),
            "Flow Control Buffer Size not equal to min");

    /* Disconnect connections and see if flow control
     * buffer size of existing conns increase
     */
    for (auto count = 0; count < max_conns / 2; count++) {
        testHarness->destroy_cookie(cookie[count]);
    }
    /* Wait for disconnected connections to be deleted */
    wait_for_stat_to_be(h, "ep_dcp_dead_conn_count", 0, "dcp");

    /* Check if the buffer size of all connections has increased */
    const auto exp_buf_size = (int)((ep_max_size * bucketMemQuotaFraction) /
                              (max_conns - (max_conns / 2)));

    /* Also check if we get control message indicating the flow control buffer
       size change from the consumer connections */
    MockDcpMessageProducers producers(h);

    for (auto i = max_conns / 2; i < max_conns; i++) {
        /* Check if the buffer size of all connections has changed */
        const auto name4(name + std::to_string(i));
        const auto stat_name3("eq_dcpq:" + name4 + ":max_buffer_bytes");
        checkeq(exp_buf_size,
                get_int_stat(h, stat_name3.c_str(), "dcp"),
                "Flow Control Buffer Size not correct");
        checkeq(ENGINE_SUCCESS,
                dcp->step(cookie[i], &producers),
                "Pending flow control buffer change not processed");
        checkeq(cb::mcbp::ClientOpcode::DcpControl,
                producers.last_op,
                "Flow ctl buf size change control message not received");
        checkeq(0,
                producers.last_key.compare("connection_buffer_size"),
                "Flow ctl buf size change control message key error");
        checkeq(exp_buf_size,
                atoi(producers.last_value.c_str()),
                "Flow ctl buf size in control message not correct");
    }
    /* Disconnect remaining connections */
    for (auto count = max_conns / 2; count < max_conns; count++) {
        testHarness->destroy_cookie(cookie[count]);
    }

    return SUCCESS;
}

static enum test_result test_dcp_producer_open(EngineIface* h) {
    const auto* cookie1 = testHarness->create_cookie(h);
    const std::string name("unittest");
    const uint32_t opaque = 0;
    const uint32_t seqno = 0;
    auto dcp = requireDcpIface(h);

    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie1,
                      opaque,
                      seqno,
                      cb::mcbp::request::DcpOpenPayload::Producer,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp producer open connection.");
    const auto stat_type("eq_dcpq:" + name + ":type");
    auto type = get_str_stat(h, stat_type.c_str(), "dcp");
    const auto stat_created("eq_dcpq:" + name + ":created");
    const auto created = get_int_stat(h, stat_created.c_str(), "dcp");
    checkeq(0, type.compare("producer"), "Producer not found");
    testHarness->destroy_cookie(cookie1);

    testHarness->time_travel(600);

    const auto* cookie2 = testHarness->create_cookie(h);
    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie2,
                      opaque,
                      seqno,
                      cb::mcbp::request::DcpOpenPayload::Producer,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp producer open connection.");
    type = get_str_stat(h, stat_type.c_str(), "dcp");
    checkeq(0, type.compare("producer"), "Producer not found");
    checklt(created, get_int_stat(h, stat_created.c_str(), "dcp"),
            "New dcp stream is not newer");
    testHarness->destroy_cookie(cookie2);

    return SUCCESS;
}

static enum test_result test_dcp_producer_open_same_cookie(EngineIface* h) {
    const auto* cookie = testHarness->create_cookie(h);
    const std::string name("unittest");
    uint32_t opaque = 0;
    const uint32_t seqno = 0;
    auto dcp = requireDcpIface(h);

    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie,
                      opaque,
                      seqno,
                      cb::mcbp::request::DcpOpenPayload::Producer,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp producer open connection.");

    const auto stat_type("eq_dcpq:" + name + ":type");
    auto type = get_str_stat(h, stat_type.c_str(), "dcp");
    checkeq(0, type.compare("producer"), "Producer not found");
    /*
     * Number of references is 2 (as opposed to 1) because a
     * mock_connstuct is initialised to having 1 reference
     * to represent a client being connected to it.
     */
    checkeq(2,
            testHarness->get_number_of_mock_cookie_references(cookie),
            "Number of cookie references is not two");
    /*
     * engine_data needs to be reset so that it passes the check that
     * a connection does not already exist on the same socket.
     */
    testHarness->store_engine_specific(cookie, nullptr);

    checkeq(ENGINE_DISCONNECT,
            dcp->open(cookie,
                      opaque++,
                      seqno,
                      cb::mcbp::request::DcpOpenPayload::Producer,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed to return ENGINE_DISCONNECT");

    checkeq(2,
            testHarness->get_number_of_mock_cookie_references(cookie),
            "Number of cookie references is not two");

    testHarness->destroy_cookie(cookie);

    checkeq(1,
            testHarness->get_number_of_mock_cookie_references(cookie),
            "Number of cookie references is not one");

    return SUCCESS;
}

static enum test_result test_dcp_noop(EngineIface* h) {
    const auto* cookie = testHarness->create_cookie(h);
    const std::string name("unittest");
    const uint32_t seqno = 0;
    uint32_t opaque = 0;
    auto dcp = requireDcpIface(h);
    MockDcpMessageProducers producers(h);

    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie,
                      opaque,
                      seqno,
                      cb::mcbp::request::DcpOpenPayload::Producer,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp producer open connection.");
    const std::string param1_name("connection_buffer_size");
    const std::string param1_value("1024");
    checkeq(ENGINE_SUCCESS,
            dcp->control(cookie, ++opaque, param1_name, param1_value),
            "Failed to establish connection buffer");
    const std::string param2_name("enable_noop");
    const std::string param2_value("true");
    checkeq(ENGINE_SUCCESS,
            dcp->control(cookie, ++opaque, param2_name, param2_value),
            "Failed to enable no-ops");

    testHarness->time_travel(201);

    auto done = false;
    while (!done) {
        if (dcp->step(cookie, &producers) == ENGINE_DISCONNECT) {
            done = true;
        } else if (producers.last_op == cb::mcbp::ClientOpcode::DcpNoop) {
            done = true;
            // Producer opaques are hard coded to start from 10M
            checkeq(10000001,
                    (int)producers.last_opaque,
                    "last_opaque != 10,000,001");
            const auto stat_name("eq_dcpq:" + name + ":noop_wait");
            checkeq(true,
                    get_bool_stat(h, stat_name.c_str(), "dcp"),
                    "Didn't send noop");
            sendDcpAck(h,
                       cookie,
                       cb::mcbp::ClientOpcode::DcpNoop,
                       cb::mcbp::Status::Success,
                       producers.last_opaque);
            checkeq(false,
                    get_bool_stat(h, stat_name.c_str(), "dcp"),
                    "Didn't ack noop");
        } else if (producers.last_op != cb::mcbp::ClientOpcode::Invalid) {
            abort();
        }
        producers.last_op = cb::mcbp::ClientOpcode::Invalid;
    }
    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_noop_fail(EngineIface* h) {
    const auto* cookie = testHarness->create_cookie(h);
    const std::string name("unittest");
    const uint32_t seqno = 0;
    uint32_t opaque = 0;
    auto dcp = requireDcpIface(h);

    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie,
                      opaque,
                      seqno,
                      cb::mcbp::request::DcpOpenPayload::Producer,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp producer open connection.");
    const std::string param1_name("connection_buffer_size");
    const std::string param1_value("1024");
    checkeq(ENGINE_SUCCESS,
            dcp->control(cookie, ++opaque, param1_name, param1_value),
            "Failed to establish connection buffer");
    const std::string param2_name("enable_noop");
    const std::string param2_value("true");
    checkeq(ENGINE_SUCCESS,
            dcp->control(cookie, ++opaque, param2_name, param2_value),
            "Failed to enable no-ops");

    testHarness->time_travel(201);

    MockDcpMessageProducers producers(h);
    while (dcp->step(cookie, &producers) != ENGINE_DISCONNECT) {
        if (producers.last_op == cb::mcbp::ClientOpcode::DcpNoop) {
            // Producer opaques are hard coded to start from 10M
            checkeq(10000001,
                    (int)producers.last_opaque,
                    "last_opaque != 10,000,001");
            const auto stat_name("eq_dcpq:" + name + ":noop_wait");
            checkeq(true,
                    get_bool_stat(h, stat_name.c_str(), "dcp"),
                    "Didn't send noop");
            testHarness->time_travel(201);
        } else if (producers.last_op != cb::mcbp::ClientOpcode::Invalid) {
            abort();
        }
    }
    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_consumer_noop(EngineIface* h) {
    check(set_vbucket_state(h, Vbid(0), vbucket_state_replica),
          "Failed to set vbucket state.");
    const auto* cookie = testHarness->create_cookie(h);
    const std::string name("unittest");
    const uint32_t seqno = 0;
    const uint32_t flags = 0;
    const Vbid vbucket = Vbid(0);
    uint32_t opaque = 0;
    auto dcp = requireDcpIface(h);

    // Open consumer connection
    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie,
                      opaque,
                      seqno,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp Consumer open connection.");
    add_stream_for_consumer(
            h, cookie, opaque, vbucket, flags, cb::mcbp::Status::Success);
    testHarness->time_travel(201);
    // No-op not recieved for 201 seconds. Should be ok.
    MockDcpMessageProducers producers(h);
    checkeq(ENGINE_EWOULDBLOCK,
            dcp->step(cookie, &producers),
            "Expected engine would block");

    testHarness->time_travel(200);

    // Message not recieved for over 400 seconds. Should disconnect.
    checkeq(ENGINE_DISCONNECT,
            dcp->step(cookie, &producers),
            "Expected engine disconnect");
    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}

/**
 * Creates a DCP producer stream with the specified values for noop_manditory,
 * noop_enabled, and XATTRs enabled, and then attempts to open a stream,
 * checking for the expectedResult code.
 */
static void test_dcp_noop_mandatory_combo(EngineIface* h,
                                          bool noopManditory,
                                          bool enableNoop,
                                          bool enableXAttrs,
                                          ENGINE_ERROR_CODE expectedResult) {
    const void* cookie = testHarness->create_cookie(h);

    // Configure manditory noop as requested.
    set_param(h,
              cb::mcbp::request::SetParamPayload::Type::Flush,
              "dcp_noop_mandatory_for_v5_features",
              noopManditory ? "true" : "false");
    checkeq(noopManditory,
            get_bool_stat(h, "ep_dcp_noop_mandatory_for_v5_features"),
            "Incorrect value for dcp_noop_mandatory_for_v5_features");

    // Create DCP consumer with requested flags.
    TestDcpConsumer tdc("dcp_noop_manditory_test", cookie, h);
    uint32_t flags = cb::mcbp::request::DcpOpenPayload::Producer;
    if (enableXAttrs) {
        flags |= cb::mcbp::request::DcpOpenPayload::IncludeXattrs;
    }
    tdc.openConnection(flags);

    // Setup noop on the DCP connection.
    checkeq(ENGINE_SUCCESS,
            tdc.sendControlMessage("enable_noop",
                                   enableNoop ? "true" : "false"),
            "Failed to configure noop");

    // Finally, attempt to create the stream and verify we get the expeced
    // response.
    DcpStreamCtx ctx;
    ctx.vb_uuid = get_ull_stat(h, "vb_0:0:id", "failovers");
    ctx.seqno = {0, static_cast<uint64_t>(-1)};
    ctx.exp_err = expectedResult;
    tdc.addStreamCtx(ctx);

    tdc.openStreams();

    testHarness->destroy_cookie(cookie);
}

static enum test_result test_dcp_noop_mandatory(EngineIface* h) {
    // Test all combinations of {manditoryNoop, enable_noop, includeXAttr}
    test_dcp_noop_mandatory_combo(h, false, false, false, ENGINE_SUCCESS);
    test_dcp_noop_mandatory_combo(h, false, false, true, ENGINE_SUCCESS);
    test_dcp_noop_mandatory_combo(h, false, true, false, ENGINE_SUCCESS);
    test_dcp_noop_mandatory_combo(h, false, true, true, ENGINE_SUCCESS);
    test_dcp_noop_mandatory_combo(h, true, false, false, ENGINE_SUCCESS);
    test_dcp_noop_mandatory_combo(h, true, false, true, ENGINE_ENOTSUP);
    test_dcp_noop_mandatory_combo(h, true, true, false, ENGINE_SUCCESS);
    test_dcp_noop_mandatory_combo(h, true, true, true, ENGINE_SUCCESS);

    return SUCCESS;
}

static enum test_result test_dcp_producer_stream_req_open(EngineIface* h) {
    const void* cookie = testHarness->create_cookie(h);
    const int num_items = 3;

    DcpStreamCtx ctx;
    ctx.vb_uuid = get_ull_stat(h, "vb_0:0:id", "failovers");
    ctx.seqno = {0, static_cast<uint64_t>(-1)};

    std::string name("unittest");
    TestDcpConsumer tdc(name.c_str(), cookie, h);
    tdc.addStreamCtx(ctx);

    tdc.openConnection();

    /* Create a separate thread that does tries to get any DCP items */
    std::thread dcp_step_thread(
            dcp_waiting_step, h, cookie, 0, num_items, std::ref(tdc.producers));

    /* We need to wait till the 'dcp_waiting_step' thread begins its wait */
    while (true) {
        /* Busy wait is ok here. To do a non busy wait we must use
         another condition variable which is an overkill here */
        testHarness->lock_cookie(cookie);
        if (wait_started) {
            testHarness->unlock_cookie(cookie);
            break;
        }
        testHarness->unlock_cookie(cookie);
    }

    /* Now create a stream */
    tdc.openStreams();

    /* Write items */
    write_items(h, num_items, 0);
    wait_for_flusher_to_settle(h);
    verify_curr_items(h, num_items, "Wrong amount of items");

    /* If the notification (to 'dcp_waiting_step' thread upon writing an item)
     mechanism is efficient, we must see the 'dcp_waiting_step' finish before
     test time out */
    dcp_step_thread.join();

    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_producer_stream_req_partial(EngineIface* h) {
    // Should start at checkpoint_id 2
    const auto initial_ckpt_id =
            get_int_stat(h, "vb_0:open_checkpoint_id", "checkpoint");
    checkeq(2, initial_ckpt_id, "Expected to start at checkpoint ID 2");

    // Create two 'full' checkpoints by storing exactly 2 x 'chk_max_items'
    // into the VBucket.
    const auto max_ckpt_items = get_int_stat(h, "ep_chk_max_items");

    write_items(h, max_ckpt_items);
    wait_for_flusher_to_settle(h);
    wait_for_stat_to_be(h, "ep_items_rm_from_checkpoints", max_ckpt_items);
    checkeq(initial_ckpt_id + 1,
            get_int_stat(h, "vb_0:open_checkpoint_id", "checkpoint"),
            "Expected #checkpoints to increase by 1 after storing items");

    write_items(h, max_ckpt_items, max_ckpt_items);
    wait_for_flusher_to_settle(h);
    wait_for_stat_to_be(h, "ep_items_rm_from_checkpoints", max_ckpt_items * 2);
    checkeq(initial_ckpt_id + 2,
            get_int_stat(h, "vb_0:open_checkpoint_id", "checkpoint"),
            "Expected #checkpoints to increase by 2 after storing 2x "
            "max_ckpt_items");

    // Stop persistece (so the persistence cursor cannot advance into the
    // deletions below, and hence de-dupe them with respect to the
    // additions we just did).
    stop_persistence(h);

    // Now delete half of the keys. Given that we have reached the
    // maximum checkpoint size above, all the deletes should be in a
    // subsequent checkpoint.
    for (int j = 0; j < max_ckpt_items; ++j) {
        std::stringstream ss;
        ss << "key" << j;
        checkeq(ENGINE_SUCCESS,
                del(h, ss.str().c_str(), 0, Vbid(0)),
                "Expected delete to succeed");
    }

    const void* cookie = testHarness->create_cookie(h);

    // Verify that we recieve full checkpoints when we only ask for
    // sequence numbers which lie within partial checkpoints.  We
    // should have the following Checkpoints in existence:
    //
    //   {  1,100} - MUTATE(key0..key99), from disk.
    //   {101,200} - MUTATE(key100.key199), from disk.
    //   {201,300} - DELETE(key0..key99), in memory (as persistence has been stopped).
    //
    // We request a start and end which lie in the middle of checkpoints -
    // start at 105 and end at 209. We should recieve to the end of
    // complete checkpoints, i.e. from 105 all the way to 300.
    DcpStreamCtx ctx;
    ctx.vb_uuid = get_ull_stat(h, "vb_0:0:id", "failovers");
    ctx.seqno = {105, 209};
    ctx.snapshot = {105, 105};
    ctx.exp_mutations = 95; // 105 to 200
    ctx.exp_deletions = 100; // 201 to 300

    if (isPersistentBucket(h)) {
        ctx.exp_markers = 2;
    } else {
        // the ephemeral stream request won't be broken into two snapshots of
        // backfill from disk vs the checkpoint in memory
        ctx.exp_markers = 1;
    }

    TestDcpConsumer tdc("unittest", cookie, h);
    tdc.addStreamCtx(ctx);
    tdc.run();

    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_producer_stream_req_full_merged_snapshots(
        EngineIface* h) {
    const int num_items = 300, batch_items = 100;
    for (int start_seqno = 0; start_seqno < num_items;
         start_seqno += batch_items) {
        wait_for_flusher_to_settle(h);
        write_items(h, batch_items, start_seqno);
    }

    wait_for_flusher_to_settle(h);
    verify_curr_items(h, num_items, "Wrong amount of items");
    wait_for_stat_to_be(h, "vb_0:num_checkpoints", 1, "checkpoint");

    const void* cookie = testHarness->create_cookie(h);

    DcpStreamCtx ctx;
    ctx.vb_uuid = get_ull_stat(h, "vb_0:0:id", "failovers");
    ctx.seqno = {0, get_ull_stat(h, "vb_0:high_seqno", "vbucket-seqno")};
    ctx.exp_mutations = num_items;
    /* Disk backfill sends all items in disk as one snapshot */
    ctx.exp_markers = 1;

    TestDcpConsumer tdc("unittest", cookie, h);
    tdc.addStreamCtx(ctx);
    tdc.run();

    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_producer_stream_req_full(EngineIface* h) {
    const int num_items = 300, batch_items = 100;
    for (int start_seqno = 0; start_seqno < num_items;
         start_seqno += batch_items) {
        wait_for_flusher_to_settle(h);
        write_items(h, batch_items, start_seqno);
    }

    wait_for_flusher_to_settle(h);
    verify_curr_items(h, num_items, "Wrong amount of items");
    wait_for_stat_to_be(h, "vb_0:num_checkpoints", 1, "checkpoint");

    checkne(num_items - get_stat<uint64_t>(h, "ep_items_rm_from_checkpoints"),
            uint64_t{0},
            "Require a non-zero number of items to still be present in "
            "CheckpointManager to be able to get 2x snapshot markers "
            "(1x disk, 1x memory)");

    const void* cookie = testHarness->create_cookie(h);

    DcpStreamCtx ctx;
    ctx.vb_uuid = get_ull_stat(h, "vb_0:0:id", "failovers");
    ctx.seqno = {0, get_ull_stat(h, "vb_0:high_seqno", "vbucket-seqno")};
    ctx.exp_mutations = num_items;
    /* Memory backfill sends items from checkpoint snapshots as much as possible
       Relies on backfill only when checkpoint snapshot is cleaned up */
    ctx.exp_markers = 2;

    TestDcpConsumer tdc("unittest", cookie, h);
    tdc.addStreamCtx(ctx);
    tdc.run();

    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}

/*
 * Test that deleted items (with values) backfill correctly
 */
static enum test_result test_dcp_producer_deleted_item_backfill(
        EngineIface* h) {
    const int deletions = 10;
    write_items(h,
                deletions,
                0,
                "del",
                "value",
                0 /*exp*/,
                Vbid(0),
                DocumentState::Deleted);
    wait_for_flusher_to_settle(h);

    const void* cookie = testHarness->create_cookie(h);

    DcpStreamCtx ctx;
    ctx.vb_uuid = get_ull_stat(h, "vb_0:0:id", "failovers");
    ctx.seqno = {0, deletions};
    ctx.exp_deletions = deletions;
    ctx.expected_values = deletions;
    ctx.flags |= DCP_ADD_STREAM_FLAG_DISKONLY;
    ctx.exp_markers = 1;

    TestDcpConsumer tdc("unittest", cookie, h);
    tdc.addStreamCtx(ctx);
    tdc.run();

    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}

// Function to parameterize whether expiries should be outputted or not.
static test_result testDcpProducerExpiredItemBackfill(
        EngineIface* h, EnableExpiryOutput enableExpiryOutput) {
    const int expiries = 5;
    const int start_seqno = 0;
    write_items(h,
                expiries,
                start_seqno,
                "exp",
                "value",
                5 /*exp*/,
                Vbid(0),
                DocumentState::Alive);

    wait_for_flusher_to_settle(h);
    verify_curr_items(h, expiries, "Wrong number of items");

    testHarness->time_travel(256);
    const void* cookie1 = testHarness->create_cookie(h);
    for (int i = 0; i < expiries; ++i) {
        std::string key("exp" + std::to_string(i + start_seqno));
        cb::EngineErrorItemPair ret =
                get(h, cookie1, key.c_str(), Vbid(0), DocStateFilter::Alive);
        checkeq(cb::engine_errc::no_such_key,
                ret.first,
                "Expected get to return 'no_such_key'");
    }
    testHarness->destroy_cookie(cookie1);

    wait_for_flusher_to_settle(h);
    checkeq(get_stat<int>(h, "vb_active_expired"),
            expiries,
            "Expected vb_active_expired to contain correct number of expiries");

    DcpStreamCtx ctx;
    ctx.vb_uuid = get_ull_stat(h, "vb_0:0:id", "failovers");
    ctx.seqno = {0, expiries * 2}; // doubled as each will cause a mutation,
    // as well as an expiry

    if (enableExpiryOutput == EnableExpiryOutput::Yes) {
        ctx.exp_expirations = expiries;
    } else {
        ctx.exp_deletions = expiries;
    }

    ctx.flags |= DCP_ADD_STREAM_FLAG_DISKONLY;
    ctx.exp_markers = 1;

    const void* cookie = testHarness->create_cookie(h);
    TestDcpConsumer tdc("unittest", cookie, h);
    uint32_t flags = cb::mcbp::request::DcpOpenPayload::Producer;

    if (enableExpiryOutput == EnableExpiryOutput::Yes) {
        // dcp expiry requires the connection to opt in to delete times
        flags |= cb::mcbp::request::DcpOpenPayload::IncludeDeleteTimes;
    }
    tdc.openConnection(flags);

    if (enableExpiryOutput == EnableExpiryOutput::Yes) {
        checkeq(ENGINE_SUCCESS,
                tdc.sendControlMessage("enable_expiry_opcode", "true"),
                "Failed to enable_expiry_opcode");
    }

    tdc.addStreamCtx(ctx);

    tdc.run(false);

    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_producer_stream_req_backfill(EngineIface* h) {
    const int num_items = 400, batch_items = 200;
    for (int start_seqno = 0; start_seqno < num_items;
         start_seqno += batch_items) {
        if (200 == start_seqno) {
            wait_for_flusher_to_settle(h);
            wait_for_stat_to_be(h, "ep_items_rm_from_checkpoints", 200);
            stop_persistence(h);
        }
        write_items(h, batch_items, start_seqno);
    }

    wait_for_stat_to_be_gte(h, "vb_0:num_checkpoints", 2, "checkpoint");

    const void* cookie = testHarness->create_cookie(h);

    DcpStreamCtx ctx;
    ctx.vb_uuid = get_ull_stat(h, "vb_0:0:id", "failovers");
    ctx.seqno = {0, 200};
    ctx.exp_mutations = 200;
    ctx.exp_markers = 1;

    TestDcpConsumer tdc("unittest", cookie, h);
    tdc.addStreamCtx(ctx);
    tdc.run();

    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}

/*
 * Test that expired items (with values) backfill correctly with Expiry Output
 * disabled
 */
static enum test_result test_dcp_producer_expired_item_backfill_delete(
        EngineIface* h) {
    return testDcpProducerExpiredItemBackfill(h, EnableExpiryOutput::No);
}

/*
 * Test that expired items (with values) backfill correctly with Expiry Output
 * enabled
 */
static enum test_result test_dcp_producer_expired_item_backfill_expire(
        EngineIface* h) {
    return testDcpProducerExpiredItemBackfill(h, EnableExpiryOutput::Yes);
}

static enum test_result test_dcp_producer_stream_req_diskonly(EngineIface* h) {
    const int num_items = 300, batch_items = 100;
    for (int start_seqno = 0; start_seqno < num_items;
         start_seqno += batch_items) {
        wait_for_flusher_to_settle(h);
        write_items(h, batch_items, start_seqno);
    }

    wait_for_flusher_to_settle(h);
    verify_curr_items(h, num_items, "Wrong amount of items");
    wait_for_stat_to_be(h, "vb_0:num_checkpoints", 1, "checkpoint");

    const void* cookie = testHarness->create_cookie(h);

    DcpStreamCtx ctx;
    ctx.flags = DCP_ADD_STREAM_FLAG_DISKONLY;
    ctx.vb_uuid = get_ull_stat(h, "vb_0:0:id", "failovers");
    ctx.seqno = {0, static_cast<uint64_t>(-1)};
    ctx.exp_mutations = 300;
    ctx.exp_markers = 1;

    TestDcpConsumer tdc("unittest", cookie, h);
    tdc.addStreamCtx(ctx);
    tdc.run();

    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_producer_disk_backfill_buffer_limits(
        EngineIface* h) {
    const int num_items = 3;
    write_items(h, num_items);

    wait_for_flusher_to_settle(h);
    verify_curr_items(h, num_items, "Wrong amount of items");

    /* Wait for the checkpoint to be removed so that upon DCP connection
       backfill is scheduled */
    wait_for_stat_to_be(h, "ep_items_rm_from_checkpoints", num_items);

    const void* cookie = testHarness->create_cookie(h);

    DcpStreamCtx ctx;
    ctx.vb_uuid = get_ull_stat(h, "vb_0:0:id", "failovers");
    ctx.seqno = {0, num_items};
    ctx.exp_mutations = 3;
    ctx.exp_markers = 1;

    TestDcpConsumer tdc("unittest", cookie, h);
    tdc.addStreamCtx(ctx);
    tdc.run();

    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_producer_stream_req_mem(EngineIface* h) {
    const int num_items = 300, batch_items = 100;
    for (int start_seqno = 0; start_seqno < num_items;
         start_seqno += batch_items) {
        wait_for_flusher_to_settle(h);
        write_items(h, batch_items, start_seqno);
    }

    wait_for_flusher_to_settle(h);
    verify_curr_items(h, num_items, "Wrong amount of items");

    const void* cookie = testHarness->create_cookie(h);

    DcpStreamCtx ctx;
    ctx.vb_uuid = get_ull_stat(h, "vb_0:0:id", "failovers");
    ctx.seqno = {200, 300};
    ctx.snapshot = {200, 200};
    ctx.exp_mutations = 100;
    ctx.exp_markers = 1;

    TestDcpConsumer tdc("unittest", cookie, h);
    tdc.addStreamCtx(ctx);
    tdc.run();

    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}

/**
 * Test that a DCP stream request in DGM scenarios correctly receives items
 * from both memory and disk.
 */
static enum test_result test_dcp_producer_stream_req_dgm(EngineIface* h) {
    // Test only works for the now removed 2-bit LRU eviction algorithm as it
    // relies on looking at the LRU state.
    // @todo Investigate converting the test to work with the new hifi_mfu
    // eviction algorithm.
    return SUCCESS;

    const void* cookie = testHarness->create_cookie(h);

    int i = 0;  // Item count
    while (true) {
        // Gathering stats on every store is expensive, just check every 100 iterations
        if ((i % 100) == 0) {
            if (get_int_stat(h, "vb_active_perc_mem_resident") < 50) {
                break;
            }
        }

        std::stringstream ss;
        ss << "key" << i;
        ENGINE_ERROR_CODE ret =
                store(h, cookie, OPERATION_SET, ss.str().c_str(), "somevalue");
        if (ret == ENGINE_SUCCESS) {
            i++;
        }
    }

    // Sanity check - ensure we have enough vBucket quota (max_size)
    // such that we have 1000 items - enough to give us 0.1%
    // granuarity in any residency calculations. */
    checkge(i, 1000,
            "Does not have expected min items; Check max_size setting");

    wait_for_flusher_to_settle(h);

    verify_curr_items(h, i, "Wrong number of items");
    double num_non_resident = get_int_stat(h, "vb_active_num_non_resident");
    checkge(num_non_resident,
            i * 0.5,
            "Expected at least 50% of items to be non-resident");

    // Reduce max_size from 6291456 to 6000000
    set_param(h,
              cb::mcbp::request::SetParamPayload::Type::Flush,
              "max_size",
              "6000000");
    checkgt(50,
            get_int_stat(h, "vb_active_perc_mem_resident"),
            "Too high percentage of memory resident");

    DcpStreamCtx ctx;
    ctx.vb_uuid = get_ull_stat(h, "vb_0:0:id", "failovers");
    ctx.seqno = {0, get_ull_stat(h, "vb_0:high_seqno", "vbucket-seqno")};
    ctx.exp_mutations = i;
    ctx.exp_markers = 1;

    TestDcpConsumer tdc("unittest", cookie, h);
    tdc.addStreamCtx(ctx);
    tdc.run();

    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}

/**
 * Test that eviction hotness data is passed in DCP stream.
 */
static enum test_result test_dcp_producer_stream_req_coldness(EngineIface* h) {
    const void* cookie = testHarness->create_cookie(h);

    for (int ii = 0; ii < 10; ii++) {
        std::stringstream ss;
        ss << "key" << ii;
        store(h, cookie, OPERATION_SET, ss.str().c_str(), "somevalue");
    }

    wait_for_flusher_to_settle(h);
    verify_curr_items(h, 10, "Wrong number of items");

    for (int ii = 0; ii < 5; ii++) {
        std::stringstream ss;
        ss << "key" << ii;
        evict_key(h, ss.str().c_str(), Vbid(0), "Ejected.");
    }
    wait_for_flusher_to_settle(h);
    wait_for_stat_to_be(h, "ep_num_value_ejects", 5);

    TestDcpConsumer tdc("unittest", cookie, h);
    uint32_t flags = cb::mcbp::request::DcpOpenPayload::Producer;

    tdc.openConnection(flags);

    checkeq(ENGINE_SUCCESS,
            tdc.sendControlMessage("supports_hifi_MFU", "true"),
            "Failed to configure MFU");

    DcpStreamCtx ctx;
    ctx.vb_uuid = get_ull_stat(h, "vb_0:0:id", "failovers");
    ctx.seqno = {0, get_ull_stat(h, "vb_0:high_seqno", "vbucket-seqno")};
    ctx.exp_mutations = 10;
    ctx.exp_markers = 1;

    // Only stream from disk to ensure that we only ever get a single snapshot.
    // If we got unlucky we could see 2 snapshots due to creation of a second
    // checkpoint if we were streaming from the checkpoint manager.
    ctx.flags |= DCP_ADD_STREAM_FLAG_DISKONLY;

    tdc.addStreamCtx(ctx);
    tdc.run(false);

    checkeq(tdc.getNruCounters()[1],
            5, "unexpected number of hot items");
    checkeq(tdc.getNruCounters()[0],
            5, "unexpected number of cold items");
    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}

/**
 * Test that eviction hotness data is picked up by the DCP consumer
 */
static enum test_result test_dcp_consumer_hotness_data(EngineIface* h) {
    const void* cookie = testHarness->create_cookie(h);
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    Vbid vbid = Vbid(0);
    const char* name = "unittest";

    // Set vbucket 0 to a replica so we can consume a mutation over DCP
    check(set_vbucket_state(h, vbid, vbucket_state_replica),
          "Failed to set vbucket state.");

    // Open consumer connection
    auto dcp = requireDcpIface(h);
    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie,
                      opaque,
                      0,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp Consumer open connection.");

    add_stream_for_consumer(h,
                            cookie,
                            opaque++,
                            vbid,
                            DCP_ADD_STREAM_FLAG_TAKEOVER,
                            cb::mcbp::Status::Success);

    uint32_t stream_opaque =
            get_int_stat(h, "eq_dcpq:unittest:stream_0_opaque", "dcp");

    // Snapshot marker indicating a mutation will follow
    checkeq(ENGINE_SUCCESS,
            dcp->snapshot_marker(cookie,
                                 stream_opaque,
                                 vbid,
                                 0,
                                 1,
                                 0,
                                 {} /*HCS*/,
                                 {} /*maxVisibleSeqno*/),
            "Failed to send marker!");

    const DocKey docKey("key", DocKeyEncodesCollectionId::No);
    checkeq(ENGINE_SUCCESS,
            dcp->mutation(cookie,
                          stream_opaque,
                          docKey,
                          {(const uint8_t*)"value", 5},
                          0, // privileged bytes
                          PROTOCOL_BINARY_RAW_BYTES,
                          0, // cas
                          vbid,
                          0, // flags
                          1, // by_seqno
                          0, // rev_seqno
                          0, // expiration
                          0, // lock_time
                          {}, // meta
                          128 // frequency value
                          ),
            "Failed to send dcp mutation");

    // Set vbucket 0 to active so we can perform a get
    check(set_vbucket_state(h, vbid, vbucket_state_active),
          "Failed to set vbucket state.");

    // Perform a get to retrieve the frequency counter value
    auto rv = get(h, cookie, "key", vbid, DocStateFilter::AliveOrDeleted);
    checkeq(cb::engine_errc::success, rv.first, "Failed to fetch");
    const Item* it = reinterpret_cast<const Item*>(rv.second.get());

    // Confirm that the item that was consumed over DCP has picked up
    // the correct hotness data value.
    // Performing the get may increase the hotness value by 1 and therefore
    // it is valid for the value to be 128 or 129.
    checkle(128,
            int(it->getFreqCounterValue()),
            "Failed to set the hotness data to the correct value");
    checkge(129,
            int(it->getFreqCounterValue()),
            "Failed to set the hotness data to the correct value");

    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_producer_stream_latest(EngineIface* h) {
    const int num_items = 300, batch_items = 100;
    for (int start_seqno = 0; start_seqno < num_items;
         start_seqno += batch_items) {
        wait_for_flusher_to_settle(h);
        write_items(h, batch_items, start_seqno);
    }

    wait_for_flusher_to_settle(h);
    verify_curr_items(h, num_items, "Wrong amount of items");

    const void* cookie = testHarness->create_cookie(h);

    DcpStreamCtx ctx;
    ctx.flags = DCP_ADD_STREAM_FLAG_LATEST;
    ctx.vb_uuid = get_ull_stat(h, "vb_0:0:id", "failovers");
    ctx.seqno = {200, 205};
    ctx.snapshot = {200, 200};
    ctx.exp_mutations = 100;
    ctx.exp_markers = 1;

    TestDcpConsumer tdc("unittest", cookie, h);
    tdc.addStreamCtx(ctx);
    tdc.run();

    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_producer_keep_stream_open(EngineIface* h) {
    const std::string conn_name("unittest");
    const int num_items = 2, vb = 0;

    write_items(h, num_items);

    wait_for_flusher_to_settle(h);
    verify_curr_items(h, num_items, "Wrong amount of items");

    const void* cookie = testHarness->create_cookie(h);

    /* We want to stream items till end and keep the stream open. Then we want
       to verify the stream is still open */
    struct continuous_dcp_ctx cdc = {
            h,
            cookie,
            Vbid(0),
            conn_name,
            0,
            std::make_unique<TestDcpConsumer>(conn_name, cookie, h)};
    cb_thread_t dcp_thread;
    cb_assert(cb_create_thread(&dcp_thread, continuous_dcp_thread, &cdc, 0)
              == 0);

    /* Wait for producer to be created */
    wait_for_stat_to_be(h, "ep_dcp_producer_count", 1, "dcp");

    /* Wait for an active stream to be created */
    const std::string stat_stream_count("eq_dcpq:" + conn_name +
                                        ":num_streams");
    wait_for_stat_to_be(h, stat_stream_count.c_str(), 1, "dcp");

    /* Wait for the dcp test client to receive upto highest seqno we have */
    cb::RelaxedAtomic<uint64_t> exp_items(num_items);
    wait_for_val_to_be("last_sent_seqno",
                       cdc.dcpConsumer->producers.last_byseqno,
                       exp_items);

    /* Check if the stream is still open after sending out latest items */
    std::string stat_stream_state("eq_dcpq:" + conn_name + ":stream_" +
                             std::to_string(vb) + "_state");
    std::string state = get_str_stat(h, stat_stream_state.c_str(), "dcp");
    checkeq(state.compare("in-memory"), 0, "Stream is not open");

    /* Before closing the connection stop the thread that continuously polls
       for dcp data */
    cdc.dcpConsumer->stop();
    testHarness->notify_io_complete(cookie, ENGINE_SUCCESS);
    cb_assert(cb_join_thread(dcp_thread) == 0);
    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_producer_keep_stream_open_replica(
        EngineIface* h) {
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
    check(set_vbucket_state(h, Vbid(0), vbucket_state_replica),
          "Failed to set vbucket state.");

    const void* cookie = testHarness->create_cookie(h);
    uint32_t opaque = 0xFFFF0000;
    uint32_t seqno = 0;
    uint32_t flags = 0;
    const int num_items = 10;
    const std::string conn_name("unittest");
    int vb = 0;
    auto dcp = requireDcpIface(h);

    /* Open an DCP consumer connection */
    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie,
                      opaque,
                      seqno,
                      flags,
                      conn_name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp producer open connection.");

    std::string type = get_str_stat(h, "eq_dcpq:unittest:type", "dcp");
    checkeq(0, type.compare("consumer"), "Consumer not found");

    opaque = add_stream_for_consumer(
            h, cookie, opaque, Vbid(0), 0, cb::mcbp::Status::Success);

    /* Send DCP mutations with in memory flag (0x01) */
    dcp_stream_to_replica(
            h, cookie, opaque, Vbid(0), 0x01, 1, num_items, 0, num_items);

    /* Send 10 more DCP mutations with checkpoint creation flag (0x04) */
    uint64_t start = num_items;
    dcp_stream_to_replica(h,
                          cookie,
                          opaque,
                          Vbid(0),
                          0x04,
                          start + 1,
                          start + 10,
                          start,
                          start + 10);

    wait_for_flusher_to_settle(h);
    stop_persistence(h);
    checkeq(2 * num_items,
            get_int_stat(h, "vb_replica_curr_items"),
            "wrong number of items in replica vbucket");

    /* Add 10 more items to the replica node on a new checkpoint.
       Send with flag (0x04) indicating checkpoint creation */
    start = 2 * num_items;
    dcp_stream_to_replica(h,
                          cookie,
                          opaque,
                          Vbid(0),
                          0x04,
                          start + 1,
                          start + 10,
                          start,
                          start + 10);

    /* Expecting for Disk backfill + in memory snapshot merge.
       Wait for a checkpoint to be removed */
    wait_for_stat_to_be_lte(h, "vb_0:num_checkpoints", 2, "checkpoint");

    /* Part (ii): Open a stream (in a DCP conn) and see if all the items are
                  received correctly */
    /* We want to stream items till end and keep the stream open. Then we want
       to verify the stream is still open */
    const void* cookie1 = testHarness->create_cookie(h);
    const std::string conn_name1("unittest1");
    struct continuous_dcp_ctx cdc = {
            h,
            cookie1,
            Vbid(0),
            conn_name1,
            0,
            std::make_unique<TestDcpConsumer>(conn_name1, cookie1, h)};
    cb_thread_t dcp_thread;
    cb_assert(cb_create_thread(&dcp_thread, continuous_dcp_thread, &cdc, 0)
              == 0);

    /* Wait for producer to be created */
    wait_for_stat_to_be(h, "ep_dcp_producer_count", 1, "dcp");

    /* Wait for an active stream to be created */
    const std::string stat_stream_count("eq_dcpq:" + conn_name1 +
                                        ":num_streams");
    wait_for_stat_to_be(h, stat_stream_count.c_str(), 1, "dcp");

    /* Wait for the dcp test client to receive upto highest seqno we have */
    cb::RelaxedAtomic<uint64_t> exp_items(3 * num_items);
    wait_for_val_to_be("last_sent_seqno",
                       cdc.dcpConsumer->producers.last_byseqno,
                       exp_items);

    /* Check if correct snap end seqno is sent */
    std::string stat_stream_last_sent_snap_end_seqno("eq_dcpq:" + conn_name1 +
                                                     ":stream_" +
                                                     std::to_string(vb) +
                                                     "_last_sent_snap_end_seqno");
    wait_for_stat_to_be(h,
                        stat_stream_last_sent_snap_end_seqno.c_str(),
                        3 * num_items,
                        "dcp");

    /* Check if the stream is still open after sending out latest items */
    std::string stat_stream_state("eq_dcpq:" + conn_name1 + ":stream_" +
                                  std::to_string(vb) + "_state");
    std::string state = get_str_stat(h, stat_stream_state.c_str(), "dcp");
    checkeq(state.compare("in-memory"), 0, "Stream is not open");

    /* Before closing the connection stop the thread that continuously polls
       for dcp data */
    cdc.dcpConsumer->stop();
    testHarness->notify_io_complete(cookie1, ENGINE_SUCCESS);
    cb_assert(cb_join_thread(dcp_thread) == 0);

    testHarness->destroy_cookie(cookie);
    testHarness->destroy_cookie(cookie1);

    return SUCCESS;
}

static enum test_result test_dcp_producer_stream_cursor_movement(
        EngineIface* h) {
    const std::string conn_name("unittest");
    const int num_items = 30;
    uint64_t curr_chkpt_id = 0;
    for (int j = 0; j < num_items; ++j) {
        if (j % 10 == 0) {
            wait_for_flusher_to_settle(h);
        }
        if (j == (num_items - 1)) {
            /* Since checkpoint max items is set to 10 and we are going to
               write 30th item, a new checkpoint could be added after
               writing and persisting the 30th item. I mean, if the checkpoint
               id is got outside the while loop, there could be an error due to
               race (flusher and checkpoint remover could run before getting
               this stat) */
            curr_chkpt_id =
                    get_ull_stat(h, "vb_0:open_checkpoint_id", "checkpoint");
        }
        std::string key("key" + std::to_string(j));
        checkeq(ENGINE_SUCCESS,
                store(h, nullptr, OPERATION_SET, key.c_str(), "data"),
                "Failed to store a value");
    }

    wait_for_flusher_to_settle(h);
    verify_curr_items(h, num_items, "Wrong amount of items");

    const void* cookie = testHarness->create_cookie(h);

    /* We want to stream items till end and keep the stream open. We want to
       verify if the DCP cursor has moved to new open checkpoint */
    struct continuous_dcp_ctx cdc = {
            h,
            cookie,
            Vbid(0),
            conn_name,
            20,
            std::make_unique<TestDcpConsumer>(conn_name, cookie, h)};
    cb_thread_t dcp_thread;
    cb_assert(cb_create_thread(&dcp_thread, continuous_dcp_thread, &cdc, 0)
              == 0);

    /* Wait for producer to be created */
    wait_for_stat_to_be(h, "ep_dcp_producer_count", 1, "dcp");

    /* Wait for an active stream to be created */
    const std::string stat_stream_count("eq_dcpq:" + conn_name +
                                        ":num_streams");
    wait_for_stat_to_be(h, stat_stream_count.c_str(), 1, "dcp");

    /* Wait for the dcp test client to receive upto highest seqno we have */
    cb::RelaxedAtomic<uint64_t> exp_items(num_items);
    wait_for_val_to_be("last_sent_seqno",
                       cdc.dcpConsumer->producers.last_byseqno,
                       exp_items);

    /* Wait for new open (empty) checkpoint to be added */
    wait_for_stat_to_be(
            h, "vb_0:open_checkpoint_id", curr_chkpt_id + 1, "checkpoint");

    /* We want to make sure that no cursors are lingering on any of the previous
       checkpoints. For that we wait for checkpoint remover to remove all but
       the latest open checkpoint cursor */
    wait_for_stat_to_be(h, "vb_0:num_checkpoints", 1, "checkpoint");

    /* Before closing the connection stop the thread that continuously polls
       for dcp data */
    cdc.dcpConsumer->stop();
    testHarness->notify_io_complete(cookie, ENGINE_SUCCESS);
    cb_assert(cb_join_thread(dcp_thread) == 0);
    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}

static test_result test_dcp_producer_stream_req_nmvb(EngineIface* h) {
    const void* cookie1 = testHarness->create_cookie(h);
    uint32_t opaque = 0;
    uint32_t seqno = 0;
    uint32_t flags = cb::mcbp::request::DcpOpenPayload::Producer;
    const char *name = "unittest";
    auto dcp = requireDcpIface(h);

    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie1,
                      opaque,
                      seqno,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp producer open connection.");

    Vbid req_vbucket = Vbid(1);
    uint64_t rollback = 0;

    checkeq(ENGINE_NOT_MY_VBUCKET,
            dcp->stream_req(cookie1,
                            0,
                            0,
                            req_vbucket,
                            0,
                            0,
                            0,
                            0,
                            0,
                            &rollback,
                            mock_dcp_add_failover_log,
                            {}),
            "Expected not my vbucket");
    testHarness->destroy_cookie(cookie1);

    return SUCCESS;
}

static test_result test_dcp_agg_stats(EngineIface* h) {
    const int num_items = 300, batch_items = 100;
    for (int start_seqno = 0; start_seqno < num_items;
         start_seqno += batch_items) {
        wait_for_flusher_to_settle(h);
        write_items(h, batch_items, start_seqno);
    }

    wait_for_flusher_to_settle(h);
    verify_curr_items(h, num_items, "Wrong amount of items");

    const void *cookie[5];

    uint64_t total_bytes = 0;
    for (int j = 0; j < 5; ++j) {
        std::string name("unittest_" + std::to_string(j));
        cookie[j] = testHarness->create_cookie(h);

        DcpStreamCtx ctx;
        ctx.vb_uuid = get_ull_stat(h, "vb_0:0:id", "failovers");
        ctx.seqno = {200, 300};
        ctx.snapshot = {200, 200};
        ctx.exp_mutations = 100;
        ctx.exp_markers = 1;

        TestDcpConsumer tdc(name, cookie[j], h);
        tdc.addStreamCtx(ctx);
        tdc.run();
        total_bytes += tdc.getTotalBytes();
    }

    checkeq(5,
            get_int_stat(h, "unittest:producer_count", "dcpagg _"),
            "producer count mismatch");
    checkeq((int)total_bytes,
            get_int_stat(h, "unittest:total_bytes", "dcpagg _"),
            "aggregate total bytes sent mismatch");
    checkeq(500,
            get_int_stat(h, "unittest:items_sent", "dcpagg _"),
            "aggregate total items sent mismatch");
    checkeq(0,
            get_int_stat(h, "unittest:items_remaining", "dcpagg _"),
            "aggregate total items remaining mismatch");

    for (auto& c : cookie) {
        testHarness->destroy_cookie(c);
    }

    return SUCCESS;
}

static test_result test_dcp_cursor_dropping(EngineIface* h,
                                            bool replicationStream) {
    /* Initially write a few items */
    int num_items = 25;
    const int initialSnapshotSize = num_items;

    // 75% is so that we don't hit the HWM (triggering the pager) yet is above
    // the thresholds required for cursor dropping
    const int cursor_dropping_mem_thres_perc = 75;

    write_items(h, num_items, 1);

    wait_for_flusher_to_settle(h);
    verify_curr_items(h, num_items, "Wrong amount of items");

    /* Set up a dcp producer conn and stream a few items. This will cause the
       stream to transition from pending -> backfill -> in-memory state */
    MockDcpMessageProducers producers(h);

    const void* cookie = testHarness->create_cookie(h);
    std::string conn_name = replicationStream ? "replication" : "unittest";
    uint32_t opaque = 1;
    uint64_t last_seqno_streamed = 0;

    DcpStreamCtx ctx;
    ctx.vb_uuid = get_ull_stat(h, "vb_0:0:id", "failovers");
    ctx.seqno = {0, static_cast<uint64_t>(-1)};
    ctx.snapshot = {0, 0};

    TestDcpConsumer tdc(conn_name, cookie, h);

    tdc.addStreamCtx(ctx);

    tdc.openConnection();

    if (replicationStream) {
        tdc.sendControlMessage("supports_cursor_dropping_vulcan", "true");
    }

    tdc.openStreams();

    /* Stream (from in-memory state) less than the number of items written.
       We want to do this because we want to test if the stream drops the items
       in the readyQ when we later switch from in-memory -> backfill state */
    dcp_stream_from_producer_conn(h,
                                  cookie,
                                  opaque,
                                  last_seqno_streamed + 1,
                                  num_items - 5,
                                  0,
                                  tdc.producers);
    last_seqno_streamed = num_items - 5;

    /* Check if the stream is still in in-memory state after sending out
       items */
    std::string stat_stream_state("eq_dcpq:" + conn_name + ":stream_" +
                                  std::to_string(0) + "_state");
    std::string state = get_str_stat(h, stat_stream_state.c_str(), "dcp");
    checkeq(state.compare("in-memory"), 0, "Stream is in memory state");

    /* Write items such that cursor is dropped due to heavy memory usage and
       stream state changes from memory->backfill */
    stop_persistence(h);
    num_items += write_items_upto_mem_perc(
            h, cursor_dropping_mem_thres_perc, num_items + 1);

    // Sanity check - ensure we have enough vBucket quota (max_size)
    // such that we have 1000 items - enough to give us 0.1%
    // granularity in any residency calculations. */
    checkge(num_items, 1000,
            "Does not have expected min items; Check max_size setting");

    /* Persist all items */
    start_persistence(h);
    wait_for_flusher_to_settle(h);

    /* wait for cursor to be dropped. You need to make sure that there are
       2 checkpoints so that the cursors of one of the checkpoint is dropped.
       For this we need to have correct combination of max_size and
       chk_max_items (max_size=6291456;chk_max_items=8000 in this case) */
    wait_for_stat_to_be_gte(h, "ep_cursors_dropped", 1);
    dcp_stream_from_producer_conn(h,
                                  cookie,
                                  opaque,
                                  last_seqno_streamed + 1,
                                  num_items,
                                  initialSnapshotSize + 1,
                                  tdc.producers);
    last_seqno_streamed = num_items;

    /* Write 10 more items to test if stream transitions correctly from
       backfill -> in-memory and sends out items */
    write_items(h, 10, num_items + 1);
    num_items += 10;
    dcp_stream_from_producer_conn(h,
                                  cookie,
                                  opaque,
                                  last_seqno_streamed + 1,
                                  num_items,
                                  last_seqno_streamed + 1,
                                  tdc.producers);

    testHarness->destroy_cookie(cookie);
    return SUCCESS;
}

static test_result test_dcp_cursor_dropping(EngineIface* h) {
    return test_dcp_cursor_dropping(h, false);
}

static test_result test_dcp_cursor_dropping_replication(EngineIface* h) {
    return test_dcp_cursor_dropping(h, true);
}
static test_result test_dcp_cursor_dropping_backfill(EngineIface* h) {
    /* Initially write a few items */
    int num_items = 50;
    const int initialSnapshotSize = num_items;
    // 75% is so that we don't hit the HWM (triggering the pager) yet is above
    // the thresholds required for cursor dropping
    const int cursor_dropping_mem_thres_perc = 75;

    write_items(h, num_items, 1);

    wait_for_flusher_to_settle(h);
    verify_curr_items(h, num_items, "Wrong amount of items");
    wait_for_stat_to_be_gte(h, "vb_0:open_checkpoint_id", 3, "checkpoint");

    // Wait for something to be removed from the checkpoint manager to ensure
    // that when we create our stream we will start backfilling from disk. If we
    // did not wait for this then we would often end up just attempting to
    // stream from memory which is not what we want to test and would fail later
    // on in the test when we expect to receive at least 2 snapshots.
    wait_for_stat_to_be_gte(h, "ep_items_rm_from_checkpoints", 1);

    /* Set up a connection */
    const void* cookie = testHarness->create_cookie(h);
    std::string conn_name("unittest");
    uint32_t opaque = 1;

    DcpStreamCtx ctx;
    ctx.vb_uuid = get_ull_stat(h, "vb_0:0:id", "failovers");
    ctx.seqno = {0, static_cast<uint64_t>(-1)};
    ctx.snapshot = {0, 0};

    TestDcpConsumer tdc(conn_name, cookie, h);

    tdc.addStreamCtx(ctx);

    tdc.openConnection();
    tdc.openStreams();

    /* Write items such that we cross threshold for cursor dropping */
    stop_persistence(h);
    num_items += write_items_upto_mem_perc(
            h, cursor_dropping_mem_thres_perc, num_items + 1);

    /* Sanity check - ensure we have enough vBucket quota (max_size)
       such that we have 1000 items - enough to give us 0.1%
       granuarity in any residency calculations. */
    checkge(num_items, 1000,
            "Does not have expected min items; Check max_size setting");

    /* Persist all items so that we can drop the replication cursor and
       schedule another backfill */
    start_persistence(h);
    wait_for_flusher_to_settle(h);

    wait_for_stat_to_be_gte(h, "ep_cursors_dropped", 1);

    /* Read all the items from the producer. This ensures that the items are
       backfilled correctly after scheduling 2 successive backfills. */
    dcp_stream_from_producer_conn(h,
                                  cookie,
                                  opaque,
                                  1,
                                  num_items,
                                  initialSnapshotSize + 1,
                                  tdc.producers);

    testHarness->destroy_cookie(cookie);
    return SUCCESS;
}

static test_result test_dcp_takeover(EngineIface* h) {
    const int num_items = 10;
    write_items(h, num_items);

    const void* cookie = testHarness->create_cookie(h);

    DcpStreamCtx ctx;
    ctx.flags = DCP_ADD_STREAM_FLAG_TAKEOVER;
    ctx.vb_uuid = get_ull_stat(h, "vb_0:0:id", "failovers");
    ctx.seqno = {0, 1000};
    ctx.exp_mutations = 20;
    ctx.exp_markers = 2;
    ctx.extra_takeover_ops = 10;

    TestDcpConsumer tdc("unittest", cookie, h);
    tdc.addStreamCtx(ctx);
    tdc.run();

    check(verify_vbucket_state(h, Vbid(0), vbucket_state_dead),
          "Wrong vb state");

    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}

static test_result test_dcp_takeover_no_items(EngineIface* h) {
    const int num_items = 10;
    write_items(h, num_items);

    const void* cookie = testHarness->create_cookie(h);
    const char *name = "unittest";
    uint32_t opaque = 1;
    auto dcp = requireDcpIface(h);

    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie,
                      ++opaque,
                      0,
                      cb::mcbp::request::DcpOpenPayload::Producer,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp producer open connection.");

    Vbid vbucket = Vbid(0);
    uint32_t flags = DCP_ADD_STREAM_FLAG_TAKEOVER;
    uint64_t start_seqno = 10;
    uint64_t end_seqno = std::numeric_limits<uint64_t>::max();
    uint64_t vb_uuid = get_ull_stat(h, "vb_0:0:id", "failovers");
    uint64_t snap_start_seqno = 10;
    uint64_t snap_end_seqno = 10;

    uint64_t rollback = 0;
    checkeq(ENGINE_SUCCESS,
            dcp->stream_req(cookie,
                            flags,
                            ++opaque,
                            vbucket,
                            start_seqno,
                            end_seqno,
                            vb_uuid,
                            snap_start_seqno,
                            snap_end_seqno,
                            &rollback,
                            mock_dcp_add_failover_log,
                            {}),
            "Failed to initiate stream request");

    MockDcpMessageProducers producers(h);

    bool done = false;
    int num_snapshot_markers = 0;
    int num_set_vbucket_pending = 0;
    int num_set_vbucket_active = 0;

    do {
        ENGINE_ERROR_CODE err = dcp->step(cookie, &producers);
        if (err == ENGINE_DISCONNECT) {
            done = true;
        } else {
            switch (producers.last_op) {
            case cb::mcbp::ClientOpcode::DcpStreamEnd:
                done = true;
                break;
            case cb::mcbp::ClientOpcode::DcpSnapshotMarker:
                num_snapshot_markers++;
                break;
            case cb::mcbp::ClientOpcode::DcpSetVbucketState:
                if (producers.last_vbucket_state == vbucket_state_pending) {
                    num_set_vbucket_pending++;
                } else if (producers.last_vbucket_state ==
                           vbucket_state_active) {
                    num_set_vbucket_active++;
                }
                sendDcpAck(h,
                           cookie,
                           cb::mcbp::ClientOpcode::DcpSetVbucketState,
                           cb::mcbp::Status::Success,
                           producers.last_opaque);
                break;
            case cb::mcbp::ClientOpcode::Invalid:
                break;
            default:
                break;
                abort();
            }
            producers.last_op = cb::mcbp::ClientOpcode::Invalid;
        }
    } while (!done);

    checkeq(0, num_snapshot_markers, "Invalid number of snapshot marker");
    checkeq(1, num_set_vbucket_pending, "Didn't receive pending set state");
    checkeq(1, num_set_vbucket_active, "Didn't receive active set state");

    check(verify_vbucket_state(h, Vbid(0), vbucket_state_dead),
          "Wrong vb state");
    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}

/**
 * Part of the Consumer-Producer negotiation happens over DCP_CONTROL and
 * introduces a blocking step, so we have to simulate the Producer response for
 * letting dcp_step() proceed.
 * Note that the blocking DCP_CONTROL request is signed at Consumer by
 * tracking the opaque value sent to the Producer, so we need to set the
 * proper opaque.
 *
 * At the time of writing, the SyncReplication and the IncludeDeletedUserXattrs
 * negotiations follow the described pattern.
 *
 * @param engine The engine interface
 * @param cookie The cookie representing the DCP Consumer into the engine
 * @param producers The MockDcpMessageProducers used by the Consumer
 */
static void simulateProdRespToDcpControlBlockingNegotiation(
        EngineIface* engine,
        const void* cookie,
        MockDcpMessageProducers& producers) {
    protocol_binary_response_header resp{};
    resp.response.setMagic(cb::mcbp::Magic::ClientResponse);
    resp.response.setOpcode(cb::mcbp::ClientOpcode::DcpControl);
    resp.response.setStatus(cb::mcbp::Status::Success);
    resp.response.setOpaque(producers.last_opaque);
    dcpHandleResponse(engine, cookie, &resp, producers);
}

static uint32_t add_stream_for_consumer(EngineIface* h,
                                        const void* cookie,
                                        uint32_t opaque,
                                        Vbid vbucket,
                                        uint32_t flags,
                                        cb::mcbp::Status response,
                                        uint64_t exp_snap_start,
                                        uint64_t exp_snap_end) {
    using cb::mcbp::ClientOpcode;

    auto dcp = requireDcpIface(h);
    checkeq(ENGINE_SUCCESS,
            dcp->add_stream(cookie, opaque, vbucket, flags),
            "Add stream request failed");

    MockDcpMessageProducers producers(h);

    auto dcpStepAndExpectControlMsg =
            [&h, cookie, opaque, &producers](std::string controlKey) {
                dcp_step(h, cookie, producers);
                checkeq(cb::mcbp::ClientOpcode::DcpControl,
                        producers.last_op,
                        "Unexpected last_op");
                checkeq(std::move(controlKey),
                        producers.last_key,
                        "Unexpected key");
                checkne(opaque, producers.last_opaque, "Unexpected opaque");
            };

    if (get_str_stat(h, "ep_dcp_flow_control_policy") != "none") {
        dcpStepAndExpectControlMsg("connection_buffer_size"s);
    }

    if (get_bool_stat(h, "ep_dcp_enable_noop")) {
        // MB-29441: Check that the GetErrorMap message is sent
        dcp_step(h, cookie, producers);
        checkeq(ClientOpcode::GetErrorMap,
                producers.last_op,
                "Unexpected last_op");
        checkeq(""s, producers.last_key, "Unexpected non-empty key");

        // Simulate that the GetErrorMap response has been received.
        // This step is necessary, as a pending GetErrorMap response would
        // not let the next dcp_step() to execute the
        // DcpControl/set_noop_interval call.
        protocol_binary_response_header resp{};
        resp.response.setMagic(cb::mcbp::Magic::ClientResponse);
        resp.response.setOpcode(cb::mcbp::ClientOpcode::GetErrorMap);
        resp.response.setStatus(cb::mcbp::Status::Success);
        dcpHandleResponse(h, cookie, &resp, producers);

        // Check that the enable noop message is sent
        dcpStepAndExpectControlMsg("enable_noop"s);

        // Check that the set noop interval message is sent
        dcpStepAndExpectControlMsg("set_noop_interval"s);
    }

    dcpStepAndExpectControlMsg("set_priority"s);
    dcpStepAndExpectControlMsg("enable_ext_metadata"s);
    dcpStepAndExpectControlMsg("supports_cursor_dropping_vulcan"s);
    dcpStepAndExpectControlMsg("supports_hifi_MFU"s);
    dcpStepAndExpectControlMsg("send_stream_end_on_client_close_stream"s);
    dcpStepAndExpectControlMsg("enable_expiry_opcode"s);
    dcpStepAndExpectControlMsg("enable_sync_writes"s);
    simulateProdRespToDcpControlBlockingNegotiation(h, cookie, producers);
    dcpStepAndExpectControlMsg("consumer_name"s);
    dcpStepAndExpectControlMsg("include_deleted_user_xattrs"s);
    simulateProdRespToDcpControlBlockingNegotiation(h, cookie, producers);

    dcp_step(h, cookie, producers);
    uint32_t stream_opaque = producers.last_opaque;
    checkeq(ClientOpcode::DcpStreamReq,
            producers.last_op,
            "Unexpected last_op");
    checkne(opaque, producers.last_opaque, "Unexpected opaque");

    if (exp_snap_start != 0) {
        checkeq(exp_snap_start,
                producers.last_snap_start_seqno,
                "Unexpected snap start");
    }

    if (exp_snap_end != 0) {
        checkeq(exp_snap_end,
                producers.last_snap_end_seqno,
                "Unexpected snap end");
    }

    size_t bodylen = 0;
    if (response == cb::mcbp::Status::Success) {
        bodylen = 16;
    } else if (response == cb::mcbp::Status::Rollback) {
        bodylen = 8;
    }

    size_t headerlen = sizeof(protocol_binary_response_header);
    size_t pkt_len = headerlen + bodylen;

    auto* pkt =
        (protocol_binary_response_header*)cb_malloc(pkt_len);
    memset(pkt->bytes, '\0', pkt_len);
    pkt->response.setMagic(cb::mcbp::Magic::ClientResponse);
    pkt->response.setOpcode(cb::mcbp::ClientOpcode::DcpStreamReq);
    pkt->response.setStatus(response);
    pkt->response.setOpaque(producers.last_opaque);

    if (response == cb::mcbp::Status::Rollback) {
        bodylen = sizeof(uint64_t);
        uint64_t rollbackSeqno = 0;
        memcpy(pkt->bytes + headerlen, &rollbackSeqno, bodylen);
    }

    pkt->response.setBodylen(bodylen);

    if (response == cb::mcbp::Status::Success) {
        uint64_t vb_uuid = htonll(123456789);
        uint64_t by_seqno = 0;
        memcpy(pkt->bytes + headerlen, &vb_uuid, sizeof(uint64_t));
        memcpy(pkt->bytes + headerlen + 8, &by_seqno, sizeof(uint64_t));
    }

    checkeq(ENGINE_SUCCESS,
            dcp->response_handler(cookie, pkt),
            "Expected success");
    dcp_step(h, cookie, producers);
    cb_free(pkt);

    if (response == cb::mcbp::Status::Rollback) {
        return stream_opaque;
    }

    if (producers.last_op == cb::mcbp::ClientOpcode::DcpStreamReq) {
        checkne(opaque, producers.last_opaque, "Unexpected opaque");
        verify_curr_items(h, 0, "Wrong amount of items");

        auto* pkt =
            (protocol_binary_response_header*)cb_malloc(pkt_len);
        memset(pkt->bytes, '\0', 40);
        pkt->response.setMagic(cb::mcbp::Magic::ClientResponse);
        pkt->response.setOpcode(cb::mcbp::ClientOpcode::DcpStreamReq);
        pkt->response.setStatus(cb::mcbp::Status::Success);
        pkt->response.setOpaque(producers.last_opaque);
        pkt->response.setBodylen(16);

        uint64_t vb_uuid = htonll(123456789);
        uint64_t by_seqno = 0;
        memcpy(pkt->bytes + headerlen, &vb_uuid, sizeof(uint64_t));
        memcpy(pkt->bytes + headerlen + 8, &by_seqno, sizeof(uint64_t));

        checkeq(ENGINE_SUCCESS,
                dcp->response_handler(cookie, pkt),
                "Expected success");
        dcp_step(h, cookie, producers);

        checkeq(cb::mcbp::ClientOpcode::DcpAddStream,
                producers.last_op,
                "Unexpected opcode");
        checkeq(cb::mcbp::Status::Success,
                producers.last_status,
                "Unexpected status");
        checkeq(stream_opaque,
                producers.last_stream_opaque,
                "Unexpected stream opaque");
        cb_free(pkt);
    } else {
        checkeq(cb::mcbp::ClientOpcode::DcpAddStream,
                producers.last_op,
                "Unexpected opcode");
        checkeq(response, producers.last_status, "Unexpected status");
        checkeq(stream_opaque,
                producers.last_stream_opaque,
                "Unexpected stream opaque");
    }

    if (response == cb::mcbp::Status::Success) {
        uint64_t uuid = get_ull_stat(h, "vb_0:0:id", "failovers");
        uint64_t seq = get_ull_stat(h, "vb_0:0:seq", "failovers");
        checkeq(uint64_t{123456789}, uuid, "Unexpected UUID");
        checkeq(uint64_t{0}, seq, "Unexpected seqno");
    }

    return stream_opaque;
}

static enum test_result test_dcp_reconnect(EngineIface* h,
                                           bool full,
                                           bool restart) {
    // Test reconnect when we were disconnected after receiving a full snapshot
    check(set_vbucket_state(h, Vbid(0), vbucket_state_replica),
          "Failed to set vbucket state.");

    const void* cookie = testHarness->create_cookie(h);
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    const char *name = "unittest";
    int items = full ? 10 : 5;
    auto dcp = requireDcpIface(h);

    // Open consumer connection
    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie,
                      opaque,
                      0,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp Consumer open connection.");

    add_stream_for_consumer(
            h, cookie, opaque++, Vbid(0), 0, cb::mcbp::Status::Success);

    uint32_t stream_opaque =
            get_int_stat(h, "eq_dcpq:unittest:stream_0_opaque", "dcp");
    checkeq(ENGINE_SUCCESS,
            dcp->snapshot_marker(cookie,
                                 stream_opaque,
                                 Vbid(0),
                                 0,
                                 10,
                                 2,
                                 0 /*HCS*/,
                                 {} /*maxVisibleSeqno*/),
            "Failed to send snapshot marker");

    for (int i = 1; i <= items; i++) {
        const std::string key{"key" + std::to_string(i)};
        const DocKey docKey(key, DocKeyEncodesCollectionId::No);
        checkeq(ENGINE_SUCCESS,
                dcp->mutation(cookie,
                              stream_opaque,
                              docKey,
                              {(const uint8_t*)"value", 5},
                              0, // privileged bytes
                              PROTOCOL_BINARY_RAW_BYTES,
                              i * 3, // cas
                              Vbid(0),
                              0, // flags
                              i, // by_seqno
                              0, // rev_seqno
                              0, // expiration
                              0, // lock_time
                              {}, // meta
                              INITIAL_NRU_VALUE),
                "Failed to send dcp mutation");
    }

    wait_for_flusher_to_settle(h);
    wait_for_stat_to_be(h, "vb_replica_curr_items", items);

    testHarness->destroy_cookie(cookie);

    if (restart) {
        testHarness->reload_engine(&h,

                                   testHarness->get_current_testcase()->cfg,
                                   true,
                                   true);
        wait_for_warmup_complete(h);
        dcp = requireDcpIface(h);
    }

    cookie = testHarness->create_cookie(h);

    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie,
                      opaque,
                      0,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp Consumer open connection.");

    uint64_t snap_start = full ? 10 : 0;
    uint64_t snap_end = 10;
    add_stream_for_consumer(h,
                            cookie,
                            opaque++,
                            Vbid(0),
                            0,
                            cb::mcbp::Status::Success,
                            snap_start,
                            snap_end);

    testHarness->destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_dcp_reconnect_full(EngineIface* h) {
    // Test reconnect after a dropped connection with a full snapshot
    return test_dcp_reconnect(h, true, false);
}

static enum test_result test_dcp_reconnect_partial(EngineIface* h) {
    // Test reconnect after a dropped connection with a partial snapshot
    return test_dcp_reconnect(h, false, false);
}

static enum test_result test_dcp_crash_reconnect_full(EngineIface* h) {
    if (!isWarmupEnabled(h)) {
        return SKIPPED;
    }

    // Test reconnect after we crash with a full snapshot
    return test_dcp_reconnect(h, true, true);
}

static enum test_result test_dcp_crash_reconnect_partial(EngineIface* h) {
    if (!isWarmupEnabled(h)) {
        return SKIPPED;
    }

    // Test reconnect after we crash with a partial snapshot
    return test_dcp_reconnect(h, false, true);
}

static enum test_result test_dcp_consumer_takeover(EngineIface* h) {
    const void* cookie = testHarness->create_cookie(h);
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    const char *name = "unittest";

    check(set_vbucket_state(h, Vbid(0), vbucket_state_replica),
          "Failed to set vbucket state.");

    // Open consumer connection
    auto dcp = requireDcpIface(h);
    MockDcpMessageProducers producers(h);
    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie,
                      opaque,
                      0,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp Consumer open connection.");

    add_stream_for_consumer(h,
                            cookie,
                            opaque++,
                            Vbid(0),
                            DCP_ADD_STREAM_FLAG_TAKEOVER,
                            cb::mcbp::Status::Success);

    uint32_t stream_opaque =
            get_int_stat(h, "eq_dcpq:unittest:stream_0_opaque", "dcp");

    dcp->snapshot_marker(cookie,
                         stream_opaque,
                         Vbid(0),
                         1,
                         5,
                         10,
                         0 /*HCS*/,
                         {} /*maxVisibleSeqno*/);
    for (int i = 1; i <= 5; i++) {
        const std::string key{"key" + std::to_string(i)};
        const DocKey docKey(key, DocKeyEncodesCollectionId::No);
        checkeq(ENGINE_SUCCESS,
                dcp->mutation(cookie,
                              stream_opaque,
                              docKey,
                              {(const uint8_t*)"value", 5},
                              0, // privileged bytes
                              PROTOCOL_BINARY_RAW_BYTES,
                              i * 3, // cas
                              Vbid(0),
                              0, // flags
                              i, // by_seqno
                              0, // rev_seqno
                              0, // expiration
                              0, // lock_time
                              {}, // meta
                              INITIAL_NRU_VALUE),
                "Failed to send dcp mutation");
    }

    wait_for_flusher_to_settle(h);

    dcp->snapshot_marker(cookie,
                         stream_opaque,
                         Vbid(0),
                         6,
                         10,
                         10,
                         0 /*HCS*/,
                         {} /*maxVisibleSeqno*/);
    for (int i = 6; i <= 10; i++) {
        const std::string key{"key" + std::to_string(i)};
        const DocKey docKey(key, DocKeyEncodesCollectionId::No);
        checkeq(ENGINE_SUCCESS,
                dcp->mutation(cookie,
                              stream_opaque,
                              docKey,
                              {(const uint8_t*)"value", 5},
                              0, // privileged bytes
                              PROTOCOL_BINARY_RAW_BYTES,
                              i * 3, // cas
                              Vbid(0),
                              0, // flags
                              i, // by_seqno
                              0, // rev_seqno
                              0, // expiration
                              0, // lock_time
                              {}, // meta
                              INITIAL_NRU_VALUE),
                "Failed to send dcp mutation");
    }

    wait_for_flusher_to_settle(h);

    wait_for_stat_to_be(h, "eq_dcpq:unittest:stream_0_buffer_items", 0, "dcp");

    // Might get a buffer ack which we don't care about here so step past
    // anything not a snapshot marker (the first thing we care about).
    do {
        dcp_step(h, cookie, producers);
        checkne(cb::mcbp::ClientOpcode::Invalid,
                producers.last_op,
                "Failed, got EWOULDBLOCK from engine");
    } while (producers.last_op != cb::mcbp::ClientOpcode::DcpSnapshotMarker);
    checkeq(cb::mcbp::Status::Success,
            producers.last_status,
            "Failed, not success");
    checkne(opaque, producers.last_opaque, "Failed, opaque doesn't match");

    dcp_step(h, cookie, producers);
    checkeq(cb::mcbp::ClientOpcode::DcpSeqnoAcknowledged,
            producers.last_op,
            "Failed, not seqno ack");
    checkeq(cb::mcbp::Status::Success,
            producers.last_status,
            "Failed, not success");
    checkne(opaque, producers.last_opaque, "Failed, opaque doesn't match");

    dcp_step(h, cookie, producers);
    checkeq(cb::mcbp::ClientOpcode::DcpSnapshotMarker,
            producers.last_op,
            "Failed, not snapshot marker");
    checkeq(cb::mcbp::Status::Success,
            producers.last_status,
            "Failed, not success");
    checkne(opaque, producers.last_opaque, "Failed, opaque doesn't match");

    dcp_step(h, cookie, producers);
    checkeq(cb::mcbp::ClientOpcode::DcpSeqnoAcknowledged,
            producers.last_op,
            "Failed, not seqno ack");
    checkeq(cb::mcbp::Status::Success,
            producers.last_status,
            "Failed, not success");
    checkne(opaque, producers.last_opaque, "Failed, opaque doesn't match");

    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_failover_scenario_one_with_dcp(EngineIface* h) {
    const int num_items = 50, batch_items = 10;
    for (int start_seqno = 0; start_seqno < num_items;
         start_seqno += batch_items) {
        write_items(h, batch_items, start_seqno);
        wait_for_flusher_to_settle(h);
        createCheckpoint(h);
    }

    createCheckpoint(h);
    wait_for_flusher_to_settle(h);

    const void* cookie = testHarness->create_cookie(h);
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    const char *name = "unittest";

    check(set_vbucket_state(h, Vbid(0), vbucket_state_replica),
          "Failed to set vbucket state.");

    // Open consumer connection
    auto dcp = requireDcpIface(h);
    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie,
                      opaque,
                      0,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp Consumer open connection.");

    add_stream_for_consumer(h,
                            cookie,
                            opaque++,
                            Vbid(0),
                            DCP_ADD_STREAM_FLAG_TAKEOVER,
                            cb::mcbp::Status::Success);

    uint32_t stream_opaque =
            get_int_stat(h, "eq_dcpq:unittest:stream_0_opaque", "dcp");

    checkeq(ENGINE_SUCCESS,
            dcp->snapshot_marker(cookie,
                                 stream_opaque,
                                 Vbid(0),
                                 num_items,
                                 num_items + 100,
                                 0 /*flags*/,
                                 {} /*HCS*/,
                                 {} /*maxVisibleSeqno*/),
            "Failed to send snapshot marker");

    wait_for_stat_to_be(h, "eq_dcpq:unittest:stream_0_buffer_items", 0, "dcp");

    checkeq(ENGINE_SUCCESS,
            dcp->close_stream(cookie, stream_opaque, Vbid(0), {}),
            "Expected success");

    // Simulating a failover scenario, where the replica vbucket will
    // be marked as active.
    check(set_vbucket_state(h, Vbid(0), vbucket_state_active),
          "Failed to set vbucket state.");

    checkeq(ENGINE_SUCCESS,
            store(h, nullptr, OPERATION_SET, "key", "somevalue"),
            "Error in SET operation.");

    wait_for_flusher_to_settle(h);
    checkeq(0, get_int_stat(h, "ep_diskqueue_items"), "Unexpected diskqueue");

    testHarness->destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_failover_scenario_two_with_dcp(EngineIface* h) {
    check(set_vbucket_state(h, Vbid(0), vbucket_state_replica),
          "Failed to set vbucket state.");
    wait_for_flusher_to_settle(h);

    const void* cookie = testHarness->create_cookie(h);
    uint32_t opaque = 0xFFFF0000;
    const char *name = "unittest";

    // Open consumer connection
    auto dcp = requireDcpIface(h);
    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie,
                      opaque,
                      0,
                      0,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp Consumer open connection.");

    // Set up a passive stream
    add_stream_for_consumer(
            h, cookie, opaque++, Vbid(0), 0, cb::mcbp::Status::Success);

    uint32_t stream_opaque =
            get_int_stat(h, "eq_dcpq:unittest:stream_0_opaque", "dcp");

    // Snapshot marker indicating 5 mutations will follow
    checkeq(ENGINE_SUCCESS,
            dcp->snapshot_marker(cookie,
                                 stream_opaque,
                                 Vbid(0),
                                 0,
                                 5,
                                 0,
                                 {} /*HCS*/,
                                 {} /*maxVisibleSeqno*/),
            "Failed to send marker!");

    // Send 4 mutations
    uint64_t i;
    for (i = 1; i <= 4; i++) {
        const std::string key("key" + std::to_string(i));
        const DocKey docKey(key, DocKeyEncodesCollectionId::No);
        checkeq(ENGINE_SUCCESS,
                dcp->mutation(cookie,
                              stream_opaque,
                              docKey,
                              {(const uint8_t*)"value", 5},
                              0, // privileged bytes
                              PROTOCOL_BINARY_RAW_BYTES,
                              i * 3, // cas
                              Vbid(0),
                              0, // flags
                              i, // by_seqno
                              0, // rev_seqno
                              0, // expiration
                              0, // lock_time
                              {}, // meta
                              INITIAL_NRU_VALUE),
                "Failed to dcp mutate.");
    }

    // Simulate failover
    check(set_vbucket_state(h, Vbid(0), vbucket_state_active),
          "Failed to set vbucket state.");
    wait_for_flusher_to_settle(h);

    int openCheckpointId =
            get_int_stat(h, "vb_0:open_checkpoint_id", "checkpoint");

    // Front-end operations (sets)
    write_items(h, 2, 1, "key_");

    // Wait for a new open checkpoint
    wait_for_stat_to_be(
            h, "vb_0:open_checkpoint_id", openCheckpointId + 1, "checkpoint");

    // Consumer processes 5th mutation
    const std::string key("key" + std::to_string(i));
    const DocKey docKey(key, DocKeyEncodesCollectionId::No);
    checkeq(ENGINE_KEY_ENOENT,
            dcp->mutation(cookie,
                          stream_opaque,
                          docKey,
                          {(const uint8_t*)"value", 5},
                          0, // privileged bytes
                          PROTOCOL_BINARY_RAW_BYTES,
                          i * 3, // cas
                          Vbid(0),
                          0, // flags
                          i, // by_seqno
                          0, // rev_seqno
                          0, // expiration
                          0, // lock_time
                          {}, // meta
                          INITIAL_NRU_VALUE),
            "Unexpected response for the mutation!");

    testHarness->destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_dcp_add_stream(EngineIface* h) {
    const void* cookie = testHarness->create_cookie(h);
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    std::string name("unittest");

    check(set_vbucket_state(h, Vbid(0), vbucket_state_replica),
          "Failed to set vbucket state.");

    // Open consumer connection
    auto dcp = requireDcpIface(h);
    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie,
                      opaque,
                      0,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp Consumer open connection.");

    std::string flow_ctl_stat_buf("eq_dcpq:" + name + ":unacked_bytes");
    checkeq(0,
            get_int_stat(h, flow_ctl_stat_buf.c_str(), "dcp"),
            "Consumer flow ctl unacked bytes not starting from 0");

    add_stream_for_consumer(
            h, cookie, opaque++, Vbid(0), 0, cb::mcbp::Status::Success);

    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_consumer_backoff_stat(EngineIface* h) {
    set_param(h,
              cb::mcbp::request::SetParamPayload::Type::Replication,
              "replication_throttle_queue_cap",
              "10");
    checkeq(10,
            get_int_stat(h, "ep_replication_throttle_queue_cap"),
            "Incorrect replication_throttle_queue_cap value.");

    stop_persistence(h);

    const void* cookie = testHarness->create_cookie(h);
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    const char *name = "unittest";

    check(set_vbucket_state(h, Vbid(0), vbucket_state_replica),
          "Failed to set vbucket state.");

    // Open consumer connection
    auto dcp = requireDcpIface(h);
    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie,
                      opaque,
                      0,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp Consumer open connection.");

    add_stream_for_consumer(
            h, cookie, opaque++, Vbid(0), 0, cb::mcbp::Status::Success);

    testHarness->time_travel(30);
    checkeq(0,
            get_int_stat(h, "eq_dcpq:unittest:total_backoffs", "dcp"),
            "Expected backoffs to be 0");

    uint32_t stream_opaque =
            get_int_stat(h, "eq_dcpq:unittest:stream_0_opaque", "dcp");
    checkeq(dcp->snapshot_marker(cookie,
                                 stream_opaque,
                                 Vbid(0),
                                 0,
                                 20,
                                 1,
                                 {} /*HCS*/,
                                 {} /*maxVisibleSeqno*/),
            ENGINE_SUCCESS,
            "Failed to send snapshot marker");

    for (int i = 1; i <= 20; i++) {
        const std::string key("key" + std::to_string(i));
        const DocKey docKey(key, DocKeyEncodesCollectionId::No);
        checkeq(ENGINE_SUCCESS,
                dcp->mutation(cookie,
                              stream_opaque,
                              docKey,
                              {(const uint8_t*)"value", 5},
                              0, // privileged bytes
                              PROTOCOL_BINARY_RAW_BYTES,
                              i * 3, // cas
                              Vbid(0),
                              0, // flags
                              i, // by_seqno
                              0, // rev_seqno
                              0, // expiration
                              0, // lock_time
                              {}, // meta
                              INITIAL_NRU_VALUE),
                "Failed to send dcp mutation");
    }

    wait_for_stat_change(h, "eq_dcpq:unittest:total_backoffs", 0, "dcp");
    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_rollback_to_zero(EngineIface* h) {
    const int num_items = 10;
    write_items(h, num_items);

    wait_for_flusher_to_settle(h);
    verify_curr_items(h, num_items, "Wrong amount of items");

    check(set_vbucket_state(h, Vbid(0), vbucket_state_replica),
          "Failed to set vbucket state.");

    const void* cookie = testHarness->create_cookie(h);
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    const char *name = "unittest";

    // Open consumer connection
    auto dcp = requireDcpIface(h);
    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie,
                      opaque,
                      0,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp Consumer open connection.");

    add_stream_for_consumer(
            h, cookie, opaque++, Vbid(0), 0, cb::mcbp::Status::Rollback);

    wait_for_flusher_to_settle(h);
    wait_for_rollback_to_finish(h);

    checkeq(0,
            get_int_stat(h, "curr_items"),
            "All items should be rolled back");
    checkeq(num_items,
            get_int_stat(h, "vb_replica_rollback_item_count"),
            "Replica rollback count does not match");
    checkeq(num_items,
            get_int_stat(h, "rollback_item_count"),
            "Aggr rollback count does not match");

    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}

/**
 * Call dcp_step() for the given Consumer (identified by cookie) until all
 * DCP_CONTROL messages have been processed.
 *
 * @param engine The engine interface
 * @param cookie The cookie representing the DCP Consumer into the engine
 * @param producers The MockDcpMessageProducers used by the Consumer
 */
static void drainDcpControl(EngineIface* engine,
                            const void* cookie,
                            MockDcpMessageProducers& producers) {
    do {
        dcp_step(engine, cookie, producers);
        // The Sync Repl negotiation introduces a blocking step
        if (producers.last_key == "enable_sync_writes" ||
            producers.last_key == "include_deleted_user_xattrs") {
            simulateProdRespToDcpControlBlockingNegotiation(
                    engine, cookie, producers);
        }
    } while (producers.last_op == cb::mcbp::ClientOpcode::DcpControl);
}

static enum test_result test_chk_manager_rollback(EngineIface* h) {
    if (!isWarmupEnabled(h)) {
        return SKIPPED;
    }

    Vbid vbid = Vbid(0);
    const int num_items = 40;
    stop_persistence(h);
    write_items(h, num_items);

    start_persistence(h);
    wait_for_flusher_to_settle(h);
    verify_curr_items(h, num_items, "Wrong amount of items");

    testHarness->reload_engine(&h,

                               testHarness->get_current_testcase()->cfg,
                               true,
                               false);

    wait_for_warmup_complete(h);
    stop_persistence(h);

    for (int j = 0; j < num_items / 2; ++j) {
        std::stringstream ss;
        ss << "key" << (j + num_items);
        checkeq(ENGINE_SUCCESS,
                store(h, nullptr, OPERATION_SET, ss.str().c_str(), "data"),
                "Failed to store a value");
    }

    start_persistence(h);
    wait_for_flusher_to_settle(h);
    verify_curr_items(h, 60, "Wrong amount of items");
    set_vbucket_state(h, vbid, vbucket_state_replica);

    // Create rollback stream
    const void* cookie = testHarness->create_cookie(h);
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    const char *name = "unittest";

    auto dcp = requireDcpIface(h);
    MockDcpMessageProducers producers(h);
    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie,
                      opaque,
                      0,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp Consumer open connection.");

    checkeq(ENGINE_SUCCESS,
            dcp->add_stream(cookie, ++opaque, vbid, 0),
            "Add stream request failed");

    // When drainDcpControl stops producers.last_XXXX contains the value
    // for the first non-control message
    drainDcpControl(h, cookie, producers);

    uint32_t stream_opaque = producers.last_opaque;
    cb_assert(producers.last_op == cb::mcbp::ClientOpcode::DcpStreamReq);
    cb_assert(producers.last_opaque != opaque);

    uint64_t rollbackSeqno = htonll(40);
    auto* pkt =
        (protocol_binary_response_header*)cb_malloc(32);
    memset(pkt->bytes, '\0', 32);
    pkt->response.setMagic(cb::mcbp::Magic::ClientResponse);
    pkt->response.setOpcode(cb::mcbp::ClientOpcode::DcpStreamReq);
    pkt->response.setStatus(cb::mcbp::Status::Rollback);
    pkt->response.setOpaque(stream_opaque);
    pkt->response.setBodylen(8);
    memcpy(pkt->bytes + 24, &rollbackSeqno, sizeof(uint64_t));

    checkeq(ENGINE_SUCCESS,
            dcp->response_handler(cookie, pkt),
            "Expected success");

    do {
        dcp_step(h, cookie, producers);
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    } while (producers.last_op != cb::mcbp::ClientOpcode::DcpStreamReq);

    stream_opaque = producers.last_opaque;
    cb_free(pkt);

    // Send success

    uint64_t vb_uuid = htonll(123456789);
    uint64_t by_seqno = 0;
    pkt = (protocol_binary_response_header*)cb_malloc(40);
    memset(pkt->bytes, '\0', 40);
    pkt->response.setMagic(cb::mcbp::Magic::ClientResponse);
    pkt->response.setOpcode(cb::mcbp::ClientOpcode::DcpStreamReq);
    pkt->response.setStatus(cb::mcbp::Status::Success);
    pkt->response.setOpaque(stream_opaque);
    pkt->response.setBodylen(16);
    memcpy(pkt->bytes + 24, &vb_uuid, sizeof(uint64_t));
    memcpy(pkt->bytes + 22, &by_seqno, sizeof(uint64_t));

    checkeq(ENGINE_SUCCESS,
            dcp->response_handler(cookie, pkt),
            "Expected success");
    dcp_step(h, cookie, producers);
    cb_free(pkt);

    int items = get_int_stat(h, "curr_items_tot");
    int seqno = get_int_stat(h, "vb_0:high_seqno", "vbucket-seqno");

    checkeq(40, items, "Got invalid amount of items");
    checkeq(40, seqno, "Seqno should be 40 after rollback");
    checkeq(num_items / 2,
            get_int_stat(h, "vb_replica_rollback_item_count"),
            "Replica rollback count does not match");
    checkeq(num_items / 2,
            get_int_stat(h, "rollback_item_count"),
            "Aggr rollback count does not match");

    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_fullrollback_for_consumer(EngineIface* h) {
    const int num_items = 11;
    write_items(h, num_items);

    wait_for_flusher_to_settle(h);
    checkeq(num_items,
            get_int_stat(h, "curr_items"),
            "Item count should've been 10");

    const void* cookie = testHarness->create_cookie(h);
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    const char *name = "unittest";

    check(set_vbucket_state(h, Vbid(0), vbucket_state_replica),
          "Failed to set vbucket state.");

    // Open consumer connection
    auto dcp = requireDcpIface(h);
    MockDcpMessageProducers producers(h);
    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie,
                      opaque,
                      0,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp Consumer open connection.");

    checkeq(ENGINE_SUCCESS,
            dcp->add_stream(cookie, opaque, Vbid(0), 0),
            "Add stream request failed");

    // drainDcpControl keeps on consuming the messages via DCP step
    // and when it returns the producers.last_XXX contains the first
    // non-DCP-Control message
    drainDcpControl(h, cookie, producers);
    cb_assert(producers.last_op == cb::mcbp::ClientOpcode::DcpStreamReq);
    cb_assert(producers.last_opaque != opaque);

    uint32_t headerlen = sizeof(protocol_binary_response_header);
    uint32_t bodylen = sizeof(uint64_t);
    uint64_t rollbackSeqno = htonll(5);
    auto *pkt1 =
        (protocol_binary_response_header*)cb_malloc(headerlen + bodylen);
    memset(pkt1->bytes, '\0', headerlen + bodylen);
    pkt1->response.setMagic(cb::mcbp::Magic::ClientResponse);
    pkt1->response.setOpcode(cb::mcbp::ClientOpcode::DcpStreamReq);
    pkt1->response.setStatus(cb::mcbp::Status::Rollback);
    pkt1->response.setBodylen(bodylen);
    pkt1->response.setOpaque(producers.last_opaque);
    memcpy(pkt1->bytes + headerlen, &rollbackSeqno, bodylen);

    checkeq(ENGINE_SUCCESS,
            dcp->response_handler(cookie, pkt1),
            "Expected Success after Rollback");
    wait_for_stat_to_be(h, "ep_rollback_count", 1);
    dcp_step(h, cookie, producers);

    opaque++;

    cb_assert(producers.last_opaque != opaque);

    bodylen = 2 *sizeof(uint64_t);
    auto* pkt2 =
        (protocol_binary_response_header*)cb_malloc(headerlen + bodylen);
    memset(pkt2->bytes, '\0', headerlen + bodylen);
    pkt2->response.setMagic(cb::mcbp::Magic::ClientResponse);
    pkt2->response.setOpcode(cb::mcbp::ClientOpcode::DcpStreamReq);
    pkt2->response.setStatus(cb::mcbp::Status::Success);
    pkt2->response.setOpaque(producers.last_opaque);
    pkt2->response.setBodylen(bodylen);
    uint64_t vb_uuid = htonll(123456789);
    uint64_t by_seqno = 0;
    memcpy(pkt2->bytes + headerlen, &vb_uuid, sizeof(uint64_t));
    memcpy(pkt2->bytes + headerlen + 8, &by_seqno, sizeof(uint64_t));

    checkeq(ENGINE_SUCCESS,
            dcp->response_handler(cookie, pkt2),
            "Expected success");

    dcp_step(h, cookie, producers);
    cb_assert(producers.last_op == cb::mcbp::ClientOpcode::DcpAddStream);

    cb_free(pkt1);
    cb_free(pkt2);

    //Verify that all items have been removed from consumer
    wait_for_flusher_to_settle(h);
    checkeq(0,
            get_int_stat(h, "vb_replica_curr_items"),
            "Item count should've been 0");
    checkeq(1,
            get_int_stat(h, "ep_rollback_count"),
            "Rollback count expected to be 1");
    checkeq(num_items,
            get_int_stat(h, "vb_replica_rollback_item_count"),
            "Replica rollback count does not match");
    checkeq(num_items,
            get_int_stat(h, "rollback_item_count"),
            "Aggr rollback count does not match");

    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_partialrollback_for_consumer(EngineIface* h) {
    stop_persistence(h);

    const int numInitialItems = 100;
    write_items(h, numInitialItems, 0, "key_");

    start_persistence(h);
    wait_for_flusher_to_settle(h);
    checkeq(100,
            get_int_stat(h, "curr_items"),
            "Item count should've been 100");

    stop_persistence(h);

    /* Write items from 90 to 109 */
    const int numUpdateAndWrites = 20, updateStartSeqno = 90;
    write_items(h, numUpdateAndWrites, updateStartSeqno, "key_");
    start_persistence(h);
    wait_for_flusher_to_settle(h);

    const int expItems = std::max((numUpdateAndWrites + updateStartSeqno),
                                  numInitialItems);
    checkeq(expItems,
            get_int_stat(h, "curr_items"),
            "Item count should've been 110");

    check(set_vbucket_state(h, Vbid(0), vbucket_state_replica),
          "Failed to set vbucket state.");

    const void* cookie = testHarness->create_cookie(h);
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    const char *name = "unittest";

    // Open consumer connection
    auto dcp = requireDcpIface(h);
    MockDcpMessageProducers producers(h);
    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie,
                      opaque,
                      0,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp Consumer open connection.");

    checkeq(ENGINE_SUCCESS,
            dcp->add_stream(cookie, opaque, Vbid(0), 0),
            "Add stream request failed");

    // drainDcpControl keeps on consuming the messages via DCP step
    // and when it returns the producers.last_XXX contains the first
    // non-DCP-Control message
    drainDcpControl(h, cookie, producers);
    cb_assert(producers.last_op == cb::mcbp::ClientOpcode::DcpStreamReq);
    cb_assert(producers.last_opaque != opaque);

    uint32_t headerlen = sizeof(protocol_binary_response_header);
    uint32_t bodylen = sizeof(uint64_t);
    uint64_t rollbackSeqno = 100;
    auto *pkt1 =
        (protocol_binary_response_header*)cb_malloc(headerlen + bodylen);
    memset(pkt1->bytes, '\0', headerlen + bodylen);
    pkt1->response.setMagic(cb::mcbp::Magic::ClientResponse);
    pkt1->response.setOpcode(cb::mcbp::ClientOpcode::DcpStreamReq);
    pkt1->response.setStatus(cb::mcbp::Status::Rollback);
    pkt1->response.setBodylen(bodylen);
    pkt1->response.setOpaque(producers.last_opaque);
    uint64_t rollbackPt = htonll(rollbackSeqno);
    memcpy(pkt1->bytes + headerlen, &rollbackPt, bodylen);

    checkeq(ENGINE_SUCCESS,
            dcp->response_handler(cookie, pkt1),
            "Expected Success after Rollback");
    wait_for_stat_to_be(h, "ep_rollback_count", 1);
    dcp_step(h, cookie, producers);
    opaque++;

    bodylen = 2 * sizeof(uint64_t);
    auto* pkt2 =
        (protocol_binary_response_header*)cb_malloc(headerlen + bodylen);
    memset(pkt2->bytes, '\0', headerlen + bodylen);
    pkt2->response.setMagic(cb::mcbp::Magic::ClientResponse);
    pkt2->response.setOpcode(cb::mcbp::ClientOpcode::DcpStreamReq);
    pkt2->response.setStatus(cb::mcbp::Status::Success);
    pkt2->response.setOpaque(producers.last_opaque);
    pkt2->response.setBodylen(bodylen);
    uint64_t vb_uuid = htonll(123456789);
    uint64_t by_seqno = 0;
    memcpy(pkt2->bytes + headerlen, &vb_uuid, sizeof(uint64_t));
    memcpy(pkt2->bytes + headerlen + 8, &by_seqno, sizeof(uint64_t));

    checkeq(ENGINE_SUCCESS,
            dcp->response_handler(cookie, pkt2),
            "Expected success");
    dcp_step(h, cookie, producers);

    cb_free(pkt1);
    cb_free(pkt2);

    //?Verify that 10 items plus 10 updates have been removed from consumer
    wait_for_flusher_to_settle(h);
    checkeq(1,
            get_int_stat(h, "ep_rollback_count"),
            "Rollback count expected to be 1");

    if (isPersistentBucket(h)) {
        checkeq(rollbackSeqno,
                get_ull_stat(h, "vb_replica_curr_items"),
                "Item count should've been 100");
        checkeq(numUpdateAndWrites,
                get_int_stat(h, "vb_replica_rollback_item_count"),
                "Replica rollback count does not match");
        checkeq(numUpdateAndWrites,
                get_int_stat(h, "rollback_item_count"),
                "Aggr rollback count does not match");
    } else {
        /* We always rollback to 0 in 'Ephemeral Buckets' */
        checkeq(0,
                get_int_stat(h, "vb_replica_curr_items"),
                "Item count should've been 0");
        checkeq(numInitialItems + numUpdateAndWrites,
                get_int_stat(h, "vb_replica_rollback_item_count"),
                "Replica rollback count does not match");
        checkeq(numInitialItems + numUpdateAndWrites,
                get_int_stat(h, "rollback_item_count"),
                "Aggr rollback count does not match");
    }

    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_buffer_log_size(EngineIface* h) {
    const void* cookie = testHarness->create_cookie(h);
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = cb::mcbp::request::DcpOpenPayload::Producer;
    const char *name = "unittest";
    char stats_buffer[50];
    char status_buffer[50];

    // Open consumer connection
    auto dcp = requireDcpIface(h);
    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie,
                      opaque,
                      0,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp Consumer open connection.");

    checkeq(ENGINE_SUCCESS,
            dcp->control(cookie, ++opaque, "connection_buffer_size", "0"),
            "Failed to establish connection buffer");
    snprintf(status_buffer, sizeof(status_buffer),
             "eq_dcpq:%s:flow_control", name);
    std::string status = get_str_stat(h, status_buffer, "dcp");
    checkeq(0, status.compare("disabled"), "Flow control enabled!");

    checkeq(ENGINE_SUCCESS,
            dcp->control(cookie, ++opaque, "connection_buffer_size", "512"),
            "Failed to establish connection buffer");

    snprintf(stats_buffer, sizeof(stats_buffer),
             "eq_dcpq:%s:max_buffer_bytes", name);

    checkeq(512,
            get_int_stat(h, stats_buffer, "dcp"),
            "Buffer Size did not get set");

    checkeq(ENGINE_SUCCESS,
            dcp->control(cookie, ++opaque, "connection_buffer_size", "1024"),
            "Failed to establish connection buffer");

    checkeq(1024,
            get_int_stat(h, stats_buffer, "dcp"),
            "Buffer Size did not get reset");

    /* Set flow control buffer size to zero which implies disable it */
    checkeq(ENGINE_SUCCESS,
            dcp->control(cookie, ++opaque, "connection_buffer_size", "0"),
            "Failed to establish connection buffer");
    status = get_str_stat(h, status_buffer, "dcp");
    checkeq(0, status.compare("disabled"), "Flow control enabled!");

    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_producer_flow_control(EngineIface* h) {
    /* Write 10 items */
    const int num_items = 10;
    write_items(h, 10, 0, "key", "123456789");

    wait_for_flusher_to_settle(h);
    verify_curr_items(h, num_items, "Wrong amount of items");

    /* Disable flow control and stream all items. The producer should stream all
     items even when we do not send acks */
    std::string name("unittest");

    DcpStreamCtx ctx1;
    ctx1.vb_uuid = get_ull_stat(h, "vb_0:0:id", "failovers");
    ctx1.seqno = {0, num_items};
    ctx1.exp_mutations = num_items;
    ctx1.exp_markers = 1;

    const void* cookie = testHarness->create_cookie(h);
    TestDcpConsumer tdc1(name, cookie, h);
    tdc1.setFlowControlBufSize(0);  // Disabling flow control
    tdc1.disableAcking();           // Do not ack
    tdc1.addStreamCtx(ctx1);
    tdc1.run();

    /* Set flow control buffer to a very low value such that producer is not
     expected to send more than 1 item when we do not send acks */
    std::string name1("unittest1");

    DcpStreamCtx ctx2;
    ctx2.vb_uuid = get_ull_stat(h, "vb_0:0:id", "failovers");
    ctx2.seqno = {0, num_items};
    ctx2.exp_mutations = 1;
    ctx2.exp_markers = 1;

    const void* cookie1 = testHarness->create_cookie(h);
    TestDcpConsumer tdc2(name1, cookie1, h);
    tdc2.setFlowControlBufSize(100);    // Flow control buf set to low value
    tdc2.disableAcking();               // Do not ack
    tdc2.addStreamCtx(ctx2);
    tdc2.run();

    testHarness->destroy_cookie(cookie);
    testHarness->destroy_cookie(cookie1);

    return SUCCESS;
}

static enum test_result test_dcp_get_failover_log(EngineIface* h) {
    const void* cookie = testHarness->create_cookie(h);
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = cb::mcbp::request::DcpOpenPayload::Producer;
    const char *name = "unittest";

    // Open consumer connection
    auto dcp = requireDcpIface(h);
    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie,
                      opaque,
                      0,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp Consumer open connection.");

    checkeq(ENGINE_SUCCESS,
            dcp->get_failover_log(
                    cookie, opaque, Vbid(0), mock_dcp_add_failover_log),
            "Failed to retrieve failover log");

    testHarness->destroy_cookie(cookie);

    checkeq(ENGINE_SUCCESS,
            get_stats(h, "failovers"sv, {}, add_stats),
            "Failed to get stats.");

    size_t i = 0;
    for (i = 0; i < dcp_failover_log.size(); i++) {
        std::string itr;
        std::ostringstream ss;
        ss << i;
        itr = ss.str();
        std::string uuid = "vb_0:" + itr + ":id";
        std::string seqno = "vb_0:" + itr + ":seq";
        checkeq(static_cast<unsigned long long int>(dcp_failover_log[i].first),
                strtoull((vals[uuid]).c_str(), nullptr, 10),
                "UUID mismatch in failover stats");
        checkeq(static_cast<unsigned long long int>(dcp_failover_log[i].second),
                strtoull((vals[seqno]).c_str(), nullptr, 10),
                "SEQNO mismatch in failover stats");
    }

    vals.clear();
    return SUCCESS;
}

static enum test_result test_dcp_add_stream_exists(EngineIface* h) {
    const void* cookie = testHarness->create_cookie(h);
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    const char *name = "unittest";
    Vbid vbucket = Vbid(0);

    check(set_vbucket_state(h, vbucket, vbucket_state_replica),
          "Failed to set vbucket state.");

    /* Open consumer connection */
    auto dcp = requireDcpIface(h);
    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie,
                      opaque,
                      0,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp consumer open connection.");

    /* Send add stream to consumer */
    checkeq(ENGINE_SUCCESS,
            dcp->add_stream(cookie, ++opaque, vbucket, 0),
            "Add stream request failed");

    /* Send add stream to consumer twice and expect failure */
    checkeq(ENGINE_KEY_EEXISTS,
            dcp->add_stream(cookie, ++opaque, Vbid(0), 0),
            "Stream exists for this vbucket");

    /* Try adding another stream for the vbucket in another consumer conn */
    /* Open another consumer connection */
    const void* cookie1 = testHarness->create_cookie(h);
    uint32_t opaque1 = 0xFFFF0000;
    std::string name1("unittest1");
    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie1,
                      opaque1,
                      0,
                      flags,
                      name1,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp consumer open connection.");

    /* Send add stream */
    checkeq(ENGINE_KEY_EEXISTS,
            dcp->add_stream(cookie1, ++opaque1, vbucket, 0),
            "Stream exists for this vbucket");

    /* Just check that we can add passive stream for another vbucket in this
       conn*/
    checkeq(true,
            set_vbucket_state(
                    h, Vbid(vbucket.get() + 1), vbucket_state_replica),
            "Failed to set vbucket state.");
    checkeq(ENGINE_SUCCESS,
            dcp->add_stream(cookie1, ++opaque1, Vbid(vbucket.get() + 1), 0),
            "Add stream request failed in the second conn");
    testHarness->destroy_cookie(cookie);
    testHarness->destroy_cookie(cookie1);
    return SUCCESS;
}

static enum test_result test_dcp_add_stream_nmvb(EngineIface* h) {
    const void* cookie = testHarness->create_cookie(h);
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    const char *name = "unittest";

    check(set_vbucket_state(h, Vbid(0), vbucket_state_replica),
          "Failed to set vbucket state.");

    // Open consumer connection
    auto dcp = requireDcpIface(h);
    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie,
                      opaque,
                      0,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp consumer open connection.");

    // Send add stream to consumer for vbucket that doesn't exist
    opaque++;
    checkeq(ENGINE_NOT_MY_VBUCKET,
            dcp->add_stream(cookie, opaque, Vbid(1), 0),
            "Add stream expected not my vbucket");
    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_add_stream_prod_exists(EngineIface* h) {
    const void* cookie = testHarness->create_cookie(h);
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    const char *name = "unittest";

    check(set_vbucket_state(h, Vbid(0), vbucket_state_replica),
          "Failed to set vbucket state.");

    // Open consumer connection
    auto dcp = requireDcpIface(h);
    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie,
                      opaque,
                      0,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp consumer open connection.");

    add_stream_for_consumer(
            h, cookie, opaque++, Vbid(0), 0, cb::mcbp::Status::KeyEexists);
    testHarness->destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_dcp_add_stream_prod_nmvb(EngineIface* h) {
    const void* cookie = testHarness->create_cookie(h);
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    const char *name = "unittest";

    check(set_vbucket_state(h, Vbid(0), vbucket_state_replica),
          "Failed to set vbucket state.");

    auto dcp = requireDcpIface(h);
    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie,
                      opaque,
                      0,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp producer open connection.");

    add_stream_for_consumer(
            h, cookie, opaque++, Vbid(0), 0, cb::mcbp::Status::NotMyVbucket);
    testHarness->destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_dcp_close_stream_no_stream(EngineIface* h) {
    const void* cookie = testHarness->create_cookie(h);
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    const char *name = "unittest";

    auto dcp = requireDcpIface(h);
    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie,
                      opaque,
                      0,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp producer open connection.");

    checkeq(ENGINE_KEY_ENOENT,
            dcp->close_stream(cookie, opaque + 1, Vbid(0), {}),
            "Expected stream doesn't exist");

    testHarness->destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_dcp_close_stream(EngineIface* h) {
    const void* cookie = testHarness->create_cookie(h);
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    const char *name = "unittest";

    check(set_vbucket_state(h, Vbid(0), vbucket_state_replica),
          "Failed to set vbucket state.");

    auto dcp = requireDcpIface(h);
    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie,
                      opaque,
                      0,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp producer open connection.");

    add_stream_for_consumer(
            h, cookie, opaque++, Vbid(0), 0, cb::mcbp::Status::Success);

    uint32_t stream_opaque =
            get_int_stat(h, "eq_dcpq:unittest:stream_0_opaque", "dcp");
    std::string state =
            get_str_stat(h, "eq_dcpq:unittest:stream_0_state", "dcp");
    checkeq(0, state.compare("reading"), "Expected stream in reading state");

    checkeq(ENGINE_SUCCESS,
            dcp->close_stream(cookie, stream_opaque, Vbid(0), {}),
            "Expected success");

    testHarness->destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_dcp_consumer_end_stream(EngineIface* h) {
    const void* cookie = testHarness->create_cookie(h);
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    Vbid vbucket = Vbid(0);
    const char *name = "unittest";

    check(set_vbucket_state(h, Vbid(0), vbucket_state_replica),
          "Failed to set vbucket state.");

    auto dcp = requireDcpIface(h);
    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie,
                      opaque,
                      0,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp producer open connection.");

    add_stream_for_consumer(
            h, cookie, opaque++, vbucket, 0, cb::mcbp::Status::Success);

    uint32_t stream_opaque =
            get_int_stat(h, "eq_dcpq:unittest:stream_0_opaque", "dcp");
    std::string state =
            get_str_stat(h, "eq_dcpq:unittest:stream_0_state", "dcp");
    checkeq(0, state.compare("reading"), "Expected stream in reading state");

    checkeq(ENGINE_SUCCESS,
            dcp->stream_end(cookie,
                            stream_opaque,
                            vbucket,
                            cb::mcbp::DcpStreamEndStatus::Ok),
            "Expected success");

    testHarness->destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_dcp_consumer_mutate(EngineIface* h) {
    check(set_vbucket_state(h, Vbid(0), vbucket_state_replica),
          "Failed to set vbucket state.");

    const void* cookie = testHarness->create_cookie(h);
    uint32_t opaque = 0xFFFF0000;
    uint32_t seqno = 0;
    uint32_t flags = 0;
    std::string name("unittest");

    // Open an DCP connection
    auto dcp = requireDcpIface(h);
    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie,
                      opaque,
                      seqno,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp producer open connection.");

    std::string type = get_str_stat(h, "eq_dcpq:unittest:type", "dcp");
    checkeq(0, type.compare("consumer"), "Consumer not found");

    int exp_unacked_bytes = 0;
    std::string flow_ctl_stat_buf("eq_dcpq:" + name + ":unacked_bytes");
    checkeq(exp_unacked_bytes,
            get_int_stat(h, flow_ctl_stat_buf.c_str(), "dcp"),
            "Consumer flow ctl unacked bytes not starting from 0");

    opaque = add_stream_for_consumer(
            h, cookie, opaque, Vbid(0), 0, cb::mcbp::Status::Success);

    std::string key("key");
    uint32_t dataLen = 100;
    char *data = static_cast<char *>(cb_malloc(dataLen));
    memset(data, 'x', dataLen);

    uint8_t cas = 0x1;
    Vbid vbucket = Vbid(0);
    uint8_t datatype = 1;
    uint64_t bySeqno = 10;
    uint64_t revSeqno = 0;
    uint32_t exprtime = 0;
    uint32_t lockTime = 0;

    checkeq(ENGINE_SUCCESS,
            dcp->snapshot_marker(cookie,
                                 opaque,
                                 Vbid(0),
                                 10,
                                 10,
                                 1,
                                 {} /*HCS*/,
                                 {} /*maxVisibleSeqno*/),
            "Failed to send snapshot marker");

    /* Add snapshot marker bytes to unacked bytes. Since we are shipping out
       acks by calling dcp->step(), the unacked bytes will increase */
    const auto flowCtlEabled =
            get_str_stat(h, "ep_dcp_flow_control_policy") != "none";
    if (flowCtlEabled) {
        exp_unacked_bytes += dcp_snapshot_marker_base_msg_bytes;
        checkeq(exp_unacked_bytes,
                get_int_stat(h, flow_ctl_stat_buf.c_str(), "dcp"),
                "Consumer flow ctl snapshot marker bytes not accounted "
                "correctly");
    }

    // Ensure that we don't accept invalid opaque values
    const DocKey docKey{key, DocKeyEncodesCollectionId::No};
    checkeq(ENGINE_KEY_EEXISTS,
            dcp->mutation(cookie,
                          opaque + 1,
                          docKey,
                          {(const uint8_t*)data, dataLen},
                          0,
                          datatype,
                          cas,
                          vbucket,
                          flags,
                          bySeqno,
                          revSeqno,
                          exprtime,
                          lockTime,
                          {},
                          0),
            "Failed to detect invalid DCP opaque value");

    /* Add mutation bytes to unacked bytes. Since we are shipping out
       acks by calling dcp->step(), the unacked bytes will increase */
    if (flowCtlEabled) {
        exp_unacked_bytes +=
                (dcp_mutation_base_msg_bytes + key.length() + dataLen);
        checkeq(exp_unacked_bytes,
                get_int_stat(h, flow_ctl_stat_buf.c_str(), "dcp"),
                "Consumer flow ctl mutation bytes not accounted correctly");
    }

    // Send snapshot marker
    checkeq(ENGINE_SUCCESS,
            dcp->snapshot_marker(cookie,
                                 opaque,
                                 Vbid(0),
                                 10,
                                 15,
                                 300,
                                 {} /*HCS*/,
                                 {} /*maxVisibleSeqno*/),
            "Failed to send marker!");

    if (flowCtlEabled) {
        exp_unacked_bytes += dcp_snapshot_marker_base_msg_bytes;
        checkeq(exp_unacked_bytes,
                get_int_stat(h, flow_ctl_stat_buf.c_str(), "dcp"),
                "Consumer flow ctl snapshot marker bytes not accounted "
                "correctly");
    }

    // Consume an DCP mutation
    checkeq(ENGINE_SUCCESS,
            dcp->mutation(cookie,
                          opaque,
                          docKey,
                          {(const uint8_t*)data, dataLen},
                          0,
                          datatype,
                          cas,
                          vbucket,
                          flags,
                          bySeqno,
                          revSeqno,
                          exprtime,
                          lockTime,
                          {},
                          0),
            "Failed dcp mutate.");

    if (flowCtlEabled) {
        exp_unacked_bytes +=
                (dcp_mutation_base_msg_bytes + key.length() + dataLen);
        checkeq(exp_unacked_bytes,
                get_int_stat(h, flow_ctl_stat_buf.c_str(), "dcp"),
                "Consumer flow ctl mutation bytes not accounted correctly");
    }

    wait_for_stat_to_be(h, "eq_dcpq:unittest:stream_0_buffer_items", 0, "dcp");

    check(set_vbucket_state(h, Vbid(0), vbucket_state_active),
          "Failed to set vbucket state.");

    check_key_value(h, "key", data, dataLen);

    testHarness->destroy_cookie(cookie);
    cb_free(data);

    return SUCCESS;
}

static enum test_result test_dcp_consumer_delete(EngineIface* h) {
    // Store an item
    checkeq(ENGINE_SUCCESS,
            store(h, nullptr, OPERATION_ADD, "key", "value"),
            "Failed to fail to store an item.");
    wait_for_flusher_to_settle(h);
    verify_curr_items(h, 1, "one item stored");

    check(set_vbucket_state(h, Vbid(0), vbucket_state_replica),
          "Failed to set vbucket state.");

    const void* cookie = testHarness->create_cookie(h);
    uint32_t opaque = 0;
    uint8_t cas = 0x1;
    Vbid vbucket = Vbid(0);
    uint32_t flags = 0;
    uint64_t bySeqno = 10;
    uint64_t revSeqno = 0;
    const char *name = "unittest";
    uint32_t seqno = 0;

    // Open an DCP connection
    auto dcp = requireDcpIface(h);
    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie,
                      opaque,
                      seqno,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp producer open connection.");

    std::string type = get_str_stat(h, "eq_dcpq:unittest:type", "dcp");

    checkeq(0, type.compare("consumer"), "Consumer not found");

    opaque = add_stream_for_consumer(
            h, cookie, opaque, Vbid(0), 0, cb::mcbp::Status::Success);

    int exp_unacked_bytes = dcp_snapshot_marker_base_msg_bytes;
    checkeq(ENGINE_SUCCESS,
            dcp->snapshot_marker(cookie,
                                 opaque,
                                 Vbid(0),
                                 10,
                                 10,
                                 1,
                                 {} /*HCS*/,
                                 {} /*maxVisibleSeqno*/),
            "Failed to send snapshot marker");

    const std::string key{"key"};
    const DocKey docKey{key, DocKeyEncodesCollectionId::No};
    // verify that we don't accept invalid opaque id's
    checkeq(ENGINE_KEY_ENOENT,
            dcp->deletion(cookie,
                          opaque + 1,
                          docKey,
                          {},
                          0,
                          PROTOCOL_BINARY_RAW_BYTES,
                          cas,
                          vbucket,
                          bySeqno,
                          revSeqno,
                          {}),
            "Failed to detect invalid DCP opaque value.");
    exp_unacked_bytes += dcp_deletion_base_msg_bytes + key.length();

    // Consume an DCP deletion
    checkeq(ENGINE_SUCCESS,
            dcp->deletion(cookie,
                          opaque,
                          docKey,
                          {},
                          0,
                          PROTOCOL_BINARY_RAW_BYTES,
                          cas,
                          vbucket,
                          bySeqno,
                          revSeqno,
                          {}),
            "Failed dcp delete.");

    if (get_str_stat(h, "ep_dcp_flow_control_policy") != "none") {
        exp_unacked_bytes += dcp_deletion_base_msg_bytes + key.length();
        checkeq(exp_unacked_bytes,
                get_int_stat(h, "eq_dcpq:unittest:unacked_bytes", "dcp"),
                "Consumer flow ctl mutation bytes not accounted correctly");
    }

    wait_for_stat_to_be(h, "eq_dcpq:unittest:stream_0_buffer_items", 0, "dcp");

    wait_for_stat_change(h, "curr_items", 1);
    verify_curr_items(h, 0, "one item deleted");
    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}
/**
 * This test drives the consumer during an item's expiration sequence,
 * ensuring that the correct number of bytes are sent in response as well as
 * the success of handling the expiration.
 */
static enum test_result test_dcp_consumer_expire(EngineIface* h) {
    check(set_vbucket_state(h, Vbid(0), vbucket_state_replica),
          "Failed to set vbucket state.");

    const void* cookie = testHarness->create_cookie(h);
    uint32_t opaque = 0;
    uint8_t cas = 0x1;
    Vbid vbucket = Vbid(0);
    uint32_t flags = cb::mcbp::request::DcpOpenPayload::IncludeDeleteTimes;
    uint64_t bySeqno = 10;
    uint64_t revSeqno = 0;
    const char* name = "unittest";
    uint32_t seqno = 0;

    // Open an DCP connection
    auto dcp = requireDcpIface(h);
    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie,
                      opaque,
                      seqno,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp producer open connection.");

    std::string type = get_str_stat(h, "eq_dcpq:unittest:type", "dcp");

    checkeq(0, type.compare("consumer"), "Consumer not found");

    opaque = add_stream_for_consumer(
            h, cookie, opaque, Vbid(0), 0, cb::mcbp::Status::Success);

    int exp_unacked_bytes = dcp_snapshot_marker_base_msg_bytes;
    checkeq(ENGINE_SUCCESS,
            dcp->snapshot_marker(cookie,
                                 opaque,
                                 Vbid(0),
                                 10,
                                 10,
                                 1,
                                 {} /*HCS*/,
                                 {} /*maxVisibleSeqno*/),
            "Failed to send snapshot marker");

    const std::string key{"key"};
    const DocKey docKey{key, DocKeyEncodesCollectionId::No};
    // verify that we don't accept invalid opaque id's
    checkeq(ENGINE_KEY_ENOENT,
            dcp->expiration(cookie,
                            opaque + 1,
                            docKey,
                            {},
                            0,
                            PROTOCOL_BINARY_RAW_BYTES,
                            cas,
                            vbucket,
                            bySeqno,
                            revSeqno,
                            {}),
            "Failed to detect invalid DCP opaque value.");
    exp_unacked_bytes += dcp_expiration_base_msg_bytes + key.length();

    // Consume an DCP expiration
    checkeq(ENGINE_SUCCESS,
            dcp->expiration(cookie,
                            opaque,
                            docKey,
                            {},
                            0,
                            PROTOCOL_BINARY_RAW_BYTES,
                            cas,
                            vbucket,
                            bySeqno,
                            revSeqno,
                            {}),
            "Failed dcp expire.");

    if (get_str_stat(h, "ep_dcp_flow_control_policy") != "none") {
        exp_unacked_bytes += dcp_expiration_base_msg_bytes + key.length();
        checkeq(exp_unacked_bytes,
                get_int_stat(h, "eq_dcpq:unittest:unacked_bytes", "dcp"),
                "Consumer flow ctl expiration bytes not accounted correctly");
    }

    wait_for_stat_to_be(h, "eq_dcpq:unittest:stream_0_buffer_items", 0, "dcp");

    wait_for_stat_change(h, "curr_items", 1);
    verify_curr_items(h, 0, "one item expired");
    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_replica_stream_backfill(EngineIface* h) {
    check(set_vbucket_state(h, Vbid(0), vbucket_state_replica),
          "Failed to set vbucket state.");

    const void* cookie = testHarness->create_cookie(h);
    uint32_t opaque = 0xFFFF0000;
    uint32_t seqno = 0;
    uint32_t flags = 0;
    const int num_items = 100;
    const char *name = "unittest";

    /* Open an DCP consumer connection */
    auto dcp = requireDcpIface(h);
    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie,
                      opaque,
                      seqno,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp producer open connection.");

    std::string type = get_str_stat(h, "eq_dcpq:unittest:type", "dcp");
    checkeq(0, type.compare("consumer"), "Consumer not found");

    opaque = add_stream_for_consumer(
            h, cookie, opaque, Vbid(0), 0, cb::mcbp::Status::Success);

    /* Write backfill elements on to replica, flag (0x02) */
    dcp_stream_to_replica(
            h, cookie, opaque, Vbid(0), 0x02, 1, num_items, 0, num_items);

    /* Stream in mutations from replica */
    wait_for_flusher_to_settle(h);
    wait_for_stat_to_be(h, "vb_0:high_seqno", num_items, "vbucket-seqno");

    DcpStreamCtx ctx;
    ctx.vb_uuid = get_ull_stat(h, "vb_0:0:id", "failovers");
    ctx.seqno = {0, num_items};
    ctx.exp_mutations = num_items;
    ctx.exp_markers = 1;

    const void* cookie1 = testHarness->create_cookie(h);
    TestDcpConsumer tdc("unittest1", cookie1, h);
    tdc.addStreamCtx(ctx);
    tdc.run();

    testHarness->destroy_cookie(cookie1);
    testHarness->destroy_cookie(cookie);
    return SUCCESS;
}

// Test generates a replica VB and splits the generation with a warmup.
// Importantly the very first batch of DCP items are marked as 'backfill' and
// the test requires that the individual flusher_batch_split_trigger setting is
// less than the size of the first batch.
static enum test_result test_dcp_replica_stream_backfill_MB_34173(
        EngineIface* h) {
    check(set_vbucket_state(h, Vbid(0), vbucket_state_replica),
          "Failed to set vbucket state.");
    const int items = 100;

    // Validate that the flusher will split the items
    checkgt(items,
            get_int_stat(h, "ep_flusher_total_batch_limit", "config"),
            "flusher_batch_split_trigger must be less than items");

    const void* cookie = testHarness->create_cookie(h);
    uint32_t opaque = 0xFFFF0000;
    uint32_t seqno = 0;
    uint32_t flags = 0;
    const char* name = "MB_34173";

    auto dcp = requireDcpIface(h);
    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie,
                      opaque,
                      seqno,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp producer open connection.");
    std::string type = get_str_stat(h, "eq_dcpq:MB_34173:type", "dcp");
    checkeq(0, type.compare("consumer"), "Consumer not found");

    opaque = add_stream_for_consumer(
            h, cookie, opaque, Vbid(0), 0, cb::mcbp::Status::Success);
    // backfill items 1 to 100
    dcp_stream_to_replica(h, cookie, opaque, Vbid(0), 0x02, 1, items, 1, items);
    wait_for_flusher_to_settle(h);
    wait_for_stat_to_be(h, "vb_0:high_seqno", items, "vbucket-seqno");

    testHarness->destroy_cookie(cookie);

    testHarness->reload_engine(&h,

                               testHarness->get_current_testcase()->cfg,
                               true,
                               true);
    wait_for_warmup_complete(h);

    cookie = testHarness->create_cookie(h);
    opaque = 0xFFFF0000;
    dcp = requireDcpIface(h);
    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie,
                      opaque,
                      seqno,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp producer open connection.");

    type = get_str_stat(h, "eq_dcpq:MB_34173:type", "dcp");
    checkeq(0, type.compare("consumer"), "Consumer not found");

    opaque = add_stream_for_consumer(
            h, cookie, opaque, Vbid(0), 0, cb::mcbp::Status::Success);

    // A second batch could fail if MB-34173 is not fixed, I say could because
    // the corruption of the snapshot range may not yield a failure...
    dcp_stream_to_replica(h,
                          cookie,
                          opaque,
                          Vbid(0),
                          0x02,
                          items + 1,
                          items + 10,
                          items + 1,
                          items + 10);

    testHarness->destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_dcp_replica_stream_in_memory(EngineIface* h) {
    check(set_vbucket_state(h, Vbid(0), vbucket_state_replica),
          "Failed to set vbucket state.");

    const void* cookie = testHarness->create_cookie(h);
    uint32_t opaque = 0xFFFF0000;
    uint32_t seqno = 0;
    uint32_t flags = 0;
    const int num_items = 100;
    const char *name = "unittest";

    /* Open an DCP consumer connection */
    auto dcp = requireDcpIface(h);
    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie,
                      opaque,
                      seqno,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp producer open connection.");

    std::string type = get_str_stat(h, "eq_dcpq:unittest:type", "dcp");
    checkeq(0, type.compare("consumer"), "Consumer not found");

    opaque = add_stream_for_consumer(
            h, cookie, opaque, Vbid(0), 0, cb::mcbp::Status::Success);

    /* Send DCP mutations with in memory flag (0x01) */
    dcp_stream_to_replica(
            h, cookie, opaque, Vbid(0), 0x01, 1, num_items, 0, num_items);

    /* Stream in memory mutations from replica */
    wait_for_flusher_to_settle(h);
    wait_for_stat_to_be(h, "vb_0:high_seqno", num_items, "vbucket-seqno");

    DcpStreamCtx ctx;
    ctx.vb_uuid = get_ull_stat(h, "vb_0:0:id", "failovers");
    ctx.seqno = {0, num_items};
    ctx.exp_mutations = num_items;
    ctx.exp_markers = 1;

    const void* cookie1 = testHarness->create_cookie(h);
    TestDcpConsumer tdc("unittest1", cookie1, h);
    tdc.addStreamCtx(ctx);
    tdc.run();

    testHarness->destroy_cookie(cookie1);
    testHarness->destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_dcp_replica_stream_all(EngineIface* h) {
    check(set_vbucket_state(h, Vbid(0), vbucket_state_replica),
          "Failed to set vbucket state.");

    const void* cookie = testHarness->create_cookie(h);
    uint32_t opaque = 0xFFFF0000;
    uint32_t seqno = 0;
    uint32_t flags = 0;
    const int num_items = 100;
    const char *name = "unittest";

    /* Open an DCP consumer connection */
    auto dcp = requireDcpIface(h);
    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie,
                      opaque,
                      seqno,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp producer open connection.");

    std::string type = get_str_stat(h, "eq_dcpq:unittest:type", "dcp");
    checkeq(0, type.compare("consumer"), "Consumer not found");

    opaque = add_stream_for_consumer(
            h, cookie, opaque, Vbid(0), 0, cb::mcbp::Status::Success);

    /* Send DCP mutations with in memory flag (0x01) */
    dcp_stream_to_replica(
            h, cookie, opaque, Vbid(0), 0x01, 1, num_items, 0, num_items);

    /* Send 100 more DCP mutations with checkpoint creation flag (0x04) */
    uint64_t start = num_items;
    dcp_stream_to_replica(h,
                          cookie,
                          opaque,
                          Vbid(0),
                          0x04,
                          start + 1,
                          start + 100,
                          start,
                          start + 100);

    wait_for_flusher_to_settle(h);
    stop_persistence(h);
    checkeq(2 * num_items,
            get_int_stat(h, "vb_replica_curr_items"),
            "wrong number of items in replica vbucket");

    /* Add 100 more items to the replica node on a new checkpoint */
    /* Send with flag (0x04) indicating checkpoint creation */
    start = 2 * num_items;
    dcp_stream_to_replica(h,
                          cookie,
                          opaque,
                          Vbid(0),
                          0x04,
                          start + 1,
                          start + 100,
                          start,
                          start + 100);

    /* Disk backfill + in memory stream from replica */
    /* Wait for a checkpoint to be removed */
    wait_for_stat_to_be_lte(h, "vb_0:num_checkpoints", 2, "checkpoint");

    DcpStreamCtx ctx;
    ctx.vb_uuid = get_ull_stat(h, "vb_0:0:id", "failovers");
    ctx.seqno = {0, get_ull_stat(h, "vb_0:high_seqno", "vbucket-seqno")};
    ctx.exp_mutations = 300;
    ctx.exp_markers = 1;

    const void* cookie1 = testHarness->create_cookie(h);
    TestDcpConsumer tdc("unittest1", cookie1, h);
    tdc.addStreamCtx(ctx);
    tdc.run();

    testHarness->destroy_cookie(cookie1);
    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}

/*
 * Perform the same test as test_dcp_replica_stream_all(), but this time
 * enable collections on the DCP stream.
 *
 * This test performs the following steps:
 * 1. Stream 100 keys in the default collection to the replica for fist
 * checkpoint
 * 2. Stream another 100 keys in the default collection to the replica creating
 * a new checkpoint
 * 3. flush the mutations to disk in closed checkpoints
 * 4. stream another 100 new keys to the replica
 * 5. Now create a new DCP consumer
 * 6. New DCP consumer the streams all 300 mutations from the replica vbucket
 * in this case we should receive all 300 mutations as one disk snapshot.
 * Under the hood the first 100 should be streamed from backfill and the next
 * 100 should come from memory. We should not see a SeqnoAdvanced op as we're
 * only streaming the default collection and no other collections have been
 * created.
 */
static enum test_result test_dcp_replica_stream_all_collection_enabled(
        EngineIface* h) {
    check(set_vbucket_state(h, Vbid(0), vbucket_state_replica),
          "Failed to set vbucket state.");

    const void* cookie = testHarness->create_cookie(h);
    uint32_t opaque = 0xFFFF0000;
    uint32_t seqno = 0;
    uint32_t flags = 0;
    const int num_items = 100;
    const char* name = "unittest";

    /* Open an DCP consumer connection */
    auto dcp = requireDcpIface(h);
    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie,
                      opaque,
                      seqno,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp producer open connection.");

    std::string type = get_str_stat(h, "eq_dcpq:unittest:type", "dcp");
    checkeq(0, type.compare("consumer"), "Consumer not found");

    opaque = add_stream_for_consumer(
            h, cookie, opaque, Vbid(0), 0, cb::mcbp::Status::Success);

    /* Send DCP mutations with in memory flag (0x01) */
    dcp_stream_to_replica(
            h, cookie, opaque, Vbid(0), 0x01, 1, num_items, 0, num_items);

    /* Send 100 more DCP mutations with checkpoint creation flag (0x04) */
    uint64_t start = num_items;
    dcp_stream_to_replica(h,
                          cookie,
                          opaque,
                          Vbid(0),
                          0x04,
                          start + 1,
                          start + 100,
                          start,
                          start + 100);

    wait_for_flusher_to_settle(h);
    stop_persistence(h);
    checkeq(2 * num_items,
            get_int_stat(h, "vb_replica_curr_items"),
            "wrong number of items in replica vbucket");

    /* Add 100 more items to the replica node on a new checkpoint */
    /* Send with flag (0x04) indicating checkpoint creation */
    start = 2 * num_items;
    dcp_stream_to_replica(h,
                          cookie,
                          opaque,
                          Vbid(0),
                          0x04,
                          start + 1,
                          start + 100,
                          start,
                          start + 100);

    /* Disk backfill + in memory stream from replica */
    /* Wait for a checkpoint to be removed */
    wait_for_stat_to_be_lte(h, "vb_0:num_checkpoints", 2, "checkpoint");

    DcpStreamCtx ctx;
    ctx.vb_uuid = get_ull_stat(h, "vb_0:0:id", "failovers");
    ctx.seqno = {0, get_ull_stat(h, "vb_0:high_seqno", "vbucket-seqno")};
    ctx.exp_mutations = 300;
    ctx.exp_markers = 1;
    ctx.exp_seqno_advanced = 0;

    const void* cookie1 = testHarness->create_cookie(h);
    TestDcpConsumer tdc("unittest1", cookie1, h);
    tdc.addStreamCtx(ctx);
    tdc.setCollectionsFilter();
    tdc.run();

    testHarness->destroy_cookie(cookie1);
    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}

/*
 * This test is focused on ensuring we received a SeqnoAdvanced when streaming
 * from one collection and from disk.
 *
 * This test performs the following steps:
 * 1. Stream 100 new mutations for the default collection to the replica as one
 * checkpoint
 * 2. Stream 100 new mutations for the "meat" collection to the replica as a new
 * checkpoint
 * 3. flush the 200 mutations to disk as the checkpoints can be closed
 * 4. stream another 100 new keys for the default collection to the replica
 * 5. Now create a new DCP consumer
 * 6. New DCP consumer to stream just the "meat" collection. We should get
 * 100 mutations from disk and then a SeqnoAdvance to move us to the end
 * of the snapshot of all 300 mutations.
 */
static enum test_result test_dcp_replica_stream_one_collection_on_disk(
        EngineIface* h) {
    const void* cookie = testHarness->create_cookie(h);
    std::string manifest(R"({
   "scopes":[
      {
         "collections":[
            {
               "name":"_default",
               "uid":"0"
            },
            {
               "name":"meat",
               "uid":"8"
            }
         ],
         "name":"_default",
         "uid":"0"
      }
   ],
   "uid":"1"
})");

    checkeq(cb::engine_errc::success,
            h->set_collection_manifest(cookie, manifest),
            "Failed to set collection manifest");

    check(set_vbucket_state(h, Vbid(0), vbucket_state_replica),
          "Failed to set vbucket state.");

    uint32_t opaque = 0xFFFF0000;
    uint32_t seqno = 0;
    uint32_t flags = 0;
    const int num_items = 100;
    const char* name = "unittest";

    /* Open an DCP consumer connection */
    auto dcp = requireDcpIface(h);
    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie,
                      opaque,
                      seqno,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp producer open connection.");

    std::string type = get_str_stat(h, "eq_dcpq:unittest:type", "dcp");
    checkeq(0, type.compare("consumer"), "Consumer not found");

    opaque = add_stream_for_consumer(
            h, cookie, opaque, Vbid(0), 0, cb::mcbp::Status::Success);

    uint64_t startSeqno = 1;
    /* Send DCP mutations with in memory flag (0x01) */
    dcp_stream_to_replica(h,
                          cookie,
                          opaque,
                          Vbid(0),
                          0x01,
                          1 + startSeqno,
                          num_items + startSeqno,
                          0,
                          startSeqno + num_items);

    /*
     * Send 100 more DCP mutations with checkpoint creation flag (0x04)
     * These will be for collection 0x8 and will be streamed from disk
     */
    uint64_t start = num_items + startSeqno;
    dcp_stream_to_replica(h,
                          cookie,
                          opaque,
                          Vbid(0),
                          0x04,
                          start + 1,
                          start + 100,
                          start,
                          start + 100,
                          0x1,
                          PROTOCOL_BINARY_RAW_BYTES,
                          0,
                          0,
                          0,
                          CollectionID(8));

    wait_for_flusher_to_settle(h);
    stop_persistence(h);
    checkeq(2 * num_items,
            get_int_stat(h, "vb_replica_curr_items"),
            "wrong number of items in replica vbucket");

    /* Add 100 more items to the replica node on a new checkpoint */
    /* Send with flag (0x04) indicating checkpoint creation */
    start = 2 * num_items + startSeqno;
    dcp_stream_to_replica(h,
                          cookie,
                          opaque,
                          Vbid(0),
                          0x04,
                          start + 1,
                          start + 100,
                          start,
                          start + 100);

    /* Disk backfill + in memory stream from replica */
    /* Wait for a checkpoint to be removed */
    wait_for_stat_to_be_lte(h, "vb_0:num_checkpoints", 2, "checkpoint");

    DcpStreamCtx ctx;
    ctx.vb_uuid = get_ull_stat(h, "vb_0:0:id", "failovers");
    ctx.seqno = {0, get_ull_stat(h, "vb_0:high_seqno", "vbucket-seqno")};
    ctx.exp_mutations = 100;
    ctx.exp_markers = 1;
    ctx.exp_system_events = 1;
    ctx.exp_collection_ids.emplace_back(8);
    ctx.exp_seqno_advanced = 1;

    const void* cookie1 = testHarness->create_cookie(h);
    TestDcpConsumer tdc("unittest1", cookie1, h);
    tdc.addStreamCtx(ctx);
    tdc.setCollectionsFilter({R"({"collections":["8"]})"});
    tdc.run();

    testHarness->destroy_cookie(cookie1);
    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}

/*
 * This test is focused on ensuring we do not see a SeqnoAdvanced when streaming
 * from one collection when the last mutation in the snapshot is a mutation in
 * the collection we are streaming.
 *
 * This test performs the following steps:
 * 1. Stream 100 new mutations for the default collection to the replica as one
 * checkpoint
 * 2. Stream 100 new mutations for the "meat" collection to the replica creating
 * a new checkpoint
 * 3. flush the mutations to disk from the closed checkpoints
 * 4. stream another 100 new keys for the default collection to the replica
 * 5. stream another 100 new keys for the "meat" collection to the replica
 * 6. Now create a new DCP consumer
 * 7. New DCP consumer to stream just the "meat" collection. We should get
 * 100 mutations from disk, 100 mutations from memory that takes us to the end
 * of the backfill snapshot. Thus, meaning we should not see a SeqnoAdvanced.
 */
static enum test_result test_dcp_replica_stream_one_collection(EngineIface* h) {
    const void* cookie = testHarness->create_cookie(h);
    std::string manifest(R"({
   "scopes":[
      {
         "collections":[
            {
               "name":"_default",
               "uid":"0"
            },
            {
               "name":"meat",
               "uid":"8"
            }
         ],
         "name":"_default",
         "uid":"0"
      }
   ],
   "uid":"1"
})");

    checkeq(h->set_collection_manifest(cookie, manifest),
            cb::engine_errc::success,
            "Failed to set collection manifest");

    check(set_vbucket_state(h, Vbid(0), vbucket_state_replica),
          "Failed to set vbucket state.");

    uint32_t opaque = 0xFFFF0000;
    uint32_t seqno = 0;
    uint32_t flags = 0;
    const int num_items = 100;
    const char* name = "unittest";

    /* Open an DCP consumer connection */
    auto dcp = requireDcpIface(h);
    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie,
                      opaque,
                      seqno,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp producer open connection.");

    std::string type = get_str_stat(h, "eq_dcpq:unittest:type", "dcp");
    checkeq(0, type.compare("consumer"), "Consumer not found");

    opaque = add_stream_for_consumer(
            h, cookie, opaque, Vbid(0), 0, cb::mcbp::Status::Success);

    uint64_t startSeqno = 1;
    /* Send DCP mutations with in memory flag (0x01) */
    dcp_stream_to_replica(h,
                          cookie,
                          opaque,
                          Vbid(0),
                          0x01,
                          1 + startSeqno,
                          num_items + startSeqno,
                          0,
                          startSeqno + num_items);

    /*
     * Send 100 more DCP mutations with checkpoint creation flag (0x04)
     * These will be for collection 0x8 and will be streamed from disk
     */
    uint64_t start = num_items + startSeqno;
    dcp_stream_to_replica(h,
                          cookie,
                          opaque,
                          Vbid(0),
                          0x04,
                          start + 1,
                          start + 100,
                          start,
                          start + 100,
                          0x1,
                          PROTOCOL_BINARY_RAW_BYTES,
                          0,
                          0,
                          0,
                          CollectionID(8));

    wait_for_flusher_to_settle(h);
    stop_persistence(h);
    checkeq(2 * num_items,
            get_int_stat(h, "vb_replica_curr_items"),
            "wrong number of items in replica vbucket");

    /* Add 100 more items to the replica node on a new checkpoint */
    /* Send with flag (0x04) indicating checkpoint creation */
    start = 2 * num_items + startSeqno;
    dcp_stream_to_replica(h,
                          cookie,
                          opaque,
                          Vbid(0),
                          0x04,
                          start + 1,
                          start + 100,
                          start,
                          start + 100);
    // Add another 100 items for collection 0x8
    // These should be streamed from memory
    start = 3 * num_items + startSeqno;
    dcp_stream_to_replica(h,
                          cookie,
                          opaque,
                          Vbid(0),
                          0x04,
                          start + 1,
                          start + 100,
                          start,
                          start + 100,
                          0x1,
                          PROTOCOL_BINARY_RAW_BYTES,
                          0,
                          0,
                          0,
                          CollectionID(8));

    /* Disk backfill + in memory stream from replica */
    /* Wait for a checkpoint to be removed */
    wait_for_stat_to_be_lte(h, "vb_0:num_checkpoints", 3, "checkpoint");

    DcpStreamCtx ctx;
    ctx.vb_uuid = get_ull_stat(h, "vb_0:0:id", "failovers");
    ctx.seqno = {0, get_ull_stat(h, "vb_0:high_seqno", "vbucket-seqno")};
    ctx.exp_mutations = 200;
    ctx.exp_markers = 1;
    ctx.exp_system_events = 1;
    ctx.exp_collection_ids.emplace_back(8);
    ctx.exp_seqno_advanced = 0;

    const void* cookie1 = testHarness->create_cookie(h);
    TestDcpConsumer tdc("unittest1", cookie1, h);
    tdc.addStreamCtx(ctx);
    tdc.setCollectionsFilter({R"({"collections":["8"]})"});
    tdc.run();

    testHarness->destroy_cookie(cookie1);
    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}

/**
 * A test to check that expiries streamed to a replica behave as expected,
 * including the difference between enabling an expiryOutput on the consumer
 * @param enableExpiryOutput This controls whether the test consumer should
 *                           request DCP expiry opcodes, and hence whether the
 *                           test should check for expirations or deletions.
 */
static test_result test_dcp_replica_stream_expiries(
        EngineIface* h, EnableExpiryOutput enableExpiryOutput) {
    uint32_t opaque = 0xFFFF0000;
    uint32_t seqno = 0;
    // dcp expiry requires the connection to opt in to delete times
    uint32_t flags =
            enableExpiryOutput == EnableExpiryOutput::Yes
                    ? cb::mcbp::request::DcpOpenPayload::IncludeDeleteTimes
                    : 0;
    const int num_items = 5;
    const char* name = "unittest";
    const uint32_t expiryTime = 256;

    check(set_vbucket_state(h, Vbid(0), vbucket_state_replica),
          "Failed to set vbucket state.");

    const void* cookie = testHarness->create_cookie(h);

    /* Open an DCP consumer connection */
    auto dcp = requireDcpIface(h);
    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie,
                      opaque,
                      seqno,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp producer open connection.");

    std::string type = get_str_stat(h, "eq_dcpq:unittest:type", "dcp");
    checkeq(0, type.compare("consumer"), "Consumer not found");

    opaque = add_stream_for_consumer(
            h, cookie, opaque, Vbid(0), 0, cb::mcbp::Status::Success);

    testHarness->time_travel(expiryTime + 100);

    /* Write expiries to replica, with disk flag (0x02) */
    dcp_stream_expiries_to_replica(h,
                                   cookie,
                                   opaque,
                                   Vbid(0),
                                   0x02,
                                   1,
                                   num_items,
                                   0,
                                   num_items,
                                   expiryTime);

    /* Streaming expiries shouldn't have added any items */
    checkeq(0, get_int_stat(h, "curr_items"), "curr_items count not 0");

    DcpStreamCtx ctx;
    ctx.vb_uuid = get_ull_stat(h, "vb_0:0:id", "failovers");
    ctx.seqno = {0, num_items};
    if (enableExpiryOutput == EnableExpiryOutput::Yes) {
        ctx.exp_expirations = num_items;
    } else {
        ctx.exp_deletions = num_items;
    }
    ctx.exp_markers = 1;

    const void* cookie1 = testHarness->create_cookie(h);
    TestDcpConsumer tdc("unittest1", cookie1, h);
    tdc.openConnection(flags | cb::mcbp::request::DcpOpenPayload::Producer);
    if (enableExpiryOutput == EnableExpiryOutput::Yes) {
        checkeq(ENGINE_SUCCESS,
                tdc.sendControlMessage("enable_expiry_opcode", "true"),
                "Failed to enable_expiry_opcode");
    }
    tdc.addStreamCtx(ctx);
    tdc.run(false);

    testHarness->destroy_cookie(cookie1);
    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_replica_stream_expiry_enabled(EngineIface* h) {
    return test_dcp_replica_stream_expiries(h, EnableExpiryOutput::Yes);
}

static enum test_result test_dcp_replica_stream_expiry_disabled(
        EngineIface* h) {
    return test_dcp_replica_stream_expiries(h, EnableExpiryOutput::No);
}

/*
 * Test that we can send option of IS_EXPIRATION through deleteWithMeta and
 * stream an expiration from the outcome.
 */
static test_result test_stream_deleteWithMeta_expiration(
        EngineIface* h, EnableExpiryOutput enableExpiryOutput) {
    const char* key = "delete_with_meta_key";
    const size_t keylen = strlen(key);
    ItemMetaData itemMeta;
    itemMeta.revSeqno = 10;
    itemMeta.cas = 0x1;
    itemMeta.flags = 0xdeadbeef;

    // store an item
    checkeq(ENGINE_SUCCESS,
            store(h,
                  nullptr,
                  OPERATION_SET,
                  key,
                  "somevalue",
                  nullptr,
                  0,
                  Vbid(0)),
            "Failed set.");
    wait_for_flusher_to_settle(h);

    // check the item stat
    auto temp = get_int_stat(h, "curr_items_tot");
    checkeq(1, temp, "Expected an item");

    // delete an item with meta data indicating expiration
    checkeq(ENGINE_SUCCESS,
            del_with_meta(h, key, keylen, Vbid(0), &itemMeta, 0, IS_EXPIRATION),
            "Expected delete success");
    checkeq(cb::mcbp::Status::Success, last_status.load(), "Expected success");

    wait_for_flusher_to_settle(h);

    // check the item stat
    temp = get_int_stat(h, "curr_items_tot");
    checkeq(0, temp, "Expected item to be removed");

    DcpStreamCtx ctx;
    ctx.vb_uuid = get_ull_stat(h, "vb_0:0:id", "failovers");
    ctx.seqno = {0, 2};
    if (enableExpiryOutput == EnableExpiryOutput::Yes) {
        ctx.exp_expirations = 1;
    } else {
        ctx.exp_deletions = 1;
    }
    ctx.exp_markers = 1;

    const void* cookie = testHarness->create_cookie(h);
    TestDcpConsumer tdc("unittest", cookie, h);
    uint32_t flags = cb::mcbp::request::DcpOpenPayload::Producer;
    if (enableExpiryOutput == EnableExpiryOutput::Yes) {
        // dcp expiry requires the connection to opt in to delete times
        flags |= cb::mcbp::request::DcpOpenPayload::IncludeDeleteTimes;
    }
    tdc.openConnection(flags);

    if (enableExpiryOutput == EnableExpiryOutput::Yes) {
        checkeq(ENGINE_SUCCESS,
                tdc.sendControlMessage("enable_expiry_opcode", "true"),
                "Failed to enable_expiry_opcode");
    }

    tdc.addStreamCtx(ctx);

    tdc.run(false);

    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_stream_deleteWithMeta_expiration_enabled(
        EngineIface* h) {
    return test_stream_deleteWithMeta_expiration(h, EnableExpiryOutput::Yes);
}

static enum test_result test_stream_deleteWithMeta_expiration_disabled(
        EngineIface* h) {
    return test_stream_deleteWithMeta_expiration(h, EnableExpiryOutput::No);
}

static enum test_result test_dcp_persistence_seqno(EngineIface* h) {
    /* write 2 items */
    const int num_items = 2;
    write_items(h, num_items, 0, "key", "somevalue");

    wait_for_flusher_to_settle(h);

    checkeq(ENGINE_SUCCESS,
            seqnoPersistence(h, nullptr, Vbid(0), /*seqno*/ num_items),
            "Expected success for seqno persistence request");

    /* the test chooses to handle the EWOULDBLOCK here */
    const void* cookie = testHarness->create_cookie(h);
    testHarness->set_ewouldblock_handling(cookie, false);

    /* seqno 'num_items + 1' is not yet seen buy the vbucket */
    checkeq(ENGINE_EWOULDBLOCK,
            seqnoPersistence(h, cookie, Vbid(0), /*seqno*/ num_items + 1),
            "Expected temp failure for seqno persistence request");
    checkeq(1,
            get_int_stat(h, "vb_0:hp_vb_req_size", "vbucket-details 0"),
            "High priority request count incorrect");

    /* acquire the mutex to wait on the condition variable */
    testHarness->lock_cookie(cookie);

    /* write another item to reach seqno 'num_items +  1'.
       the notification (arising from the write) will not win the race to
       notify the condition variable since the mutex is still held.
       Note: we need another writer thread because in ephemeral buckets, the
             writer thread itself notifies the waiting condition variable,
             hence it cannot be this thread (as we are in the mutex associated
             with the condition variable) */
    cb_thread_t writerThread;
    struct writer_thread_ctx t1 = {h, 1, Vbid(0)};
    checkeq(0,
            cb_create_thread(&writerThread, writer_thread, &t1, 0),
            "Error creating the writer thread");

    /* now wait on the condition variable; the condition variable is signaled
       by the notification from the seqnoPersistence request that had received
       EWOULDBLOCK */
    testHarness->waitfor_cookie(cookie);

    /* unlock the mutex */
    testHarness->unlock_cookie(cookie);

    /* delete the cookie created */
    testHarness->destroy_cookie(cookie);

    /* wait for the writer thread to complete */
    checkeq(0,
            cb_join_thread(writerThread),
            "Error in writer thread join");
    return SUCCESS;
}

/* This test checks whether writing of backfill items on a replica vbucket
   would result in notification for a pending "CMD_SEQNO_PERSISTENCE" request */
static enum test_result test_dcp_persistence_seqno_backfillItems(
        EngineIface* h) {
    /* we want backfill items on a replica vbucket */
    check(set_vbucket_state(h, Vbid(0), vbucket_state_replica),
          "Failed to set vbucket state.");

    /* set up a DCP consumer connection */
    const void* consumerCookie = testHarness->create_cookie(h);
    uint32_t opaque = 0xFFFF0000;
    const char* name = "unittest";

    /* Open an DCP consumer connection */
    auto dcp = requireDcpIface(h);
    checkeq(ENGINE_SUCCESS,
            dcp->open(consumerCookie,
                      opaque,
                      /*start_seqno*/ 0,
                      /*flags*/ 0,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp consumer open connection.");

    std::string type = get_str_stat(h, "eq_dcpq:unittest:type", "dcp");
    checkeq(0, type.compare("consumer"), "Consumer not found");

    opaque = add_stream_for_consumer(
            h, consumerCookie, opaque, Vbid(0), 0, cb::mcbp::Status::Success);

    /* Now make a seqnoPersistence call that will cause a high priority
       vbucket entry to be queued */
    const int num_items = 2;

    /* the test chooses to handle the EWOULDBLOCK here */
    const void* cookie = testHarness->create_cookie(h);
    testHarness->set_ewouldblock_handling(cookie, false);

    /* seqno 'num_items + 1' is not yet seen by the vbucket */
    checkeq(ENGINE_EWOULDBLOCK,
            seqnoPersistence(h, cookie, Vbid(0), /*seqno*/ num_items),
            "Expected temp failure for seqno persistence request");
    checkeq(1,
            get_int_stat(h, "vb_0:hp_vb_req_size", "vbucket-details 0"),
            "High priority request count incorrect");

    /* acquire the mutex to wait on the condition variable */
    testHarness->lock_cookie(cookie);

    /* write backfill items on the replica vbucket to reach seqno 'num_items'.
       the notification (arising from the write) will not win the race to
       notify the condition variable since the mutex is still held.
       Note: we need another writer thread because in ephemeral buckets, the
       writer thread itself notifies the waiting condition variable,
       hence it cannot be this thread (as we are in the mutex associated
       with the condition variable) */
    std::thread backfillWriter(dcp_stream_to_replica,
                               h,
                               consumerCookie,
                               opaque,
                               Vbid(0),
                               /*MARKER_FLAG_DISK*/ 0x02,
                               /*start*/ 1,
                               /*end*/ num_items,
                               /*snap_start_seqno*/ 1,
                               /*snap_end_seqno*/ num_items,
                               /*cas*/ 1,
                               /*datatype*/ 1,
                               /*exprtime*/ 0,
                               /*lockTime*/ 0,
                               /*revSeqno*/ 0,
                               CollectionID::Default);

    /* now wait on the condition variable; the condition variable is signaled
       by the notification from the seqnoPersistence request that had received
       EWOULDBLOCK.
       This would HANG if the backfill writes do not cause a notify for the
       "seqnoPersistence" request above */
    testHarness->waitfor_cookie(cookie);

    /* unlock the mutex */
    testHarness->unlock_cookie(cookie);

    /* delete the cookies created */
    testHarness->destroy_cookie(consumerCookie);
    testHarness->destroy_cookie(cookie);

    /* wait for the writer thread to complete */
    backfillWriter.join();

    return SUCCESS;
}

static enum test_result test_dcp_last_items_purged(EngineIface* h) {
    mutation_descr_t mut_info = {};
    uint64_t cas = 0;
    uint32_t high_seqno = 0;
    const int num_items = 3;
    const char* key[3] = {"k1", "k2", "k3"};

    /* Set 3 items */
    for (const auto& k : key) {
        checkeq(ENGINE_SUCCESS,
                store(h, nullptr, OPERATION_SET, k, "somevalue"),
                "Error setting.");
    }

    /* Delete last 2 items */
    for (int count = 1; count < num_items; count++){
        checkeq(ENGINE_SUCCESS,
                del(h,
                    key[count],
                    &cas,
                    Vbid(0),
                    nullptr /*cookie*/,
                    &mut_info),
                "Failed remove with value.");
        cas = 0;
    }

    high_seqno = get_ull_stat(h, "vb_0:high_seqno", "vbucket-seqno");

    /* wait for flusher to settle */
    wait_for_flusher_to_settle(h);

    /* Run compaction */
    compact_db(h, Vbid(0), Vbid(0), 2, high_seqno, 1);
    wait_for_stat_to_be(h, "ep_pending_compactions", 0);
    checkeq(static_cast<int>(high_seqno - 1),
            get_int_stat(h, "vb_0:purge_seqno", "vbucket-seqno"),
            "purge_seqno didn't match expected value");

    wait_for_stat_to_be(h, "vb_0:open_checkpoint_id", 3, "checkpoint");
    wait_for_stat_to_be(h, "vb_0:num_checkpoints", 1, "checkpoint");

    /* Create a DCP stream */
    DcpStreamCtx ctx;
    ctx.vb_uuid = get_ull_stat(h, "vb_0:0:id", "failovers");
    ctx.seqno = {0, get_ull_stat(h, "vb_0:high_seqno", "vbucket-seqno")};
    ctx.exp_mutations = 1;
    ctx.exp_deletions = 1;
    ctx.exp_markers = 1;
    ctx.skip_estimate_check = true;

    const void* cookie = testHarness->create_cookie(h);
    TestDcpConsumer tdc("unittest", cookie, h);
    tdc.addStreamCtx(ctx);
    tdc.run();

    testHarness->destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_dcp_rollback_after_purge(EngineIface* h) {
    item_info info;
    mutation_descr_t mut_info;
    uint64_t vb_uuid = 0;
    uint64_t cas = 0;
    uint32_t high_seqno = 0;
    const int num_items = 3;
    const char* key[3] = {"k1", "k2", "k3"};

    vb_uuid = get_ull_stat(h, "vb_0:0:id", "failovers");

    /* Set 3 items */
    for (const auto& k : key) {
        checkeq(ENGINE_SUCCESS,
                store(h, nullptr, OPERATION_SET, k, "somevalue"),
                "Error setting.");
    }
    high_seqno = get_ull_stat(h, "vb_0:high_seqno", "vbucket-seqno");
    /* wait for flusher to settle */
    wait_for_flusher_to_settle(h);

    /* Create a DCP stream to send 3 items to the replica */
    DcpStreamCtx ctx;
    ctx.vb_uuid = vb_uuid;
    ctx.seqno = {0, high_seqno};
    ctx.exp_mutations = 3;
    ctx.exp_markers = 1;
    ctx.skip_estimate_check = true;

    const void* cookie = testHarness->create_cookie(h);
    TestDcpConsumer tdc("unittest", cookie, h);
    tdc.addStreamCtx(ctx);
    tdc.run();

    testHarness->destroy_cookie(cookie);

    memset(&mut_info, 0, sizeof(mut_info));
    /* Delete last 2 items */
    for (int count = 1; count < num_items; count++){
        checkeq(ENGINE_SUCCESS,
                del(h,
                    key[count],
                    &cas,
                    Vbid(0),
                    nullptr /*cookie*/,
                    &mut_info),
                "Failed remove with value.");
        cas = 0;
    }
    high_seqno = get_ull_stat(h, "vb_0:high_seqno", "vbucket-seqno");
    /* wait for flusher to settle */
    wait_for_flusher_to_settle(h);

    /* Run compaction */
    compact_db(h, Vbid(0), Vbid(0), 2, high_seqno, 1);
    wait_for_stat_to_be(h, "ep_pending_compactions", 0);
    checkeq(static_cast<int>(high_seqno - 1),
            get_int_stat(h, "vb_0:purge_seqno", "vbucket-seqno"),
            "purge_seqno didn't match expected value");

    wait_for_stat_to_be(h, "vb_0:open_checkpoint_id", 3, "checkpoint");
    wait_for_stat_to_be(h, "vb_0:num_checkpoints", 1, "checkpoint");

    /* DCP stream, expect a rollback to seq 0 */
    DcpStreamCtx ctx1;
    ctx1.vb_uuid = vb_uuid;
    ctx1.seqno = {3, high_seqno};
    ctx1.snapshot = {3, high_seqno};
    ctx1.exp_err = ENGINE_ROLLBACK;
    ctx1.exp_rollback = 0;

    const void* cookie1 = testHarness->create_cookie(h);
    TestDcpConsumer tdc1("unittest1", cookie1, h);
    tdc1.addStreamCtx(ctx1);

    tdc1.openConnection();
    tdc1.openStreams();

    testHarness->destroy_cookie(cookie1);

    /* Do not expect rollback when you already have all items in the snapshot
       (that is, start == snap_end_seqno)*/
    DcpStreamCtx ctx2;
    ctx2.vb_uuid = vb_uuid;
    ctx2.seqno = {high_seqno, high_seqno + 10};
    ctx2.snapshot = {0, high_seqno};
    ctx2.exp_err = ENGINE_SUCCESS;

    const void* cookie2 = testHarness->create_cookie(h);
    TestDcpConsumer tdc2("unittest2", cookie2, h);
    tdc2.addStreamCtx(ctx2);

    tdc2.openConnection();
    tdc2.openStreams();

    testHarness->destroy_cookie(cookie2);

    /* Do not expect rollback when start_seqno == 0 */
    DcpStreamCtx ctx3;
    ctx3.vb_uuid = vb_uuid;
    ctx3.seqno = {0, high_seqno};
    ctx3.snapshot = {0, high_seqno};
    ctx3.exp_err = ENGINE_SUCCESS;

    const void* cookie3 = testHarness->create_cookie(h);
    TestDcpConsumer tdc3("unittest3", cookie3, h);
    tdc3.addStreamCtx(ctx3);

    tdc3.openConnection();
    tdc3.openStreams();

    testHarness->destroy_cookie(cookie3);

    return SUCCESS;
}

static enum test_result test_dcp_erroneous_mutations(EngineIface* h) {
    check(set_vbucket_state(h, Vbid(0), vbucket_state_replica),
          "Failed to set vbucket state");
    wait_for_flusher_to_settle(h);

    const void* cookie = testHarness->create_cookie(h);
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    std::string name("err_mutations");

    auto dcp = requireDcpIface(h);

    checkeq(dcp->open(cookie,
                      opaque,
                      0,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            ENGINE_SUCCESS,
            "Failed to open DCP consumer connection!");
    add_stream_for_consumer(
            h, cookie, opaque++, Vbid(0), 0, cb::mcbp::Status::Success);

    std::string opaqueStr("eq_dcpq:" + name + ":stream_0_opaque");
    uint32_t stream_opaque = get_int_stat(h, opaqueStr.c_str(), "dcp");

    checkeq(dcp->snapshot_marker(cookie,
                                 stream_opaque,
                                 Vbid(0),
                                 5,
                                 10,
                                 300,
                                 {} /*HCS*/,
                                 {} /*maxVisibleSeqno*/),
            ENGINE_SUCCESS,
            "Failed to send snapshot marker!");
    for (int i = 5; i <= 10; i++) {
        const std::string key("key" + std::to_string(i));
        const DocKey docKey{key, DocKeyEncodesCollectionId::No};
        checkeq(dcp->mutation(cookie,
                              stream_opaque,
                              docKey,
                              {(const uint8_t*)"value", 5},
                              0,
                              PROTOCOL_BINARY_RAW_BYTES,
                              i * 3,
                              Vbid(0),
                              0,
                              i,
                              0,
                              0,
                              0,
                              {},
                              INITIAL_NRU_VALUE),
                ENGINE_SUCCESS,
                "Unexpected return code for mutation!");
    }

    // Send a mutation and a deletion both out-of-sequence
    const DocKey key{(const uint8_t*)"key", 3, DocKeyEncodesCollectionId::No};
    checkeq(dcp->mutation(cookie,
                          stream_opaque,
                          key,
                          {(const uint8_t*)"val", 3},
                          0,
                          PROTOCOL_BINARY_RAW_BYTES,
                          35,
                          Vbid(0),
                          0,
                          2,
                          0,
                          0,
                          0,
                          {},
                          INITIAL_NRU_VALUE),
            ENGINE_ERANGE,
            "Mutation should've returned ERANGE!");
    const DocKey key5{(const uint8_t*)"key5", 4, DocKeyEncodesCollectionId::No};
    checkeq(dcp->deletion(cookie,
                          stream_opaque,
                          key5,
                          {},
                          0,
                          PROTOCOL_BINARY_RAW_BYTES,
                          40,
                          Vbid(0),
                          3,
                          0,
                          {}),
            ENGINE_ERANGE,
            "Deletion should've returned ERANGE!");

    std::string bufferItemsStr("eq_dcpq:" + name + ":stream_0_buffer_items");

    int buffered_items = get_int_stat(h, bufferItemsStr.c_str(), "dcp");

    const DocKey docKey20{
            (const uint8_t*)"key20", 5, DocKeyEncodesCollectionId::No};
    ENGINE_ERROR_CODE err = dcp->mutation(cookie,
                                          stream_opaque,
                                          docKey20,
                                          {(const uint8_t*)"val", 3},
                                          0,
                                          PROTOCOL_BINARY_RAW_BYTES,
                                          45,
                                          Vbid(0),
                                          0,
                                          20,
                                          0,
                                          0,
                                          0,
                                          {},
                                          INITIAL_NRU_VALUE);

    if (buffered_items == 0) {
        checkeq(err, ENGINE_ERANGE, "Mutation shouldn't have been accepted!");
    } else {
        checkeq(err, ENGINE_SUCCESS, "Mutation should have been buffered!");
    }

    wait_for_stat_to_be(h, bufferItemsStr.c_str(), 0, "dcp");

    // Full Evictions: must wait for all items to have been flushed before
    // asserting item counts
    if (isPersistentBucket(h) && is_full_eviction(h)) {
        wait_for_flusher_to_settle(h);
    }

    checkeq(6,
            get_int_stat(h, "vb_0:num_items", "vbucket-details 0"),
            "The last mutation should've been dropped!");

    checkeq(dcp->close_stream(cookie, stream_opaque, Vbid(0), {}),
            ENGINE_SUCCESS,
            "Expected to close stream!");
    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_erroneous_marker(EngineIface* h) {
    check(set_vbucket_state(h, Vbid(0), vbucket_state_replica),
          "Failed to set vbucket state");
    wait_for_flusher_to_settle(h);

    const void* cookie1 = testHarness->create_cookie(h);
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    std::string name("first_marker");

    auto dcp = requireDcpIface(h);
    checkeq(dcp->open(cookie1,
                      opaque,
                      0,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            ENGINE_SUCCESS,
            "Failed to open DCP consumer connection!");
    add_stream_for_consumer(
            h, cookie1, opaque++, Vbid(0), 0, cb::mcbp::Status::Success);

    std::string opaqueStr("eq_dcpq:" + name + ":stream_0_opaque");
    uint32_t stream_opaque = get_int_stat(h, opaqueStr.c_str(), "dcp");

    checkeq(dcp->snapshot_marker(cookie1,
                                 stream_opaque,
                                 Vbid(0),
                                 1,
                                 10,
                                 300,
                                 {} /*HCS*/,
                                 {} /*maxVisibleSeqno*/),
            ENGINE_SUCCESS,
            "Failed to send snapshot marker!");
    for (int i = 1; i <= 10; i++) {
        const std::string key("key" + std::to_string(i));
        const DocKey docKey{key, DocKeyEncodesCollectionId::No};
        checkeq(dcp->mutation(cookie1,
                              stream_opaque,
                              docKey,
                              {(const uint8_t*)"value", 5},
                              0,
                              PROTOCOL_BINARY_RAW_BYTES,
                              i * 3,
                              Vbid(0),
                              0,
                              i,
                              0,
                              0,
                              0,
                              {},
                              INITIAL_NRU_VALUE),
                ENGINE_SUCCESS,
                "Unexpected return code for mutation!");
    }

    std::string bufferItemsStr("eq_dcpq:" + name + ":stream_0_buffer_items");
    wait_for_stat_to_be(h, bufferItemsStr.c_str(), 0, "dcp");

    checkeq(dcp->close_stream(cookie1, stream_opaque, Vbid(0), {}),
            ENGINE_SUCCESS,
            "Expected to close stream1!");
    testHarness->destroy_cookie(cookie1);

    const void* cookie2 = testHarness->create_cookie(h);
    opaque = 0xFFFFF000;
    name.assign("second_marker");

    checkeq(dcp->open(cookie2,
                      opaque,
                      0,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            ENGINE_SUCCESS,
            "Failed to open DCP consumer connection!");
    add_stream_for_consumer(
            h, cookie2, opaque++, Vbid(0), 0, cb::mcbp::Status::Success);

    opaqueStr.assign("eq_dcpq:" + name + ":stream_0_opaque");
    stream_opaque = get_int_stat(h, opaqueStr.c_str(), "dcp");

    // Send a snapshot marker that would be rejected
    checkeq(dcp->snapshot_marker(cookie2,
                                 stream_opaque,
                                 Vbid(0),
                                 5,
                                 10,
                                 1,
                                 {} /*HCS*/,
                                 {} /*maxVisibleSeqno*/),
            ENGINE_ERANGE,
            "Snapshot marker should have been dropped!");

    // Send a snapshot marker that would be accepted, but a few of
    // the mutations that are part of this snapshot will be dropped
    checkeq(dcp->snapshot_marker(cookie2,
                                 stream_opaque,
                                 Vbid(0),
                                 5,
                                 15,
                                 1,
                                 {} /*HCS*/,
                                 {} /*maxVisibleSeqno*/),
            ENGINE_SUCCESS,
            "Failed to send snapshot marker!");
    for (int i = 5; i <= 15; i++) {
        const std::string key("key_" + std::to_string(i));
        const DocKey docKey(key, DocKeyEncodesCollectionId::No);
        ENGINE_ERROR_CODE err = dcp->mutation(cookie2,
                                              stream_opaque,
                                              docKey,
                                              {(const uint8_t*)"val", 3},
                                              0,
                                              PROTOCOL_BINARY_RAW_BYTES,
                                              i * 3,
                                              Vbid(0),
                                              0,
                                              i,
                                              0,
                                              0,
                                              0,
                                              {},
                                              INITIAL_NRU_VALUE);
        if (i <= 10) {
            checkeq(err, ENGINE_ERANGE, "Mutation should have been dropped!");
        } else {
            checkeq(err, ENGINE_SUCCESS, "Failed to send mutation!");
        }
    }

    checkeq(dcp->close_stream(cookie2, stream_opaque, Vbid(0), {}),
            ENGINE_SUCCESS,
            "Expected to close stream2!");
    testHarness->destroy_cookie(cookie2);

    return SUCCESS;
}

static enum test_result test_dcp_invalid_mutation_deletion(EngineIface* h) {
    check(set_vbucket_state(h, Vbid(0), vbucket_state_replica),
          "Failed to set vbucket state");
    wait_for_flusher_to_settle(h);

    const void* cookie = testHarness->create_cookie(h);
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    std::string name("err_mutations");

    auto dcp = requireDcpIface(h);
    checkeq(dcp->open(cookie,
                      opaque,
                      0,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            ENGINE_SUCCESS,
            "Failed to open DCP consumer connection!");
    add_stream_for_consumer(
            h, cookie, opaque++, Vbid(0), 0, cb::mcbp::Status::Success);

    std::string opaqueStr("eq_dcpq:" + name + ":stream_0_opaque");
    uint32_t stream_opaque = get_int_stat(h, opaqueStr.c_str(), "dcp");

    // Mutation(s) or deletion(s) with seqno 0 are invalid!
    const std::string key("key");
    DocKey docKey{key, DocKeyEncodesCollectionId::No};
    cb::const_byte_buffer value{(const uint8_t*)"value", 5};

    checkeq(dcp->mutation(cookie,
                          stream_opaque,
                          docKey,
                          value,
                          0,
                          PROTOCOL_BINARY_RAW_BYTES,
                          10,
                          Vbid(0),
                          0,
                          /*seqno*/ 0,
                          0,
                          0,
                          0,
                          {},
                          INITIAL_NRU_VALUE),
            ENGINE_EINVAL,
            "Mutation should have returned EINVAL!");

    checkeq(dcp->deletion(cookie,
                          stream_opaque,
                          docKey,
                          {},
                          0,
                          PROTOCOL_BINARY_RAW_BYTES,
                          10,
                          Vbid(0),
                          /*seqno*/ 0,
                          0,
                          {}),
            ENGINE_EINVAL,
            "Deletion should have returned EINVAL!");

    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_invalid_snapshot_marker(EngineIface* h) {
    check(set_vbucket_state(h, Vbid(0), vbucket_state_replica),
          "Failed to set vbucket state");
    wait_for_flusher_to_settle(h);

    const void* cookie = testHarness->create_cookie(h);
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    std::string name("unittest");

    auto dcp = requireDcpIface(h);
    checkeq(dcp->open(cookie,
                      opaque,
                      0,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            ENGINE_SUCCESS,
            "Failed to open DCP consumer connection!");
    add_stream_for_consumer(
            h, cookie, opaque++, Vbid(0), 0, cb::mcbp::Status::Success);

    std::string opaqueStr("eq_dcpq:" + name + ":stream_0_opaque");
    uint32_t stream_opaque = get_int_stat(h, opaqueStr.c_str(), "dcp");

    checkeq(dcp->snapshot_marker(cookie,
                                 stream_opaque,
                                 Vbid(0),
                                 1,
                                 10,
                                 300,
                                 {} /*HCS*/,
                                 {} /*maxVisibleSeqno*/),
            ENGINE_SUCCESS,
            "Failed to send snapshot marker!");
    for (int i = 1; i <= 10; i++) {
        const std::string key("key" + std::to_string(i));
        const DocKey docKey(key, DocKeyEncodesCollectionId::No);
        checkeq(ENGINE_SUCCESS,
                dcp->mutation(cookie,
                              stream_opaque,
                              docKey,
                              {(const uint8_t*)"value", 5},
                              0, // privileged bytes
                              PROTOCOL_BINARY_RAW_BYTES,
                              i * 3, // cas
                              Vbid(0),
                              0, // flags
                              i, // by_seqno
                              1, // rev_seqno
                              0, // expiration
                              0, // lock_time
                              {}, // meta
                              INITIAL_NRU_VALUE),
                "Unexpected return code for mutation!");
    }

    std::string bufferItemsStr("eq_dcpq:" + name + ":stream_0_buffer_items");
    wait_for_stat_to_be(h, bufferItemsStr.c_str(), 0, "dcp");

    // Invalid snapshot marker with end <= start
    checkeq(dcp->snapshot_marker(cookie,
                                 stream_opaque,
                                 Vbid(0),
                                 11,
                                 8,
                                 300,
                                 {} /*HCS*/,
                                 {} /*maxVisibleSeqno*/),
            ENGINE_EINVAL,
            "Failed to send snapshot marker!");

    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}

/*
 * Test that destroying a DCP producer before it ends
 * works. MB-16915 reveals itself via valgrind.
 */
static enum test_result test_dcp_early_termination(EngineIface* h) {
    // create enough streams that some backfill tasks should overlap
    // with the connection deletion task.
    const int streams = 100;

    // 1 item so that we will at least allow backfill to be scheduled
    const int num_items = 1;
    uint64_t vbuuid[streams];
    for (int i = 0; i < streams; i++) {
        check(set_vbucket_state(h, Vbid(i), vbucket_state_active),
              "Failed to set vbucket state");
        std::stringstream statkey;
        statkey << "vb_" << i <<  ":0:id";
        vbuuid[i] = get_ull_stat(h, statkey.str().c_str(), "failovers");

        /* Set n items */
        write_items(h, num_items, 0, "KEY", "somevalue");
    }
    wait_for_flusher_to_settle(h);

    const void* cookie = testHarness->create_cookie(h);
    uint32_t opaque = 1;
    auto dcp = requireDcpIface(h);
    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie,
                      ++opaque,
                      0,
                      cb::mcbp::request::DcpOpenPayload::Producer,
                      "unittest"),
            "Failed dcp producer open connection.");

    checkeq(ENGINE_SUCCESS,
            dcp->control(cookie, ++opaque, "connection_buffer_size", "1024"),
            "Failed to establish connection buffer");

    MockDcpMessageProducers producers(h);
    for (int i = 0; i < streams; i++) {
        uint64_t rollback = 0;
        checkeq(ENGINE_SUCCESS,
                dcp->stream_req(cookie,
                                DCP_ADD_STREAM_FLAG_DISKONLY,
                                ++opaque,
                                Vbid(i),
                                0,
                                num_items,
                                vbuuid[i],
                                0,
                                num_items,
                               &rollback,
                               mock_dcp_add_failover_log,
                               {}),
              "Failed to initiate stream request");
        dcp->step(cookie, &producers);
    }

    // Destroy the connection
    testHarness->destroy_cookie(cookie);

    // Let all backfills finish
    wait_for_stat_to_be(h, "ep_dcp_num_running_backfills", 0, "dcp");

    return SUCCESS;
}

static enum test_result test_failover_log_dcp(EngineIface* h) {
    if (!isWarmupEnabled(h)) {
        // TODO: Ephemeral - Should re-enable some of these tests, where after
        // restart we expect all requests to rollback (as should be no matching
        // entries as the failover table will just have a single entry with
        // a new UUID.
        return SKIPPED;
    }

    const int num_items = 50;
    uint64_t end_seqno = num_items + 1000;
    uint32_t high_seqno = 0;

    write_items(h, num_items);

    wait_for_flusher_to_settle(h);
    wait_for_stat_to_be(h, "curr_items", num_items);

    testHarness->reload_engine(&h,

                               testHarness->get_current_testcase()->cfg,
                               true,
                               true);
    wait_for_warmup_complete(h);

    wait_for_stat_to_be(h, "curr_items", num_items);

    high_seqno = get_ull_stat(h, "vb_0:high_seqno", "vbucket-seqno");
    uint64_t uuid = get_ull_stat(h, "vb_0:1:id", "failovers");

    typedef struct dcp_params {
        uint32_t flags;
        uint64_t vb_uuid;
        uint64_t start_seqno;
        uint64_t snap_start_seqno;
        uint64_t snap_end_seqno;
        uint64_t exp_rollback;
        ENGINE_ERROR_CODE exp_err_code;
    } dcp_params_t;

    dcp_params_t params[] = {
            /* Do not expect rollback when start_seqno is 0 and vb_uuid match */
            {0, uuid, 0, 0, 0, 0, ENGINE_SUCCESS},
            /* Do not expect rollback when start_seqno is 0 and vb_uuid == 0 */
            {0, 0x0, 0, 0, 0, 0, ENGINE_SUCCESS},
            /* Expect rollback when start_seqno is 0 and vb_uuid mismatch with
             'STRICT_VBUUID' flag set */
            {DCP_ADD_STREAM_STRICT_VBUUID, 0xBAD, 0, 0, 0, 0, ENGINE_ROLLBACK},
            /* Don't expect rollback when start_seqno is 0 and vb_uuid mismatch with
             'STRICT_VBUUID' flag not set */
            {0, 0xBAD, 0, 0, 0, 0, ENGINE_SUCCESS},
            /* Don't expect rollback when you already have all items in the
               snapshot
               (that is, start == snap_end) and upper >= snap_end */
            {0, uuid, high_seqno, 0, high_seqno, 0, ENGINE_SUCCESS},
            {0, uuid, high_seqno - 1, 0, high_seqno - 1, 0, ENGINE_SUCCESS},
            /* Do not expect rollback when you have no items in the snapshot
             (that is, start == snap_start) and upper >= snap_end */
            {0,
             uuid,
             high_seqno - 10,
             high_seqno - 10,
             high_seqno,
             0,
             ENGINE_SUCCESS},
            {0,
             uuid,
             high_seqno - 10,
             high_seqno - 10,
             high_seqno - 1,
             0,
             ENGINE_SUCCESS},
            /* Do not expect rollback when you are in middle of a snapshot (that
               is,
               snap_start < start < snap_end) and upper >= snap_end */
            {0, uuid, 10, 0, high_seqno, 0, ENGINE_SUCCESS},
            {0, uuid, 10, 0, high_seqno - 1, 0, ENGINE_SUCCESS},
            /* Expect rollback when you are in middle of a snapshot (that is,
               snap_start < start < snap_end) and upper < snap_end. Rollback to
               snap_start if snap_start < upper */
            {0, uuid, 20, 10, high_seqno + 1, 10, ENGINE_ROLLBACK},
            /* Expect rollback when upper < snap_start_seqno. Rollback to upper
               */
            {0,
             uuid,
             high_seqno + 20,
             high_seqno + 10,
             high_seqno + 30,
             high_seqno,
             ENGINE_ROLLBACK},
            {0,
             uuid,
             high_seqno + 10,
             high_seqno + 10,
             high_seqno + 10,
             high_seqno,
             ENGINE_ROLLBACK},
            /* vb_uuid not found in failover table, rollback to zero */
            {0, 0xBAD, 10, 0, high_seqno, 0, ENGINE_ROLLBACK},

            /* start_seqno > vb_high_seqno and DCP_ADD_STREAM_FLAG_LATEST
               set - expect rollback */
            {DCP_ADD_STREAM_FLAG_LATEST,
             uuid,
             high_seqno + 1,
             high_seqno + 1,
             high_seqno + 1,
             high_seqno,
             ENGINE_ROLLBACK},

            /* start_seqno > vb_high_seqno and DCP_ADD_STREAM_FLAG_DISKONLY
               set - expect rollback */
            {DCP_ADD_STREAM_FLAG_DISKONLY,
             uuid,
             high_seqno + 1,
             high_seqno + 1,
             high_seqno + 1,
             high_seqno,
             ENGINE_ROLLBACK},

            /* Add new test case here */
    };

    for (const auto& testcase : params) {
        DcpStreamCtx ctx;
        ctx.flags = testcase.flags;
        ctx.vb_uuid = testcase.vb_uuid;
        ctx.seqno = {testcase.start_seqno, end_seqno};
        ctx.snapshot = {testcase.snap_start_seqno, testcase.snap_end_seqno};
        ctx.exp_err = testcase.exp_err_code;
        ctx.exp_rollback = testcase.exp_rollback;

        const void* cookie = testHarness->create_cookie(h);
        std::string conn_name("test_failover_log_dcp");
        TestDcpConsumer tdc(conn_name.c_str(), cookie, h);
        tdc.addStreamCtx(ctx);

        tdc.openConnection();
        tdc.openStreams();

        testHarness->destroy_cookie(cookie);
    }
    return SUCCESS;
}

static enum test_result test_mb16357(EngineIface* h) {
    // Load up vb0 with n items, expire in 1 second
    const int num_items = 1000;

    write_items(h, num_items, 0, "key-", "value", /*expiration*/ 1);

    wait_for_flusher_to_settle(h);
    testHarness->time_travel(3617); // force expiry pushing time forward.

    struct mb16357_ctx ctx(h, num_items);
    cb_thread_t cp_thread, dcp_thread;

    // First thread used to start a background compaction; note this waits
    // on the DCP thread to start before initiating compaction.
    cb_assert(cb_create_thread(&cp_thread,
                               compact_thread_func,
                               &ctx, 0) == 0);
    // Second thread flips vbucket to replica, notifies compaction to start,
    // and then performs DCP on the vbucket.
    cb_assert(cb_create_thread(&dcp_thread,
                               dcp_thread_func,
                               &ctx, 0) == 0);

    cb_assert(cb_join_thread(cp_thread) == 0);
    cb_assert(cb_join_thread(dcp_thread) == 0);

    return SUCCESS;
}

// Check that an incoming DCP mutation which has an invalid CAS is fixed up
// by the engine.
static enum test_result test_mb17517_cas_minus_1_dcp(EngineIface* h) {
    // Attempt to insert a item with CAS of -1 via dcp->
    const void* cookie = testHarness->create_cookie(h);
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    std::string name = "test_mb17517_cas_minus_1";

    // Switch vb 0 to replica (to accept DCP mutaitons).
    check(set_vbucket_state(h, Vbid(0), vbucket_state_replica),
          "Failed to set vbucket state to replica.");

    // Open consumer connection
    auto dcp = requireDcpIface(h);
    checkeq(dcp->open(cookie,
                      opaque,
                      0,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            ENGINE_SUCCESS,
            "Failed DCP Consumer open connection.");

    add_stream_for_consumer(
            h, cookie, opaque++, Vbid(0), 0, cb::mcbp::Status::Success);

    uint32_t stream_opaque = get_int_stat(
            h, ("eq_dcpq:" + name + ":stream_0_opaque").c_str(), "dcp");

    dcp->snapshot_marker(cookie,
                         stream_opaque,
                         Vbid(0),
                         /*start*/ 0,
                         /*end*/ 3,
                         /*flags*/ 2,
                         /*HCS*/ 0,
                         /*maxVisibleSeqno*/ {});

    // Create two items via a DCP mutation.
    const std::string prefix{"bad_CAS_DCP"};
    std::string value{"value"};
    for (unsigned int ii = 0; ii < 2; ii++) {
        const std::string key{prefix + std::to_string(ii)};
        const DocKey docKey(key, DocKeyEncodesCollectionId::No);
        checkeq(ENGINE_SUCCESS,
                dcp->mutation(cookie,
                              stream_opaque,
                              docKey,
                              {(const uint8_t*)value.c_str(), value.size()},
                              0, // privileged bytes
                              PROTOCOL_BINARY_RAW_BYTES,
                              -1, // cas
                              Vbid(0),
                              0, // flags
                              ii + 1, // by_seqno
                              1, // rev_seqno
                              0, // expiration
                              0, // lock_time
                              {}, // meta
                              INITIAL_NRU_VALUE),
                "Expected DCP mutation with CAS:-1 to succeed");
    }

    // Ensure we have processed the mutations.
    wait_for_stat_to_be(h, "vb_replica_curr_items", 2);

    // Delete one of them (to allow us to test DCP deletion).
    const std::string delete_key{prefix + "0"};
    const DocKey docKey{delete_key, DocKeyEncodesCollectionId::No};
    checkeq(ENGINE_SUCCESS,
            dcp->deletion(cookie,
                          stream_opaque,
                          docKey,
                          {},
                          0,
                          PROTOCOL_BINARY_RAW_BYTES,
                          -1, // cas
                          Vbid(0),
                          3, // by_seqno
                          2, // rev_seqno,
                          {}), // meta
            "Expected DCP deletion with CAS:-1 to succeed");

    // Ensure we have processed the deletion.
    wait_for_stat_to_be(h, "vb_replica_curr_items", 1);

    // Flip vBucket to active so we can access the documents in it.
    check(set_vbucket_state(h, Vbid(0), vbucket_state_active),
          "Failed to set vbucket state to active.");

    // Check that a valid CAS was regenerated for the (non-deleted) mutation.
    std::string key{prefix + "1"};
    auto cas = get_CAS(h, key);
    checkne(~uint64_t(0), cas, "CAS via get() is still -1");

    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}

/*
 * This test case creates and test multiple streams
 * between a single producer and consumer.
 */
static enum test_result test_dcp_multiple_streams(EngineIface* h) {
    check(set_vbucket_state(h, Vbid(1), vbucket_state_active),
          "Failed set vbucket state on 1");
    check(set_vbucket_state(h, Vbid(2), vbucket_state_active),
          "Failed set vbucket state on 2");
    wait_for_flusher_to_settle(h);

    int num_items = 100;
    for (int i = 0; i < num_items; ++i) {
        std::string key("key_1_" + std::to_string(i));
        checkeq(ENGINE_SUCCESS,
                store(h,
                      nullptr,
                      OPERATION_SET,
                      key.c_str(),
                      "data",
                      nullptr,
                      0,
                      Vbid(1)),
                "Failed store on vb:1");

        key = "key_2_" + std::to_string(i);
        checkeq(ENGINE_SUCCESS,
                store(h,
                      nullptr,
                      OPERATION_SET,
                      key.c_str(),
                      "data",
                      nullptr,
                      0,
                      Vbid(2)),
                "Failed store on vb:2");
    }

    std::string name("unittest");
    const void* cookie = testHarness->create_cookie(h);

    DcpStreamCtx ctx1, ctx2;

    int extra_items = 100;

    ctx1.vbucket = Vbid(1);
    ctx1.vb_uuid = get_ull_stat(h, "vb_1:0:id", "failovers");
    ctx1.seqno = {0, static_cast<uint64_t>(num_items + extra_items)};
    ctx1.exp_mutations = num_items + extra_items;
    ctx1.live_frontend_client = true;

    ctx2.vbucket = Vbid(2);
    ctx2.vb_uuid = get_ull_stat(h, "vb_2:0:id", "failovers");
    ctx2.seqno = {0, static_cast<uint64_t>(num_items + extra_items)};
    ctx2.exp_mutations = num_items + extra_items;
    ctx2.live_frontend_client = true;

    TestDcpConsumer tdc("unittest", cookie, h);
    tdc.addStreamCtx(ctx1);
    tdc.addStreamCtx(ctx2);

    cb_thread_t thread1, thread2;
    struct writer_thread_ctx t1 = {h, extra_items, Vbid(1)};
    struct writer_thread_ctx t2 = {h, extra_items, Vbid(2)};
    cb_assert(cb_create_thread(&thread1, writer_thread, &t1, 0) == 0);
    cb_assert(cb_create_thread(&thread2, writer_thread, &t2, 0) == 0);

    tdc.run();

    cb_assert(cb_join_thread(thread1) == 0);
    cb_assert(cb_join_thread(thread2) == 0);

    testHarness->destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_dcp_on_vbucket_state_change(EngineIface* h) {
    const std::string conn_name = "unittest";
    const void* cookie = testHarness->create_cookie(h);

    // Set up a DcpTestConsumer that would remain in in-memory mode
    struct continuous_dcp_ctx cdc = {
            h,
            cookie,
            Vbid(0),
            conn_name,
            0,
            std::make_unique<TestDcpConsumer>(conn_name, cookie, h)};
    cb_thread_t dcp_thread;
    cb_assert(cb_create_thread(&dcp_thread, continuous_dcp_thread, &cdc, 0) == 0);

    // Wait for producer to be created
    wait_for_stat_to_be(h, "ep_dcp_producer_count", 1, "dcp");

    // Write a mutation
    checkeq(ENGINE_SUCCESS,
            store(h, nullptr, OPERATION_SET, "key", "value"),
            "Failed to store a value");

    // Wait for producer to stream that item
    const std::string items_sent_str = "eq_dcpq:" + conn_name + ":items_sent";
    wait_for_stat_to_be(h, items_sent_str.c_str(), 1, "dcp");

    // Change vbucket state to pending
    check(set_vbucket_state(h, Vbid(0), vbucket_state_pending),
          "Failed set vbucket state on 1");

    // Expect DcpTestConsumer to close
    cb_assert(cb_join_thread(dcp_thread) == 0);

    // Expect producers->last_end_status to carry StateChanged as reason
    // for stream closure
    check(cb::mcbp::DcpStreamEndStatus::StateChanged ==
                  cdc.dcpConsumer->producers.last_end_status,
          "Last DCP flag not StateChanged");

    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_dcp_consumer_processer_behavior(EngineIface* h) {
    check(set_vbucket_state(h, Vbid(0), vbucket_state_replica),
          "Failed to set vbucket state.");
    wait_for_flusher_to_settle(h);

    const void* cookie = testHarness->create_cookie(h);
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    const char *name = "unittest";

    // Open consumer connection
    auto dcp = requireDcpIface(h);
    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie,
                      opaque,
                      0,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp Consumer open connection.");

    add_stream_for_consumer(
            h, cookie, opaque++, Vbid(0), 0, cb::mcbp::Status::Success);

    uint32_t stream_opaque =
            get_int_stat(h, "eq_dcpq:unittest:stream_0_opaque", "dcp");

    int i = 1;
    while (true) {
        // Stats lookup is costly; only perform check every 100
        // iterations (we only need to be greater than 1.25 *
        // ep_max_size, not exactly at that point).
        if ((i % 100) == 0) {
            if (get_int_stat(h, "mem_used") >=
                1.25 * get_int_stat(h, "ep_max_size")) {
                break;
            }
        }

        if (i % 20) {
            checkeq(ENGINE_SUCCESS,
                    dcp->snapshot_marker(cookie,
                                         stream_opaque,
                                         Vbid(0),
                                         i,
                                         i + 20,
                                         0x01,
                                         {} /*HCS*/,
                                         {} /*maxVisibleSeqno*/),
                    "Failed to send snapshot marker");
        }
        const std::string key("key" + std::to_string(i));
        const DocKey docKey(key, DocKeyEncodesCollectionId::No);
        checkeq(ENGINE_SUCCESS,
                dcp->mutation(cookie,
                              stream_opaque,
                              docKey,
                              {(const uint8_t*)"value", 5},
                              0, // privileged bytes
                              PROTOCOL_BINARY_RAW_BYTES,
                              i * 3, // cas
                              Vbid(0),
                              0, // flags
                              i, // by_seqno
                              0, // rev_seqno
                              0, // expiration
                              0, // lock_time
                              {}, // meta
                              INITIAL_NRU_VALUE),
                "Failed to send dcp mutation");
        ++i;
    }

    // Expect buffered items and the processer's task state to be
    // CANNOT_PROCESS, because of numerous backoffs.
    checklt(0, get_int_stat(h, "eq_dcpq:unittest:stream_0_buffer_items", "dcp"),
          "Expected buffered items for the stream");
    wait_for_stat_to_be_gte(h, "eq_dcpq:unittest:total_backoffs", 1, "dcp");
    checkne("ALL_PROCESSED"s,
            get_str_stat(h, "eq_dcpq:unittest:processor_task_state", "dcp"),
            "Expected Processer's task state not to be ALL_PROCESSED!");

    testHarness->destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_get_all_vb_seqnos(EngineIface* h) {
    const void* cookie = testHarness->create_cookie(h);
    testHarness->set_collections_support(cookie, true);

    const int num_vbuckets = 10;

    /* Replica vbucket 0; snapshot 0 to 10, but write just 1 item */
    const Vbid rep_vb_num = Vbid(0);
    check(set_vbucket_state(h, rep_vb_num, vbucket_state_replica),
          "Failed to set vbucket state");
    wait_for_flusher_to_settle(h);

    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    std::string name("unittest");
    uint8_t cas = 0;
    uint8_t datatype = 1;
    uint64_t bySeqno = 10;
    uint64_t revSeqno = 0;
    uint32_t exprtime = 0;
    uint32_t lockTime = 0;

    auto dcp = requireDcpIface(h);
    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie,
                      opaque,
                      0,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed to open DCP consumer connection!");
    add_stream_for_consumer(
            h, cookie, opaque++, rep_vb_num, 0, cb::mcbp::Status::Success);

    std::string opaqueStr("eq_dcpq:" + name + ":stream_0_opaque");
    uint32_t stream_opaque = get_int_stat(h, opaqueStr.c_str(), "dcp");

    checkeq(ENGINE_SUCCESS,
            dcp->snapshot_marker(cookie,
                                 stream_opaque,
                                 rep_vb_num,
                                 0,
                                 10,
                                 1,
                                 {} /*HCS*/,
                                 {} /*maxVisibleSeqno*/),
            "Failed to send snapshot marker!");

    const std::string key("key");
    const DocKey docKey(key, DocKeyEncodesCollectionId::No);
    checkeq(ENGINE_SUCCESS,
            dcp->mutation(cookie,
                          stream_opaque,
                          docKey,
                          {(const uint8_t*)"value", 5},
                          0, // privileged bytes
                          datatype,
                          cas, // cas
                          rep_vb_num, // vbucket
                          flags, // flags
                          bySeqno, // by_seqno
                          revSeqno, // rev_seqno
                          exprtime, // expiration
                          lockTime, // lock_time
                          {}, // meta
                          0),
            "Failed dcp mutate.");

    /* Create active vbuckets */
    for (int i = 1; i < num_vbuckets; i++) {
        /* Active vbuckets */
        check(set_vbucket_state(h, Vbid(i), vbucket_state_active),
              "Failed to set vbucket state.");
    }

    // Set the manifest now that we have created vBuckets - do this on a new
    // cookie as DCP is on the other one.
    const void* admCookie = testHarness->create_cookie(h);
    checkeq(cb::engine_errc::success,
            h->set_collection_manifest(admCookie, R"({
  "uid": "1",
  "scopes": [
    {
      "name": "_default",
      "uid": "0",
      "collections": [
        {
          "name": "_default",
          "uid": "0"
        },
        {
          "name": "beer",
          "uid": "8"
        },
        {
          "name": "brewery",
          "uid": "9"
        }
      ]
    }
  ]
})"),
            "Failed set_collections");
    testHarness->destroy_cookie(admCookie);

    wait_for_flusher_to_settle(h);

    // Now insert items
    for (int i = 1; i < num_vbuckets; i++) {
        // Insert into default collection
        for (int j= 0; j < i; j++) {
            std::string key("key" + std::to_string(i));
            checkeq(ENGINE_SUCCESS,
                    store(h,
                          cookie,
                          OPERATION_SET,
                          key.c_str(),
                          "value",
                          nullptr,
                          0,
                          Vbid(i)),
                    "Failed to store an item.");
        }

        // Insert into collection with id 8
        for (int j = 0; j < i; j++) {
            std::string str_key("key" + std::to_string(i));
            StoredDocKey key{str_key, 8};
            checkeq(ENGINE_SUCCESS,
                    store(h,
                          cookie,
                          OPERATION_SET,
                          key.c_str(),
                          "value",
                          nullptr,
                          0,
                          Vbid(i)),
                    "Failed to store an item.");
        }
    }

    /* Create a pending vbucket */
    check(set_vbucket_state(h, Vbid(num_vbuckets), vbucket_state_pending),
          "Failed to set vbucket state.");

    /* Create request to get vb seqno of all alive vbuckets without supplying
     * the state*/
    get_all_vb_seqnos(h, {}, cookie);

    /* Check if the response received is correct */
    verify_all_vb_seqnos(h, 0, num_vbuckets);

    /* Create request to get vb seqno of all alive vbuckets by supplying a 0
     * state */
    get_all_vb_seqnos(h, RequestedVBState::Alive, cookie);

    /* Check if the response received is correct */
    verify_all_vb_seqnos(h, 0, num_vbuckets);

    /* Create request to get vb seqno of active vbuckets */
    get_all_vb_seqnos(h, RequestedVBState::Active, cookie);

    /* Check if the response received is correct */
    verify_all_vb_seqnos(h, 1, num_vbuckets - 1);

    /* Create request to get vb seqno of replica vbuckets */
    get_all_vb_seqnos(h, RequestedVBState::Replica, cookie);

    /* Check if the response received is correct */
    verify_all_vb_seqnos(h, 0, 0);

    /* Create request to get vb seqno of replica vbuckets */
    get_all_vb_seqnos(h, RequestedVBState::Pending, cookie);

    /* Check if the response received is correct */
    verify_all_vb_seqnos(h, num_vbuckets, num_vbuckets);

    /* Check the correctness of each collection high seqno (we should return
     * values for the default collection from replica and pending vBuckets) */
    get_all_vb_seqnos(
            h, RequestedVBState::Alive, cookie, CollectionID::Default);
    verify_all_vb_seqnos(h, 0, num_vbuckets, CollectionID(0));

    /*
     * Check our collections on the active vBucket
     */
    get_all_vb_seqnos(h, RequestedVBState::Active, cookie, 8);
    verify_all_vb_seqnos(h, 1, num_vbuckets - 1, CollectionID(8));

    get_all_vb_seqnos(h, RequestedVBState::Active, cookie, 9);
    verify_all_vb_seqnos(h, 1, num_vbuckets - 1, CollectionID(9));

    /*
     * We won't return anything from the replica (vbid 0) because it doesn't
     * know about any collections (didn't step dcp).
     */
    get_all_vb_seqnos(h, RequestedVBState::Replica, cookie, 8);
    verify_all_vb_seqnos(h, 0, -1, CollectionID(8));

    get_all_vb_seqnos(h, RequestedVBState::Replica, cookie, 9);
    verify_all_vb_seqnos(h, 0, -1, CollectionID(9));

    /*
     * We won't have created the collections on the pending VB.
     */
    get_all_vb_seqnos(h, RequestedVBState::Pending, cookie, 8);
    verify_all_vb_seqnos(h, 0, -1, CollectionID(8));

    get_all_vb_seqnos(h, RequestedVBState::Pending, cookie, 9);
    verify_all_vb_seqnos(h, 0, -1, CollectionID(9));

    // Priv checking
    mock_set_check_privilege_function(
            [](gsl::not_null<const void*> c,
               cb::rbac::Privilege priv,
               std::optional<ScopeID> sid,
               std::optional<CollectionID> cid) -> cb::rbac::PrivilegeAccess {
                if (cid && cid.value() == 8) {
                    return cb::rbac::PrivilegeAccessFail;
                }
                return cb::rbac::PrivilegeAccessOk;
            });
    get_all_vb_seqnos(
            h, RequestedVBState::Active, cookie, 8, cb::engine_errc::no_access);
    get_all_vb_seqnos(h, RequestedVBState::Active, cookie, 9);
    mock_set_check_privilege_function(
            [](gsl::not_null<const void*> c,
               cb::rbac::Privilege priv,
               std::optional<ScopeID> sid,
               std::optional<CollectionID> cid) -> cb::rbac::PrivilegeAccess {
                return cb::rbac::PrivilegeAccessFail;
            });
    get_all_vb_seqnos(h,
                      RequestedVBState::Active,
                      cookie,
                      {},
                      cb::engine_errc::no_access);

    mock_reset_check_privilege_function();
    /*
     * What happens when we don't tell the server that we can talk collections?
     */

    testHarness->destroy_cookie(cookie);
    cookie = testHarness->create_cookie(h);

    /*
     * We should just get back the default collection high seqno for the desired
     * vBuckets, regardless of whether or not we send a collection ID.
     */
    get_all_vb_seqnos(h, {}, cookie);
    verify_all_vb_seqnos(h, 0, num_vbuckets, CollectionID(0));

    get_all_vb_seqnos(h, RequestedVBState::Alive, cookie);
    verify_all_vb_seqnos(h, 0, num_vbuckets, CollectionID(0));

    get_all_vb_seqnos(h, RequestedVBState::Active, cookie);
    verify_all_vb_seqnos(h, 1, num_vbuckets - 1, CollectionID(0));

    get_all_vb_seqnos(h, RequestedVBState::Replica, cookie);
    verify_all_vb_seqnos(h, 0, 0, CollectionID(0));

    get_all_vb_seqnos(h, RequestedVBState::Pending, cookie);
    verify_all_vb_seqnos(h, num_vbuckets, num_vbuckets, CollectionID(0));

    get_all_vb_seqnos(h, RequestedVBState::Alive, cookie, 8);
    verify_all_vb_seqnos(h, 0, num_vbuckets, CollectionID(0));

    get_all_vb_seqnos(h, RequestedVBState::Active, cookie, 8);
    verify_all_vb_seqnos(h, 1, num_vbuckets - 1, CollectionID(0));

    get_all_vb_seqnos(h, RequestedVBState::Replica, cookie, 8);
    verify_all_vb_seqnos(h, 0, 0, CollectionID(0));

    get_all_vb_seqnos(h, RequestedVBState::Pending, cookie, 8);
    verify_all_vb_seqnos(h, num_vbuckets, num_vbuckets, CollectionID(0));

    testHarness->destroy_cookie(cookie);

    return SUCCESS;
}

/**
 * This test demonstrates bucket shutdown when there is a rogue
 * backfill (whose producer and stream are already closed).
 */
static enum test_result test_mb19153(EngineIface* h) {
    putenv(cb_strdup("ALLOW_NO_STATS_UPDATE=yeah"));

    // Set max num AUX IO to 0, so no backfill would start
    // immediately
    set_param(h,
              cb::mcbp::request::SetParamPayload::Type::Flush,
              "num_auxio_threads",
              "0");

    int num_items = 10000;

    for (int j = 0; j < num_items; ++j) {
        std::stringstream ss;
        ss << "key-" << j;
        checkeq(ENGINE_SUCCESS,
                store(h, NULL, OPERATION_SET, ss.str().c_str(), "data"),
                "Failed to store a value");
    }

    const void* cookie = testHarness->create_cookie(h);
    uint32_t flags = cb::mcbp::request::DcpOpenPayload::Producer;
    const char *name = "unittest";

    uint32_t opaque = 1;
    uint64_t start = 0;
    uint64_t end = num_items;

    // Setup a producer connection
    auto dcp = requireDcpIface(h);
    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie,
                      ++opaque,
                      0,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            "Failed dcp Consumer open connection.");

    // Initiate a stream request
    uint64_t vb_uuid = get_ull_stat(h, "vb_0:0:id", "failovers");
    uint64_t rollback = 0;
    checkeq(ENGINE_SUCCESS,
            dcp->stream_req(cookie,
                            0,
                            opaque,
                            Vbid(0),
                            start,
                            end,
                            vb_uuid,
                            0,
                            0,
                            &rollback,
                            mock_dcp_add_failover_log,
                            {}),
            "Expected success");

    // Disconnect the producer
    testHarness->destroy_cookie(cookie);

    // Wait for ConnManager to clear out dead connections from dcpConnMap
    wait_for_stat_to_be(h, "ep_dcp_dead_conn_count", 0, "dcp");

    // Set auxIO threads to 1, so the backfill for the closed producer
    // is picked up, and begins to run.
    set_param(h,
              cb::mcbp::request::SetParamPayload::Type::Flush,
              "num_auxio_threads",
              "1");

    // Terminate engine
    return SUCCESS;
}

static void mb19982_add_stat(std::string_view key,
                             std::string_view value,
                             gsl::not_null<const void*> cookie) {
    // do nothing
}

/*
 * This test creates a DCP consumer on a replica VB and then from a second thread
 * fires get_stats("dcp") whilst the main thread changes VB state from
 * replica->active->replica (and so on).
 * MB-19982 idenified a lock inversion between these two functional paths and this
 * test proves and protects the issue.
 */
static enum test_result test_mb19982(EngineIface* h) {
    // Load up vb0 with num_items
    int num_items = 1000;
    int iterations = 1000; // how many stats calls

    const void* cookie = testHarness->create_cookie(h);
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    std::string name = "unittest";
    // Switch to replica
    check(set_vbucket_state(h, Vbid(0), vbucket_state_replica),
          "Failed to set vbucket state.");

    // Open consumer connection
    auto dcp = requireDcpIface(h);
    checkeq(dcp->open(cookie,
                      opaque,
                      0,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            ENGINE_SUCCESS,
            "Failed dcp Consumer open connection.");

    add_stream_for_consumer(
            h, cookie, opaque++, Vbid(0), 0, cb::mcbp::Status::Success);

    std::thread thread([h, iterations]() {
        for (int ii = 0; ii < iterations; ii++) {
            checkeq(ENGINE_SUCCESS,
                    get_stats(h, "dcp"sv, {}, &mb19982_add_stat),
                    "failed get_stats(dcp)");
        }
    });

    uint32_t stream_opaque =
            get_int_stat(h, "eq_dcpq:unittest:stream_0_opaque", "dcp");

    for (int i = 1; i <= num_items; i++) {
        checkeq(dcp->snapshot_marker(cookie,
                                     stream_opaque,
                                     Vbid(0),
                                     num_items,
                                     num_items + i,
                                     2,
                                     0 /*HCS*/,
                                     {} /*maxVisibleSeqno*/),
                ENGINE_SUCCESS,
                "Failed to send snapshot marker");

        const std::string key("key-" + std::to_string(i));
        const DocKey docKey(key, DocKeyEncodesCollectionId::No);
        checkeq(ENGINE_SUCCESS,
                dcp->mutation(cookie,
                              stream_opaque,
                              docKey,
                              {(const uint8_t*)"value", 5},
                              0, // privileged bytes
                              PROTOCOL_BINARY_RAW_BYTES,
                              i * 3, // cas
                              Vbid(0),
                              0, // flags
                              i + num_items, // by_seqno
                              i + num_items, // rev_seqno
                              0, // expiration
                              0, // lock_time
                              {}, // meta
                              INITIAL_NRU_VALUE),
                "Failed to send dcp mutation");

        // And flip VB state (this can have a lock inversion with stats)
        checkeq(dcp->set_vbucket_state(
                        cookie, stream_opaque, Vbid(0), vbucket_state_active),
                ENGINE_SUCCESS,
                "failed to change to active");
        checkeq(dcp->set_vbucket_state(
                        cookie, stream_opaque, Vbid(0), vbucket_state_replica),
                ENGINE_SUCCESS,
                "failed to change to replica");
    }

    thread.join();
    testHarness->destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_set_dcp_param(EngineIface* h) {
    auto func = [h](std::string key, size_t newValue, bool expectedSetParam) {
        std::string statKey = "ep_" + key;
        size_t param = get_int_stat(h, statKey.c_str());
        std::string value = std::to_string(newValue);
        checkeq(expectedSetParam,
                set_param(h,
                          cb::mcbp::request::SetParamPayload::Type::Dcp,
                          key.c_str(),
                          value.c_str()),
                "Set param not expected");
        checkne(newValue, param,
                "Forcing failure as nothing will change");

        if (expectedSetParam) {
            checkeq(newValue,
                    size_t(get_int_stat(h, statKey.c_str())),
                    "Incorrect dcp param value after calling set_param");
        }
    };

    func("dcp_consumer_process_buffered_messages_yield_limit", 1000, true);
    func("dcp_consumer_process_buffered_messages_batch_size", 1000, true);
    func("dcp_consumer_process_buffered_messages_yield_limit", 0, false);
    func("dcp_consumer_process_buffered_messages_batch_size", 0, false);
    return SUCCESS;
}

// Test checks that if a backfill sends a prepare(k1), commit(k1) that the
// vbucket can successfully become active and produce data. MB-34634 meant that
// with these steps a crash occurred in a producer because the commit(k1) was
// effectively sent when a consumer indicated by stream-request they already
// had it (the crash was an exception inside KV-engine.)
static enum test_result test_MB_34634(EngineIface* h) {
    const void* cookie = testHarness->create_cookie(h);
    uint32_t opaque = 0xFFFF0000;
    uint32_t seqno = 0;
    const std::string conn_name("test_MB_34634");
    auto dcp = requireDcpIface(h);

    Vbid vb(0);

    // 1) Begin with our victim vbucket in pending state
    check(set_vbucket_state(h, vb, vbucket_state_pending),
          "Failed to set vbucket to pending");

    // 2) Create a DCP Consumer and add a stream so we can send DCP traffic to
    //    the victim
    checkeq(ENGINE_SUCCESS,
            dcp->open(cookie,
                      opaque,
                      seqno,
                      0,
                      conn_name,
                      R"({"consumer_name":"replica1"})"),
            "Failed to open a DCP Consumer");

    opaque = add_stream_for_consumer(
            h, cookie, opaque, vb, 0, cb::mcbp::Status::Success);

    // 3) Send a single 'disk' snapshot with two items.
    //    a) prepare(key)
    //    b) commit(key)
    // The commit must be the high-seqno, i.e. last item of the snapshot
    checkeq(ENGINE_SUCCESS,
            dcp->snapshot_marker(cookie,
                                 opaque,
                                 vb,
                                 0, // start-seq
                                 2, // end-seq
                                 MARKER_FLAG_DISK,
                                 0 /*HCS*/,
                                 {} /*maxVisibleSeqno*/),
            "snapshot_marker returned an error");
    const DocKey docKey1{"syncw", DocKeyEncodesCollectionId::No};
    checkeq(ENGINE_SUCCESS,
            dcp->prepare(cookie,
                         opaque,
                         docKey1,
                         {/*empty value*/},
                         0, // priv bytes
                         PROTOCOL_BINARY_RAW_BYTES,
                         10000, // cas
                         vb,
                         0, // flags
                         1, // by-seqno
                         1, // rev-seqno
                         0, // expiry
                         0, // lock-time
                         INITIAL_NRU_VALUE,
                         DocumentState::Alive,
                         cb::durability::Level::Majority),
            "DCP consumer failed the prepare");

    checkeq(ENGINE_SUCCESS,
            dcp->commit(cookie, opaque, vb, docKey1, 1, 2),
            "DCP consumer failed the commit");

    wait_for_flusher_to_settle(h);

    MockDcpMessageProducers producers(h);

    // 4) Close the stream and proceed to takeover, resulting in an active VB
    checkeq(ENGINE_SUCCESS,
            dcp->close_stream(cookie, opaque, vb, {}),
            "DCP consumer failed the close_stream request");

    // 4.1) takeover stream, switch pending -> active
    opaque = 0xDDDD0000;
    checkeq(ENGINE_SUCCESS,
            dcp->add_stream(cookie, opaque, vb, DCP_ADD_STREAM_FLAG_TAKEOVER),
            "Add stream request failed");

    dcp_step(h, cookie, producers);
    opaque = producers.last_opaque;
    checkeq(cb::mcbp::ClientOpcode::DcpStreamReq,
            producers.last_op,
            "Unexpected last_op");

    checkeq(ENGINE_SUCCESS,
            dcp->set_vbucket_state(cookie, opaque, vb, vbucket_state_active),
            "Add stream request failed");

    checkeq(ENGINE_SUCCESS,
            dcp->close_stream(cookie, opaque, vb, {}),
            "Expected success");

    // 5) Set the topology
    {
        char vbState = static_cast<char>(vbucket_state_active);
        const char* topology = R"({"topology":[["active"]]})";
        auto request = createPacket(cb::mcbp::ClientOpcode::SetVbucket,
                                    vb,
                                    0,
                                    {&vbState, 1},
                                    {/*no key*/},
                                    {topology, strlen(topology)});

        checkeq(ENGINE_SUCCESS,
                h->unknown_command(nullptr, *request, add_response),
                "Calling set vb state failed");
    }

    // 6) Create a producer and request a stream. If the MB is not fixed, an
    // exception is generated from a background DCP task, otherwise we should
    // be able to write and receive 1 extra item
    const void* cookie2 = testHarness->create_cookie(h);
    DcpStreamCtx ctx;
    ctx.vb_uuid = get_ull_stat(h, "vb_0:0:id", "failovers");
    ctx.seqno = {2, 0xfffffffffffff};
    ctx.snapshot = {2, 2};

    TestDcpConsumer tdc("test_MB_34634_producer", cookie2, h);
    tdc.addStreamCtx(ctx);
    tdc.openConnection();
    tdc.openStreams();

    write_items(h, 1);
    dcp_stream_from_producer_conn(h, cookie2, opaque, 3, 3, 0, tdc.producers);

    testHarness->destroy_cookie(cookie);
    testHarness->destroy_cookie(cookie2);

  return SUCCESS;
}

// Test reproduces the situation which caused the snapshot range exception
// seen in MB-34664. The test streams items to a consumer, followed by a
// snapshot marker (and no more items), switching to active and with no fix the
// snapshot_range structure detects an inconsistency (start > end)
static enum test_result test_MB_34664(EngineIface* h) {
    // Load up vb0 with num_items
    int num_items = 2;

    const void* cookie = testHarness->create_cookie(h);
    uint32_t opaque = 0xFFFF0000;
    uint32_t flags = 0;
    std::string name = "unittest";
    // Switch to replica
    check(set_vbucket_state(h, Vbid(0), vbucket_state_replica),
          "Failed to set vbucket state.");

    // Open consumer connection
    auto dcp = requireDcpIface(h);
    checkeq(dcp->open(cookie,
                      opaque,
                      0,
                      flags,
                      name,
                      R"({"consumer_name":"replica1"})"),
            ENGINE_SUCCESS,
            "Failed dcp Consumer open connection.");

    add_stream_for_consumer(
            h, cookie, opaque++, Vbid(0), 0, cb::mcbp::Status::Success);

    uint32_t stream_opaque =
            get_int_stat(h, "eq_dcpq:unittest:stream_0_opaque", "dcp");

    checkeq(dcp->snapshot_marker(cookie,
                                 stream_opaque,
                                 Vbid(0),
                                 1,
                                 1000,
                                 MARKER_FLAG_CHK,
                                 {} /*HCS*/,
                                 {} /*maxVisibleSeqno*/),
            ENGINE_SUCCESS,
            "Failed to send snapshot marker");

    for (int i = 1; i <= num_items; i++) {
        const std::string key("key-" + std::to_string(i));
        const DocKey docKey(key, DocKeyEncodesCollectionId::No);
        checkeq(ENGINE_SUCCESS,
                dcp->mutation(cookie,
                              stream_opaque,
                              docKey,
                              {(const uint8_t*)"value", 5},
                              0, // privileged bytes
                              PROTOCOL_BINARY_RAW_BYTES,
                              i * 3, // cas
                              Vbid(0),
                              0, // flags
                              i, // by_seqno
                              i + num_items, // rev_seqno
                              0, // expiration
                              0, // lock_time
                              {}, // meta
                              INITIAL_NRU_VALUE),
                "Failed to send dcp mutation");
    }

    wait_for_flusher_to_settle(h);

    checkeq(ENGINE_SUCCESS,
            dcp->snapshot_marker(cookie,
                                 stream_opaque,
                                 Vbid(0),
                                 num_items + 1,
                                 num_items + 1,
                                 MARKER_FLAG_CHK,
                                 {} /*HCS*/,
                                 {} /*maxVisibleSeqno*/),
            "Failed to send second snapshot marker");

    wait_for_flusher_to_settle(h);

    // Close and switch to active
    checkeq(ENGINE_SUCCESS,
            dcp->close_stream(cookie, opaque, Vbid(0), {}),
            "DCP consumer failed the close_stream request");

    check(set_vbucket_state(h, Vbid(0), vbucket_state_active),
          "Failed to set vbucket to active");

    wait_for_flusher_to_settle(h);
    testHarness->destroy_cookie(cookie);
    return SUCCESS;
}

// Test manifest //////////////////////////////////////////////////////////////

const char *default_dbname = "./ep_testsuite_dcp";

BaseTestCase testsuite_testcases[] = {

        TestCase("test dcp vbtakeover stat no stream",
                 test_dcp_vbtakeover_no_stream,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
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
                 test_setup,
                 teardown,
                 "max_vbuckets=7;max_num_shards=4;dcp_flow_control_policy="
                 "aggressive",
                 prepare,
                 cleanup),
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
                 "chk_remover_stime=1;max_checkpoints=2",
                 prepare,
                 cleanup),
        TestCase("test dcp replica stream backfill and warmup (MB-34173)",
                 test_dcp_replica_stream_backfill_MB_34173,
                 test_setup,
                 teardown,
                 "chk_remover_stime=1;max_checkpoints=2;"
                 "flusher_total_batch_limit=10",
                 prepare_ep_bucket,
                 cleanup),
        TestCase("test dcp replica stream in-memory",
                 test_dcp_replica_stream_in_memory, test_setup, teardown,
                 "chk_remover_stime=1;max_checkpoints=2", prepare, cleanup),
        TestCase("test dcp replica stream all",
                 test_dcp_replica_stream_all,
                 test_setup,
                 teardown,
                 "chk_remover_stime=1;max_checkpoints=2",
                 prepare,
                 cleanup),
        TestCase("test dcp replica stream all with collections enabled stream",
                 test_dcp_replica_stream_all_collection_enabled,
                 test_setup,
                 teardown,
                 "chk_remover_stime=1;max_checkpoints=2",
                 prepare,
                 cleanup),
        TestCase("test dcp replica stream one collection with mutations just "
                 "from disk",
                 test_dcp_replica_stream_one_collection_on_disk,
                 test_setup,
                 teardown,
                 "chk_remover_stime=1;max_checkpoints=2",
                 prepare,
                 cleanup),
        TestCase("test dcp replica stream one collection",
                 test_dcp_replica_stream_one_collection,
                 test_setup,
                 teardown,
                 "chk_remover_stime=1;max_checkpoints=2",
                 prepare,
                 cleanup),
        TestCase("test dcp replica stream expiries - ExpiryOutput Enabled",
                 test_dcp_replica_stream_expiry_enabled,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test dcp replica stream expiries - ExpiryOutput Disabled",
                 test_dcp_replica_stream_expiry_disabled,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test dcp producer stream open",
                 test_dcp_producer_stream_req_open,
                 test_setup,
                 teardown,
                 /* Expecting the connection manager background thread to notify
                    the connection at its default time interval is not very
                    efficent when we have items to be sent in a DCP stream.
                    Hence increase the default time to very high value, so that
                    the test fails if we are not doing a notification correctly
                 */
                 "connection_manager_interval=200000000",
                 prepare,
                 cleanup),
        TestCase("test producer stream request (partial)",
                 test_dcp_producer_stream_req_partial, test_setup, teardown,
                 /* set chk_period to essentially infinity so it won't run
                    during this test and create extra checkpoints we don't want.*/
                 "chk_remover_stime=1;chk_max_items=100;"
                 "chk_period=1000000",
                 prepare,
                 cleanup),
        TestCase("test producer stream request (full merged snapshots)",
                 test_dcp_producer_stream_req_full_merged_snapshots,
                 test_setup,
                 teardown,
                 "chk_remover_stime=1;chk_max_items=100",
                 prepare_ep_bucket,
                 cleanup),
        TestCase("test producer stream request (full)",
                 test_dcp_producer_stream_req_full, test_setup, teardown,
                 "chk_remover_stime=1;max_checkpoints=2;chk_max_items=100",
                 prepare_ephemeral_bucket,
                 cleanup),
        TestCase("test producer stream request (backfill)",
                 test_dcp_producer_stream_req_backfill,
                 test_setup,
                 teardown,
                 "chk_remover_stime=1;chk_max_items=100;dcp_scan_item_limit=50",
                 prepare,
                 cleanup),
        TestCase("test producer stream request (disk only)",
                 test_dcp_producer_stream_req_diskonly,
                 test_setup,
                 teardown,
                 "chk_remover_stime=1;chk_max_items=100",
                 prepare,
                 cleanup),
        TestCase("test producer disk backfill buffer limits",
                 test_dcp_producer_disk_backfill_buffer_limits,
                 test_setup,
                 teardown,
                 /* Set buffer size to a very low value (less than the size
                    of a mutation) */
                 "dcp_backfill_byte_limit=1;chk_remover_stime=1;"
                 "chk_max_items=3",
                 prepare,
                 cleanup),
        TestCase("test producer stream request (memory only)",
                 test_dcp_producer_stream_req_mem,
                 test_setup,
                 teardown,
                 "chk_remover_stime=1;chk_max_items=100",
                 prepare,
                 cleanup),
        TestCase("test producer stream request (DGM)",
                 test_dcp_producer_stream_req_dgm,
                 test_setup,
                 teardown,
                 // Need fewer than the number of items we write (at least 1000)
                 // in each checkpoint.
                 "chk_max_items=500;"
                 "chk_remover_stime=1;max_size=6291456",
                 /* not needed in ephemeral as it is DGM case */
                 /* TODO RDB: Relies on resident ratio - not valid yet */
                 prepare_ep_bucket_skip_broken_under_rocks,
                 cleanup),
        TestCase("test producer stream request coldness",
                 test_dcp_producer_stream_req_coldness,
                 test_setup,
                 teardown,
                 "chk_remover_stime=1;chk_max_items=2",
                 prepare_skip_broken_under_ephemeral,
                 cleanup),
        TestCase("test dcp consumer hotness data",
                 test_dcp_consumer_hotness_data,
                 test_setup,
                 teardown,
                 // If itemFreqDecayerTask runs ensure it has no effect.
                 "item_freq_decayer_percent=100",
                 prepare,
                 cleanup),
        TestCase("test producer stream request (latest flag)",
                 test_dcp_producer_stream_latest,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test producer keep stream open",
                 test_dcp_producer_keep_stream_open,
                 test_setup,
                 teardown,
                 "chk_remover_stime=1;chk_max_items=100",
                 prepare,
                 cleanup),
        TestCase("test producer keep stream open replica",
                 test_dcp_producer_keep_stream_open_replica,
                 test_setup,
                 teardown,
                 "chk_remover_stime=1;chk_max_items=100",
                 prepare,
                 cleanup),
        TestCase("test producer stream cursor movement",
                 test_dcp_producer_stream_cursor_movement,
                 test_setup,
                 teardown,
                 "chk_remover_stime=1;chk_max_items=10",
                 prepare,
                 cleanup),
        TestCase("test producer stream request nmvb",
                 test_dcp_producer_stream_req_nmvb, test_setup, teardown, nullptr,
                 prepare, cleanup),
        TestCase("test dcp agg stats",
                 test_dcp_agg_stats,
                 test_setup,
                 teardown,
                 "chk_max_items=100;chk_expel_enabled=false",
                 /*
                  * Checkpoint expelling needs to be disabled for this test because
                  * the test expects each of the five streams to contain only a
                  * single snapshot marker.  This relies on either all the items
                  * being in memory or all being pulled in from a backfill.  It
                  * does not expect to retrieve items from a backfill and
                  * in-memory which results in more than one snaphot marker.
                  */
                 prepare,
                 cleanup),
        TestCase("test dcp cursor dropping",
                 test_dcp_cursor_dropping,
                 test_setup,
                 teardown,
                 /* max_size set so that it's big enough that we can
                    create at least 1000 items when our residency
                    ratio gets to 90%. See test body for more details. */
                 "cursor_dropping_lower_mark=60;"
                 "cursor_dropping_upper_mark=70;"
                 "chk_remover_stime=1;"
                 "max_size=6291456;"
                 "chk_max_items=8000;"
                 "ephemeral_full_policy=fail_new_data;"
                 "chk_expel_enabled=false;"
                 "max_num_shards=4",
                 /*
                  * Checkpoint expelling needs to be disabled for this test because
                  * the expects to stream from memory.  So if items have been
                  * expelled the number of mutations streamed is invalid.
                 */

                 // TODO RDB: Cannot store any item (ENGINE_ENOMEM).
                 // Needs to resize 'max_size' to consider RocksDB
                 // pre-allocations.
                 // TODO magma: Need to recalc for magma
                 prepare_skip_broken_under_rocks_and_magma,
                 cleanup),
        TestCase("test dcp cursor dropping (replication)",
                 test_dcp_cursor_dropping_replication,
                 test_setup,
                 teardown,
                 /* max_size set so that it's big enough that we can
                    create at least 1000 items when our residency
                    ratio gets to 90%. See test body for more details. */
                 "cursor_dropping_lower_mark=60;"
                 "cursor_dropping_upper_mark=70;"
                 "chk_remover_stime=1;"
                 "max_size=6291456;"
                 "chk_max_items=8000;"
                 "ephemeral_full_policy=fail_new_data;"
                 "chk_expel_enabled=false;"
                 "max_num_shards=4",
                 /*
                  * Checkpoint expelling needs to be disabled for this test because
                  * the expects to stream from memory.  So if items have been
                  * expelled the number of mutations streamed is invalid.
                 */

                 // TODO RDB: Cannot store any item (ENGINE_ENOMEM).
                 // Needs to resize 'max_size' to consider RocksDB
                 // pre-allocations.
                 // TODO magma: Need to recalc for magma
                 prepare_skip_broken_under_rocks_and_magma,
                 cleanup),
        TestCase("test dcp cursor dropping backfill",
                 test_dcp_cursor_dropping_backfill,
                 test_setup,
                 teardown,
                 /* max_size set so that it's big enough that we can
                  create at least 1000 items when our residency
                  ratio gets to 90%. See test body for more details. */
                 "cursor_dropping_lower_mark=60;cursor_dropping_upper_mark=70;"
                 "chk_remover_stime=1;max_size=6291456;chk_max_items=8000;"
                 "chk_period=1;max_num_shards=4",
                 prepare_skip_broken_under_rocks_and_magma,
                 cleanup),
        TestCase("test dcp stream takeover", test_dcp_takeover, test_setup,
                teardown, "chk_remover_stime=1", prepare, cleanup),
        TestCase("test dcp stream takeover no items", test_dcp_takeover_no_items,
                 test_setup, teardown, "chk_remover_stime=1", prepare, cleanup),
        TestCase("test dcp consumer takeover", test_dcp_consumer_takeover,
                 test_setup, teardown, nullptr, prepare, cleanup),
        TestCase("test failover scenario one with dcp",
                 test_failover_scenario_one_with_dcp, test_setup, teardown,
                 nullptr, prepare, cleanup),
        TestCase("test failover scenario two with dcp",
                 test_failover_scenario_two_with_dcp, test_setup, teardown,
                 nullptr, prepare, cleanup),
        TestCase("test add stream", test_dcp_add_stream, test_setup, teardown,
                 "dcp_enable_noop=false", prepare, cleanup),
        TestCase("test consumer backoff stat", test_consumer_backoff_stat,
                 test_setup, teardown, nullptr,
                 prepare_ep_bucket,  // relies on persistence (disk queue)
                 cleanup),
        /* [TODO]: Write a test case for backoff based on high memory usage */
        TestCase("test dcp reconnect full snapshot",
                 test_dcp_reconnect_full,
                 test_setup,
                 teardown,
                 "dcp_enable_noop=false",
                 prepare,
                 cleanup),
        TestCase("test reconnect partial snapshot",
                 test_dcp_reconnect_partial,
                 test_setup,
                 teardown,
                 "dcp_enable_noop=false",
                 prepare,
                 cleanup),
        TestCase("test crash full snapshot",
                 test_dcp_crash_reconnect_full,
                 test_setup,
                 teardown,
                 "dcp_enable_noop=false",
                 prepare,
                 cleanup),
        TestCase("test crash partial snapshot",
                 test_dcp_crash_reconnect_partial,
                 test_setup,
                 teardown,
                 "dcp_enable_noop=false",
                 prepare,
                 cleanup),
        TestCase("test rollback to zero on consumer",
                 test_rollback_to_zero,
                 test_setup,
                 teardown,
                 "dcp_enable_noop=false",
                 prepare,
                 cleanup),
        TestCase("test chk manager rollback",
                 test_chk_manager_rollback,
                 test_setup,
                 teardown,
                 "dcp_flow_control_policy=none;dcp_enable_noop=false",
                 // TODO RDB: implement getItemCount.
                 // Needs the 'curr_items_tot' stat.
                 // TODO magma: requires collections
                 prepare_skip_broken_under_rocks_and_magma,
                 cleanup),
        TestCase("test full rollback on consumer",
                 test_fullrollback_for_consumer,
                 test_setup,
                 teardown,
                 "dcp_enable_noop=false",
                 // TODO RDB: Intermittently failing with SegFault.
                 // Probably we have to implement getItemCount. Needs the
                 // 'vb_replica_curr_items' stat.
                 // TODO: magma needs collections support
                 prepare_skip_broken_under_rocks_and_magma,
                 cleanup),
        TestCase("test partial rollback on consumer",
                 test_partialrollback_for_consumer,
                 test_setup,
                 teardown,
                 "dcp_enable_noop=false",
                 // TODO RDB: implement getItemCount.
                 // Needs the 'vb_replica_curr_items' stat.
                 // TODO: magma needs collections support
                 prepare_skip_broken_under_rocks_and_magma,
                 cleanup),
        TestCase("test change dcp buffer log size", test_dcp_buffer_log_size,
                test_setup, teardown, nullptr, prepare, cleanup),
        TestCase("test dcp producer flow control",
                 test_dcp_producer_flow_control,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test get failover log", test_dcp_get_failover_log,
                test_setup, teardown, nullptr, prepare, cleanup),
        TestCase("test add stream exists", test_dcp_add_stream_exists,
                 test_setup, teardown, "dcp_enable_noop=false", prepare,
                 cleanup),
        TestCase("test add stream nmvb", test_dcp_add_stream_nmvb, test_setup,
                 teardown, nullptr, prepare, cleanup),
        TestCase("test add stream prod exists", test_dcp_add_stream_prod_exists,
                 test_setup, teardown, "dcp_enable_noop=false", prepare,
                 cleanup),
        TestCase("test add stream prod nmvb", test_dcp_add_stream_prod_nmvb,
                 test_setup, teardown, "dcp_enable_noop=false", prepare,
                 cleanup),
        TestCase("test close stream (no stream)",
                 test_dcp_close_stream_no_stream, test_setup, teardown, nullptr,
                 prepare, cleanup),
        TestCase("test close stream", test_dcp_close_stream,
                 test_setup, teardown, "dcp_enable_noop=false", prepare,
                 cleanup),
        TestCase("test dcp consumer end stream", test_dcp_consumer_end_stream,
                 test_setup, teardown, "dcp_enable_noop=false", prepare,
                 cleanup),
        TestCase("dcp consumer mutate", test_dcp_consumer_mutate, test_setup,
                 teardown, "dcp_enable_noop=false", prepare, cleanup),
        TestCase("dcp consumer delete", test_dcp_consumer_delete, test_setup,
                 teardown, "dcp_enable_noop=false", prepare, cleanup),
        TestCase("dcp consumer expire", test_dcp_consumer_expire, test_setup,
                 teardown, "dcp_enable_noop=false", prepare, cleanup),
        TestCase("dcp failover log",
                 test_failover_log_dcp,
                 test_setup,
                 teardown,
                 nullptr,
                 // TODO RDB: implement getItemCount. Needs the 'curr_items'
                 // stat.
                 prepare_skip_broken_under_rocks,
                 cleanup),
        TestCase("dcp persistence seqno", test_dcp_persistence_seqno, test_setup,
                 teardown, nullptr,
                 prepare,
                 cleanup),
        TestCase("dcp persistence seqno for backfill items",
                 test_dcp_persistence_seqno_backfillItems,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("dcp last items purged",
                 test_dcp_last_items_purged,
                 test_setup,
                 teardown,
                 nullptr,
                 /* In ephemeral buckets the test is run from module tests:
                    EphTombstoneTest.ImmediateDeletedPurge() */
                 /* TODO RDB: Need to purge in a compaction filter,
                  * and store purged seqno.
                  * Relies upon triggering compaction directly, which
                  * is not currently done (avoids the compaction task
                  * triggering unneeded rocksdb compactions using the
                  * same api) */
                 prepare_ep_bucket_skip_broken_under_rocks,
                 cleanup),
        TestCase("dcp rollback after purge",
                 test_dcp_rollback_after_purge,
                 test_setup,
                 teardown,
                 nullptr,
                 /* In ephemeral buckets the test is run from module tests:
                    StreamTest.RollbackDueToPurge() */
                 /* TODO RDB: Need to purge in a compaction filter,
                  * and store purged seqno.
                  * Relies on triggering compaction directly */
                 prepare_ep_bucket_skip_broken_under_rocks,
                 cleanup),
        TestCase("dcp erroneous mutations scenario",
                 test_dcp_erroneous_mutations,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("dcp erroneous snapshot marker scenario", test_dcp_erroneous_marker,
                 test_setup, teardown, nullptr, prepare, cleanup),
        TestCase("dcp invalid mutation(s)/deletion(s)",
                 test_dcp_invalid_mutation_deletion,
                 test_setup, teardown, nullptr, prepare, cleanup),
        TestCase("dcp invalid snapshot marker",
                 test_dcp_invalid_snapshot_marker,
                 test_setup, teardown, nullptr, prepare, cleanup),
        TestCase("test MB-16357", test_mb16357,
                 test_setup, teardown, "compaction_exp_mem_threshold=85",
                 prepare, cleanup),
        TestCase("test dcp early termination",
                 test_dcp_early_termination,
                 test_setup,
                 teardown,
                 "max_vbuckets=100;max_num_shards=4",
                 prepare,
                 cleanup),
        TestCase("test MB-17517 CAS -1 DCP",
                 test_mb17517_cas_minus_1_dcp,
                 test_setup,
                 teardown,
                 nullptr,
                 /* TODO RDB: curr_items not correct under RocksDB */
                 prepare_skip_broken_under_rocks,
                 cleanup),
        TestCase("test dcp multiple streams", test_dcp_multiple_streams,
                 test_setup, teardown, nullptr, prepare, cleanup),
        TestCase("test dcp on vbucket state change",
                 test_dcp_on_vbucket_state_change,
                 test_setup, teardown, nullptr, prepare, cleanup),
        TestCase("test dcp consumer's processer task behavior",
                 test_dcp_consumer_processer_behavior,
                 test_setup, teardown, "max_size=1048576",
                 prepare, cleanup),
        TestCase("test get all vb seqnos",
                 test_get_all_vb_seqnos,
                 test_setup,
                 teardown,
                 "max_vbuckets=11;max_num_shards=4",
                 prepare,
                 cleanup),
        TestCase("test MB-19153", test_mb19153,
                 test_setup, teardown, nullptr, prepare, cleanup),
        TestCase("test MB-19982", test_mb19982, test_setup,
                 teardown, nullptr, prepare, cleanup),
        TestCase("test MB-19982 (buffer input)", test_mb19982, test_setup,
                 teardown, "replication_throttle_cap_pcnt=0;replication_throttle_queue_cap=0", prepare, cleanup),
        TestCase("test_set_dcp_param",
                 test_set_dcp_param, test_setup, teardown, nullptr,
                 prepare, cleanup),
        TestCase("test MB-23863 backfill deleted value",
                 test_dcp_producer_deleted_item_backfill, test_setup, teardown,
                 nullptr, prepare_ep_bucket, cleanup),
        TestCase("test MB-26907 backfill expired value - ExpiryOutput Disabled",
                 test_dcp_producer_expired_item_backfill_delete,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test MB-26907 backfill expired value - ExpiryOutput Enabled",
                 test_dcp_producer_expired_item_backfill_expire,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),
        TestCase("test MB-32443 delete with meta with expiration stream "
                 "- ExpiryOutput Disabled",
                 test_stream_deleteWithMeta_expiration_disabled,
                 test_setup,
                 teardown,
                 nullptr,
                /* TODO RDB: curr_items not correct under RocksDB */
                 prepare_skip_broken_under_rocks,
                 cleanup),
        TestCase("test MB-32443 delete with meta with expiration stream "
                 "- ExpiryOutput Enabled",
                 test_stream_deleteWithMeta_expiration_enabled,
                 test_setup,
                 teardown,
                 nullptr,
                /* TODO RDB: curr_items not correct under RocksDB */
                 prepare_skip_broken_under_rocks,
                 cleanup),
        TestCase("test noop mandatory",test_dcp_noop_mandatory,
                 test_setup, teardown, nullptr, prepare, cleanup),

        TestCase("test_MB_34634",
                 test_MB_34634,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),

        TestCase("test MB-34664",
                 test_MB_34664,
                 test_setup,
                 teardown,
                 nullptr,
                 prepare,
                 cleanup),

        TestCase(nullptr, nullptr, nullptr, nullptr, nullptr, prepare, cleanup)};
