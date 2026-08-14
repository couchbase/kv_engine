/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "testapp_client_test.h"

#include <folly/Portability.h>
#include <folly/ScopeGuard.h>
#include <folly/portability/GMock.h>
#include <platform/timeutils.h>

#include <cstdlib>
#include <iostream>
#include <iterator>
#include <sstream>
#include <string>
#include <vector>

class MemTrackingBucketTest : public TestappClientTest {
public:
    static void SetUpTestCase() {
        // Note: Important to set the env BEFORE starting memcached,
        // env vars wouldn't be passed to the memcached process otherwise
        // (unless testapp is run in -e (embedded) mode)
        if (!getenv("CB_ARENA_MALLOC_VERIFY_DEALLOC_CLIENT")) {
            ASSERT_EQ(0,
                      setenv("CB_ARENA_MALLOC_VERIFY_DEALLOC_CLIENT", "1", 0));
        }

        auto config = generate_config();
        config["threads"] = 1;
        TestappTest::doSetUpTestCaseWithConfiguration(config);
    }

    static void TearDownTestCase() {
        EXPECT_EQ(0, unsetenv("CB_ARENA_MALLOC_VERIFY_DEALLOC_CLIENT"));
        TestappTest::TearDownTestCase();
    }
};

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         MemTrackingBucketTest,
                         ::testing::Values(TransportProtocols::McbpSsl),
                         ::testing::PrintToStringParamName());

TEST_P(MemTrackingBucketTest, MB_68823) {
    // Note: Not using adminConnection as the connection in the test is
    // forcibly disconnected and we need adminConnection at TearDown.
    auto& conn = getConnection();
    conn.authenticate("@admin");
    conn.selectBucket(bucketName);
    conn.setFeature(cb::mcbp::Feature::JSON, true);
    conn.setFeature(cb::mcbp::Feature::Collections, true);
    conn.dcpOpenProducer("dcp-conn_invalid-stream-req-filter");
    conn.dcpControl("enable_noop", "true");

    // Invalid StreamReq filter (with cid duplicate) throws in Filter::ctor.
    // Before the fix the test fails by:
    //
    // ===ERROR===: JeArenaMalloc deallocation mismatch
    //     Memory freed by client:100 domain:None which is assigned arena:0,
    //     but memory was previously allocated from arena:2 (client-specific
    //     arena).
    //     Allocation address:0x10b1b1080 size:192
    try {
        conn.dcpStreamRequest(Vbid(0),
                              cb::mcbp::DcpAddStreamFlag::None,
                              0, // startSeq
                              ~0ULL, // endSeq,
                              0, // vbUuid
                              0, // snapStart
                              0, // snapEnd
                              R"({"collections":["0", "0"]})"_json); // filter
    } catch (const std::exception&) {
        const auto timeout =
                std::chrono::steady_clock::now() + std::chrono::seconds{10};
        const auto line =
                "EventuallyPersistentEngine::stream_req: Exception GSL: "
                "Precondition failure: 'emplaced'";
        constexpr auto expectedLogInstances = 1;
        do {
            if (mcd_env->verifyLogLine(line) == expectedLogInstances) {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds{100});
        } while (std::chrono::steady_clock::now() < timeout);

        FAIL() << "Timeout before the log line was dumped to the file";
    }
    FAIL() << "StreamRequest should have failed";
}

TEST_P(MemTrackingBucketTest, MB_71836) {
    auto& conn = getConnection();
    conn.authenticate("@admin");
    conn.selectBucket(bucketName);

    // Craft an invalid SetWithMeta payload
    Document doc{};
    const auto key = "key";
    doc.info.id = key;
    doc.info.cas = 123;
    doc.info.expiration = 0;
    doc.info.flags = 0xaabbccdd;
    doc.value = "value";
    doc.info.datatype = cb::mcbp::Datatype::Raw;
    // 1-byte meta
    const std::vector<uint8_t> meta = {0xff};

    class InvalidSetWithMetaCommand : public BinprotSetWithMetaCommand {
    public:
        InvalidSetWithMetaCommand(const Document& doc,
                                  Vbid vbid,
                                  uint64_t cas,
                                  uint64_t seqno,
                                  uint32_t options,
                                  const std::vector<uint8_t>& meta)
            : BinprotSetWithMetaCommand(doc, vbid, cas, seqno, options, meta) {
        }

        void encode(std::vector<uint8_t>& buf) const override {
            BinprotSetWithMetaCommand::encode(buf);
            // Poison the meta_size with its max value (uint16_t, 0xffff).
            // Before the fix, that triggers broken code that underflows the
            // internal doc value representation by making its size huge.
            buf.at(48) = static_cast<uint16_t>(0xff);
            buf.at(49) = static_cast<uint16_t>(0xff);
        }
    };

    InvalidSetWithMetaCommand cmd(
            doc, Vbid(0), cb::mcbp::cas::Wildcard, 1, 0, meta);
    // We want to validate the invalid request and force disconnection.
    // Before the fix the invalid payload goes through, sign is that we try to
    // allocate a huge blob by that, the connection stays alive we just return
    // E2big.
    try {
        conn.execute(cmd);
        FAIL() << "Server should force disconnection";
    } catch (const std::system_error& e) {
        EXPECT_THAT(e.what(),
                    testing::HasSubstr("AsyncSocketException: Network error"));
    }
}

// The fragmentation test below (and this jemalloc-specific slab helper) only
// build with jemalloc: on the system allocator there are no arenas/slabs to
// fragment and the ep_arena_memory_* stats it relies on are not produced.
#if defined(HAVE_JEMALLOC)

// Regions-per-slab for the smallest jemalloc bin that fits `minSize`, read from
// the server's jemalloc dump. The bins table columns are:
//   size ind allocated nmalloc (#/sec) ndalloc (#/sec) nrequests (#/sec)
//   nshards curregs curslabs nonfull_slabs regs pgs util ...
// so `size` is column 0 and `regs` is column 13. `regs` is a size-class
// constant, so the first (merged) bins table is fine.
static size_t regionsPerSlab(const std::string& allocatorStatsDump,
                             size_t minSize) {
    std::istringstream iss(allocatorStatsDump);
    std::string line;
    bool inBins = false;
    while (std::getline(iss, line)) {
        if (line.find("bins:") != std::string::npos) {
            inBins = true; // this line is the header; data rows follow
            continue;
        }
        if (!inBins) {
            continue;
        }
        std::istringstream ls(line);
        std::vector<std::string> col{std::istream_iterator<std::string>(ls),
                                     std::istream_iterator<std::string>()};
        if (col.size() < 14) {
            continue; // blank line / "---" / section end
        }
        char* end = nullptr;
        const auto size = std::strtoul(col[0].c_str(), &end, 10);
        if (end == col[0].c_str()) {
            continue; // non-numeric first token -> not a data row
        }
        if (size >= minSize) {
            return static_cast<size_t>(std::stoul(col[13])); // regs
        }
    }
    return 0;
}

// MB-55537: reproduce a bucket whose resident (RSS) exceeds its quota because
// of external jemalloc fragmentation while allocated (mem_used) stays under
// quota, then verify the RSS/fragmentation back-pressure rejects a mutation
// with a temporary failure.
//
// Mechanism -- two size classes, so freed regions in the first are not refilled
// by the second (that is what lets resident climb above the working set while
// allocated stays low):
//   1. Fill a class-A working set up to ~0.7x quota (dense).
//   2. Delete all but one survivor per A slab (delete stride = the bin's
//      regions-per-slab, nregs): each slab keeps a live region and stays
//      resident while its other regions become holes, so allocated drops while
//      resident holds.
//   3. Fill class-B (a different bin, so it cannot refill A's holes) until
//      resident is comfortably over the quota.
//   4. Do the same survivor-per-slab delete on B (its own nregs), dropping
//      allocated further while resident holds.
// End state: allocated < quota < resident, ~55-60% fragmentation measure on
// local test run -- the real MB-55537 condition (the gate only needs 25%).
// Throughout, allocated is kept under ~3/4 quota so mem_used stays below the
// high water mark and the value_only item pager never ejects the values we are
// pinning. The bucket quota is set to 100 MB to keep the run as quick as
// possible.
//
// The defragmenter is disabled during the build (so it does not compact A's
// sparse slabs) and stays disabled while we assert the back-pressure gate
// (phase 1): recovery is driven by the same switch as the gate, so leaving it
// able to run would let it pull RSS back under quota before we observe the
// temp-OOM. It is re-enabled to assert recovery (phase 2). The gate does not
// depend on the defragmenter being enabled, only on the published RSS/frag.
TEST_P(MemTrackingBucketTest, HighFragmentation) {
    adminConnection->selectBucket(bucketName);

    const auto setParam = [](std::string_view key, std::string_view value) {
        adminConnection->execute(BinprotSetParamCommand{
                cb::mcbp::request::SetParamPayload::Type::Flush,
                std::string(key),
                std::string(value)});
    };
    const auto resident = [] {
        return getStat<uint64_t>(
                *adminConnection, "", "ep_arena_memory_resident");
    };
    const auto allocated = [] {
        return getStat<uint64_t>(
                *adminConnection, "", "ep_arena_memory_allocated");
    };

    // checkpoint_memory_ratio is a Checkpoint-category param, so it must be set
    // with Type::Checkpoint.
    // lowerCheckpointMemRatio drops it to trigger checkpoint memory recovery
    // (async) and free the deleted Blobs; cache the original and restore it on
    // exit so it does not leak into later tests.
    const auto lowerCheckpointMemRatio = [] {
        const auto resp = adminConnection->execute(BinprotSetParamCommand{
                cb::mcbp::request::SetParamPayload::Type::Checkpoint,
                "checkpoint_memory_ratio",
                "0.01"});
        EXPECT_TRUE(resp.isSuccess())
                << "failed to set checkpoint_memory_ratio: "
                << to_string(resp.getStatus());
    };
    const auto originalCheckpointMemRatio =
            getStat<float>(*adminConnection, "", "ep_checkpoint_memory_ratio");
    const auto restoreCheckpointMemRatio =
            folly::makeGuard([originalCheckpointMemRatio] {
                adminConnection->execute(BinprotSetParamCommand{
                        cb::mcbp::request::SetParamPayload::Type::Checkpoint,
                        "checkpoint_memory_ratio",
                        std::to_string(originalCheckpointMemRatio)});
            });

    // Disable the defragmenter while we build fragmentation and assert the
    // back-pressure gate (phase 1). This does two things: it does not compact
    // the fragmentation we build, and it keeps recovery from running. Recovery
    // is driven by the same fragmentation_backpressure_enabled switch, so if it
    // ran it would pull RSS back under quota before we could observe the temp-
    // OOM. The gate itself does not depend on the defragmenter being enabled --
    // only on the MonitorTask-published RSS/fragmentation. It is re-enabled
    // below to assert recovery (phase 2).
    setParam("defragmenter_enabled", "false");

    // Use a fixed 100 MB quota so the test does not depend on any -c max_size
    // and stays quick (less to load). The change is applied asynchronously.
    constexpr auto quota = 100 * 1024 * 1024;
    setParam("max_size", std::to_string(quota));
    cb::waitForPredicate([] {
        return getStat<uint64_t>(*adminConnection, "", "ep_max_size") == quota;
    });

    // Dump the memory state after each macro step so the run is easy to follow
    // (DEBUG builds only). kv_size is Blob memory held in the HashTable;
    // non_resident/value_ejects reveal whether item values are being ejected.
    const auto logDebugState = [&]([[maybe_unused]] std::string_view stage) {
        if constexpr (folly::kIsDebug) {
            const auto stat = [](const char* key) {
                return getStat<uint64_t>(*adminConnection, "", key);
            };
            const auto r = resident();
            const auto a = allocated();
            std::cout << "[ HighFragmentation ] " << stage
                      << ": quota=" << quota << " resident=" << r
                      << " allocated=" << a << " frag="
                      << (r > a ? 100.0 * double(r - a) / double(r) : 0.0)
                      << "%"
                      << " rss/alloc=" << (a ? double(r) / double(a) : 0.0)
                      << " items=" << stat("curr_items")
                      << " mem_used=" << stat("mem_used")
                      << " kv_size=" << stat("ep_kv_size")
                      << " non_resident=" << stat("ep_num_non_resident")
                      << " value_ejects=" << stat("ep_num_value_ejects")
                      << " high_wat=" << stat("ep_mem_high_wat") << std::endl;
        }
    };
    logDebugState("start");

    const std::string valueA(1024, 'a');
    const std::string valueB(4096, 'b');
    constexpr size_t chunk = 1000; // stores between stat checks

    // Phase 1: dense class-A working set up to ~0.7x quota. Stays under quota,
    // so the load itself does not temp-OOM. The loop terminates because
    // allocated grows monotonically (no ejection below the high water mark) and
    // the quota bounds it.
    size_t aCount = 0;
    while (allocated() < quota * 7 / 10) {
        for (size_t i = 0; i < chunk; ++i, ++aCount) {
            adminConnection->store(
                    "A_" + std::to_string(aCount), Vbid(0), valueA);
        }
    }
    logDebugState("after load A");

    // Phase 2: delete all but one survivor per A slab, so each slab keeps a
    // live region and stays resident while its other regions become holes --
    // allocated drops while resident holds. Items were stored sequentially, so
    // the delete stride is the bin's regions-per-slab (nregs): keeping indices
    // 0, nregs, 2*nregs, ... leaves ~one survivor per slab. nregs is read from
    // the server's jemalloc dump for the bin the ~1KB Blob lands in (value + a
    // small header); the test client is not jemalloc-backed, hence the
    // server-side query.
    //
    //   one slab, nregs=8 regions of `sz` bytes, fully packed:
    //     +----+----+----+----+----+----+----+----+
    //     | A0 | A1 | A2 | A3 | A4 | A5 | A6 | A7 |  resident: 1 slab
    //     +----+----+----+----+----+----+----+----+  allocated: 8*sz
    //
    //   after deleting all but survivor A0 (stride = nregs):
    //     +----+----+----+----+----+----+----+----+
    //     | A0 | .  | .  | .  | .  | .  | .  | .  |  resident: 1 slab (same)
    //     +----+----+----+----+----+----+----+----+  allocated: 1*sz
    //
    //   A0 alone pins the whole slab resident, so resident holds while
    //   allocated drops ~nregs-fold; across many slabs resident/allocated
    //   approaches nregs.
    // The +64 is a Blob-header allowance. value.size() (1024) is itself a size-
    // class boundary, but the stored Blob is value + a ~9-byte header, so it
    // lands in the NEXT class (1280; 5120 for the 4KB B). Adding the header
    // bumps the query past the boundary so we read that next class's nregs, not
    // the value's.
    const auto nregsA = regionsPerSlab(
            adminConnection->stats("allocator")["allocator"].get<std::string>(),
            valueA.size() + 64);
    ASSERT_GT(nregsA, 0u) << "could not parse regions-per-slab for the A bin";
    uint64_t lastSeqno = 0;
    for (size_t i = 0; i < aCount; ++i) {
        if (i % nregsA != 0) {
            lastSeqno =
                    adminConnection->remove("A_" + std::to_string(i), Vbid(0))
                            .seqno;
        }
    }
    adminConnection->waitForSeqnoToPersist(Vbid(0), lastSeqno);
    lowerCheckpointMemRatio();
    logDebugState("after delete A");

    // Phase 3: fill class-B (a different bin, so it cannot refill A's holes)
    // until resident is comfortably over the quota. Bound by allocated < 3/4
    // quota so mem_used (~1.1x allocated) stays under the high water mark and
    // the value_only item pager never fires -- an ejection here would free the
    // Blobs and destroy the fragmentation we are building.
    size_t bCount = 0;
    while (resident() <= quota * 11 / 10 && allocated() < quota * 3 / 4) {
        for (size_t i = 0; i < chunk; ++i, ++bCount) {
            adminConnection->store(
                    "B_" + std::to_string(bCount), Vbid(0), valueB);
        }
    }
    logDebugState("after load B");

    // Phase 4: the same survivor-per-slab delete for B, using B's own
    // regions-per- slab (a different bin from A, so a different nregs),
    // dropping allocated further while resident holds. B is small (mem_used
    // stayed under the high water mark, so no ejection), so this handful of
    // deletes cannot temp-OOM. +64: Blob-header allowance, as in phase 2 --
    // lands B in its next class (5120).
    const auto nregsB = regionsPerSlab(
            adminConnection->stats("allocator")["allocator"].get<std::string>(),
            valueB.size() + 64);
    ASSERT_GT(nregsB, 0u) << "could not parse regions-per-slab for the B bin";
    lastSeqno = 0;
    for (size_t i = 0; i < bCount; ++i) {
        if (i % nregsB != 0) {
            lastSeqno =
                    adminConnection->remove("B_" + std::to_string(i), Vbid(0))
                            .seqno;
        }
    }
    adminConnection->waitForSeqnoToPersist(Vbid(0), lastSeqno);
    lowerCheckpointMemRatio();
    logDebugState("after delete B");

    const auto rss = resident();
    const auto alloc = allocated();
    ASSERT_GT(rss, quota) << "did not reach RSS>quota; rss=" << rss
                          << " alloc=" << alloc << " quota=" << quota;
    ASSERT_LT(alloc, quota)
            << "allocated over quota (would be a normal temp-OOM); rss=" << rss
            << " alloc=" << alloc << " quota=" << quota;

    // Baseline the defragmenter's moved counter before recovery runs. The
    // defragmenter is disabled and our survivors are freshly stored (age 0), so
    // nothing has moved them. During recovery this counter rises: the task is
    // woken and, running in aggressive mode (age thresholds 0), relocates even
    // the age-0 survivors that the default age threshold (1) would skip.
    const auto defragMovedBefore = getStat<uint64_t>(
            *adminConnection, "", "ep_defragmenter_num_moved");

    // The configured min sleep. Recovery collapses the defragmenter's sleep to
    // this, which we assert below as the direct "aggressive scheduling" signal.
    const auto minSleep = getStat<float>(
            *adminConnection, "", "ep_defragmenter_auto_min_sleep");

    // Phase 1: back-pressure. Enable the feature; the defragmenter is still
    // disabled, so recovery cannot run and RSS stays put. The gate reads the
    // MonitorTask-published RSS/fragmentation, which lags the build by up to
    // one monitor interval, so poll until the probe is rejected rather than
    // probing once. With recovery disabled there is no race -- once the monitor
    // publishes the over-quota RSS the gate stays critical and the probe stays
    // rejected. This is back-pressure: RSS > quota with high fragmentation and
    // allocated < quota, so it is not a normal over-quota temp-OOM.
    setParam("fragmentation_backpressure_enabled", "true");
    adminConnection->setAutoRetryTmpfail(false);
    const auto probeRejected = [&] {
        BinprotMutationCommand probe;
        probe.setKey("frag_backpressure_probe");
        probe.setMutationType(MutationType::Set);
        probe.setValue(std::vector<uint8_t>(valueA.begin(), valueA.end()));
        probe.setVBucket(Vbid(0));
        return adminConnection->execute(probe).getStatus() ==
               cb::mcbp::Status::Etmpfail;
    };
    cb::waitForPredicate(probeRejected);

    // Phase 2: recovery. Enable the defragmenter. The MonitorTask, still seeing
    // critical fragmentation, wakes it and it runs in aggressive mode (min
    // sleep, age thresholds 0). First assert it is in that mode: its moved
    // counter rises -- it only runs because it was woken, and only moves the
    // age-0 survivors because the age threshold dropped to 0.
    setParam("defragmenter_enabled", "true");
    cb::waitForPredicate([defragMovedBefore] {
        return getStat<uint64_t>(
                       *adminConnection, "", "ep_defragmenter_num_moved") >
               defragMovedBefore;
    });

    // Assert the aggressive scheduling directly: recovery collapses the sleep
    // to the configured min. It is transient -- it reverts once recovery
    // completes and RSS is back under quota. (The age-0 override is not yet
    // observable; exposing the effective age thresholds as a stat and asserting
    // them is a follow-up patch.)
    cb::waitForPredicate([minSleep] {
        return getStat<float>(*adminConnection,
                              "",
                              "ep_defragmenter_sleep_time") <= minSleep;
    });

    // The aggressive defrag compacts the scattered survivors and frees the
    // sparse slabs, so RSS returns under quota.
    cb::waitForPredicate([&] { return resident() <= quota; });

    // Once the MonitorTask republishes the recovered RSS, the gate lifts and
    // mutations are accepted again.
    cb::waitForPredicate([&] { return !probeRejected(); });
    logDebugState("after recovery");
    adminConnection->setAutoRetryTmpfail(true);
}

#endif // HAVE_JEMALLOC
