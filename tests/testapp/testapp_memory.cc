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

#include <folly/ScopeGuard.h>
#include <folly/portability/GMock.h>

#include <cstdlib>
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

// Baseline for MB-55537: verifies the *current* (pre-fix) behaviour that a
// fragmenting workload drives the bucket's resident bytes far above its
// allocated bytes, with nothing to mitigate it. Once the RSS/fragmentation
// back-pressure lands this test will be updated to assert the mitigated
// behaviour instead.
TEST_P(MemTrackingBucketTest, HighFragmentation) {
    auto& conn = getConnection();
    conn.authenticate("@admin");
    conn.selectBucket(bucketName);

    // WHAT: reproduce the MB-55537 condition where a bucket's resident (RSS)
    // sits well above its allocated (mem_used) because of external jemalloc
    // fragmentation. WHY: this is the pre-fix baseline the RSS/fragmentation
    // back-pressure will later be shown to mitigate. HOW: fill one size-class'
    // slabs, then delete all but one survivor per slab -- each slab keeps a
    // live region so it stays resident, while allocated collapses:
    //
    //   one slab, nregs=8 regions of `sz` bytes, fully packed:
    //     +----+----+----+----+----+----+----+----+
    //     | A  | B  | C  | D  | E  | F  | G  | H  |   resident = 1 slab
    //     +----+----+----+----+----+----+----+----+   allocated = 8*sz
    //
    //   after deleting all but survivor A (delete stride = nregs):
    //     +----+----+----+----+----+----+----+----+
    //     | A  | .  | .  | .  | .  | .  | .  | .  |   resident = 1 slab (same)
    //     +----+----+----+----+----+----+----+----+   allocated = 1*sz
    //
    //   A alone pins the whole slab resident, so resident holds while allocated
    //   drops ~nregs-fold. Across many slabs resident/allocated approaches
    //   nregs.
    //
    // We target resident/allocated > 2x. The feature engages at fragmentation
    // ratio 0.25, i.e. resident/allocated ~= 1.33, so 2x is comfortably above
    // what the fix reacts to while staying reliably reachable over the wire.
    constexpr auto targetRatio = 2.0;

    // --- Step 1: pick the value and learn the target bin's regions-per-slab.
    // A large (~1KB) value matters: every item also carries a StoredValue, and
    // every deleted item a tombstone, which are NOT fragmented but count in
    // both resident and allocated. A tiny value lets that fixed per-item
    // overhead dominate and dilutes the ratio; a large value makes the
    // fragmentable Blob dominate. The stored Blob is value + a small header, so
    // we query the bin for value.size()+64 to land on the bin the Blob actually
    // uses. That bin's regions-per-slab (nregs) is both the fragmentation
    // ceiling and the delete stride (keep 1 of every nregs => one survivor per
    // slab). The test client is not jemalloc-backed, so we read nregs from the
    // server's stats dump.
    const std::string value(1024, 'x');
    const auto allocatorStats =
            conn.stats("allocator")["allocator"].get<std::string>();
    const auto nregs = regionsPerSlab(allocatorStats, value.size() + 64);
    ASSERT_GT(nregs, 0u)
            << "Could not parse regions-per-slab from allocator stats";
    // The ratio ceiling is ~nregs, so the bin must pack more than kTargetRatio
    // regions per slab for the target to be reachable at all.
    ASSERT_GT(nregs, targetRatio)
            << "bin packs " << nregs << " regions/slab, below target ratio";

    // --- Step 2: snapshot the baseline and compute how much to grow the arena.
    // We cannot measure the real ratio while loading (it only appears after the
    // deletes), and we must NOT delete-then-store (new stores refill the freed
    // regions and un-fragment the slabs). So we load until resident reaches a
    // precomputed target, then fragment in one pass. Derivation: after
    // fragmenting, allocated ~= baseline + resident/nregs, so
    // resident/allocated > R needs resident > R*baseline / (1 - R/nregs). The
    // 3x is margin for tombstones and imperfect survivor placement (see Step
    // 4).
    const auto baseAllocated =
            getStat<uint64_t>(conn, "", "ep_arena_memory_allocated");
    const auto baseResident =
            getStat<uint64_t>(conn, "", "ep_arena_memory_resident");
    const uint64_t targetResident =
            baseResident +
            static_cast<uint64_t>(3.0 * targetRatio * baseAllocated /
                                  (1.0 - targetRatio / nregs));

    // --- Step 3: grow the arena by storing whole slabs' worth of items until
    // resident hits the target. No deletes here (see Step 2). maxItems bounds
    // the test so a pathological bin fails fast instead of looping forever.
    constexpr size_t maxItems = 100000;
    const size_t batch = 20 * nregs; // whole slabs per batch
    size_t stored = 0;
    while (getStat<uint64_t>(conn, "", "ep_arena_memory_resident") <
                   targetResident &&
           stored < maxItems) {
        const size_t end = stored + batch;
        for (; stored < end; ++stored) {
            conn.store("key_" + std::to_string(stored), Vbid(0), value);
        }
    }
    ASSERT_LT(stored, maxItems)
            << "could not grow resident to " << targetResident;

    // --- Step 4: fragment in a single pass -- delete all but one survivor per
    // slab. Items were stored sequentially, so deleting every index that is not
    // a multiple of nregs keeps indices 0, nregs, 2*nregs, ... ~one survivor
    // per slab. (Placement is approximate over the wire -- the client can't see
    // Blob addresses -- which is what the 3x growth margin in Step 2 covers.)
    // The lone survivor pins each slab resident while its freed neighbours drop
    // allocated.
    uint64_t lastSeqno = 0;
    for (size_t i = 0; i < stored; ++i) {
        if (i % nregs != 0) {
            lastSeqno = conn.remove("key_" + std::to_string(i), Vbid(0)).seqno;
        }
    }

    // --- Step 5: actually free the deleted Blobs. A remove does not free the
    // Blob immediately -- it is still referenced by the persistence/DCP
    // checkpoint queue. Persist past the last delete so the items are no longer
    // pinned by the open checkpoint, then squeeze checkpoint_memory_ratio to
    // force checkpoint removal; only then are the Blobs released and allocated
    // drops. This runs on a background task, hence the poll in Step 6.
    conn.waitForSeqnoToPersist(Vbid(0), lastSeqno);

    // Cache the current ratio and restore it on exit (via the guard) so
    // squeezing it here does not leak into later tests sharing the bucket, even
    // if an assertion below returns early.
    const auto originalCheckpointMemRatio =
            getStat<float>(conn, "", "ep_checkpoint_memory_ratio");
    const auto restoreCheckpointMemRatio =
            folly::makeGuard([&conn, originalCheckpointMemRatio] {
                conn.execute(BinprotSetParamCommand{
                        cb::mcbp::request::SetParamPayload::Type::Checkpoint,
                        "checkpoint_memory_ratio",
                        std::to_string(originalCheckpointMemRatio)});
            });
    const auto setRatioResp = conn.execute(BinprotSetParamCommand{
            cb::mcbp::request::SetParamPayload::Type::Checkpoint,
            "checkpoint_memory_ratio",
            "0.01"});
    ASSERT_TRUE(setRatioResp.isSuccess())
            << "failed to set checkpoint_memory_ratio: "
            << to_string(setRatioResp.getStatus());

    // --- Step 6: wait for the async release, then assert the fragmentation.
    // resident stays high (slabs pinned by survivors) while allocated falls, so
    // the ratio climbs past the target. Poll until it does, or fail on timeout.
    const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds{10};
    uint64_t allocated = 0;
    uint64_t resident = 0;
    do {
        allocated = getStat<uint64_t>(conn, "", "ep_arena_memory_allocated");
        resident = getStat<uint64_t>(conn, "", "ep_arena_memory_resident");
        if (resident > allocated * targetRatio) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{100});
    } while (std::chrono::steady_clock::now() < deadline);

    // Current (pre-fix) behaviour: nothing mitigates the fragmentation.
    EXPECT_GT(resident, allocated * targetRatio)
            << "resident=" << resident << " allocated=" << allocated
            << " ratio=" << (allocated ? double(resident) / allocated : 0)
            << " nregs=" << nregs << " stored=" << stored;
}

#endif // HAVE_JEMALLOC
