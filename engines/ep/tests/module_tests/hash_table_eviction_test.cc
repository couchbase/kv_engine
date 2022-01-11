/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/**
 * This test is used for hash table eviction investigation.
 * It produces output to standard error that can be fed into a visualisation
 * tool - see kv_engine/engines/ep/scripts/evictionVisualiser.py
 */

#include "checkpoint_manager.h"
#include "evp_store_single_threaded_test.h"
#include "item.h"
#include "test_helpers.h"
#include "tests/mock/mock_ep_bucket.h"
#include "tests/mock/mock_synchronous_ep_engine.h"
#include "vbucket.h"

#include <folly/portability/GTest.h>
#include <platform/cb_arena_malloc.h>
#include <programs/engine_testapp/mock_server.h>

#include <random>

/**
 * Zipf Distribution algorithm - port of java implementation provided here
 * https://medium.com/@jasoncrease/ \
 * rejection-sampling-the-zipf-distribution-6b359792cffa
 */

class ZipfRejectionSampler {
public:
    ZipfRejectionSampler(int N_, double skew_) : N(N_), skew(skew_) {
        t = (std::pow(N, 1 - skew) - skew) / (1 - skew);

        // Seed random number generator with zero to make experiments
        // more repeatable.
        rng.seed(0);
        // Initialise a uniform distribution between 0 and 1
        std::uniform_real_distribution<double> unifInitialise(0, 1);
        unif = unifInitialise;
    };

    int getSample() {
        while (true) {
            double invB = bInvCdf(unif(rng));
            int sampleX = (int)(invB + 1);
            double yRand = unif(rng);
            double ratioTop = std::pow(sampleX, -skew);
            double ratioBottom =
                    (sampleX <= 1) ? 1.0 / t : std::pow(invB, -skew) / t;
            double rat = (ratioTop) / (ratioBottom * t);
            if (yRand < rat) {
                return sampleX;
            }
        }
    }

private:
    double bInvCdf(double p) {
        double res = p * t;
        if (res > 1.0) {
            res = std::pow((res) * (1 - skew) + skew, 1 / (1 - skew));
        }
        return res;
    }

    int N;
    double skew;
    double t;
    std::mt19937_64 rng;
    std::uniform_real_distribution<double> unif;
};

class STParameterizedEvictionTest
        : public SingleThreadedEPBucketTest,
          public ::testing::WithParamInterface<
                  std::tuple</*bucket_type*/ std::string,
                             /*max_size*/ std::string,
                             /*mem_low_wat*/ std::string,
                             /*mem_high_wat*/ std::string,
                             /*skew*/ double,
                             /*noOfAccesses*/ uint32_t>> {};

class STHashTableEvictionTest : public STParameterizedEvictionTest {
protected:
    void SetUp() override {
        // Set specific ht_size given we need to control expected memory usage.
        config_string +=
                "ht_size=24571;"
                "max_size=" +
                std::get<1>(GetParam()) + ";mem_low_wat=" +
                std::get<2>(GetParam()) + ";mem_high_wat=" +
                std::get<3>(GetParam());

        config_string += ";bucket_type=" + std::get<0>(GetParam());
        isEphemeral = (std::get<0>(GetParam()) == "ephemeral");
        STParameterizedEvictionTest::SetUp();

        // Sanity check - need memory tracker to be able to check our memory
        // usage.
        ASSERT_TRUE(cb::ArenaMalloc::canTrackAllocations())
                << "Memory tracker not enabled - cannot continue";

        for (int ii = 0; ii < noOfVBs; ii++) {
            store->setVBucketState(Vbid(ii), vbucket_state_active);
        }

        // Sanity check - to ensure memory usage doesn't increase without us
        // noticing.
        ASSERT_EQ(24571, store->getVBucket(vbid)->ht.getSize())
                << "Expected to have a HashTable of size 24571";
        auto& stats = engine->getEpStats();
        ASSERT_LE(stats.getEstimatedTotalMemoryUsed(), 200 * 1024 * 4)
                << "Expected to start with less than 200KB of memory used";
        ASSERT_LT(stats.getEstimatedTotalMemoryUsed(),
                  stats.getMaxDataSize() * 0.5)
                << "Expected to start below 50% of bucket quota";

        scheduleItemPager();
        static_cast<MockEPBucket*>(store)->createItemFreqDecayerTask();
        static_cast<MockEPBucket*>(store)->disableItemFreqDecayerTask();

        // Sanity check - should be no nonIO tasks ready to run,
        // and two in futureQ EphTombstoneHTCleaner and ItemPager)
        auto& lpNonioQ = *task_executor->getLpTaskQ()[NONIO_TASK_IDX];
        EXPECT_EQ(0, lpNonioQ.getReadyQueueSize());
        auto expectedFutureQueueSize = isEphemeral ? 2 : 1;
        EXPECT_EQ(expectedFutureQueueSize, lpNonioQ.getFutureQueueSize());

        // We shouldn't be able to schedule the Item Pager task yet as it's not
        // ready.
        try {
            SCOPED_TRACE("");
            runNextTask(lpNonioQ, "Paging out items.");
            FAIL() << "Unexpectedly managed to run Item Pager";
        } catch (std::logic_error&) {
        }

        zipfScew = std::get<4>(GetParam());
        noOfAccesses = std::get<5>(GetParam());
    }

    cb::engine_errc storeItem(Item& item) {
        uint64_t cas = 0;
        return engine->storeInner(
                cookie, item, cas, StoreSemantics::Set, false);
    }

    /**
     * Write documents to the bucket until they fail with TMP_FAIL.
     * Note this stores via external API (epstore) so we trigger the
     * memoryCondition() code in the event of cb::engine_errc::no_memory.
     */
    void populateUntilTmpFail() {
        const std::string value(512, 'x'); // 512B value to use for documents.
        cb::engine_errc result = cb::engine_errc::success;
        uint16_t vbid = 0;

        while (result == cb::engine_errc::success) {
            noOfDocs++;
            //
            if (noOfDocs % int(500 / noOfVBs) == 0) {
                vbid++;
                if (vbid == noOfVBs) {
                    vbid = 0;
                }
            }
            auto key = makeStoredDocKey("DOC_" + std::to_string(noOfDocs));
            auto item = make_item(Vbid(vbid), key, value, 0);
            result = storeItem(item);
            vbucketLookup.insert(std::pair<size_t, Vbid>(noOfDocs, Vbid(vbid)));
        }

        EXPECT_EQ(cb::engine_errc::temporary_failure, result);
        // Fixup noOfDocs for last loop iteration.
        --noOfDocs;

        if (!isEphemeral) {
            for (int ii = 0; ii < noOfVBs; ii++) {
                store->getVBucket(Vbid(ii))
                        ->checkpointManager->createNewCheckpoint();
                while (getEPBucket().flushVBucket(Vbid(ii)).numFlushed != 0)
                    ;
            }
        }

        auto& stats = engine->getEpStats();
        EXPECT_GT(stats.getEstimatedTotalMemoryUsed(),
                  stats.getMaxDataSize() * 0.8)
                << "Expected to exceed 80% of bucket quota after hitting "
                   "TMPFAIL";
        EXPECT_GT(stats.getEstimatedTotalMemoryUsed(), stats.mem_low_wat.load())
                << "Expected to exceed low watermark after hitting TMPFAIL";
    }

    /**
     * Perform a sequence of gets on the documents in the hash table.
     * Use Zipf distribution for the access pattern.
     */
    void accessPattern() {
        // Calculate the access frequencies
        ZipfRejectionSampler zipf(noOfDocs, zipfScew);
        for (uint32_t ii = 0; ii < noOfAccesses; ii++) {
            auto itemNumber = zipf.getSample();
            frequencies[itemNumber]++;
        }

        // Sort the access frequencies into descending order
        std::sort(std::begin(frequencies),
                  std::end(frequencies),
                  std::greater<uint64_t>());

        // Iterate through the frequency table and call get the correct number
        // of times
        bool complete = false;
        while (!complete) {
            complete = true;
            uint32_t kk = 1;
            while (kk <= noOfDocs && frequencies[kk] > 0) {
                complete = false;
                auto key = makeStoredDocKey("DOC_" + std::to_string(kk));
                EventuallyPersistentEngine* epe =
                        ObjectRegistry::onSwitchThread(NULL, true);
                Vbid vbucket = Vbid(0);
                auto it = vbucketLookup.find(kk);
                if (it != vbucketLookup.end()) {
                    vbucket = it->second;
                }
                auto options = static_cast<get_options_t>(
                        QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE |
                        DELETE_TEMP | HIDE_LOCKED_CAS | TRACK_STATISTICS);
                {
                    auto [status, item] =
                            engine->getInner(cookie, key, vbucket, options);
                    EXPECT_EQ(cb::engine_errc::success, status);
                    EXPECT_TRUE(item);
                }
                ObjectRegistry::onSwitchThread(epe);

                frequencies[kk]--;
                kk++;
            }
        }
    }

    /**
     * Run the item pager evicted items until the low water mark is reached.
     */
    void eviction() {
        auto& stats = engine->getEpStats();
        auto& lpNonioQ = *task_executor->getLpTaskQ()[NONIO_TASK_IDX];

        auto current = static_cast<double>(stats.getEstimatedTotalMemoryUsed());
        auto lower = static_cast<double>(stats.mem_low_wat);

        while (current > lower) {
            store->wakeItemPager();
            runNextTask(lpNonioQ);
            current = static_cast<double>(stats.getEstimatedTotalMemoryUsed());
            lower = static_cast<double>(stats.mem_low_wat);
            int vbucketcount = 0;
            // Note:  The reason for the /2 in while condition below is that
            // only half the vbuckets are active (the other half being
            // replica).  We evict from these different types of vbuckets in
            // separate runs of the ItemPager.
            while ((current > lower) && vbucketcount < noOfVBs / 2) {
                runNextTask(lpNonioQ);
                if (!isEphemeral) {
                    while (getEPBucket()
                                   .flushVBucket(Vbid(vbucketcount))
                                   .numFlushed != 0)
                        ;
                }
                vbucketcount++;
                current = static_cast<double>(
                        stats.getEstimatedTotalMemoryUsed());
                lower = static_cast<double>(stats.mem_low_wat);
            }
        }
    }

    /**
     * For each document display whether it is resident in the hash table or
     * has been evicted.
     */
    uint32_t residentOrEvicted() {
        uint32_t kk = 1;
        uint32_t evicted = 0;
        while (kk <= noOfDocs) {
            std::string key = "DOC_" + std::to_string(kk);
            auto result = cb::engine_errc::no_such_key;
            key_stats kstats;
            kstats.resident = false;

            Vbid vbucket = Vbid(0);
            auto it = vbucketLookup.find(kk);
            if (it != vbucketLookup.end()) {
                vbucket = it->second;
            }

            result = store->getKeyStats(makeStoredDocKey(key),
                                        vbucket,
                                        cookie,
                                        kstats,
                                        WantsDeleted::No);

            if (result == cb::engine_errc::success && kstats.resident) {
                std::cerr << key << " " << vbucket << " RESIDENT" << std::endl;
            } else {
                std::cerr << key << " " << vbucket << " EVICT" << std::endl;
                evicted++;
            }
            kk++;
        }
        return evicted;
    }

    /**
     * Helper function that prints out the number of docs resident in each
     * vbucket.
     */
    void printNoOfResidentDocs() {
        for (uint16_t ii = 0; ii < noOfVBs; ii++) {
            auto vb = engine->getVBucket(Vbid(ii));
            const auto numResidentItems =
                    vb->getNumItems() - vb->getNumNonResidentItems();
            std::cout << "number of resident docs for vb:" << ii << " = "
                      << numResidentItems << std::endl;
        }
    }

    const static uint32_t maxNoOfDocs = 300000;
    const static uint16_t noOfVBs = 4;
    uint32_t noOfAccesses = 0;
    double zipfScew = 0;
    uint32_t noOfDocs = 0;
    // map of document number to vbucket
    std::map<uint32_t, Vbid> vbucketLookup;
    std::array<uint64_t, maxNoOfDocs> frequencies{};
    bool isEphemeral{false};
};

#if defined(HAVE_JEMALLOC)

// Test is DISABLED so that it does not get run as part of automated testing.
// To run pass --gtest_also_run_disabled_tests to Gtest.
TEST_P(STHashTableEvictionTest, DISABLED_STHashTableEvictionItemPagerTest) {
    populateUntilTmpFail();
    printNoOfResidentDocs();
    accessPattern();

    for (int ii = 0; ii < noOfVBs / 2; ii++) {
        store->setVBucketState(Vbid(ii), vbucket_state_replica);
    }

    eviction();
    auto evictedCount = residentOrEvicted();
    std::cout << "evictedCount = " << evictedCount << std::endl;
    printNoOfResidentDocs();

    auto& stats = engine->getEpStats();
    EXPECT_LT(stats.getEstimatedTotalMemoryUsed(), stats.mem_low_wat.load())
            << "Expected to be below low watermark after running item pager";
}

static auto parameters =
        std::make_tuple(/*max_size*/ std::to_string(200 * 1024 * 1024),
                        /*mem_low_wat*/ std::to_string(120 * 1024 * 1024),
                        /*mem_high_wat*/ std::to_string(160 * 1024 * 1024),
                        /*skew*/ 0.9,
                        /*noOfAccesses*/ 20000000);

static auto persistentTestConfigValues = ::testing::Values(
        std::tuple_cat(std::make_tuple(std::string("persistent")), parameters));
static auto ephemeralTestConfigValues = ::testing::Values(
        std::tuple_cat(std::make_tuple(std::string("ephemeral")), parameters));

INSTANTIATE_TEST_SUITE_P(EvictionPersistent,
                         STHashTableEvictionTest,
                         persistentTestConfigValues);
INSTANTIATE_TEST_SUITE_P(EvictionEphemeral,
                         STHashTableEvictionTest,
                         ephemeralTestConfigValues);

#endif
