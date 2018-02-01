/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

/**
 * This test is used for hash table eviction investigation.
 * It produces output to standard error that can be fed into a visualisation
 * tool - see kv_engine/engines/ep/scripts/evictionVisualiser.py
 */

#include "checkpoint.h"
#include "evp_store_single_threaded_test.h"
#include "memory_tracker.h"
#include "test_helpers.h"
#include "tests/mock/mock_synchronous_ep_engine.h"

#include <gtest/gtest.h>

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
          public ::testing::WithParamInterface<std::tuple<
                  /*max_size*/ std::string,
                  /*mem_low_wat*/ std::string,
                  /*mem_high_wat*/ std::string,
                  /*skew*/ double,
                  /*noOfAccesses*/ uint32_t>> {};

class STHashTableEvictionTest : public STParameterizedEvictionTest {
public:
    static void SetUpTestCase() {
        // Setup the MemoryTracker.
        MemoryTracker::getInstance(*get_mock_server_api()->alloc_hooks);
    }

    static void TearDownTestCase() {
        MemoryTracker::destroyInstance();
    }

protected:
    void SetUp() override {
        // Set specific ht_size given we need to control expected memory usage.
        config_string +=
                "ht_size=24571;"
                "max_size=" +
                std::get<0>(GetParam()) + ";mem_low_wat=" +
                std::get<1>(GetParam()) + ";mem_high_wat=" +
                std::get<2>(GetParam());

        config_string += ";bucket_type=ephemeral";
        STParameterizedEvictionTest::SetUp();

        // Sanity check - need memory tracker to be able to check our memory
        // usage.
        ASSERT_TRUE(MemoryTracker::trackingMemoryAllocations())
                << "Memory tracker not enabled - cannot continue";

        for (int ii = 0; ii < noOfVBs; ii++) {
            store->setVBucketState(ii, vbucket_state_active, false);
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

        // Sanity check - should be no nonIO tasks ready to run,
        // and two in futureQ EphTombstoneHTCleaner and ItemPager)
        auto& lpNonioQ = *task_executor->getLpTaskQ()[NONIO_TASK_IDX];
        EXPECT_EQ(0, lpNonioQ.getReadyQueueSize());
        EXPECT_EQ(2, lpNonioQ.getFutureQueueSize());

        // We shouldn't be able to schedule the Item Pager task yet as it's not
        // ready.
        try {
            SCOPED_TRACE("");
            runNextTask(lpNonioQ, "Paging out items.");
            FAIL() << "Unexpectedly managed to run Item Pager";
        } catch (std::logic_error&) {
        }

        zipfScew = std::get<3>(GetParam());
        noOfAccesses = std::get<4>(GetParam());
    }

    ENGINE_ERROR_CODE storeItem(Item& item) {
        uint64_t cas = 0;
        return engine->store(cookie, &item, cas, OPERATION_SET);
    }

    /**
     * Write documents to the bucket until they fail with TMP_FAIL.
     * Note this stores via external API (epstore) so we trigger the
     * memoryCondition() code in the event of ENGINE_ENOMEM.
     */
    void populateUntilTmpFail() {
        const std::string value(512, 'x'); // 512B value to use for documents.
        ENGINE_ERROR_CODE result = ENGINE_SUCCESS;
        uint16_t vbid = 0;

        while (result == ENGINE_SUCCESS) {
            noOfDocs++;
            //
            if (noOfDocs % int(500 / noOfVBs) == 0) {
                vbid++;
                if (vbid == noOfVBs) {
                    vbid = 0;
                }
            }
            auto key = makeStoredDocKey("DOC_" + std::to_string(noOfDocs));
            auto item = make_item(vbid, key, value, 0);
            result = storeItem(item);
            vbucketLookup.insert(std::pair<size_t, uint16_t>(noOfDocs, vbid));
        }

        EXPECT_EQ(ENGINE_TMPFAIL, result);
        // Fixup noOfDocs for last loop iteration.
        --noOfDocs;

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
                uint16_t vbucket = 0;
                auto it = vbucketLookup.find(kk);
                if (it != vbucketLookup.end()) {
                    vbucket = it->second;
                }
                get_options_t options = static_cast<get_options_t>(
                        QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE |
                        DELETE_TEMP | HIDE_LOCKED_CAS | TRACK_STATISTICS);
                item* itm = nullptr;
                engine->get(cookie, &itm, key, vbucket, options);
                delete reinterpret_cast<Item*>(itm);
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
            while ((current > lower) && vbucketcount < noOfVBs) {
                runNextTask(lpNonioQ);
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
            auto result = ENGINE_KEY_ENOENT;
            key_stats kstats;
            kstats.resident = false;

            uint16_t vbucket = 0;
            auto it = vbucketLookup.find(kk);
            if (it != vbucketLookup.end()) {
                vbucket = it->second;
            }

            result = store->getKeyStats(makeStoredDocKey(key),
                                        vbucket,
                                        cookie,
                                        kstats,
                                        WantsDeleted::No);

            if (result == ENGINE_SUCCESS && kstats.resident) {
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
            auto vb = engine->getVBucket(ii);
            const auto numResidentItems =
                    vb->getNumItems() - vb->getNumNonResidentItems();
            std::cout << "number of resident docs for vb " << ii << " = "
                      << numResidentItems << std::endl;
        }
    }

    const static uint32_t maxNoOfDocs = 300000;
    const static uint16_t noOfVBs = 4;
    uint32_t noOfAccesses = 0;
    double zipfScew = 0;
    uint32_t noOfDocs = 0;
    // map of document number to vbucket
    std::map<uint32_t, uint16_t> vbucketLookup;
    std::array<uint64_t, maxNoOfDocs> frequencies{};
};

#if defined(HAVE_JEMALLOC)

// Test is DISABLED so that it does not get run as part of automated testing.
// To run pass --gtest_also_run_disabled_tests to Gtest.
TEST_P(STHashTableEvictionTest, DISABLED_STHashTableEvictionItemPagerTest) {
    populateUntilTmpFail();
    printNoOfResidentDocs();
    accessPattern();
    eviction();
    auto evictedCount = residentOrEvicted();
    std::cout << "evictedCount = " << evictedCount << std::endl;
    printNoOfResidentDocs();

    auto& stats = engine->getEpStats();
    EXPECT_LT(stats.getEstimatedTotalMemoryUsed(), stats.mem_low_wat.load())
            << "Expected to be below low watermark after running item pager";
}

static auto testConfigValues = ::testing::Values(
        std::make_tuple(/*max_size*/ std::to_string(200 * 1024 * 1024),
                        /*mem_low_wat*/ std::to_string(120 * 1024 * 1024),
                        /*mem_high_wat*/ std::to_string(160 * 1024 * 1024),
                        /*skew*/ 0.9,
                        /*noOfAccesses*/ 20000000));

INSTANTIATE_TEST_CASE_P(Eviction, STHashTableEvictionTest, testConfigValues, );

#endif
