/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

// Note: This *must* be included first to avoid issues on Windows with ambiguous
// symbols for close() et al.
#include <folly/portability/GTest.h>

#include "bucket_logger.h"
#include "collections/collection_persisted_stats.h"
#include "collections/manager.h"
#include "collections/vbucket_manifest_handles.h"
#include "couch-kvstore_basic_fileops.h"
#include "encryption_key_provider.h"
#include "ep_vb.h"
#include "failover-table.h"
#include "kvstore/couch-kvstore/couch-kvstore-config.h"
#include "kvstore/couch-kvstore/couch-kvstore.h"
#include "kvstore/kvstore_transaction_context.h"
#include "kvstore_test.h"
#include "programs/engine_testapp/mock_cookie.h"
#include "rollback_result.h"
#include "test_helpers.h"
#include "vbucket_bgfetch_item.h"
#include "vbucket_test.h"

#include <random>

class StdOutBucketLogger : public BucketLogger {
public:
    StdOutBucketLogger(const std::string& name) : BucketLogger(name) {
        set_level(spdlog::level::level_enum::trace);
    }

protected:
    void sink_it_(const spdlog::details::log_msg& msg) override {
        fmt::println("{}. {}", ++counter, msg.payload);
    }

    uint64_t counter = 0;
};

struct KVStoreRandomizedTestErrors {
    bool open = false;
    bool read = false;
    bool write = false;
};

template <typename BasicJsonType>
static void to_json(BasicJsonType& j,
                    const KVStoreRandomizedTestErrors& errors) {
    j["open"] = errors.open;
    j["read"] = errors.read;
    j["write"] = errors.write;
}

class KVStoreRandomizedTest
    : public KVStoreTest,
      public ::testing::WithParamInterface<
              std::tuple<KVStoreTestEncryption, KVStoreTestBuffering>> {
public:
    using RandomNumberGenerator = std::mt19937;
    using State = std::unordered_map<std::string, queued_item>;

    static constexpr unsigned int MaxItems = 300;

    KVStoreRandomizedTest(const std::string& loggerName)
        : rngSeed(std::chrono::system_clock::now().time_since_epoch().count()),
          rng(gsl::narrow_cast<RandomNumberGenerator::result_type>(rngSeed)),
          logger(loggerName),
          defaultCreateItemCallback(
                  KVStoreIface::getDefaultCreateItemCallback()) {
        if (isEncrypted()) {
            auto keys = nlohmann::json::parse(R"({
    "keys": [
        {
            "id": "MyActiveKey",
            "cipher": "AES-256-GCM",
            "key": "cXOdH9oGE834Y2rWA+FSdXXi5CN3mLJ+Z+C0VpWbOdA="
        },
        {
            "id": "MyOtherKey",
            "cipher": "AES-256-GCM",
            "key": "hDYX36zHrP0/eApT7Gf3g2sQ9L5gubHSDeLQxg4v4kM="
        }
    ],
    "active": "MyActiveKey"
})");
            encryptionKeyProvider.setKeys(keys);
        }
    }

    void SetUp() override {
        KVStoreTest::SetUp();
    }

    void TearDown() override {
        fmt::print("\nRNG seed: {}\n", rngSeed);
        KVStoreTest::TearDown();
    }

    virtual void allowErrors(KVStoreRandomizedTestErrors) = 0;

    virtual KVStoreRandomizedTestErrors getErrorsOccurred() = 0;

    static bool isEncrypted() {
        return std::get<0>(GetParam()) == KVStoreTestEncryption::Encrypted;
    }

    VBucketPtr makeVBucket();

    std::vector<queued_item> generate(size_t count);

    static vb_bgfetch_queue_t make_bgfetch_queue(
            const std::vector<queued_item>& items);

    void checkGetMulti(bool committed,
                       bool allowReadErrors,
                       const State& previous,
                       const std::vector<queued_item>& mutations);

    void runTest(bool compact, std::chrono::steady_clock::duration duration);

    uint64_t rngSeed;
    RandomNumberGenerator rng;
    StdOutBucketLogger logger;
    KVStoreIface::CreateItemCB defaultCreateItemCallback;
    EncryptionKeyProvider encryptionKeyProvider;
    std::unique_ptr<KVStoreIface> kvstore;

private:
    Configuration configuration;
    EPStats globalStats;
    uint64_t seqno = 0;
};

VBucketPtr KVStoreRandomizedTest::makeVBucket() {
    return std::make_unique<EPVBucket>(
            vbid,
            vbucket_state_active,
            globalStats,
            *std::make_unique<CheckpointConfig>(configuration),
            /*kvshard*/ nullptr,
            /*lastSeqno*/ seqno,
            /*lastSnapStart*/ seqno,
            /*lastSnapEnd*/ seqno,
            /*table*/ nullptr,
            std::make_shared<DummyCB>(),
            [](Vbid) {},
            NoopSyncWriteCompleteCb,
            NoopSyncWriteTimeoutFactory,
            NoopSeqnoAckCb,
            configuration,
            EvictionPolicy::Value,
            std::make_unique<Collections::VB::Manifest>(
                    std::make_shared<Collections::Manager>()),
            CreateVbucketMethod::SetVbucket);
}

std::vector<queued_item> KVStoreRandomizedTest::generate(size_t count) {
    std::vector<queued_item> items;
    items.reserve(count);
    std::unordered_set<uint32_t> keys;
    while (keys.size() < count) {
        keys.insert(rng() % MaxItems);
    }
    for (auto key : keys) {
        ++seqno;
        auto value = "value" + std::to_string(seqno);
        uint32_t len = 4;
        for (auto rv = rng(); (rv & 1) && len < 5000; rv >>= 1) {
            len *= 2;
        }
        for (len = rng() % len; len--;) {
            value.push_back('A' + (rng() & 31));
        }
        auto qi = makeCommittedItem(
                makeStoredDocKey("key" + std::to_string(key)), value);
        qi->setBySeqno(seqno);
        items.push_back(std::move(qi));
    }
    return items;
}

vb_bgfetch_queue_t KVStoreRandomizedTest::make_bgfetch_queue(
        const std::vector<queued_item>& items) {
    vb_bgfetch_queue_t queue;
    for (const auto& item : items) {
        vb_bgfetch_item_ctx_t ctx;
        ctx.addBgFetch(std::make_unique<FrontEndBGFetchItem>(
                nullptr, ValueFilter::VALUES_DECOMPRESSED, 0));
        queue[DiskDocKey{*item}] = std::move(ctx);
    }
    return queue;
}

void KVStoreRandomizedTest::checkGetMulti(
        bool committed,
        bool allowReadErrors,
        const State& previous,
        const std::vector<queued_item>& mutations) {
    allowErrors({allowReadErrors, allowReadErrors, false});
    auto bgfetch = make_bgfetch_queue(mutations);
    kvstore->getMulti(vbid, bgfetch, defaultCreateItemCallback);
    auto errors = getErrorsOccurred();
    for (const auto& item : mutations) {
        auto fetched = bgfetch.find(DiskDocKey(item->getDocKey()));
        ASSERT_NE(bgfetch.end(), fetched);
        auto prev = previous.find(item->getDocKey().to_string());
        auto status = fetched->second.value.getStatus();
        if ((errors.open || errors.read) &&
            status == cb::engine_errc::temporary_failure) {
            continue;
        }
        if (!committed && prev != previous.end()) {
            ASSERT_EQ(cb::engine_errc::success, status)
                    << "not found previously persisted "
                    << std::string_view(fetched->first);
            ASSERT_EQ(prev->second->getValue()->to_string_view(),
                      fetched->second.value.item->getValue()->to_string_view())
                    << std::string_view(fetched->first);
        } else {
            auto expectedStatus = committed ? cb::engine_errc::success
                                            : cb::engine_errc::no_such_key;
            ASSERT_EQ(expectedStatus, fetched->second.value.getStatus())
                    << std::string_view(fetched->first);
            if (committed) {
                ASSERT_EQ(item->getValue()->to_string_view(),
                          fetched->second.value.item->getValue()
                                  ->to_string_view())
                        << std::string_view(fetched->first);
            }
        }
    }
}

void KVStoreRandomizedTest::runTest(
        bool compact, std::chrono::steady_clock::duration duration) {
    const auto deadline = std::chrono::steady_clock::now() + duration;
    State allItems;
    uint64_t iteration = 0;
    bool waitForCompaction = compact;
    kvstore->prepareToCreate(vbid);
    do {
        logger.debugWithContext("Running test",
                                {{"iteration", ++iteration},
                                 {"all_items", allItems.size()},
                                 {"compact", compact},
                                 {"rng_seed", rngSeed}});
        if (allItems.empty()) {
            allowErrors({false, false, false});
        } else {
            allowErrors({true, true, true});
        }
        auto mutations = generate(rng() % 30 + 1);
        bool committed;
        {
            auto ctx = kvstore->begin(vbid,
                                      std::make_unique<PersistenceCallback>());
            for (const auto& item : mutations) {
                kvstore->set(*ctx, item);
            }
            VB::Commit vbCommit{manifest};
            if (!mutations.empty()) {
                vbCommit.proposedVBState.lastSnapEnd =
                        mutations.back()->getBySeqno();
            }
            committed = kvstore->commit(std::move(ctx), vbCommit);
        }
        auto errors = getErrorsOccurred();
        EXPECT_EQ(errors.open || errors.read || errors.write, !committed);
        checkGetMulti(committed, false, allItems, mutations);
        if (::testing::Test::HasFailure()) {
            return;
        }
        checkGetMulti(committed, true, allItems, mutations);
        if (::testing::Test::HasFailure()) {
            return;
        }
        allowErrors({false, false, false});
        if (committed) {
            for (const auto& item : mutations) {
                allItems[item->getDocKey().to_string()] = item;
            }
        }
        ASSERT_EQ(allItems.size(), kvstore->getItemCount(vbid));
        if (compact) {
            auto info = kvstore->getDbFileInfo(vbid);
            if (info.spaceUsed > 10000 && info.fileSize > info.spaceUsed * 2) {
                waitForCompaction = false;
                CompactionConfig config;
                config.purge_before_seq = 0;
                config.purge_before_ts = 0;
                config.drop_deletes = false;
                auto vb = makeVBucket();
                auto cctx = std::make_shared<CompactionContext>(vb, config, 0);
                auto lock = getVbLock();
                allowErrors({true, true, true});
                auto compactStatus = kvstore->compactDB(lock, std::move(cctx));
                errors = getErrorsOccurred();
                auto expectedStatus =
                        (errors.open || errors.read || errors.write)
                                ? CompactDBStatus::Failed
                                : CompactDBStatus::Success;
                ASSERT_EQ(expectedStatus, compactStatus);

                allowErrors({false, false, false});
                std::vector<queued_item> items;
                items.reserve(allItems.size());
                for (const auto& [key, value] : allItems) {
                    items.push_back(value);
                }
                auto bgfetch = make_bgfetch_queue(items);
                kvstore->getMulti(vbid, bgfetch, defaultCreateItemCallback);
                for (const auto& item : items) {
                    auto fetched = bgfetch.find(DiskDocKey(item->getDocKey()));
                    ASSERT_NE(bgfetch.end(), fetched);
                    ASSERT_EQ(cb::engine_errc::success,
                              fetched->second.value.getStatus())
                            << std::string_view(fetched->first);
                }
            }
        }
    } while (waitForCompaction || std::chrono::steady_clock::now() < deadline);
}

class CouchFuzzFileOps : public CouchBasicFileOps {
public:
    CouchFuzzFileOps(BucketLogger& logger, std::mt19937& rng)
        : logger(logger), rng(rng) {
    }

    couchstore_error_t open(couchstore_error_info_t* errinfo,
                            couch_file_handle* handle,
                            const char* path,
                            int oflag) override {
        if (allowedErrors.open && (rng() & 127) == 0) {
            logger.debug("CouchFuzzFileOps::open error");
            errinfo->error = EIO;
            occurredErrors.open = true;
            return COUCHSTORE_ERROR_OPEN_FILE;
        }
        return CouchBasicFileOps::open(errinfo, handle, path, oflag);
    }

    ssize_t pread(couchstore_error_info_t* errinfo,
                  couch_file_handle handle,
                  void* buf,
                  size_t nbytes,
                  cs_off_t offset) override {
        if (allowedErrors.read) {
            auto rv = rng();
            if ((rv & 255) == 0) {
                logger.debug("CouchFuzzFileOps::pread error");
                errinfo->error = EIO;
                occurredErrors.read = true;
                return COUCHSTORE_ERROR_READ;
            }
            rv >>= 8;
            if (nbytes && (rv & 3) == 0) {
                rv >>= 2;
                nbytes = (rv % nbytes) + 1;
            }
        }
        return CouchBasicFileOps::pread(errinfo, handle, buf, nbytes, offset);
    }

    ssize_t pwrite(couchstore_error_info_t* errinfo,
                   couch_file_handle handle,
                   const void* buf,
                   size_t nbytes,
                   cs_off_t offset) override {
        if (allowedErrors.write) {
            auto rv = rng();
            if ((rv & 255) == 0) {
                logger.debug("CouchFuzzFileOps::pwrite error");
                errinfo->error = EIO;
                occurredErrors.write = true;
                return COUCHSTORE_ERROR_WRITE;
            }
            rv >>= 8;
            if (nbytes && (rv & 3) == 0) {
                rv >>= 2;
                nbytes = (rv % nbytes) + 1;
            }
        }
        return CouchBasicFileOps::pwrite(errinfo, handle, buf, nbytes, offset);
    }

    couchstore_error_t sync(couchstore_error_info_t* errinfo,
                            couch_file_handle handle) override {
        return COUCHSTORE_SUCCESS;
    }

    KVStoreRandomizedTestErrors allowedErrors;
    KVStoreRandomizedTestErrors occurredErrors;

private:
    BucketLogger& logger;
    std::mt19937& rng;
};

class CouchKVStoreRandomizedTest : public KVStoreRandomizedTest {
public:
    CouchKVStoreRandomizedTest()
        : KVStoreRandomizedTest("CouchKVStoreRandomizedTest"),
          ops(logger, rng),
          config(1024, 4, data_dir, "couchdb", 0) {
        config.setLogger(logger);
        config.setBuffered(std::get<1>(GetParam()) ==
                           KVStoreTestBuffering::Buffered);
    }

    void SetUp() override {
        KVStoreRandomizedTest::SetUp();
        kvstore = std::make_unique<CouchKVStore>(
                config, ops, &encryptionKeyProvider);
    }

    void TearDown() override {
        kvstore.reset();
        KVStoreRandomizedTest::TearDown();
    }

    void allowErrors(KVStoreRandomizedTestErrors errors) override {
        ops.allowedErrors = errors;
        ops.occurredErrors = {};
    }

    KVStoreRandomizedTestErrors getErrorsOccurred() override {
        return ops.occurredErrors;
    }

    CouchFuzzFileOps ops;
    CouchKVStoreConfig config;
};

TEST_P(CouchKVStoreRandomizedTest, FlushGetMulti) {
    runTest(false, std::chrono::seconds(2));
}

TEST_P(CouchKVStoreRandomizedTest, FlushGetMultiCompact) {
    runTest(true, std::chrono::seconds(2));
}

INSTANTIATE_TEST_SUITE_P(
        CouchKVStoreRandomizedTest,
        CouchKVStoreRandomizedTest,
        ::testing::Combine(::testing::Values(KVStoreTestEncryption::Unencrypted,
                                             KVStoreTestEncryption::Encrypted),
                           ::testing::Values(KVStoreTestBuffering::Unbuffered,
                                             KVStoreTestBuffering::Buffered)),
        [](const auto& testInfo) {
            return fmt::format("{}_{}",
                               std::get<0>(testInfo.param),
                               std::get<1>(testInfo.param));
        });
