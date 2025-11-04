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

/*
 * Unit tests for the KVBucket class.
 */

#include "kv_bucket_test.h"

#include "../mock/mock_dcp_producer.h"
#include "access_scanner.h"
#include "bgfetcher.h"
#include "checkpoint.h"
#include "checkpoint_manager.h"
#include "checkpoint_remover.h"
#include "collections/collection_persisted_stats.h"
#include "collections/vbucket_manifest_handles.h"
#include "dcp/dcpconnmap.h"
#include "dcp/flow-control-manager.h"
#include "ep_bucket.h"
#include "ep_engine.h"
#include "ep_task.h"
#include "ep_time.h"
#include "ep_vb.h"
#include "ephemeral_bucket.h"
#include "ephemeral_mem_recovery.h"
#include "evp_store_single_threaded_test.h"
#include "failover-table.h"
#include "flusher.h"
#include "tests/ep_testsuite_common.h"
#ifdef EP_USE_MAGMA
#include "kvstore/magma-kvstore/magma-kvstore_config.h"
#endif
#include "item_access_visitor.h"
#include "item_compressor.h"
#include "item_pager.h"
#include "kvstore/couch-kvstore/couch-kvstore-config.h"
#include "kvstore/couch-kvstore/couch-kvstore.h"
#include "lambda_task.h"
#include "seqno_persistence_notify_task.h"
#include "tasks.h"
#include "tests/mock/mock_couch_kvstore.h"
#include "tests/mock/mock_global_task.h"
#include "tests/mock/mock_synchronous_ep_engine.h"
#include "tests/module_tests/test_helpers.h"
#include "thread_gate.h"
#include "vbucketdeletiontask.h"
#include "warmup.h"
#include <executor/fake_executorpool.h>
#include <executor/globaltask.h>

#ifdef EP_USE_MAGMA
#include "../mock/mock_magma_filesystem.h"
#include "../mock/mock_magma_kvstore.h"
#endif

#include <folly/Random.h>
#include <folly/portability/GMock.h>
#include <mcbp/protocol/framebuilder.h>
#include <platform/cb_time.h>
#include <platform/compress.h>
#include <platform/dirutils.h>
#include <programs/engine_testapp/mock_cookie.h>
#include <programs/engine_testapp/mock_server.h>
#include <xattr/blob.h>
#include <xattr/utils.h>

#include <chrono>
#include <thread>

KVBucketTest::KVBucketTest() : test_dbname(getProcessUniqueDatabaseName()) {
}

void KVBucketTest::SetUp() {
    // Paranoia - kill any existing files in case they are left over
    // from a previous run.
    std::filesystem::remove_all(test_dbname);

    if (!ExecutorPool::exists()) {
        ExecutorPool::create();
    }
    initialise(config_string);
    Expects(ObjectRegistry::getCurrentEngine() &&
            "After initialise() the calling thread should be associated with "
            "the newly-created engine");

    if (completeWarmup && engine->getKVBucket()->getPrimaryWarmup()) {
        engine->getKVBucket()->getPrimaryWarmup()->setFinishedLoading();
        engine->getKVBucket()->getPrimaryWarmup()->notifyWaitingCookies(
                cb::engine_errc::success);
    }
}

void KVBucketTest::initialise(std::string_view baseConfig,
                              nlohmann::json encryptionKeys) {
    {
        // Build up the full config string needed to create the engine.
        // Config defined in it's own scope to ensure correct memory accounting
        // - we want to destroy (deallocate) it against the no-bucket context.
        auto config = std::string{baseConfig};

        // Add dbname to config string.
        if (!config.empty()) {
            config += ";";
        }
        config += "dbname=" + test_dbname;

        // Initialize mock server settings to default values.
        init_mock_server();
        // Need to initialize ep_real_time and friends.
        initialize_time_functions(get_mock_server_api()->core);

        if (config.find("backend=magma") != std::string::npos) {
            config += ";" + magmaConfig;
        }

        // unless otherwise specified in the config, default to disabling
        // the expiry pager. Tests which do not cover the expiry pager often
        // make expectations about the executor futurepool, and don't expect the
        // expiry pager to be present.
        if (config.find("exp_pager_enabled") == std::string::npos) {
            config += ";exp_pager_enabled=false";
        }

        // Unless specified, default to 1 BGFetcher.
        if (config.find("max_num_bgfetchers") == std::string::npos) {
            config += ";max_num_bgfetchers=1";
        }

        // Unless specified, default to disabling the ephemeral memory recovery.
        if (config.find("ephemeral_mem_recovery_enabled") ==
            std::string::npos) {
            config += ";ephemeral_mem_recovery_enabled=false";
        }

        engine = SynchronousEPEngine::build(config, std::move(encryptionKeys));
        Expects(ObjectRegistry::getCurrentEngine() &&
                "Expect current thread is associated with 'engine' after "
                "build()ing it so any subsequent allocations below are "
                "accounted to 'engine' correctly.");

        // Switch back to no-engine before scope ends, so config is destroyed
        // against no-bucket.
        ObjectRegistry::onSwitchThread(nullptr);
    }

    // Switch current engine back to the one just created, so additional
    // setup below correctly accounts any further allocations.
    ObjectRegistry::onSwitchThread(engine.get());

    store = engine->getKVBucket();

    auto& epConfig = engine->getConfiguration();
    const auto numChkTasks = epConfig.getCheckpointRemoverTaskCount();
    for (size_t id = 0; id < numChkTasks; ++id) {
        auto task = std::make_shared<CheckpointMemRecoveryTask>(
                *engine, engine->getEpStats(), id);
        store->chkRemovers.emplace_back(task);
    }

    store->initializeExpiryPager(epConfig);

    auto numCkptDestroyers =
            engine->getConfiguration().getCheckpointDestructionTasks();
    auto locked = store->ckptDestroyerTasks.wlock();
    for (size_t i = 0; i < numCkptDestroyers; ++i) {
        locked->push_back(
                std::make_shared<CheckpointDestroyerTask>(*engine));
    }

    if (epConfig.isEphemeralMemRecoveryEnabled()) {
        ASSERT_EQ(epConfig.getBucketTypeString(), "ephemeral");
        auto* ephemeralStore = dynamic_cast<EphemeralBucket*>(store);
        ephemeralStore->ephemeralMemRecoveryTask =
                std::make_shared<EphemeralMemRecovery>(
                        *engine,
                        *ephemeralStore,
                        std::chrono::milliseconds(
                                epConfig.getEphemeralMemRecoverySleepTime()));
    }

    // Note cookies are owned by the frontend (not any specific engine) so
    // should not be allocated in the engine's context.
    {
        NonBucketAllocationGuard guard;
        cookie = create_mock_cookie(engine.get());
    }
}

void KVBucketTest::TearDown() {
    destroy();
    // Shutdown the ExecutorPool singleton (initialized when we create
    // an EPBucket object). Must happen after engine
    // has been destroyed (to allow the tasks the engine has
    // registered a chance to be unregistered).
    ExecutorPool::shutdown();
    // Cleanup any files we created.
    std::filesystem::remove_all(test_dbname);
}

void KVBucketTest::destroy(bool force) {
    {
        NonBucketAllocationGuard guard;
        destroy_mock_cookie(cookie);
    }
    engine->getDcpConnMap().manageConnections();
    engine.get_deleter().force = force;
    engine.reset();
}

void KVBucketTest::reinitialise(std::string config,
                                bool force,
                                nlohmann::json encryptionKeys) {
    destroy(force);
    initialise(config, std::move(encryptionKeys));
}

Item KVBucketTest::store_item(Vbid vbid,
                              const DocKeyView& key,
                              const std::string& value,
                              uint32_t exptime,
                              const std::vector<cb::engine_errc>& expected,
                              protocol_binary_datatype_t datatype,
                              std::optional<cb::durability::Requirements> reqs,
                              bool deleted) {
    auto item = make_item(vbid, key, value, exptime, datatype);
    if (reqs) {
        item.setPendingSyncWrite(*reqs);
    }
    if (deleted) {
        item.setDeleted(DeleteSource::Explicit);
    }
    auto returnCode = store->set(item, cookie);
    // Doing the EXPECT this way as it is a less noisy when many operations fail
    auto expectedCount = std::ranges::count(expected, returnCode);
    EXPECT_NE(0, expectedCount)
            << "unexpected error:" << cb::to_string(returnCode)
            << " for key:" << key.to_string();
    return item;
}

Item KVBucketTest::store_pending_item(
        Vbid vbid,
        const DocKeyView& key,
        const std::string& value,
        uint32_t exptime,
        const std::vector<cb::engine_errc>& expected,
        protocol_binary_datatype_t datatype,
        std::optional<cb::durability::Requirements> reqs,
        bool deleted) {
    return store_item(
            vbid, key, value, exptime, expected, datatype, reqs, deleted);
}

Item KVBucketTest::store_deleted_item(
        Vbid vbid,
        const DocKeyView& key,
        const std::string& value,
        uint32_t exptime,
        const std::vector<cb::engine_errc>& expected,
        protocol_binary_datatype_t datatype,
        std::optional<cb::durability::Requirements> reqs) {
    return store_item(vbid,
                      key,
                      value,
                      exptime,
                      expected,
                      value.empty() ? PROTOCOL_BINARY_RAW_BYTES : datatype,
                      reqs,
                      true);
}

::testing::AssertionResult KVBucketTest::store_items(
        int nitems,
        Vbid vbid,
        const DocKeyView& key,
        const std::string& value,
        uint32_t exptime,
        protocol_binary_datatype_t datatype) {
    for (int ii = 0; ii < nitems; ii++) {
        auto keyii = makeStoredDocKey(
                std::string(reinterpret_cast<const char*>(
                                    key.makeDocKeyWithoutCollectionID().data()),
                            key.makeDocKeyWithoutCollectionID().size()) +
                        std::to_string(ii),
                key.getCollectionID());
        auto item = make_item(vbid, keyii, value, exptime, datatype);
        auto err = store->set(item, cookie);
        if (cb::engine_errc::success != err) {
            return ::testing::AssertionFailure()
                   << "Failed to store key:'" << keyii.to_string()
                   << "' error:" << err;
        }
    }
    return ::testing::AssertionSuccess();
}

::testing::AssertionResult KVBucketTest::store_item_replica(
        Vbid vbid,
        const DocKeyView& key,
        const std::string& value,
        uint64_t seqno,
        uint32_t exptime,
        const cb::engine_errc expected,
        protocol_binary_datatype_t datatype,
        std::optional<cb::durability::Requirements> reqs,
        bool deleted) {
    auto item = make_item(vbid, key, value, exptime, datatype);
    if (reqs) {
        item.setPendingSyncWrite(*reqs);
    }
    if (deleted) {
        item.setDeleted(DeleteSource::Explicit);
    }
    item.setBySeqno(seqno);
    item.setCas();

    auto returnCode = store->setWithMeta(item,
                                         0,
                                         nullptr,
                                         cookie,
                                         {vbucket_state_replica},
                                         CheckConflicts::No,
                                         true,
                                         GenerateBySeqno::Yes,
                                         GenerateCas::No,
                                         nullptr);
    if (returnCode != expected) {
        return ::testing::AssertionFailure()
               << "store_item_replica() unexpected error:"
               << cb::to_string(returnCode) << " for key:" << key.to_string();
    }
    return ::testing::AssertionSuccess();
}

void KVBucketTest::storeItems(CollectionID collection,
                              int items,
                              cb::engine_errc expected,
                              size_t valueSize) {
    std::string value(valueSize, 0);
    for (auto& element : value) {
        element = gsl::narrow_cast<char>(folly::Random::rand32());
    }
    for (int ii = 0; ii < items; ii++) {
        std::string key = "key" + std::to_string(ii);
        store_item(vbid, StoredDocKey{key, collection}, value, 0, {expected});
    }
}

void KVBucketTest::flush_vbucket_to_disk(Vbid vbid, size_t expected) {
    size_t actualFlushed = flushVBucket(vbid);

    ASSERT_EQ(expected, actualFlushed)
            << "Unexpected items (" << actualFlushed
            << ") in flush_vbucket_to_disk(" << vbid << ", " << expected << ")";
}

int KVBucketTest::flushVBucket(Vbid vbid) {
    int actualFlushed = 0;
    const auto time_limit = std::chrono::seconds(10);
    const auto deadline = cb::time::steady_clock::now() + time_limit;

    // Need to retry as warmup may not have completed, or if the flush is
    // in multiple parts.
    bool flush_successful = false;
    using MoreAvailable = EPBucket::MoreAvailable;
    MoreAvailable moreAvailable;
    do {
        const auto res = dynamic_cast<EPBucket&>(*store).flushVBucket(vbid);
        moreAvailable = res.moreAvailable;
        actualFlushed += res.numFlushed;
        if (moreAvailable == MoreAvailable::No) {
            flush_successful = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    } while ((cb::time::steady_clock::now() < deadline) &&
             moreAvailable == MoreAvailable::Yes);

    EXPECT_TRUE(flush_successful)
            << "Hit timeout (" << time_limit.count()
            << " seconds) waiting for "
               "warmup to complete while flushing VBucket.";

    return actualFlushed;
}

bool KVBucketTest::persistent() const {
    return engine->getConfiguration().getBucketTypeString() == "persistent";
}

void KVBucketTest::flushVBucketToDiskIfPersistent(Vbid vbid, int expected) {
    if (persistent()) {
        flush_vbucket_to_disk(vbid, expected);
    }
}

void KVBucketTest::removeCheckpoint(VBucket& vb) {
    const auto& stats = engine->getEpStats();
    const auto pre = stats.itemsRemovedFromCheckpoints;
    auto& manager = *vb.checkpointManager;
    manager.createNewCheckpoint();
    if (persistent()) {
        flushVBucket(vb.getId());
    }
    EXPECT_GT(stats.itemsRemovedFromCheckpoints, pre);
}

void KVBucketTest::flushAndRemoveCheckpoints(Vbid vbid) {
    auto& manager = *store->getVBucket(vbid)->checkpointManager;
    manager.createNewCheckpoint();
    if (persistent()) {
        dynamic_cast<EPBucket&>(*store).flushVBucket(vbid);
    }
}

size_t KVBucketTest::flushAndExpelFromCheckpoints(Vbid vbid) {
    if (persistent()) {
        dynamic_cast<EPBucket&>(*store).flushVBucket(vbid);
    }
    auto& vb = *store->getVBucket(vbid);
    return vb.checkpointManager->expelUnreferencedCheckpointItems().count;
}

void KVBucketTest::delete_item(Vbid vbid,
                               const DocKeyView& key,
                               cb::engine_errc expected) {
    uint64_t cas = 0;
    mutation_descr_t mutation_descr;
    EXPECT_EQ(expected,
              store->deleteItem(key,
                                cas,
                                vbid,
                                cookie,
                                {},
                                /*itemMeta*/ nullptr,
                                mutation_descr));
}

void KVBucketTest::evict_key(Vbid vbid, const DocKeyView& key) {
    const char* msg;
    EXPECT_EQ(cb::engine_errc::success, store->evictKey(key, vbid, &msg));
    EXPECT_STREQ("Ejected.", msg);
}

GetValue KVBucketTest::getInternal(const DocKeyView& key,
                                   Vbid vbucket,
                                   CookieIface* cookie,
                                   const ForGetReplicaOp getReplicaItem,
                                   get_options_t options) {
    return store->getInternal(key, vbucket, cookie, getReplicaItem, options);
}

void KVBucketTest::scheduleItemPager() {
    ExecutorPool::get()->schedule(store->itemPagerTask);
}

void KVBucketTest::scheduleEphemeralMemRecovery() {
    ASSERT_EQ(engine->getConfiguration().getBucketTypeString(), "ephemeral");
    auto* bucket = dynamic_cast<EphemeralBucket*>(store);
    ExecutorPool::get()->schedule(*bucket->ephemeralMemRecoveryTask.rlock());
}

void KVBucketTest::initializeExpiryPager() {
    store->initializeExpiryPager(engine->getConfiguration());
}

void KVBucketTest::initializeInitialMfuUpdater() {
    store->initializeInitialMfuUpdater(engine->getConfiguration());
}

void KVBucketTest::scheduleCheckpointRemoverTask() {
    for (auto& task : store->chkRemovers) {
        ExecutorPool::get()->schedule(task);
    }
}

void KVBucketTest::scheduleCheckpointDestroyerTasks() {
    const auto locked = store->ckptDestroyerTasks.rlock();
    for (const auto& task : *locked) {
        ExecutorPool::get()->schedule(task);
    }
}

const KVBucket::CheckpointDestroyers&
KVBucketTest::getCheckpointDestroyerTasks() const {
    return store->ckptDestroyerTasks;
}

void KVBucketTest::runBGFetcherTask() {
    MockGlobalTask mockTask(engine->getTaskable(),
                            TaskId::MultiBGFetcherTask);
    auto vb = store->getVBucket(vbid);
    ASSERT_TRUE(vb);
    auto* epVb = dynamic_cast<EPVBucket*>(vb.get());
    epVb->getBgFetcher(/* distributionKey */ 0).run(&mockTask);
}

/**
 * Create a del_with_meta packet with the key/body (body can be empty)
 */
std::vector<char> KVBucketTest::buildWithMetaPacket(
        cb::mcbp::ClientOpcode opcode,
        protocol_binary_datatype_t datatype,
        Vbid vbucket,
        uint32_t opaque,
        uint64_t cas,
        ItemMetaData metaData,
        std::string_view key,
        std::string_view body,
        const std::vector<char>& emd,
        int options) {
    EXPECT_EQ(sizeof(cb::mcbp::request::SetWithMetaPayload),
              sizeof(cb::mcbp::request::DelWithMetaPayload));

    // When using the engine interface directly by calling unknown_command
    // the packet validators have already been called (and verified the
    // content of the framing extras). None of the current engine functions
    // currently tries to inspect the framing extras, so lets's just
    // inject a blob to move the offsets around and verify that the
    // current unit tests still pass.
    std::vector<uint8_t> frame_extras(10);
    std::vector<uint8_t> extras_backing(
            sizeof(cb::mcbp::request::SetWithMetaPayload));
    auto* extdata = reinterpret_cast<cb::mcbp::request::SetWithMetaPayload*>(
            extras_backing.data());
    extdata->setFlagsInNetworkByteOrder(metaData.flags);
    extdata->setExpiration(gsl::narrow<uint32_t>(metaData.exptime));
    extdata->setSeqno(metaData.revSeqno);
    extdata->setCas(metaData.cas);
    cb::byte_buffer extras{extras_backing.data(), extras_backing.size()};

    if (options) {
        options = htonl(options);
        std::copy_n(reinterpret_cast<uint8_t*>(&options),
                    sizeof(uint32_t),
                    std::back_inserter(extras_backing));
        extras = {extras_backing.data(), extras_backing.size()};
    }

    if (!emd.empty()) {
        EXPECT_TRUE(emd.size() < std::numeric_limits<uint16_t>::max());
        uint16_t emdSize = htons(emd.size());
        std::copy_n(reinterpret_cast<uint8_t*>(&emdSize),
                    sizeof(uint16_t),
                    std::back_inserter(extras_backing));
        extras = {extras_backing.data(), extras_backing.size()};
    }

    std::vector<char> packet(sizeof(cb::mcbp::Request) + frame_extras.size() +
                             extras.size() + key.size() + body.size() +
                             emd.size());
    cb::mcbp::RequestBuilder builder(
            {reinterpret_cast<uint8_t*>(packet.data()), packet.size()});

    builder.setMagic(cb::mcbp::Magic::AltClientRequest);
    builder.setOpcode(opcode);
    builder.setDatatype(datatype);
    builder.setVBucket(vbucket);
    builder.setOpaque(opaque);
    builder.setCas(cas);
    builder.setFramingExtras({frame_extras.data(), frame_extras.size()});
    builder.setExtras(extras);
    builder.setKey({reinterpret_cast<const uint8_t*>(key.data()), key.size()});

    if (emd.empty()) {
        builder.setValue(
                {reinterpret_cast<const uint8_t*>(body.data()), body.size()});
    } else {
        std::vector<uint8_t> buffer;
        std::ranges::copy(body, std::back_inserter(buffer));
        std::ranges::copy(emd, std::back_inserter(buffer));
        builder.setValue({buffer.data(), buffer.size()});
    }

    return packet;
}

bool KVBucketTest::addResponse(std::string_view key,
                               std::string_view extras,
                               std::string_view body,
                               ValueIsJson json,
                               cb::mcbp::Status status,
                               uint64_t pcas,
                               CookieIface& cookie) {
    addResponseStatus = status;
    return true;
}

cb::mcbp::Status KVBucketTest::getAddResponseStatus(cb::mcbp::Status newval) {
    auto rv = addResponseStatus;
    addResponseStatus = newval;
    return rv;
}

cb::mcbp::Status KVBucketTest::addResponseStatus = cb::mcbp::Status::Success;

void KVBucketTest::setRandomFunction(std::function<long()>& randFunction) {
    store->getRandom = randFunction;
}

Collections::Manager& KVBucketTest::getCollectionsManager() {
    return *store->collectionsManager;
}

/**
 * Replace the rw KVStore with one that uses the given ops. If a nullptr is
 * passed, revert the KVStore back to default ops. This function will test
 * the config to be sure the KVBucket is persistent/couchstore
 */
void KVBucketTest::replaceCouchKVStore(FileOpsInterface* ops) {
    ASSERT_EQ(engine->getConfiguration().getBucketTypeString(), "persistent");
    ASSERT_EQ(engine->getConfiguration().getBackendString(), "couchdb");

    const auto& config = store->getRWUnderlying(vbid)->getConfig();

    std::unique_ptr<MockCouchKVStore> rw;
    if (ops) {
        rw = std::make_unique<MockCouchKVStore>(
                dynamic_cast<const CouchKVStoreConfig&>(config), *ops);
    } else {
        rw = std::make_unique<MockCouchKVStore>(
                dynamic_cast<const CouchKVStoreConfig&>(config));
    }

    const auto shardId = store->getShardId(vbid);
    store->setRW(shardId, std::move(rw));
}

void KVBucketTest::replaceMagmaKVStore(MagmaKVStoreConfig& config,
                                       const MockMagmaFileSystem& mockFs) {
    EXPECT_EQ(engine->getConfiguration().getBucketTypeString(), "persistent");
    EXPECT_EQ(engine->getConfiguration().getBackendString(), "magma");
#ifdef EP_USE_MAGMA
    store->takeRW(0);
    auto rw = std::make_unique<MockMagmaKVStore>(config, mockFs);
    store->setRW(0, std::move(rw));
#endif
}

void KVBucketTest::replaceMagmaKVStore(const MockMagmaFileSystem& mockFs) {
#ifdef EP_USE_MAGMA
    // Get hold of the current Magma config so we can create a MockMagmaKVStore
    // with the same config
    const auto& config = store->getRWUnderlying(vbid)->getConfig();
    auto& nonConstConfig = const_cast<KVStoreConfig&>(config);
    replaceMagmaKVStore(dynamic_cast<MagmaKVStoreConfig&>(nonConstConfig),
                        mockFs);
#endif
}

void KVBucketTest::replaceMagmaKVStore(MagmaKVStoreConfig& config) {
#ifdef EP_USE_MAGMA
    replaceMagmaKVStore(config, {});
#endif
}

void KVBucketTest::replaceMagmaKVStore() {
#ifdef EP_USE_MAGMA
    replaceMagmaKVStore({});
#endif
}

void replaceMagmaKVStore();

void KVBucketTest::writeDocToReplica(Vbid vbid,
                                     StoredDocKey key,
                                     uint64_t seqno,
                                     bool prepare) {
    auto item = make_item(vbid, key, "value");
    item.setCas(1);
    item.setBySeqno(seqno);
    uint64_t seq = 0;

    auto vb = store->getVBucket(vbid);
    ASSERT_TRUE(vb);

    if (!prepare) {
        std::shared_lock rlh(vb->getStateLock());
        EXPECT_EQ(cb::engine_errc::success,
                  vb->setWithMeta(rlh,
                                  std::ref(item),
                                  0,
                                  &seq,
                                  cookie,
                                  *engine,
                                  CheckConflicts::No,
                                  /*allowExisting*/ true,
                                  GenerateBySeqno::No,
                                  GenerateCas::No,
                                  vb->lockCollections(key),
                                  EnforceMemCheck::Yes));
        return;
    }

    using namespace cb::durability;
    item.setPendingSyncWrite(
            Requirements{Level::Majority, Timeout::Infinity()});
    std::shared_lock rlh(vb->getStateLock());
    EXPECT_EQ(cb::engine_errc::success,
              vb->prepare(rlh,
                          std::ref(item),
                          0,
                          &seq,
                          cookie,
                          *engine,
                          CheckConflicts::No,
                          /*allowExisting*/ true,
                          GenerateBySeqno::No,
                          GenerateCas::No,
                          vb->lockCollections(key),
                          EnforceMemCheck::Yes));
}

std::unordered_map<CollectionID, Collections::VB::PersistedStats>
KVBucketTest::getCollectionStats(Vbid id,
                                 const std::vector<CollectionID>& cids) {
    std::unordered_map<CollectionID, Collections::VB::PersistedStats> rv;
    auto& kvs = *store->getRWUnderlying(id);
    auto handle = kvs.makeFileHandle(id);
    for (auto cid : cids) {
        auto stats = kvs.getCollectionStats(*handle, cid);
        if (stats.first == KVStore::GetCollectionStatsStatus::Success) {
            rv[cid] = stats.second;
        }
    }
    return rv;
}

KVBucket::CheckpointDestroyer KVBucketTest::getCkptDestroyerTask(
        Vbid vbid) const {
    return store->getCkptDestroyerTask(vbid);
}

std::shared_ptr<InitialMFUTask> KVBucketTest::getInitialMfuUpdaterTask() const {
    return store->initialMfuUpdaterTask;
}

void KVBucketTest::setProcessExpiredItemHook(std::function<void()> cb) {
    store->processExpiredItemHook = std::move(cb);
}

void KVBucketTest::setupPrimaryWarmupOnly() {
    config_string += "warmup_behavior=blocking;";
}

bool KVBucketTest::itemCompressorTaskIsSleepingForever() const {
    return store->itemCompressorTask->isSleepingForever();
}

void KVBucketTest::updateItemPagerSleepTime(
        const std::chrono::milliseconds interval) {
    ASSERT_FALSE(store->isCrossBucketHtQuotaSharing());
    auto strictQuotaItemPager = std::dynamic_pointer_cast<StrictQuotaItemPager>(
            store->itemPagerTask);
    strictQuotaItemPager->updateSleepTime(interval);
}

void KVBucketTest::setupEncryptionKeys() {
    auto dbname = std::filesystem::path(engine->getConfiguration().getDbname());
    std::filesystem::create_directories(dbname / "deks");

    auto jsonKeys = getEncryptionKeys();
    // Create what will look like valid DEK files by filename only.
    // The contents of these files is not actually used by KV, here we are just
    // mimicking what ns_server would of written out so that snapshot generation
    // has files to copy.
    for (const auto& key : jsonKeys["keys"]) {
        std::ofstream(dbname / "deks" /
                      key["id"].get<std::string>().append(".key.1"))
                << key["key"].get<std::string>();
    }
    for (const auto& key : jsonKeys["keys"]) {
        EXPECT_TRUE(std::filesystem::exists(
                dbname / "deks" /
                key["id"].get<std::string>().append(".key.1")));
    }
    EXPECT_EQ(cb::engine_errc::success,
              engine->set_active_encryption_keys(
                      *cookie,
                      {{"keystore", std::move(jsonKeys)},
                       {"unavailable", nlohmann::json::array()}}));
}

nlohmann::json KVBucketTest::getEncryptionKeys() {
    return {{"active", "MyActiveKey"},
            {"keys",
             {{{"id", "MyActiveKey"},
               {"cipher", "AES-256-GCM"},
               {"key", "cXOdH9oGE834Y2rWA+FSdXXi5CN3mLJ+Z+C0VpWbOdA="}}}}};
}
class KVBucketParamTest : public STParameterizedBucketTest {
public:
    void SetUp() override {
        STParameterizedBucketTest::SetUp();
        // Have all the objects, activate vBucket zero so we can store data.
        setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    }
};

#ifdef EP_USE_MAGMA
// Test to verify stats aggregation across shards is working.
TEST_P(KVBucketParamTest, GetKVStoreStats) {
    if (!isMagma()) {
        GTEST_SKIP_("magma only");
    }
    auto vbid1 = vbid;
    auto vbid2 = vbid;
    vbid2++;
    store->setVBucketState(vbid2, vbucket_state_active);

    auto key1 = makeStoredDocKey("key1");
    store_item(vbid1, key1, "value");
    flush_vbucket_to_disk(vbid1, 1);

    auto key2 = makeStoredDocKey("key2");
    store_item(vbid2, key2, "value");
    flush_vbucket_to_disk(vbid2, 1);

    size_t nSetsVbid1 = 0;
    size_t nSetsVbid2 = 0;
    size_t nSetsAll = 0;
    constexpr auto nSetsStatName = "magma_NSets";
    constexpr auto fooStatName = "foo";
    constexpr std::array<std::string_view, 2> keys = {
            {nSetsStatName, fooStatName}};

    auto stats = store->getKVStoreStats(keys);
    store->getRWUnderlying(vbid1)->getStat(nSetsStatName, nSetsVbid1);
    store->getRWUnderlying(vbid2)->getStat(nSetsStatName, nSetsVbid2);
    store->getKVStoreStat(nSetsStatName, nSetsAll);

    EXPECT_EQ(nSetsVbid1, 1);
    EXPECT_EQ(nSetsVbid2, 1);
    EXPECT_NE(stats.find(nSetsStatName), stats.end());
    EXPECT_EQ(stats[nSetsStatName], 2);
    EXPECT_EQ(nSetsAll, 2);
    EXPECT_EQ(stats.find(fooStatName), stats.end());
}
#endif

// getKeyStats tests //////////////////////////////////////////////////////////

// Check that keystats on resident items works correctly.
TEST_P(KVBucketParamTest, GetKeyStatsResident) {
    key_stats kstats;

    // Should start with key not existing.
    auto getKeyStats = [&]() -> cb::engine_errc {
        return store->getKeyStats(makeStoredDocKey("key"),
                                  Vbid(0),
                                  *cookie,
                                  kstats,
                                  WantsDeleted::No);
    };

    auto rv = getKeyStats();
    if (needsBGFetch(rv)) {
        runBGFetcherTask();
        rv = getKeyStats();
    }
    EXPECT_EQ(cb::engine_errc::no_such_key, rv);

    store_item(Vbid(0), makeStoredDocKey("key"), "value");
    EXPECT_EQ(cb::engine_errc::success, getKeyStats())
            << "Expected to get key stats on existing item";
    EXPECT_EQ(vbucket_state_active, kstats.vb_state);
    EXPECT_FALSE(kstats.logically_deleted);
}

// Create then delete an item, checking we get key-stats reporting the item
// as deleted.
TEST_P(KVBucketParamTest, GetKeyStatsDeleted) {
    auto& kvbucket = *engine->getKVBucket();
    key_stats kstats;

    store_item(Vbid(0), makeStoredDocKey("key"), "value");
    delete_item(vbid, makeStoredDocKey("key"));

    // Should get ENOENT if we don't ask for deleted items.
    EXPECT_EQ(cb::engine_errc::no_such_key,
              kvbucket.getKeyStats(makeStoredDocKey("key"),
                                   Vbid(0),
                                   *cookie,
                                   kstats,
                                   WantsDeleted::No));

    // Should get success (and item flagged as deleted) if we ask for deleted
    // items.
    EXPECT_EQ(cb::engine_errc::success,
              kvbucket.getKeyStats(makeStoredDocKey("key"),
                                   Vbid(0),
                                   *cookie,
                                   kstats,
                                   WantsDeleted::Yes));
    EXPECT_EQ(vbucket_state_active, kstats.vb_state);
    EXPECT_TRUE(kstats.logically_deleted);
}

// Check incorrect vbucket returns not-my-vbucket.
TEST_P(KVBucketParamTest, GetKeyStatsNMVB) {
    auto& kvbucket = *engine->getKVBucket();
    key_stats kstats;

    EXPECT_EQ(cb::engine_errc::not_my_vbucket,
              kvbucket.getKeyStats(makeStoredDocKey("key"),
                                   Vbid(1),
                                   *cookie,
                                   kstats,
                                   WantsDeleted::No));
}

// Replace tests //////////////////////////////////////////////////////////////

// Test replace against a non-existent key.
TEST_P(KVBucketParamTest, ReplaceENOENT) {
    // Should start with key not existing (and hence cannot replace).
    auto item = make_item(vbid, makeStoredDocKey("key"), "value");
    auto rv = store->replace(item, cookie);
    if (needsBGFetch(rv)) {
        runBGFetcherTask();
        rv = store->replace(item, cookie);
    }
    EXPECT_EQ(cb::engine_errc::no_such_key, rv);
}

// Create then delete an item, checking replace reports ENOENT.
TEST_P(KVBucketParamTest, ReplaceDeleted) {
    store_item(vbid, makeStoredDocKey("key"), "value");
    delete_item(vbid, makeStoredDocKey("key"));

    // Replace should fail.
    auto item = make_item(vbid, makeStoredDocKey("key"), "value2");
    EXPECT_EQ(cb::engine_errc::no_such_key, store->replace(item, cookie));
}

// Check incorrect vbucket returns not-my-vbucket.
TEST_P(KVBucketParamTest, ReplaceNMVB) {
    auto item =
            make_item(Vbid(vbid.get() + 1), makeStoredDocKey("key"), "value2");
    EXPECT_EQ(cb::engine_errc::not_my_vbucket, store->replace(item, cookie));
}

// Check pending vbucket returns EWOULDBLOCK.
TEST_P(KVBucketParamTest, ReplacePendingVB) {
    store->setVBucketState(vbid, vbucket_state_pending);
    auto item = make_item(vbid, makeStoredDocKey("key"), "value2");
    EXPECT_EQ(cb::engine_errc::would_block, store->replace(item, cookie));
}

// Set tests //////////////////////////////////////////////////////////////////

// Test CAS set against a non-existent key
TEST_P(KVBucketParamTest, SetCASNonExistent) {
    // Create an item with a non-zero CAS.
    auto item = make_item(vbid, makeStoredDocKey("key"), "value");
    item.setCas();
    ASSERT_NE(0, item.getCas());

    // Should get ENOENT as we should immediately know (either from metadata
    // being resident, or by bloomfilter) that key doesn't exist. Might need to
    // bg fetch for magma which implements their own bloom filters
    auto rv = store->set(item, cookie);
    if (needsBGFetch(rv)) {
        runBGFetcherTask();
        rv = store->set(item, cookie);
    }
    EXPECT_EQ(cb::engine_errc::no_such_key, rv);
}

// Test CAS set against a deleted item
TEST_P(KVBucketParamTest, SetCASDeleted) {
    auto key = makeStoredDocKey("key");
    auto item = make_item(vbid, key, "value");

    // Store item
    EXPECT_EQ(cb::engine_errc::success, store->set(item, cookie));

    // Delete item
    uint64_t cas = 0;
    mutation_descr_t mutation_descr;
    EXPECT_EQ(cb::engine_errc::success,
              store->deleteItem(key,
                                cas,
                                vbid,
                                cookie,
                                {},
                                /*itemMeta*/ nullptr,
                                mutation_descr));

    if (engine->getConfiguration().getBucketTypeString() == "persistent") {
        // Trigger a flush to disk.
        flush_vbucket_to_disk(vbid);
    }

    // check we have the cas
    ASSERT_NE(0, cas);

    auto item2 = make_item(vbid, key, "value2");
    item2.setCas(cas);

    // Store item
    if (engine->getConfiguration().getItemEvictionPolicyString() ==
        "full_eviction") {
        EXPECT_EQ(cb::engine_errc::would_block, store->set(item2, cookie));
        runBGFetcherTask();
    }

    EXPECT_EQ(cb::engine_errc::no_such_key, store->set(item2, cookie));
}

/**
 * Regression test for MB-25398 - Test CAS set (deleted value) against a
 * deleted, non-resident key.
 */
TEST_P(KVBucketParamTest, MB_25398_SetCASDeletedItem) {
    auto key = makeStoredDocKey("key");
    store_item(vbid, key, "value");

    flushVBucketToDiskIfPersistent(vbid);

    // delete it, retaining a value.
    auto item = make_item(vbid, key, "deletedvalue");
    item.setDeleted();
    const auto inCAS = item.getCas();
    ASSERT_EQ(cb::engine_errc::success, store->set(item, cookie));
    ASSERT_NE(inCAS, item.getCas());

    // Flush, ensuring that the persistence callback runs and item is removed
    // from the HashTable.
    flushVBucketToDiskIfPersistent(vbid);

    // Create a different deleted value (with an incorrect CAS).
    auto item2 = make_item(vbid, key, "deletedvalue2");
    item2.setDeleted();
    item2.setCas(item.getCas() + 1);

    if (engine->getConfiguration().getBucketTypeString() == "persistent") {
        // Deleted item won't be resident (after a flush), so expect to need to
        // bgfetch.
        EXPECT_EQ(cb::engine_errc::would_block, store->set(item2, cookie));

        runBGFetcherTask();
    }

    // Try with incorrect CAS.
    EXPECT_EQ(cb::engine_errc::key_already_exists, store->set(item2, cookie));

    // Try again, this time with correct CAS.
    item2.setCas(item.getCas());
    EXPECT_EQ(cb::engine_errc::success, store->set(item2, cookie));
}

/**
 * Negative variant of the regression test for MB-25398 - Test that a CAS set
 * (deleted value) to a non-existent item fails.
 */
TEST_P(KVBucketParamTest, MB_25398_SetCASDeletedItemNegative) {
    auto key = makeStoredDocKey("key");

    flushVBucketToDiskIfPersistent(vbid, 0);

    // Attempt to mutate a non-existent key (with a specific, incorrect CAS)
    auto item2 = make_item(vbid, key, "deletedvalue");
    item2.setDeleted();
    item2.setCas(1234);

    if (engine->getConfiguration().getBucketTypeString() == "persistent") {
        // Deleted item won't be resident (after a flush), so expect to need to
        // bgfetch.
        EXPECT_EQ(cb::engine_errc::would_block, store->set(item2, cookie));
        runBGFetcherTask();
    }

    // Try with a specific CAS.
    EXPECT_EQ(cb::engine_errc::no_such_key, store->set(item2, cookie));

    // Try with no CAS (wildcard) - should be possible to store.
    item2.setCas(0);
    EXPECT_EQ(cb::engine_errc::success, store->set(item2, cookie));
}

// Add tests //////////////////////////////////////////////////////////////////

// Test successful add
TEST_P(KVBucketParamTest, Add) {
    auto item = make_item(vbid, makeStoredDocKey("key"), "value");
    auto rv = store->add(item, cookie);
    if (needsBGFetch(rv)) {
        runBGFetcherTask();
        rv = store->add(item, cookie);
    }
    EXPECT_EQ(cb::engine_errc::success, rv);
}

// Check incorrect vbucket returns not-my-vbucket.
TEST_P(KVBucketParamTest, AddNMVB) {
    auto item =
            make_item(Vbid(vbid.get() + 1), makeStoredDocKey("key"), "value2");
    EXPECT_EQ(cb::engine_errc::not_my_vbucket, store->add(item, cookie));
}

/**
 * Test that add() behaves correctly when "adding" a Deleted item.
 * This is a slightly obscure use-case; exposed via subdoc for transactions
 * support (see  AccessDeleted | CreateAsDeleted).
 * Such an operation should only succeed if there is neither an alive document
 * nor a deleted (tombstone) - if a deleted document exists then the operation
 * should fail.
 */
TEST_P(KVBucketParamTest, AddDeleted) {
    auto vb = store->getVBucket(vbid);
    StoredDocKey key = makeStoredDocKey("aKey");

    auto deletedItem = make_item(vbid, key, "deleted value");
    deletedItem.setDeleted();
    if (persistent()) {
        EXPECT_EQ(cb::engine_errc::would_block, store->add(deletedItem, cookie))
                << "Add() of deleted item (no alive or tombstone) should "
                   "require bgFetch to check for on-disk tombstone.";

        {
            auto result = vb->ht.findForWrite(key);
            ASSERT_TRUE(result.storedValue);
            EXPECT_TRUE(result.storedValue->isTempInitialItem());
        }
        EXPECT_EQ(cb::engine_errc::would_block, store->add(deletedItem, cookie))
                << "Add() of deleted item when a temp_initial_item has been "
                   "added for pending bgFetch should return would_block";

        runBGFetcherTask();
        EXPECT_EQ(cb::engine_errc::success, store->add(deletedItem, cookie))
                << "After bgfetch finds temp_non_existent, add of deleted item "
                   "should succeed";
    } else {
        EXPECT_EQ(cb::engine_errc::success, store->add(deletedItem, cookie))
                << "Add() of deleted item for (no alive or tombstone) for "
                   "ephemeral should succeed.";
    }

    EXPECT_EQ(cb::engine_errc::not_stored, store->add(deletedItem, cookie))
            << "Add() of deleted item (when tombstone resident) should fail as "
               "a deleted already exists";

    if (persistent()) {
        // Check behaviour when tombstone has been ejected from memory but
        // still present on disk.
        // This is not applicable to ephemeral as tombstones are only
        // ever held in memory; removing a tombstone from memory is
        // semantically the same as purging it entirely.

        // Flushing a deleted item should remove it from the HashTable
        flushVBucketToDiskIfPersistent(vbid, 1);
        {
            auto result = vb->ht.findForWrite(key);
            ASSERT_FALSE(result.storedValue);
        }

        auto deleted2 = make_item(vbid, key, "deleted value 2");
        deleted2.setDeleted();
        EXPECT_EQ(cb::engine_errc::would_block, store->add(deleted2, cookie))
                << "Add() of deleted item (when tombstone ejected) should "
                   "require bgFetch";

        runBGFetcherTask();
        EXPECT_EQ(cb::engine_errc::not_stored, store->add(deleted2, cookie))
                << "After bgfetch finds existing tombstone, add of deleted "
                   "item should fail";
    }

    // add an alive document, then attempt to "Add" a deleted one.
    auto aliveItem = make_item(vbid, key, "value2");
    ASSERT_EQ(cb::engine_errc::success, store->add(aliveItem, cookie));

    auto deleted4 = make_item(vbid, key, "deleted value 4");
    deleted4.setDeleted();
    EXPECT_EQ(cb::engine_errc::not_stored, store->add(deleted4, cookie))
            << "Add() of deleted item when alive item present should fail";
}

// SetWithMeta tests //////////////////////////////////////////////////////////

// Test basic setWithMeta
TEST_P(KVBucketParamTest, SetWithMeta) {
    auto item = make_item(vbid, makeStoredDocKey("key"), "value");
    item.setCas();
    uint64_t seqno;
    auto setWithMeta = [&]() -> cb::engine_errc {
        return store->setWithMeta(item,
                                  0,
                                  &seqno,
                                  cookie,
                                  {vbucket_state_active},
                                  CheckConflicts::Yes,
                                  /*allowExisting*/ false);
    };

    auto rv = setWithMeta();
    bool bFilterEnabled = engine->getConfiguration().isBfilterEnabled();

    if (isPersistent() && !bFilterEnabled) {
        // If the bloomfilter is disabled, a bgfetch task would have been
        // scheduled & ewould_block returned.
        EXPECT_EQ(cb::engine_errc::would_block, rv);
        auto vb = store->getVBucket(vbid);
        EXPECT_TRUE(vb->hasPendingBGFetchItems());
        runBGFetcherTask();
        rv = setWithMeta();
    }
    EXPECT_EQ(cb::engine_errc::success, rv);
}

// Test setWithMeta with a conflict with an existing item.
TEST_P(KVBucketParamTest, SetWithMeta_Conflicted) {
    auto item = make_item(vbid, makeStoredDocKey("key"), "value");
    EXPECT_EQ(cb::engine_errc::success, store->set(item, cookie));

    uint64_t seqno;
    // Attempt to set with the same rev Seqno - should get EEXISTS.
    EXPECT_EQ(cb::engine_errc::key_already_exists,
              store->setWithMeta(item,
                                 item.getCas(),
                                 &seqno,
                                 cookie,
                                 {vbucket_state_active},
                                 CheckConflicts::Yes,
                                 /*allowExisting*/ true));
}

// Test setWithMeta replacing existing item
TEST_P(KVBucketParamTest, SetWithMeta_Replace) {
    auto item = make_item(vbid, makeStoredDocKey("key"), "value");
    EXPECT_EQ(cb::engine_errc::success, store->set(item, cookie));

    // Increase revSeqno so conflict resolution doesn't fail.
    item.setRevSeqno(item.getRevSeqno() + 1);
    uint64_t seqno;
    // Should get EEXISTS if we don't force (and use wrong CAS).
    EXPECT_EQ(cb::engine_errc::key_already_exists,
              store->setWithMeta(item,
                                 item.getCas() + 1,
                                 &seqno,
                                 cookie,
                                 {vbucket_state_active},
                                 CheckConflicts::Yes,
                                 /*allowExisting*/ true));

    // Should succeed with correct CAS, and different RevSeqno.
    EXPECT_EQ(cb::engine_errc::success,
              store->setWithMeta(item,
                                 item.getCas(),
                                 &seqno,
                                 cookie,
                                 {vbucket_state_active},
                                 CheckConflicts::Yes,
                                 /*allowExisting*/ true));
}

/**
 * 1. setWithMeta to store an item with an expiry value
 * 2. Call get after expiry to ensure that item is deleted
 * 3. setWithMeta to store an item with lesser rev seqno
 *    than what is stored in hash table
 * 4. (3) should result in an EWOULDBLOCK and a temporary
 *    deleted item in hash table
 * 5. setWithMeta after BG Fetch should result in EEXISTS
 * 6. Temporary item should be deleted from the hash table
 */
TEST_P(KVBucketParamTest, MB_28078_SetWithMeta_tempDeleted) {
    auto key = makeStoredDocKey("key");
    auto item = make_item(vbid, key, "value");
    item.setExpTime(1);
    item.setCas();
    uint64_t seqno;
    EXPECT_EQ(cb::engine_errc::success,
              store->setWithMeta(item,
                                 0,
                                 &seqno,
                                 cookie,
                                 {vbucket_state_active},
                                 CheckConflicts::No,
                                 /*allowExisting*/ true));

    TimeTraveller docBrown(20);
    auto options =
            static_cast<get_options_t>(QUEUE_BG_FETCH | GET_DELETED_VALUE);

    auto doGet = [&]() { return store->get(key, vbid, cookie, options); };
    GetValue result = doGet();

    flushVBucketToDiskIfPersistent(vbid, 1);

    auto doSetWithMeta = [&]() {
        return store->setWithMeta(item,
                                  item.getCas(),
                                  &seqno,
                                  cookie,
                                  {vbucket_state_active},
                                  CheckConflicts::Yes,
                                  /*allowExisting*/ true);
    };

    if (engine->getConfiguration().getBucketTypeString() == "persistent") {
        ASSERT_EQ(cb::engine_errc::would_block, doSetWithMeta());
    }

    if (engine->getConfiguration().getBucketTypeString() == "persistent") {
        runBGFetcherTask();
        ASSERT_EQ(cb::engine_errc::key_already_exists, doSetWithMeta());
    }

    EXPECT_EQ(0, store->getVBucket(vbid)->getNumItems());
    EXPECT_EQ(0, store->getVBucket(vbid)->getNumTempItems());
}

// Test forced setWithMeta
TEST_P(KVBucketParamTest, SetWithMeta_Forced) {
    auto item = make_item(vbid, makeStoredDocKey("key"), "value");
    item.setCas();
    uint64_t seqno;
    EXPECT_EQ(cb::engine_errc::success,
              store->setWithMeta(item,
                                 0,
                                 &seqno,
                                 cookie,
                                 {vbucket_state_active,
                                  vbucket_state_replica,
                                  vbucket_state_pending},
                                 CheckConflicts::No,
                                 /*allowExisting*/ false));
}

// MB and test was raised because a few commits back this was broken but no
// existing test covered the case. I.e. run this test  against 0810540 and it
// fails, but now fixed
TEST_P(KVBucketParamTest, mb22824) {
    auto key = makeStoredDocKey("key");

    // Store key and force expiry
    store_item(Vbid(0), key, "value", 1);
    TimeTraveller docBrown(20);

    uint32_t deleted = false;
    ItemMetaData itemMeta1;
    auto datatype = PROTOCOL_BINARY_RAW_BYTES;
    EXPECT_EQ(cb::engine_errc::success,
              store->getMetaData(
                      key, vbid, cookie, itemMeta1, deleted, datatype));

    uint64_t cas = 0;
    ItemMetaData itemMeta2;
    mutation_descr_t mutation_descr;
    EXPECT_EQ(cb::engine_errc::no_such_key,
              store->deleteItem(
                      key, cas, vbid, cookie, {}, &itemMeta2, mutation_descr));

    // Should be getting the same CAS from the failed delete as getMetaData
    EXPECT_EQ(itemMeta1.cas, itemMeta2.cas);
}

/**
 *  Test that the first item updates the hlcSeqno, but not the second
 */
TEST_P(KVBucketParamTest, test_hlcEpochSeqno) {
    auto vb = store->getVBucket(vbid);

    // A persistent bucket will store something then set the hlc_epoch
    // An ephemeral bucket always has an epoch
    int64_t initialEpoch =
            engine->getConfiguration().getBucketTypeString() == "persistent"
                    ? HlcCasSeqnoUninitialised
                    : 0;

    EXPECT_EQ(initialEpoch, vb->getHLCEpochSeqno());

    auto item = make_item(vbid, makeStoredDocKey("key1"), "value");
    auto rv = store->add(item, cookie);
    if (needsBGFetch(rv)) {
        EXPECT_TRUE(vb->hasPendingBGFetchItems());
        runBGFetcherTask();
        rv = store->add(item, cookie);
    }
    EXPECT_EQ(cb::engine_errc::success, rv);

    flushVBucketToDiskIfPersistent(vbid, 1);

    auto seqno = vb->getHLCEpochSeqno();
    EXPECT_NE(HlcCasSeqnoUninitialised, seqno);

    auto item2 = make_item(vbid, makeStoredDocKey("key2"), "value");
    rv = store->add(item2, cookie);
    if (needsBGFetch(rv)) {
        EXPECT_TRUE(vb->hasPendingBGFetchItems());
        runBGFetcherTask();
        rv = store->add(item2, cookie);
    }
    EXPECT_EQ(cb::engine_errc::success, rv);

    flushVBucketToDiskIfPersistent(vbid, 1);

    // hlc seqno doesn't change was more items are stored
    EXPECT_EQ(seqno, vb->getHLCEpochSeqno());
}

TEST_F(KVBucketTest, DataRaceInDoWorkerStat) {
    if (engine->getConfiguration().getExecutorPoolBackendString() == "folly") {
        // doWorkerStat() as required by this test below not yet implemented
        // for FollyExecutorPool.
        GTEST_SKIP();
    }

    /* MB-23529: TSAN intermittently reports a data race.
     * This race appears to be caused by GGC's buggy string COW as seen
     * multiple times, e.g., MB-23454.
     * doWorkerStat calls getLog/getSlowLog to get a vector of TaskLogEntrys,
     * which have been copied out of the tasklog ringbuffer of a given
     * CB3ExecutorThread. These copies logically have copies of the original's
     * `std::string name`.
     * As the ringbuffer overwrites older entries, the deletion of the old
     * entry's `std::string name` races with doWorkerStats reading the COW'd
     * name of its copy.
     * */
    EpEngineTaskable& taskable = engine->getTaskable();
    ExecutorPool* pool = ExecutorPool::get();

    // Task which does nothing
    ExTask task = std::make_shared<LambdaTask>(
            taskable,
            TaskId::DcpConsumerTask,
            0,
            true,
            [&](LambdaTask&) -> bool {
                return true; // reschedule (immediately)
            });

    pool->schedule(task);

    // nop callback to serve as add_stat
    auto dummy_cb = [](std::string_view key,
                       std::string_view value,
                       CookieIface& ctx) {};
    auto* cookie = create_mock_cookie();
    for (uint64_t i = 0; i < 10; ++i) {
        pool->doWorkerStat(engine->getTaskable(), *cookie, dummy_cb);
    }
    destroy_mock_cookie(cookie);
    pool->cancel(task->getId());
}

TEST_F(KVBucketTest, ExpiryConfigChangeWakesTask) {
    // schedule the expiry pager task.
    store->enableExpiryPager();
    // check that the task has a longer runtime to start with
    ASSERT_GT(store->getExpiryPagerSleeptime(), 100);

    // check the task has not run yet.
    auto& epstats = engine->getEpStats();
    ASSERT_EQ(0, epstats.expiryPagerRuns);

    // try to change the config to get the task to run asap
    store->setExpiryPagerSleeptime(0);

    using namespace std::chrono_literals;
    auto deadline = cb::time::steady_clock::now() + 5s;

    while (epstats.expiryPagerRuns == 0 &&
           cb::time::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }

    // check that the task has run before our deadline - it wouldn't have
    // if the config change did not wake the task through the pool.
    EXPECT_GT(epstats.expiryPagerRuns, 0);
}

void KVBucketTest::storeAndDeleteItem(Vbid vbid,
                                      const DocKeyView& key,
                                      std::string value) {
    Item item = store_item(vbid,
                           key,
                           value,
                           0,
                           {cb::engine_errc::success},
                           PROTOCOL_BINARY_RAW_BYTES);

    delete_item(vbid, key);
    flushVBucketToDiskIfPersistent(vbid, 1);
}

cb::engine_errc KVBucketTest::getMeta(Vbid vbid,
                                      const DocKeyView key,
                                      CookieIface* cookie,
                                      ItemMetaData& itemMeta,
                                      uint32_t& deleted,
                                      uint8_t& datatype,
                                      bool retryOnEWouldBlock) {
    auto doGetMetaData = [&]() {
        return store->getMetaData(
                key, vbid, cookie, itemMeta, deleted, datatype);
    };

    auto engineResult = doGetMetaData();
    if (engine->getConfiguration().getBucketTypeString() == "persistent" &&
        retryOnEWouldBlock) {
        EXPECT_EQ(cb::engine_errc::would_block, engineResult);
        // Manually run the bgfetch task, and re-attempt getMetaData
        runBGFetcherTask();

        engineResult = doGetMetaData();
    }

    return engineResult;
}

TEST_P(KVBucketParamTest, lockKeyTempDeletedTest) {
    //This test is to check if the lockKey function will
    //remove temporary deleted items from memory
    auto key = makeStoredDocKey("key");
    storeAndDeleteItem(vbid, key, std::string("value"));

    ItemMetaData itemMeta;
    uint32_t deleted = 0;
    uint8_t datatype = 0;
    auto engineResult = getMeta(vbid, key, cookie, itemMeta, deleted, datatype);

    // Verify that GetMeta succeeded; and metadata is correct.
    ASSERT_EQ(cb::engine_errc::success, engineResult);
    ASSERT_TRUE(deleted);

    int expTempItems = 0;
    if (engine->getConfiguration().getBucketTypeString() == "persistent") {
        expTempItems = 1;
    }

    //Check that the temp item is removed for getLocked
    EXPECT_EQ(expTempItems, store->getVBucket(vbid)->getNumTempItems());
    GetValue gv = store->getLocked(key, vbid, std::chrono::seconds{10}, cookie);
    EXPECT_EQ(cb::engine_errc::no_such_key, gv.getStatus());
    EXPECT_EQ(0, store->getVBucket(vbid)->getNumTempItems());
}

TEST_P(KVBucketParamTest, unlockKeyTempDeletedTest) {
    //This test is to check if the unlockKey function will
    //remove temporary deleted items from memory
    auto key = makeStoredDocKey("key");
    std::string value("value");

    Item itm = store_item(vbid,
                          key,
                          value,
                          0,
                          {cb::engine_errc::success},
                          PROTOCOL_BINARY_RAW_BYTES);

    GetValue gv = store->getAndUpdateTtl(
            key, vbid, cookie, ep_convert_to_expiry_time(10000));
    EXPECT_EQ(cb::engine_errc::success, gv.getStatus());
    auto docCas = gv.item->getCas();

    gv = store->getLocked(key, vbid, std::chrono::seconds{10}, cookie);
    EXPECT_EQ(cb::engine_errc::success, gv.getStatus());

    // Need the "real" documents' CAS for expiry.
    gv.item->setCas(docCas);
    store->processExpiredItem(
            *gv.item, ep_real_time() + 10001, ExpireBy::Pager);

    flushVBucketToDiskIfPersistent(vbid, 1);

    ItemMetaData itemMeta;
    uint32_t deleted = 0;
    uint8_t datatype = 0;
    auto engineResult = getMeta(vbid, key, cookie, itemMeta, deleted, datatype);

    // Verify that GetMeta succeeded; and metadata is correct.
    ASSERT_EQ(cb::engine_errc::success, engineResult);
    ASSERT_TRUE(deleted);

    int expTempItems = 0;
    if (engine->getConfiguration().getBucketTypeString() == "persistent") {
        expTempItems = 1;
    }

    //Check that the temp item is removed for unlockKey
    EXPECT_EQ(expTempItems, store->getVBucket(vbid)->getNumTempItems());
    EXPECT_EQ(cb::engine_errc::no_such_key,
              store->unlockKey(key, vbid, 0, ep_current_time(), cookie));
    EXPECT_EQ(0, store->getVBucket(vbid)->getNumTempItems());
}

// Test that getLocked correctly returns ESyncWriteInProgress if targetted at
// a key which has a prepared SyncWrite in progress.
TEST_P(KVBucketParamTest, GetLockedWithPreparedSyncWrite) {
    // Setup - need a valid topology to accept SyncWrites - but don't want them
    // to auto-commit so create a topology with 2 nodes.
    auto meta =
            nlohmann::json{{"topology", nlohmann::json::array({{"a", "b"}})}};
    ASSERT_EQ(cb::engine_errc::success,
              store->setVBucketState(vbid, vbucket_state_active, &meta));

    // Store both a committed and prepared SV.
    auto key = makeStoredDocKey("key");
    ASSERT_EQ(cb::engine_errc::success,
              store->set(*makeCommittedItem(key, "value1"), cookie));
    ASSERT_EQ(cb::engine_errc::sync_write_pending,
              store->set(*makePendingItem(key, "value2"), cookie));

    // Test
    auto gv = store->getLocked(key, vbid, std::chrono::seconds{10}, cookie);
    EXPECT_EQ(cb::engine_errc::sync_write_in_progress, gv.getStatus());
}

// Test that unlock correctly returns ESyncWriteInProgress if targetted at
// a key which has a prepared SyncWrite in progress.
TEST_P(KVBucketParamTest, UnlockWithPreparedSyncWrite) {
    // Setup - need a valid topology to accept SyncWrites - but don't want them
    // to auto-commit so create a topology with 2 nodes.
    auto meta =
            nlohmann::json{{"topology", nlohmann::json::array({{"a", "b"}})}};
    ASSERT_EQ(cb::engine_errc::success,
              store->setVBucketState(vbid, vbucket_state_active, &meta));

    // Store both a committed and prepared SV.
    auto key = makeStoredDocKey("key");
    auto committed = makeCommittedItem(key, "value1");
    ASSERT_EQ(cb::engine_errc::success, store->set(*committed, cookie));
    ASSERT_EQ(cb::engine_errc::sync_write_pending,
              store->set(*makePendingItem(key, "value2"), cookie));

    // Test
    EXPECT_EQ(
            cb::engine_errc::sync_write_in_progress,
            store->unlockKey(
                    key, vbid, committed->getCas(), ep_current_time(), cookie));
}

// Test that GAT correctly returns ESyncWriteInProgress if targetted at
// a key which has a prepared SyncWrite in progress.
TEST_P(KVBucketParamTest, GetAndUpdateTtlWithPreparedSyncWrite) {
    // Setup - need a valid topology to accept SyncWrites - but don't want them
    // to auto-commit so create a topology with 2 nodes.
    auto meta =
            nlohmann::json{{"topology", nlohmann::json::array({{"a", "b"}})}};
    ASSERT_EQ(cb::engine_errc::success,
              store->setVBucketState(vbid, vbucket_state_active, &meta));

    // Store both a committed and prepared SV.
    auto key = makeStoredDocKey("key");
    ASSERT_EQ(cb::engine_errc::success,
              store->set(*makeCommittedItem(key, "value1"), cookie));
    ASSERT_EQ(cb::engine_errc::sync_write_pending,
              store->set(*makePendingItem(key, "value2"), cookie));

    // Test
    auto gv = store->getAndUpdateTtl(key, vbid, cookie, 10);
    EXPECT_EQ(cb::engine_errc::sync_write_in_progress, gv.getStatus());
}

TEST_P(KVBucketParamTest, replaceTempDeletedTest) {
    //This test is to check if the replace function will
    //remove temporary deleted items from memory
    auto key = makeStoredDocKey("key");
    storeAndDeleteItem(vbid, key, std::string("value"));

    ItemMetaData itemMeta;
    uint32_t deleted = 0;
    uint8_t datatype = 0;
    auto engineResult = getMeta(vbid, key, cookie, itemMeta, deleted, datatype);
    ASSERT_EQ(cb::engine_errc::success, engineResult);
    ASSERT_TRUE(deleted);

    int expTempItems = 0;
    if (engine->getConfiguration().getBucketTypeString() == "persistent") {
        expTempItems = 1;
    }

    //Check that the temp item is removed for replace
    EXPECT_EQ(expTempItems, store->getVBucket(vbid)->getNumTempItems());
    auto replace_item = make_item(vbid, makeStoredDocKey("key"), "value2");
    EXPECT_EQ(cb::engine_errc::no_such_key,
              store->replace(replace_item, cookie));
    EXPECT_EQ(0, store->getVBucket(vbid)->getNumTempItems());
}

TEST_P(KVBucketParamTest, statsVKeyTempDeletedTest) {
    //This test is to check if the statsVKey function will
    //remove temporary deleted items from memory
    auto key = makeStoredDocKey("key");
    storeAndDeleteItem(vbid, key, std::string("value"));

    ItemMetaData itemMeta;
    uint32_t deleted = 0;
    uint8_t datatype = 0;
    auto engineResult = getMeta(vbid, key, cookie, itemMeta, deleted, datatype);

    // Verify that GetMeta succeeded; and metadata is correct.
    ASSERT_EQ(cb::engine_errc::success, engineResult);
    ASSERT_TRUE(deleted);

    int expTempItems = 0;
    cb::engine_errc expRetCode = cb::engine_errc::not_supported;
    if (engine->getConfiguration().getBucketTypeString() == "persistent") {
        expTempItems = 1;
        expRetCode = cb::engine_errc::no_such_key;
    }

    //Check that the temp item is removed for statsVKey
    EXPECT_EQ(expTempItems, store->getVBucket(vbid)->getNumTempItems());
    EXPECT_EQ(expRetCode, store->statsVKey(key, vbid, *cookie));
    EXPECT_EQ(0, store->getVBucket(vbid)->getNumTempItems());
}

TEST_P(KVBucketParamTest, getAndUpdateTtlTempDeletedItemTest) {
    //This test is to check if the getAndUpdateTtl function will
    //remove temporary deleted items from memory
    auto key = makeStoredDocKey("key");
    storeAndDeleteItem(vbid, key, std::string("value"));

    ItemMetaData itemMeta;
    uint32_t deleted = 0;
    uint8_t datatype = 0;
    auto engineResult = getMeta(vbid, key, cookie, itemMeta, deleted, datatype);
    // Verify that GetMeta succeeded; and metadata is correct.
    ASSERT_EQ(cb::engine_errc::success, engineResult);
    ASSERT_TRUE(deleted);

    int expTempItems = 0;
    if (engine->getConfiguration().getBucketTypeString() == "persistent") {
        expTempItems = 1;
    }

    //Check that the temp item is removed for getAndUpdateTtl
    EXPECT_EQ(expTempItems, store->getVBucket(vbid)->getNumTempItems());
    GetValue gv = store->getAndUpdateTtl(makeStoredDocKey("key"),
                                         vbid,
                                         cookie,
                                         gsl::narrow<uint32_t>(time(nullptr)));
    EXPECT_EQ(cb::engine_errc::no_such_key, gv.getStatus());
    EXPECT_EQ(0, store->getVBucket(vbid)->getNumTempItems());
}

TEST_P(KVBucketParamTest, validateKeyTempDeletedItemTest) {
    //This test is to check if the getAndUpdateTtl function will
    //remove temporary deleted items from memory
    auto key = makeStoredDocKey("key");
    storeAndDeleteItem(vbid, key, std::string("value"));

    ItemMetaData itemMeta;
    uint32_t deleted;
    uint8_t datatype;
    auto engineResult = getMeta(vbid, key, cookie, itemMeta, deleted, datatype);

    // Verify that GetMeta succeeded; and metadata is correct.
    ASSERT_EQ(cb::engine_errc::success, engineResult);
    ASSERT_TRUE(deleted);

    int expTempItems = 0;
    if (engine->getConfiguration().getBucketTypeString() == "persistent") {
        expTempItems = 1;
    }

    //Check that the temp item is removed for validateKey
    EXPECT_EQ(expTempItems, store->getVBucket(vbid)->getNumTempItems());

    // dummy item; don't expect to need it for deleted case.
    auto dummy = make_item(vbid, key, {});
    std::string result = store->validateKey(key, vbid, dummy);
    EXPECT_STREQ("item_deleted", result.c_str());
    EXPECT_EQ(0, store->getVBucket(vbid)->getNumTempItems());
}

TEST_P(KVBucketParamTest, validateKeyWithRevSeqnoMismatchTest) {
    auto key = makeStoredDocKey("key");
    Item item = store_item(vbid,
                           key,
                           std::string("value"),
                           0,
                           {cb::engine_errc::success},
                           PROTOCOL_BINARY_RAW_BYTES);

    // dummy item;
    auto dummy = make_item(vbid, key, {"value"});
    dummy.setDataType(item.getDataType());
    // Set rev_seq to mismatch
    dummy.setRevSeqno(item.getRevSeqno() + 1);
    std::string result = store->validateKey(key, vbid, dummy);
    EXPECT_THAT(result.c_str(), testing::HasSubstr("revseqno_mismatch"));
    EXPECT_EQ(0, store->getVBucket(vbid)->getNumTempItems());
}

TEST_P(KVBucketParamTest, validateKeyWithCasMismatchTest) {
    auto key = makeStoredDocKey("key");
    Item item = store_item(vbid,
                           key,
                           std::string("value"),
                           0,
                           {cb::engine_errc::success},
                           PROTOCOL_BINARY_RAW_BYTES);

    // dummy item;
    auto dummy = make_item(vbid, key, {"value"});
    dummy.setDataType(item.getDataType());
    dummy.setCas(item.getCas() + 1);
    std::string result = store->validateKey(key, vbid, dummy);
    EXPECT_THAT(result.c_str(), testing::HasSubstr("cas_mismatch"));
    EXPECT_EQ(0, store->getVBucket(vbid)->getNumTempItems());
}

// Test demonstrates MB-25948 with a subtle difference. In the MB the issue
// says delete(key1), but in this test we use expiry. That is because using
// ep-engine deleteItem doesn't do the system-xattr pruning (that's part of
// memcached). So we use expiry which will use the pre_expiry hook to prune
// the xattrs.
TEST_P(KVBucketParamTest, MB_25948) {
    if (isNexus()) {
        // TODO MB-53859: Re-enable under nexus once couchstore datatype
        // on KEYS_ONLY is consistent with magma
        GTEST_SKIP();
    }

    // 1. Store key1 with an xattr value
    auto key = makeStoredDocKey("key");

    std::string value = createXattrValue("body");

    Item item = store_item(Vbid(0),
                           key,
                           value,
                           1,
                           {cb::engine_errc::success},
                           PROTOCOL_BINARY_DATATYPE_XATTR);

    TimeTraveller docBrown(20);

    // 2. Force expiry of the item and flush the delete
    auto options =
            static_cast<get_options_t>(QUEUE_BG_FETCH | GET_DELETED_VALUE);
    auto doGet = [&]() { return store->get(key, vbid, cookie, options); };
    GetValue result = doGet();

    flushVBucketToDiskIfPersistent(vbid, 1);

    // 3. GetMeta for key1, retrieving the tombstone
    ItemMetaData itemMeta;
    uint32_t deleted = 0;
    uint8_t datatype = 0;
    auto doGetMetaData = [&]() {
        return store->getMetaData(
                key, vbid, cookie, itemMeta, deleted, datatype);
    };

    auto engineResult = doGetMetaData();

    if (engine->getConfiguration().getBucketTypeString() == "persistent") {
        EXPECT_EQ(cb::engine_errc::would_block, engineResult);
        // Manually run the bgfetch task, and re-attempt getMetaData
        runBGFetcherTask();

        engineResult = doGetMetaData();
    }
    // Verify that GetMeta succeeded; and metadata is correct.
    ASSERT_EQ(cb::engine_errc::success, engineResult);
    ASSERT_TRUE(deleted);
    // strip off Snappy, as magma and couchstore differ in whether they
    // report Snappy datatype in response to getMeta (even though both
    // actually compressed the document)
    // TODO MB-53859: make couchstore report Snappy "accurately" for meta-only
    datatype &= ~PROTOCOL_BINARY_DATATYPE_SNAPPY;
    ASSERT_EQ(PROTOCOL_BINARY_DATATYPE_XATTR, datatype);
    ASSERT_EQ(item.getFlags(), itemMeta.flags);
    // CAS and revSeqno not checked as changed when the document was expired.

    // 4. Now get deleted value - we want to retrieve the _sync field.
    result = doGet();

    // Manually run the bgfetch task and retry the get()
    if (engine->getConfiguration().getBucketTypeString() == "persistent") {
        ASSERT_EQ(cb::engine_errc::would_block, result.getStatus());
        runBGFetcherTask();
        result = doGet();
    }
    ASSERT_EQ(cb::engine_errc::success, result.getStatus());

    cb::xattr::Blob blob({const_cast<char*>(result.item->getData()),
                          result.item->getNBytes()},
                         false);

    // user and meta gone, _sync remains.
    EXPECT_EQ(0, blob.get("user").size());
    EXPECT_EQ(0, blob.get("meta").size());
    ASSERT_NE(0, blob.get("_sync").size());
    EXPECT_EQ("{\"cas\":\"0xdeadbeefcafefeed\"}", blob.get("_sync"));
}

/**
 * Test performs the following operations
 * 1. Store an item
 * 2. Delete an item and make sure it is removed from memory
 * 3. Store the item again
 * 4. Evict the item from memory to ensure that meta data
 *    will be retrieved from disk
 * 5. Check that the revision seq no. retrieved from disk
 *    is equal to 3 (the number of updates on that item)
 */
TEST_P(KVBucketParamTest, MB_27162) {
     auto key = makeStoredDocKey("key");
     std::string value("value");

     Item item = store_item(vbid, key, value, 0, {cb::engine_errc::success},
                            PROTOCOL_BINARY_RAW_BYTES);

     delete_item(vbid, key);

     flushVBucketToDiskIfPersistent(vbid, 1);

     store_item(vbid, key, value, 0, {cb::engine_errc::success},
                PROTOCOL_BINARY_RAW_BYTES);

     flushVBucketToDiskIfPersistent(vbid, 1);

     if (isPersistent()) {
         evict_key(vbid, key);
     }

     ItemMetaData itemMeta;
     uint32_t deleted = 0;
     uint8_t datatype = 0;
     auto doGetMetaData = [&]() {
        return store->getMetaData(
                key, vbid, cookie, itemMeta, deleted, datatype);
     };

     auto engineResult = doGetMetaData();

     if (isPersistent() && isFullEviction()) {
         ASSERT_EQ(cb::engine_errc::would_block, engineResult);
         // Manually run the bgfetch task, and re-attempt getMetaData
         runBGFetcherTask();

         engineResult = doGetMetaData();
     }
     // Verify that GetMeta succeeded; and metadata is correct.
     ASSERT_EQ(cb::engine_errc::success, engineResult);
     EXPECT_EQ(3, itemMeta.revSeqno);
}

TEST_P(KVBucketParamTest, numberOfVBucketsInState) {
    EXPECT_EQ(1, store->getNumOfVBucketsInState(vbucket_state_active));
    EXPECT_EQ(0, store->getNumOfVBucketsInState(vbucket_state_replica));
}

/**
 * Test to verify if the vbucket opsGet stat is not incremented when
 * the vbucket is in pending state in the case of a get.
 * Test in the case of a getReplica it does not increase the opsGet
 * stat but increases the the not my vbucket stat.
 */
TEST_P(KVBucketParamTest, testGetPendingOpsStat) {
   auto key = makeStoredDocKey("key");
   store_item(vbid, key, "value");

   store->setVBucketState(vbid, vbucket_state_pending);

   auto options = static_cast<get_options_t>(
            QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE | DELETE_TEMP |
            HIDE_LOCKED_CAS | TRACK_STATISTICS);

   auto doGet = [&]() { return store->get(key, vbid, cookie, options); };
   GetValue result = doGet();
   ASSERT_EQ(cb::engine_errc::would_block, result.getStatus());
   EXPECT_EQ(0, store->getVBucket(vbid)->opsGet);
   EXPECT_EQ(1, engine->getEpStats().pendingOps);
   EXPECT_EQ(0, engine->getEpStats().numNotMyVBuckets);

   auto doGetReplica = [&]() { return store->getReplica(key, vbid, cookie, options); };
   result = doGetReplica();
   ASSERT_EQ(cb::engine_errc::not_my_vbucket, result.getStatus());
   EXPECT_EQ(0, store->getVBucket(vbid)->opsGet);
   EXPECT_EQ(1, engine->getEpStats().pendingOps);
   EXPECT_EQ(1, engine->getEpStats().numNotMyVBuckets);
}

// Test that GetReplica against an expired item correctly returns ENOENT.
// Regression test for MB-38498.
TEST_P(KVBucketParamTest, ReplicaExpiredItem) {
    // Create a document with TTL=10s, then advance clock by 20s so it is
    // past it expiration.
    auto key = makeStoredDocKey("key");
    store_item(vbid, key, "value", ep_convert_to_expiry_time(10));
    // Flush so item is clean and can be evicted.
    flushVBucketToDiskIfPersistent(vbid, 1);

    // Evict the item (to check bgfetch logic)
    if (!ephemeral()) {
        const char* msg = nullptr;
        ASSERT_EQ(cb::engine_errc::success, store->evictKey(key, vbid, &msg))
                << msg;
    }

    TimeTraveller hgWells(20);

    // Change to replica so we can test getReplica()
    store->setVBucketState(vbid, vbucket_state_replica);

    // Same default options as getReplicaCmd
    auto options = static_cast<get_options_t>(
            QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE | DELETE_TEMP |
            HIDE_LOCKED_CAS | TRACK_STATISTICS);
    // Test: Attempt to read expired item.
    if (engine->getConfiguration().getItemEvictionPolicyString() ==
        "full_eviction") {
        EXPECT_NE(nullptr, cookie);
        auto result = store->getReplica(key, vbid, cookie, options);
        EXPECT_EQ(cb::engine_errc::would_block, result.getStatus());
        runBGFetcherTask();
        mock_waitfor_cookie(cookie);
    }

    auto result = store->getReplica(key, vbid, cookie, options);
    EXPECT_EQ(cb::engine_errc::no_such_key, result.getStatus());
}

/***
 * Test class to expose the behaviour needed to create an ItemAccessVisitor
 */
class MockAccessScanner : public AccessScanner {
public:
    MockAccessScanner(KVBucket& _store,
                      Configuration& conf,
                      EPStats& st,
                      double sleeptime = 0,
                      bool useStartTime = false,
                      bool completeBeforeShutdown = false)
        : AccessScanner(_store,
                        conf,
                        st,
                        sleeptime,
                        useStartTime,
                        completeBeforeShutdown) {
    }

    void public_createAndScheduleTask(const size_t shard,
                                      std::vector<Vbid> vbuckets) {
        // Note: AccessScanner:run normally acquires tokens before calling
        // createAndScheduleTask, here we are acquiring one token.
        cb::SemaphoreGuard<> semaphoreGuard(&semaphore);
        createAndScheduleTask(
                shard, std::move(semaphoreGuard), std::move(vbuckets));
    }
};

/***
 * Test to make sure the Access Scanner doesn't throw an exception with a log
 * location specified in the config which doesn't exist.
 */
TEST_P(KVBucketParamTest, AccessScannerInvalidLogLocation) {
    /* Manually edit the configuration to change the location of the
     * access log to be somewhere that doesn't exist */
    engine->getConfiguration().setAlogPath("/path/to/somewhere");
    ASSERT_EQ(engine->getConfiguration().getAlogPath(), "/path/to/somewhere");
    ASSERT_FALSE(cb::io::isDirectory(engine->getConfiguration().getAlogPath()));

    /* Create the Access Scanner task with our modified configuration
     * In this case, the 1000 refers to the sleep time for the job, but it never
     * gets used as part of the test case. */
    auto as = std::make_unique<MockAccessScanner>(*(engine->getKVBucket()),
                                                  engine->getConfiguration(),
                                                  engine->getEpStats(),
                                                  1000);

    /* Make sure this doesn't throw an exception when tyring to run the task*/
    EXPECT_NO_THROW(as->public_createAndScheduleTask(
            0, store->getVBuckets().getShard(0)->getVBuckets()))
            << "Access Scanner threw unexpected "
               "exception where log location does "
               "not exist";
}

TEST_P(KVBucketParamTest, MutationLogFailedWrite) {
    std::string key(50, 'x');
    for (int i = 0; i < 100; i++) {
        store_item(Vbid(0), makeStoredDocKey(key + std::to_string(i)), "value");
    }

    const auto shard = store->getShardId(vbid);
    cb::Semaphore semaphore(1);
    cb::SemaphoreGuard<> semaphoreGuard(&semaphore, cb::adopt_token_t{});

    // Create the access log in a file in the database namespace to ensure
    // it won't affect other tests (the test did not have any filename
    // assigned causing a file named '.0 to be created which was later
    // read by a different test causing that to fail)
    auto& config = engine->getConfiguration();
    auto alog_file = std::filesystem::path(config.getDbname()) / "access.log";
    config.setAlogPath(alog_file.string());
    bool exceptionThrown = false;
    auto pv = std::make_unique<ItemAccessVisitor>(
            *store,
            config,
            engine->getEpStats(),
            shard,
            std::move(semaphoreGuard),
            config.getAlogMaxStoredItems(),
            store->getVBuckets().getShard(shard)->getVBuckets(),
            [&exceptionThrown](auto method) {
                if (method == "flush") {
                    exceptionThrown = true;
                    throw std::system_error(EIO,
                                            std::system_category(),
                                            "MutationLogFailedWrite: failed");
                }
            });

    store->visitAsync(std::move(pv),
                      "Item Access Scanner",
                      TaskId::AccessScannerVisitor,
                      500ms);

    auto& auxioQueue = *task_executor->getLpTaskQ(TaskType::AuxIO);
    EXPECT_NO_THROW(
            runNextTask(auxioQueue, "Item Access Scanner no vbucket assigned"));
    EXPECT_TRUE(exceptionThrown);
}

// Check that getRandomKey works correctly when given a random value of zero
TEST_P(KVBucketParamTest, MB31495_GetRandomKey) {
    std::function<long()> returnZero = []() { return 0; };
    setRandomFunction(returnZero);

    // Try with am empty hash table
    auto gv = store->getRandomKey(CollectionID::Default, *cookie);
    EXPECT_EQ(cb::engine_errc::no_such_key, gv.getStatus());

    Item item = store_item(
            vbid, {"key", DocKeyEncodesCollectionId::No}, "value", 0);
    flushVBucketToDiskIfPersistent(vbid, 1);

    // Try with a non-empty hash table
    gv = store->getRandomKey(CollectionID::Default, *cookie);
    EXPECT_EQ(cb::engine_errc::success, gv.getStatus());
}

// MB-33702: Test that SetVBucket state creates a new failover table entry when
// transitioning from non-active to active.
TEST_P(KVBucketParamTest, FailoverEntryAddedNonActiveToActive) {
    // Setup - set vBucket to a non-active state.
    store->setVBucketState(vbid, vbucket_state_replica);
    auto vb = store->getVBucket(vbid);
    ASSERT_EQ(1, vb->failovers->getNumEntries());

    // Test
    EXPECT_EQ(cb::engine_errc::success,
              store->setVBucketState(vbid, vbucket_state_active));
    EXPECT_EQ(2, vb->failovers->getNumEntries());
}

// MB-33702: Test that SetVBucket state doesn't create a new failover table
// entry when set to active when already active - this can happen if the
// replication topology is changed (but state stays as active).
TEST_P(KVBucketParamTest, FailoverEntryNotAddedActiveToActive) {
    // Setup - Should start in active state.
    auto vb = store->getVBucket(vbid);
    ASSERT_EQ(vbucket_state_active, vb->getState());
    ASSERT_EQ(1, vb->failovers->getNumEntries());

    // Test - with a topology specified, we shouldn't get a new failover entry.
    auto meta =
            nlohmann::json{{"topology", nlohmann::json::array({{"a", "b"}})}};
    EXPECT_EQ(cb::engine_errc::success,
              store->setVBucketState(vbid, vbucket_state_active, &meta));
    EXPECT_EQ(1, vb->failovers->getNumEntries());
}

// Test that expiring a compressed xattr doesn't trigger any errors
TEST_P(KVBucketParamTest, MB_34346) {
    // Create an XTTR value with only a large system xattr, and compress the lot
    // Note the large xattr should be highly compressible to make it easier to
    // trigger the MB.
    cb::xattr::Blob blob;
    blob.set("_sync",
             R"({"fffff":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"})");
    auto xattr = blob.finalize();
    cb::compression::Buffer output;
    ASSERT_TRUE(deflateSnappy({xattr.data(), xattr.size()}, output));
    EXPECT_LT(output.size(), xattr.size())
            << "Expected the compressed buffer to be smaller than the input";

    auto key = makeStoredDocKey("key_1");
    store_item(
            vbid,
            key,
            {output.data(), output.size()},
            ep_convert_to_expiry_time(10),
            {cb::engine_errc::success},
            PROTOCOL_BINARY_DATATYPE_XATTR | PROTOCOL_BINARY_DATATYPE_SNAPPY);

    flushVBucketToDiskIfPersistent(vbid, 1);

    EXPECT_EQ(1, engine->getVBucket(vbid)->getNumItems())
            << "Should have 1 item after calling store()";

    TimeTraveller docBrown(15);

    auto options = static_cast<get_options_t>(
            QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE | DELETE_TEMP |
            HIDE_LOCKED_CAS | TRACK_STATISTICS | GET_DELETED_VALUE);
    GetValue gv2 = store->get(key, vbid, cookie, options);
    EXPECT_TRUE(gv2.item->isDeleted());
    // Check that the datatype does not include SNAPPY
    EXPECT_EQ(PROTOCOL_BINARY_DATATYPE_XATTR, gv2.item->getDataType());
    // Check the returned blob is what we initially set
    cb::xattr::Blob returnedBlob(
            {const_cast<char*>(gv2.item->getData()), gv2.item->getNBytes()},
            false);
    EXPECT_EQ(
            "{\"fffff\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"}",
            returnedBlob.get("_sync"));

    flushVBucketToDiskIfPersistent(vbid, 1);

    EXPECT_EQ(0, engine->getVBucket(vbid)->getNumItems())
            << "Should still have 0 items after time-travelling/expiry";
}

// Test that calling getPerVBucketDiskStats when a vBucket file hasn't yet been
// flushed to disk doesn't throw an exception.
// Regression test for MB-35560.
TEST_P(KVBucketParamTest, VBucketDiskStatsENOENT) {
    bool addStatsCalled = false;
    auto mockStatFn = [&addStatsCalled](std::string_view key,
                                        std::string_view value,
                                        CookieIface&) {
        addStatsCalled = true;
    };

    auto expected = (isPersistent()) ? cb::engine_errc::success
                                     : cb::engine_errc::no_such_key;
    MockCookie cookie;
    EXPECT_EQ(expected, store->getPerVBucketDiskStats(cookie, mockStatFn));
    EXPECT_EQ(isPersistent(), addStatsCalled);
}

TEST_P(KVBucketParamTest, VbucketStateCounts) {
    // confirm the vbMap correctly changes the number of vbuckets in a given
    // state when vbuckets change state
    auto vbA = Vbid(0);
    auto vbB = Vbid(1);

    auto expectVbCounts = [this](uint16_t active, uint16_t replica) {
        auto message = "Expected " + std::to_string(active) + " active and " +
                       std::to_string(replica) + " replica vbs";
        EXPECT_EQ(active, store->getNumOfVBucketsInState(vbucket_state_active))
                << message;
        EXPECT_EQ(replica,
                  store->getNumOfVBucketsInState(vbucket_state_replica))
                << message;
    };
    store->setVBucketState(vbA, vbucket_state_active);
    expectVbCounts(1, 0);
    store->setVBucketState(vbB, vbucket_state_active);
    expectVbCounts(2, 0);
    store->setVBucketState(vbA, vbucket_state_replica);
    expectVbCounts(1, 1);
    store->setVBucketState(vbB, vbucket_state_replica);
    expectVbCounts(0, 2);
}

TEST_P(KVBucketParamTest, SeqnoPersistenceTimeout) {
    auto& config = engine->getConfiguration();
    const size_t newTimeout = 20;
    ASSERT_NE(newTimeout, store->getSeqnoPersistenceTimeout().count());
    config.setSeqnoPersistenceTimeout(newTimeout);
    EXPECT_EQ(newTimeout, store->getSeqnoPersistenceTimeout().count());
}

TEST_P(KVBucketParamTest, SeqnoPersistenceTimeout_LowerThanMin) {
    auto& config = engine->getConfiguration();
    const auto initialVal = store->getSeqnoPersistenceTimeout().count();
    const auto newVal = -1;
    ASSERT_NE(initialVal, newVal);
    try {
        config.setSeqnoPersistenceTimeout(newVal);
    } catch (const std::range_error& e) {
        EXPECT_THAT(e.what(),
                    testing::HasSubstr(
                            "Validation Error, seqno_persistence_timeout takes "
                            "values between 0 and 30"));
        EXPECT_EQ(initialVal, store->getSeqnoPersistenceTimeout().count());
        return;
    }
    FAIL();
}

TEST_P(KVBucketParamTest, SeqnoPersistenceTimeout_HigherThanMax) {
    auto& config = engine->getConfiguration();
    const auto initialVal = store->getSeqnoPersistenceTimeout().count();
    const auto newVal = 40;
    ASSERT_NE(initialVal, newVal);
    try {
        config.setSeqnoPersistenceTimeout(newVal);
    } catch (const std::range_error& e) {
        EXPECT_THAT(e.what(),
                    testing::HasSubstr(
                            "Validation Error, seqno_persistence_timeout takes "
                            "values between 0 and 30"));
        EXPECT_EQ(initialVal, store->getSeqnoPersistenceTimeout().count());
        return;
    }
    FAIL();
}

// Regression test for MB-51391: If we end up with multiple concurrent attempts
// to delete a vBucket, don't crash attempting to dereference a null
// VBucketPtr.
TEST_P(KVBucketParamTest, DeleteVBucket_ConcurrentDelete) {
    // Setup: Acquire exclusive lock on VBucket, so other threads will be
    // blocked waiting on ot.
    auto lockedVB = store->getLockedVBucket(vbid);
    ASSERT_TRUE(lockedVB);

    folly::Synchronized<std::multiset<cb::engine_errc>> results;
    // ThreadGate to increase the liklihood of hitting the race - ensure
    // both background threads are up and running before unlocking the lockedVB.
    ThreadGate gate{3};

    // Spin up a background thread which attempts to delete the vbucket. This
    // will be blocked waiting to acquire the locked vbucket.
    auto delThread1 = std::thread{[this, &gate, &results]() {
        gate.threadUp();
        auto result = store->deleteVBucket(vbid);
        results.wlock()->insert(result);
    }};

    // Spin up a second background thread which also attempts to delete the
    // vBucket - also blocked.
    auto delThread2 = std::thread{[this, &gate, &results]() {
        gate.threadUp();
        auto result = store->deleteVBucket(vbid);
        results.wlock()->insert(result);
    }};

    // Release the hounds^wlock - allowing the first background thread to
    // delete the vbucket, and once it has finishes, allowing the second
    // background thread to attempt to delete - which should cleanly fail.
    // (Note: technically there's no guarantee which of the two background
    // threads will be the first to run after the lock is released - but one
    // should delete it, one should fail and neither should crash.
    gate.threadUp();
    lockedVB.getLock().unlock();

    delThread1.join();
    delThread2.join();

    EXPECT_EQ(1, results.rlock()->count(cb::engine_errc::success));
    EXPECT_EQ(1, results.rlock()->count(cb::engine_errc::not_my_vbucket));
}

TEST_P(KVBucketParamTest, SeqnoPersistenceCancelledIfBucketDeleted) {
    // Test that seqno persistence requests are cancelled when the bucket is
    // deleted. If they were not, the would_block'ed connection could
    // block bucket deletion.
    auto vbid = Vbid(0);
    store->setVBucketState(vbid, vbucket_state_replica);
    auto vb = engine->getVBucket(vbid);

    // Simulate a SEQNO_PERSISTENCE request, which won't be satisfied
    ASSERT_EQ(0, vb->getHighPriorityChkSize());
    vb->checkAddHighPriorityVBEntry(std::make_unique<SeqnoPersistenceRequest>(
            cookie, 1, std::chrono::milliseconds(1)));
    ASSERT_EQ(1, vb->getHighPriorityChkSize());

    engine->cancel_all_operations_in_ewb_state();

    EXPECT_EQ(cb::engine_errc::temporary_failure, mock_waitfor_cookie(cookie));
}

TEST_P(KVBucketParamTest, HistoryRetentionSeconds) {
    if (isNexus() || !hasMagma()) {
        GTEST_SKIP();
    }

    const size_t newVal = 123;
    ASSERT_NE(newVal, store->getHistoryRetentionSeconds().count());
    std::string err;
    engine->setFlushParam(
            "history_retention_seconds", std::to_string(newVal), err);
    EXPECT_EQ(newVal, store->getHistoryRetentionSeconds().count());
}

TEST_P(KVBucketParamTest, HistoryRetentionSeconds_NotPersistent) {
    if (!ephemeral()) {
        GTEST_SKIP();
    }

    const auto initialVal = store->getHistoryRetentionSeconds().count();
    const auto newVal = 123;
    ASSERT_NE(initialVal, newVal);
    std::string err;
    EXPECT_EQ(
            cb::engine_errc::invalid_arguments,
            engine->setFlushParam(
                    "history_retention_seconds", std::to_string(newVal), err));
    EXPECT_THAT(err, testing::HasSubstr("requirements not met"));
}

TEST_P(KVBucketParamTest, HistoryRetentionSeconds_NotMagma) {
    if (hasMagma()) {
        GTEST_SKIP();
    }

    const auto initialVal = store->getHistoryRetentionSeconds().count();
    const auto newVal = 123;
    ASSERT_NE(initialVal, newVal);
    std::string err;
    EXPECT_EQ(
            cb::engine_errc::invalid_arguments,
            engine->setFlushParam(
                    "history_retention_seconds", std::to_string(newVal), err));
    EXPECT_THAT(err, testing::HasSubstr("requirements not met"));
}

TEST_P(KVBucketParamTest, HistoryRetentionBytes) {
    if (isNexus() || !hasMagma()) {
        GTEST_SKIP();
    }

    const size_t newVal = 456;
    ASSERT_NE(newVal, store->getHistoryRetentionBytes());
    std::string err;
    engine->setFlushParam(
            "history_retention_bytes", std::to_string(newVal), err);
    EXPECT_EQ(newVal, store->getHistoryRetentionBytes());
}

TEST_P(KVBucketParamTest, HistoryRetentionBytes_NotPersistent) {
    if (!ephemeral()) {
        GTEST_SKIP();
    }

    const auto initialVal = store->getHistoryRetentionBytes();
    const auto newVal = 456;
    ASSERT_NE(initialVal, newVal);
    std::string err;
    EXPECT_EQ(cb::engine_errc::invalid_arguments,
              engine->setFlushParam(
                      "history_retention_bytes", std::to_string(newVal), err));
    EXPECT_THAT(err, testing::HasSubstr("requirements not met"));
}

TEST_P(KVBucketParamTest, HistoryRetentionBytes_NotMagma) {
    if (hasMagma()) {
        GTEST_SKIP();
    }

    const auto initialVal = store->getHistoryRetentionBytes();
    const auto newVal = 456;
    ASSERT_NE(initialVal, newVal);
    std::string err;
    EXPECT_EQ(cb::engine_errc::invalid_arguments,
              engine->setFlushParam(
                      "history_retention_bytes", std::to_string(newVal), err));
    EXPECT_THAT(err, testing::HasSubstr("requirements not met"));
}

TEST_P(KVBucketParamTest, MutationMemRatio) {
    auto& config = engine->getConfiguration();
    const auto initRatio = config.getMutationMemRatio();
    const auto newRatio = 0.9345f;
    ASSERT_NE(newRatio, store->getMutationMemRatio());
    ASSERT_FLOAT_EQ(initRatio, store->getMutationMemRatio());
    config.setMutationMemRatio(newRatio);
    EXPECT_FLOAT_EQ(newRatio, config.getMutationMemRatio());
    EXPECT_FLOAT_EQ(newRatio, store->getMutationMemRatio());
}

TEST_P(KVBucketParamTest, MutationMemRatio_LowerThanMin) {
    auto& config = engine->getConfiguration();
    const auto initRatio = config.getMutationMemRatio();
    const auto newRatio = -0.001f;
    ASSERT_NE(newRatio, store->getMutationMemRatio());
    ASSERT_FLOAT_EQ(initRatio, store->getMutationMemRatio());
    try {
        config.setMutationMemRatio(newRatio);
    } catch (const std::range_error& ex) {
        EXPECT_THAT(
                ex.what(),
                testing::HasSubstr("Validation Error, mutation_mem_ratio takes "
                                   "values between 0.000000 and 1.000000"));
        EXPECT_FLOAT_EQ(initRatio, store->getMutationMemRatio());
        return;
    }
    FAIL();
}

TEST_P(KVBucketParamTest, MutationMemRatio_HigherThanMax) {
    auto& config = engine->getConfiguration();
    const auto initRatio = config.getMutationMemRatio();
    const auto newRatio = 1.001f;
    ASSERT_NE(newRatio, store->getMutationMemRatio());
    ASSERT_FLOAT_EQ(initRatio, store->getMutationMemRatio());
    try {
        config.setMutationMemRatio(newRatio);
    } catch (const std::range_error& ex) {
        EXPECT_THAT(
                ex.what(),
                testing::HasSubstr("Validation Error, mutation_mem_ratio takes "
                                   "values between 0.000000 and 1.000000"));
        EXPECT_FLOAT_EQ(initRatio, store->getMutationMemRatio());
        return;
    }
    FAIL();
}

class EPBucketParamTest : public KVBucketParamTest {
};

TEST_P(EPBucketParamTest, FlushBatchMaxBytes) {
    auto& config = engine->getConfiguration();
    const auto initialSize = config.getFlushBatchMaxBytes();
    const auto newSize = 5368709120ll; // 5GB
    const auto& bucket = dynamic_cast<EPBucket&>(*store);
    ASSERT_NE(newSize, bucket.getFlushBatchMaxBytes());
    ASSERT_EQ(initialSize, bucket.getFlushBatchMaxBytes());
    config.setFlushBatchMaxBytes(newSize);
    EXPECT_EQ(newSize, config.getFlushBatchMaxBytes());
    EXPECT_EQ(newSize, bucket.getFlushBatchMaxBytes());
}

TEST_P(EPBucketParamTest, FlushBatchMaxBytes_LowerThanMin) {
    auto& config = engine->getConfiguration();
    const auto initialSize = config.getFlushBatchMaxBytes();
    const auto& bucket = dynamic_cast<EPBucket&>(*store);
    ASSERT_NE(0, bucket.getFlushBatchMaxBytes());
    ASSERT_EQ(initialSize, bucket.getFlushBatchMaxBytes());
    try {
        config.setFlushBatchMaxBytes(0);
        FAIL();
    } catch (const std::range_error& ex) {
        EXPECT_THAT(ex.what(),
                    testing::HasSubstr(
                            "Validation Error, flush_batch_max_bytes takes "
                            "values between 1"));
        EXPECT_EQ(initialSize, bucket.getFlushBatchMaxBytes());
    }
}

// Test cases which run for EP (Full and Value eviction) and Ephemeral
INSTANTIATE_TEST_SUITE_P(
        Persistent,
        EPBucketParamTest,
        STParameterizedBucketTest::persistentAllBackendsConfigValues(),
        STParameterizedBucketTest::PrintToStringParamName);

class StoreIfTest : public KVBucketTest {
public:
    void SetUp() override {
        config_string += "warmup=false";
        KVBucketTest::SetUp();
        // Have all the objects, activate vBucket zero so we can store data.
        store->setVBucketState(vbid, vbucket_state_active);
    }
};

/**
 * Test the basic store_if (via engine) - a forced fail predicate will allow
 * add, but fail set/replace with predicate_failed
 */
TEST_F(StoreIfTest, store_if_basic) {
    cb::StoreIfPredicate pred = [](const std::optional<item_info>& existing,
                                   cb::vbucket_info vb) -> cb::StoreIfStatus {
        return cb::StoreIfStatus::Fail;
    };
    auto item = make_item(
            vbid, {"key", DocKeyEncodesCollectionId::No}, "value", 0, 0);
    auto rv = engine->storeIfInner(
            *cookie, item, 0, StoreSemantics::Add, pred, false);
    EXPECT_EQ(cb::engine_errc::success, rv.first);
    rv = engine->storeIfInner(
            *cookie, item, 0, StoreSemantics::Replace, pred, false);
    EXPECT_EQ(cb::engine_errc::predicate_failed, rv.first);
    rv = engine->storeIfInner(
            *cookie, item, 0, StoreSemantics::Set, pred, false);
    EXPECT_EQ(cb::engine_errc::predicate_failed, rv.first);
}

class RelativeExpiryLimitTest : public KVBucketTest {
public:
    void SetUp() override {
        config_string += "max_ttl=2592000";
        KVBucketTest::SetUp();
        // Have all the objects, activate vBucket zero so we can store data.
        store->setVBucketState(vbid, vbucket_state_active);
    }
};

// Test add/set/replace gets an enforced expiry
TEST_F(RelativeExpiryLimitTest, add_set_replace) {
    auto item1 = make_item(vbid, makeStoredDocKey("key1"), "value");
    auto item2 = make_item(vbid, makeStoredDocKey("key2"), "value");
    auto item3 = make_item(vbid, makeStoredDocKey("key2"), "value");

    ASSERT_EQ(0, item1.getExptime());
    ASSERT_EQ(0, item2.getExptime());

    // add a key and set a key
    EXPECT_EQ(cb::engine_errc::success, store->add(item1, cookie));
    EXPECT_EQ(cb::engine_errc::success, store->set(item2, cookie));

    std::vector<cb::EngineErrorItemPair> results;

    auto f = [](const item_info&) { return true; };
    results.push_back(engine->getIfInner(*cookie, item1.getKey(), vbid, f));
    results.push_back(engine->getIfInner(*cookie, item2.getKey(), vbid, f));

    // finally replace key2
    EXPECT_EQ(cb::engine_errc::success, store->replace(item3, cookie));
    results.push_back(engine->getIfInner(*cookie, item2.getKey(), vbid, f));

    for (const auto& rval : results) {
        ASSERT_EQ(cb::engine_errc::success, rval.first);
        Item* i = reinterpret_cast<Item*>(rval.second.get());
        auto info = engine->getItemInfo(*i);
        EXPECT_NE(0, info.exptime);
    }
}

// Test that GAT with a limit stops 0 expiry
TEST_F(RelativeExpiryLimitTest, gat) {
    // This will actually skip the initial expiry limiting code as this function
    // doesn't use itemAllocate
    Item item = store_item(
            vbid, {"key", DocKeyEncodesCollectionId::No}, "value", 0);

    // Now touch with 0
    auto rval = engine->getAndTouchInner(
            *cookie, {"key", DocKeyEncodesCollectionId::No}, vbid, 0);

    ASSERT_EQ(cb::engine_errc::success, rval.first);

    Item* i = reinterpret_cast<Item*>(rval.second.get());
    auto info = engine->getItemInfo(*i);
    EXPECT_NE(0, info.exptime);
}

class AbsoluteExpiryLimitTest : public KVBucketTest {
public:
    void SetUp() override {
        config_string += "max_ttl=2592001";
        KVBucketTest::SetUp();
        // Have all the objects, activate vBucket zero so we can store data.
        store->setVBucketState(vbid, vbucket_state_active);
    }
};

TEST_F(AbsoluteExpiryLimitTest, MB_37643) {
    // Go forwards by 30 days + 1000 seconds, we want uptime > max_ttl
    const int uptime = (60 * 60 * 24 * 30) + 1000;
    TimeTraveller biff(uptime);

    auto item = store_item(Vbid(0), makeStoredDocKey("key"), "value");

    // We expect that the expiry time is in the future. The future here being
    // current_time + our time shift.

    // We expect that the expiry is at least now+uptime+max_ttl
    EXPECT_GT(item.getExptime(), ep_abs_time(ep_current_time()));
}

TEST_P(KVBucketParamTest, addVbucketWithSeqnoPersistenceRequest) {
    store->createAndScheduleSeqnoPersistenceNotifier();

    auto ts1 = cb::time::steady_clock::now();
    auto ts2 = ts1 + std::chrono::hours(24);
    // snoozing forever, definitely greater than ts2
    EXPECT_GT(store->getSeqnoPersistenceNotifyTaskWakeTime(), ts2);
    store->addVbucketWithSeqnoPersistenceRequest(vbid, ts1);
    store->addVbucketWithSeqnoPersistenceRequest(vbid, ts2);
    // Must be lower of ts1/ts2
    EXPECT_LT(ts1, ts2);
    // Can't compare exactly as the snooze time is calculated from
    // cb::time::steady_clock::now
    EXPECT_LT(store->getSeqnoPersistenceNotifyTaskWakeTime(), ts2);
}

TEST_P(KVBucketParamTest, SeqnoPersistenceRequestNotify) {
    auto task = store->createAndScheduleSeqnoPersistenceNotifier();
    auto wake1 = task->getWaketime(); // should be sleep forever
    auto vb = store->getVBucket(vbid);

    auto item = store_item(vbid, makeStoredDocKey("key"), "value");
    flushVBucketToDiskIfPersistent(vbid, 1);

    // Already persisted, no need for a wait request
    EXPECT_EQ(HighPriorityVBReqStatus::RequestNotScheduled,
              vb->checkAddHighPriorityVBEntry(
                      std::make_unique<SeqnoPersistenceRequest>(
                              cookie,
                              item.getBySeqno(),
                              std::chrono::seconds(0))));

    // Now a request that will schedule
    EXPECT_EQ(HighPriorityVBReqStatus::RequestScheduled,
              vb->checkAddHighPriorityVBEntry(
                      std::make_unique<SeqnoPersistenceRequest>(
                              cookie,
                              item.getBySeqno() + 1,
                              std::chrono::hours(24))));

    // Task now changed and has a smaller wakeup
    EXPECT_LT(task->getWaketime(), wake1);

    // if the task runs, the waketime will recalculate
    task->run();
    auto wake2 = task->getWaketime();
    EXPECT_LT(wake2, wake1);

    // Now a request that will schedule with a smaller deadline
    auto cookie2 = create_mock_cookie(engine.get());
    EXPECT_EQ(HighPriorityVBReqStatus::RequestScheduled,
              vb->checkAddHighPriorityVBEntry(
                      std::make_unique<SeqnoPersistenceRequest>(
                              cookie2,
                              item.getBySeqno() + 1,
                              std::chrono::hours(22))));
    // Task wake time reduces
    auto wake3 = task->getWaketime();
    EXPECT_LT(wake3, wake2);

    // Next task with 0 deadline (which will expire on run)
    auto cookie3 = create_mock_cookie(engine.get());

    EXPECT_EQ(HighPriorityVBReqStatus::RequestScheduled,
              vb->checkAddHighPriorityVBEntry(
                      std::make_unique<SeqnoPersistenceRequest>(
                              cookie3,
                              item.getBySeqno() + 1,
                              std::chrono::hours(0))));
    auto wake4 = task->getWaketime();
    EXPECT_LT(wake4, wake2);

    // Now we run, the 0 deadline request will expire and wake time will change
    // to be the next deadline (~22 hours)
    task->run();
    EXPECT_GT(task->getWaketime(), wake4);

    // Expired and notified
    EXPECT_EQ(cb::engine_errc::temporary_failure, mock_waitfor_cookie(cookie3));

    destroy_mock_cookie(cookie2);
    destroy_mock_cookie(cookie3);
}

TEST_P(KVBucketParamTest, HashTableMinimumSize) {
    auto& config = engine->getConfiguration();
    auto& ht = store->getVBucket(vbid)->ht;
    EXPECT_EQ(config.getHtSize(), ht.minimumSize());
    EXPECT_EQ(config.getHtSize(), ht.getSize());

    config.setHtSize(3);
    EXPECT_EQ(3, config.getHtSize());
    EXPECT_EQ(3, ht.minimumSize());
    EXPECT_NE(3, ht.getSize());

    ht.resizeInOneStep();
    EXPECT_EQ(3, config.getHtSize());
    EXPECT_EQ(3, ht.minimumSize());
    EXPECT_EQ(3, ht.getSize());

    config.setHtSize(47);
    EXPECT_EQ(47, config.getHtSize());
    EXPECT_EQ(47, ht.minimumSize());
    EXPECT_EQ(3, ht.getSize());

    ht.resizeInOneStep();
    EXPECT_EQ(47, config.getHtSize());
    EXPECT_EQ(47, ht.minimumSize());
    EXPECT_EQ(47, ht.getSize());
}

TEST_P(KVBucketParamTest, BloomFilter) {
    if (ephemeral() || !fullEviction()) {
        GTEST_SKIP_("Applicable to only full-eviction buckets");
    }
    auto docKey = makeStoredDocKey("foo");
    auto item = store_item(vbid, docKey, "bar");
    auto vb = store->getVBucket(vbid);

    flushVBucketToDiskIfPersistent(vbid, 1);
    evict_key(vbid, docKey);
    EXPECT_TRUE(vb->maybeKeyExistsInFilter(docKey));
}

TEST_P(KVBucketParamTest, HashTableTempItemAllowed) {
    auto& config = engine->getConfiguration();
    auto& ht = store->getVBucket(vbid)->ht;

    // Expect to be configured with the default value first -
    // ht_temp_items_allowed_percent=10.
    EXPECT_EQ(10, config.getHtTempItemsAllowedPercent());
    // Expect to be configured with the default value first -
    // ht_size=47.
    EXPECT_EQ(47, ht.getSize());
    EXPECT_EQ(4, ht.getNumTempItemsAllowed());

    config.setHtTempItemsAllowedPercent(15);
    EXPECT_EQ(15, config.getHtTempItemsAllowedPercent());

    // resize hasn't been run - numTempItemsAllowed shouldn't have been updated.
    EXPECT_EQ(4, ht.getNumTempItemsAllowed());
    ht.resizeInOneStep();
    EXPECT_EQ(7, ht.getNumTempItemsAllowed());

    config.setHtSize(3);
    ht.resizeInOneStep();
    EXPECT_EQ(3, ht.getSize());
    EXPECT_EQ(0, ht.getNumTempItemsAllowed());

    config.setHtTempItemsAllowedPercent(40);
    EXPECT_EQ(40, config.getHtTempItemsAllowedPercent());
    ht.resizeInOneStep();
    EXPECT_EQ(1, ht.getNumTempItemsAllowed());
}

// Test cases which run for EP (Full and Value eviction) and Ephemeral
INSTANTIATE_TEST_SUITE_P(EphemeralOrPersistent,
                         KVBucketParamTest,
                         STParameterizedBucketTest::allConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

// No data stored in this test - only need a persistent bucket config
class AccessLogKVBucketParamTest : public KVBucketParamTest {
public:
    void SetUp() override {
        KVBucketParamTest::SetUp();
    }
    void TearDown() override {
        KVBucketParamTest::TearDown();
    }
};

// Coverage for MB-69134
TEST_P(AccessLogKVBucketParamTest, NoLogsWhenNoVBucketsMapToShard) {
    ASSERT_EQ(store->getVBuckets().getNumShards(), 2);
    engine->getConfiguration().setAlogPath(
            test_dbname + cb::io::DirectorySeparator + "access.log");
    engine->getConfiguration().setAlogResidentRatioThreshold(100);
    auto as = std::make_unique<MockAccessScanner>(*(engine->getKVBucket()),
                                                  engine->getConfiguration(),
                                                  engine->getEpStats());

    auto& auxIoQ = *task_executor->getLpTaskQ(TaskType::AuxIO);
    auto count = auxIoQ.getFutureQueueSize();
    as->run();
    // Only one task gets scheduled
    EXPECT_EQ(count + 1, auxIoQ.getFutureQueueSize())
            << "Expected one task to be scheduled as only one shard has "
               "vbuckets";

    // Need to run the task so it destructs before the 'as' object is destroyed.
    runNextTask(auxIoQ, "Item Access Scanner no vbucket assigned");
}

INSTANTIATE_TEST_SUITE_P(Persistent,
                         AccessLogKVBucketParamTest,
                         STParameterizedBucketTest::couchstoreBucket(),
                         STParameterizedBucketTest::PrintToStringParamName);