/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/*
 * Unit tests for the KVBucket class.
 *
 * These tests are instantiated with additional config strings to test over
 * ephemeral and value and full eviction persistent buckets.
 *
 */

#pragma once

#include "ep_types.h"
#include "kv_bucket.h"

#include <folly/portability/GTest.h>
#include <memcached/dockey_view.h>
#include <memcached/durability_spec.h>
#include <memcached/engine_error.h>
#include <memcached/storeddockey_fwd.h>
#include <tests/ep_request_utils.h>
#include <tests/mock/mock_synchronous_ep_engine.h>

#include <memory>
#include <unordered_map>

class CheckpointDestroyerTask;
class CookieIface;
class CouchKVStoreConfig;
class GetValue;
class FileOpsInterface;
class ItemMetaData;
class KVBucket;
class MagmaKVStoreConfig;
class VBucket;

namespace Collections {
class Manager;
namespace VB {
struct PersistedStats;
}
}

/**
 * Test fixture for KVBucket unit tests.
 *
 * Will create the appropriate subclass of KVBucket (EPBucket /
 * EphemeralBucket) based on the Configuration passed (specifically the
 * bucket_type parameter), defaulting to EPBucket if no bucket_type is
 * specified.
 *
 * These tests are normally "low-ish level" directly manipulating KVBucket and
 * lower objects - i.e. they don't go via the EngineIface as the front-end
 * normally does.
 * As such, the test fixture's SetUp() method will switch the calling thread
 * to be associated with the newly-created engine, so any allocations performed
 * by the KVBucket (or anywhere else) will by default be associated with the
 * KVBucket. If this is not desired for some tests, they should switch away
 * using NonBucketAllocationGuard (scoped).
 */
class KVBucketTest : virtual public ::testing::Test {
public:
    KVBucketTest();

    void SetUp() override;

    void TearDown() override;

    // Stores an item into the given vbucket. Returns the item stored.
    Item store_item(
            Vbid vbid,
            const DocKeyView& key,
            const std::string& value,
            uint32_t exptime = 0,
            const std::vector<cb::engine_errc>& expected =
                    {cb::engine_errc::success},
            protocol_binary_datatype_t datatype = PROTOCOL_BINARY_DATATYPE_JSON,
            std::optional<cb::durability::Requirements> reqs = {},
            bool deleted = false);

    // Stores a tombstone that can have a value
    Item store_deleted_item(
            Vbid vbid,
            const DocKeyView& key,
            const std::string& value,
            uint32_t exptime = 0,
            const std::vector<cb::engine_errc>& expected =
                    {cb::engine_errc::success},
            protocol_binary_datatype_t datatype = PROTOCOL_BINARY_DATATYPE_JSON,
            std::optional<cb::durability::Requirements> reqs = {});

    /**
     * Store multiple items into the vbucket, the given key will have an
     * iteration appended to it.
     */
    [[nodiscard]] ::testing::AssertionResult store_items(
            int nitems,
            Vbid vbid,
            const DocKeyView& key,
            const std::string& value,
            uint32_t exptime = 0,
            protocol_binary_datatype_t datatype =
                    PROTOCOL_BINARY_DATATYPE_JSON);

    /**
     * Stores an item to a replica vbucket, returns an assert if the set succeed
     * or not.
     */
    [[nodiscard]] ::testing::AssertionResult store_item_replica(
            Vbid vbid,
            const DocKeyView& key,
            const std::string& value,
            uint64_t seqno,
            uint32_t exptime = 0,
            const cb::engine_errc expected = cb::engine_errc::success,
            protocol_binary_datatype_t datatype = PROTOCOL_BINARY_DATATYPE_JSON,
            std::optional<cb::durability::Requirements> reqs = {},
            bool deleted = false);

    /**
     * Store a pending item with default level of majority
     */
    Item store_pending_item(
            Vbid vbid,
            const DocKeyView& key,
            const std::string& value,
            uint32_t exptime = 0,
            const std::vector<cb::engine_errc>& expected =
                    {cb::engine_errc::sync_write_pending},
            protocol_binary_datatype_t datatype = PROTOCOL_BINARY_RAW_BYTES,
            std::optional<cb::durability::Requirements> reqs =
                    cb::durability::Requirements(
                            cb::durability::Level::Majority,
                            cb::durability::Timeout::Infinity()),
            bool deleted = false);

    /**
     * Store the specified number of items into the given collection.
     * @param collection Collection to store to.
     * @param items Number of items to store.
     * @param expected The expected status code when storing.
     * @param valueSize Size of the value in bytes.
     */
    void storeItems(CollectionID collection,
                    int items,
                    cb::engine_errc expected = cb::engine_errc::success,
                    size_t valueSize = 5);

    /* Flush the given vbucket to disk, so any outstanding dirty items are
     * written (and are clean).
     */
    void flush_vbucket_to_disk(Vbid vbid, size_t expected = 1);

    /**
     * Flushes the given vBucket to disk, so any outstanding dirty items are
     * written
     * @param vbid VBucket to flush
     * @returns the number of items flushed.
     */
    int flushVBucket(Vbid vbid);

    /**
     * Check if the current bucket is a persistent bucket.
     */
    bool persistent() const;

    /**
     * Flush the given vBucket to disk if the bucket is peristent, otherwise
     * do nothing.
     * @param vbid vBucket to flush
     * @param expected Expected number of items to be flushed.
     */
    void flushVBucketToDiskIfPersistent(Vbid vbid, int expected = 1);

    /**
     * Creates a new open checkpoint and removes all the closed ones.
     */
    void removeCheckpoint(VBucket& vb);

    void flushAndRemoveCheckpoints(Vbid vbid);

    /**
     * @param vbid
     * @return the number of expelled items
     */
    size_t flushAndExpelFromCheckpoints(Vbid vbid);

    /**
     * Delete the given item from the given vbucket, verifying it was
     * successfully deleted.
     */
    void delete_item(Vbid vbid,
                     const DocKeyView& key,
                     cb::engine_errc expected = cb::engine_errc::success);

    /**
     * Store and delete a given key
     *
     * @param vbid   vbucket id where the key needs to be stored
     * @param key    key that needs to be stored and deleted
     * @param value  value for the key
     */
    void storeAndDeleteItem(Vbid vbid,
                            const DocKeyView& key,
                            std::string value);

    /* Evict the given key from memory according to the current eviction
     * strategy. Verifies it was successfully evicted.
     */
    void evict_key(Vbid vbid, const DocKeyView& key);

    /// Exposes the normally-protected getInternal method from the store.
    GetValue getInternal(const DocKeyView& key,
                         Vbid vbucket,
                         CookieIface* cookie,
                         ForGetReplicaOp getReplicaItem,
                         get_options_t options);

    /**
     * Get the meta data for a given key
     *
     * @param vbid     vbucket id where the key needs to be stored
     * @param key      key for which meta data needs to be retrieved
     * @param cookie   cookie for the connection
     * @param itemMeta meta data for the item
     * @param deleted  whether deleted or not
     * @param datatype datatype of the item
     * @param retryOnEWouldBlock whether to bgfetch and repeat the
     * request on a persistent bucket if the result is EWOULDBLOCK
     *
     * @result engine error code signifying result of the operation
     */
    cb::engine_errc getMeta(Vbid vbid,
                            const DocKeyView key,
                            CookieIface* cookie,
                            ItemMetaData& itemMeta,
                            uint32_t& deleted,
                            uint8_t& datatype,
                            bool retryOnEWouldBlock = true);

    /**
     * Use the vbucket setWithMeta function to write a mutation to a replica.
     * This allows tests to populate replicas directly without having to drive
     * a PassiveStream. The Item's value is set to "value" and internally the
     * function expects a successful store.
     *
     * @param vbid vbucket id where the key needs to be stored
     * @param key the key to store
     * @param seqno the seqno of the item
     * @param prepare true write a prepare, false a mutation
     */
    void writeDocToReplica(Vbid vbid,
                           StoredDocKey key,
                           uint64_t seqno,
                           bool prepare);

    /**
     * Schedules the ItemPager according to the current config. Allows testing
     * of the item pager from subclasses, without KVBucket having to grant
     * friendship to many different test classes.
     */
    void scheduleItemPager();

    void initializeExpiryPager();

    void initializeInitialMfuUpdater();

    /**
     * Schedules the CheckpointMemRecoveryTask.
     */
    void scheduleCheckpointRemoverTask();

    /**
     * Schedules the CheckpointDestroyerTask.
     *
     * Tests can chose to drive checkpoint removal directly through the manager
     * or by running the appropriate task.
     *
     * Tests which verify the CheckpointDestroyerTask notification/waking
     * require the task to be scheduled before use.
     */
    void scheduleCheckpointDestroyerTasks();

    /**
     * Allow test access to all checkpoint destroyer tasks.
     */
    const KVBucket::CheckpointDestroyers& getCheckpointDestroyerTasks() const;

    /**
     * Convenience method to run the background fetcher task once (in the
     * current thread).
     */
    void runBGFetcherTask();

    /**
     * Effectively shutdown/restart. This destroys the test engine/store/cookie
     * and re-creates them.
     *
     * @param force Force the shutdown making it appear unclean
     */
    void reinitialise(std::string config, bool force = false);

    /**
     * Create a *_with_meta packet with the key/body
     * Allows *_with_meta to be invoked via EventuallyPersistentEngine which
     * begins with a packet
     */
    static std::vector<char> buildWithMetaPacket(
            cb::mcbp::ClientOpcode opcode,
            protocol_binary_datatype_t datatype,
            Vbid vbucket,
            uint32_t opaque,
            uint64_t cas,
            ItemMetaData metaData,
            std::string_view key,
            std::string_view body,
            const std::vector<char>& emd = {},
            int options = 0);

    static bool addResponse(std::string_view key,
                            std::string_view extras,
                            std::string_view body,
                            ValueIsJson json,
                            cb::mcbp::Status status,
                            uint64_t pcas,
                            CookieIface& cookie);

    static cb::mcbp::Status getAddResponseStatus(
            cb::mcbp::Status newval = cb::mcbp::Status::Success);

    static cb::mcbp::Status addResponseStatus;

    // path for the test's database files.
    const std::string test_dbname;

    /**
     * The completeWarmup boolean is read by ::SetUp, if true the following
     * method(s) are called
     *  engine->getKVBucket()->getWarmup()->setFinishedLoading();
     *
     * The result is that it appears that warmup has completed, allowing tests
     * to call certain methods which are guarded around isWarmupComplete.
     *
     * By default this is true and tests which actually want to work with warmup
     * can disable this and genuinely warmup by invoking the appropriate methods
     * and tasks.
     */
    void setCompleteWarmup(bool value) {
        completeWarmup = value;
    }

    /**
     * set the random function used by KVBucket.
     * @param randFunction  The random function to be used by the KVBucket.
     */
    void setRandomFunction(std::function<long()>& randFunction);

    /**
     * @return a non-const version of the store's collections manager object.
     */
    Collections::Manager& getCollectionsManager();

    /**
     * Replace the r/w KVStore with a MockCouchKVStore that uses the given ops.
     * If a nullptr is passed, revert the KVStore back to default ops.
     * This function will test the config to be sure the KVBucket is
     * persistent/couchstore.
     */
    void replaceCouchKVStore(FileOpsInterface* ops);

    /**
     * Replace the r/w KVStore with a MockMagmaKVStore. This function will test
     * the config to be sure the KVBucket is persistent/magma.
     */
    void replaceMagmaKVStore(MagmaKVStoreConfig& config);

    /**
     * Replace the r/w KVStore with a MockMagmaKVStore. Without having to
     * specify the Magma config.
     */
    void replaceMagmaKVStore();

    /**
     * Initialise test objects - e.g. engine/store/cookie
     * @param baseConfig The bucket config to create the bucket from.
     * Initialise() will append additional config params (e.g. path to dbfiles)
     * as appropriate.
     */
    void initialise(std::string_view baseConfig);

    /**
     * @return the stats for the given collections using
     *         KVStore::getCollectionStats
     */
    std::unordered_map<CollectionID, Collections::VB::PersistedStats>
    getCollectionStats(Vbid id, const std::vector<CollectionID>& cids);

    /**
     * Get the destroyer task responsible for checkpoints from the given
     * vbucket.
     */
    KVBucket::CheckpointDestroyer getCkptDestroyerTask(Vbid vbid) const;

    std::shared_ptr<InitialMFUTask> getInitialMfuUpdaterTask() const;

    /**
     * Register a hook which will be called in KVBucket::processExpiredItem;
     */
    void setProcessExpiredItemHook(std::function<void()> cb);

    /**
     * Add to config_string configuration so that warmup is 100% done using
     * the primary warmup object
     */
    void setupPrimaryWarmupOnly();

    bool itemCompressorTaskIsSleepingForever() const;

    void updateItemPagerSleepTime(const std::chrono::milliseconds interval);

private:
    /**
     * Destroy the test objects - e.g. engine/store/cookie
     *
     * @param force Force the shutdown making it appear unclean
     */
    void destroy(bool force = false);

    bool completeWarmup = true;

public:
    std::string config_string;

    const Vbid vbid = Vbid(0);

    // The mock engine (needed to construct the store).
    SynchronousEPEngineUniquePtr engine;

    // The store under test. Wrapped in a mock to expose some normally
    // protected members. Uses a raw pointer as this is owned by the engine.
    KVBucket* store;

    /**
     * The (mock) server cookie. Declared as the interface type (CookieIface)
     * instead of concrete type (MockCookie) to minimise the number of places
     * where mock_cookie.h needs to be included (most places where 'cookie'
     * is used only need the interface).
     * Use cookie_to_mock_cookie() to obtain the underlying MockCookie instance
     * if required.
     */
    CookieIface* cookie = nullptr;
};
