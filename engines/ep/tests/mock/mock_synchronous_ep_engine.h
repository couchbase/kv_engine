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

#pragma once

#include "checkpoint_config.h"
#include "dcp/dcpconnmap.h"
#include "ep_engine.h"

#include <memcached/engine.h>

#include <memory>

class SynchronousEPEngine;

using SynchronousEPEngineUniquePtr =
        std::unique_ptr<SynchronousEPEngine,
                        EngineDeletor<SynchronousEPEngine>>;

/* A class which subclasses the real EPEngine. Its main purpose is to allow
 * us to construct and setup an EPStore without starting all the various
 * background tasks which are normally started by EPEngine as part of creating
 * EPStore (in the initialize() method).
 *
 * The net result is a (mostly) synchronous environment - while the
 * ExecutorPool's threads exist, none of the normally-created background Tasks
 * should be running. Note however that /if/ any new tasks are created, they
 * will be scheduled on the ExecutorPools' threads asynchronously.
 */
class SynchronousEPEngine : public EventuallyPersistentEngine {
public:
    explicit SynchronousEPEngine(const cb::ArenaMallocClient& client,
                                 std::string_view extra_config = {});

    void setKVBucket(std::unique_ptr<KVBucket> store);
    void setDcpConnMap(std::unique_ptr<DcpConnMap> dcpConnMap);
    void setServerApi(ServerApi* api);

    /**
     * Constructs a SynchronousEPEngine instance, along with the necessary
     * sub-components.
     * Similar in function to create_ep_engine_instance() +
     * EventuallyPersistentEngine::initialize(), with the main difference
     * being none of the asynchronous parts of ::initialize() are performed
     * (background tasks, etc...).
     *
     * Note: Registers the engine as a new client for memory tracking (via
     * ArenaMalloc), and accounts all allocations made during building to
     * that arena. Calling thread's current engine is set to the created
     * EpEngine instance, so direct calls to the engine or sub-objects
     * (i.e. not via EngineIface) will continue to track allocations against
     * this engine.
     *
     * @param config The configuration to use for the engine.
     * @param encryptionKeys Boot-strap encryption keys to use for the new
     * engine
     */
    static SynchronousEPEngineUniquePtr build(
            const std::string& config,
            nlohmann::json encryptionKeys = nlohmann::json::object());

    /**
     * Destroys the SynchronousEPEngine instance.
     * For symmetry with the build() method (which switches the calling thread
     * to the created engine), this method switches back to the null engine
     * before retuning to the caller.
     */
    void destroy(bool force) override;

    /**
     * Constructs a SynchronousEPEngine instance that begins with a testing
     * hook before calling EPEngine::createItem().
     */
    std::pair<cb::engine_errc, std::unique_ptr<Item>> createItem(
            const DocKeyView& key,
            size_t nbytes,
            uint32_t flags,
            rel_time_t exptime,
            const value_t& body,
            uint8_t datatype,
            uint64_t theCas,
            int64_t bySeq,
            Vbid vbid,
            int64_t revSeq) override;

    /** Construct independent managers, even for quota sharing configs. */
    QuotaSharingManager& getQuotaSharingManager() override;

    /* Allow us to call normally protected methods */

    cb::engine_errc public_doDcpVbTakeoverStats(CookieIface& cookie,
                                                const AddStatFn& add_stat,
                                                std::string& key,
                                                Vbid vbid) {
        return doDcpVbTakeoverStats(cookie, add_stat, key, vbid);
    }

    /*
     * Initialize the connmap object, which creates tasks
     * so must be done after executorpool is created
     */
    void initializeConnmap();

    std::unique_ptr<KVBucket> public_makeMockBucket(Configuration& config);

    cb::engine_errc public_setWithMeta(Vbid vbucket,
                                       DocKeyView key,
                                       cb::const_byte_buffer value,
                                       ItemMetaData itemMeta,
                                       bool isDeleted,
                                       protocol_binary_datatype_t datatype,
                                       uint64_t& cas,
                                       uint64_t* seqno,
                                       CookieIface& cookie,
                                       PermittedVBStates permittedVBStates,
                                       CheckConflicts checkConflicts,
                                       bool allowExisting,
                                       GenerateBySeqno genBySeqno,
                                       GenerateCas genCas,
                                       cb::const_byte_buffer emd);

    DocKeyView public_makeDocKey(CookieIface& cookie,
                                 const std::string& key) const;

    bool public_enableTraffic(bool enable) {
        return enableTraffic(enable);
    }

    void public_setActiveEncryptionKeys(nlohmann::json keys);

    using EventuallyPersistentEngine::doCollectionStats;
    using EventuallyPersistentEngine::doConnAggStats;
    using EventuallyPersistentEngine::doContinuousBackupStats;
    using EventuallyPersistentEngine::doEngineStats;
    using EventuallyPersistentEngine::fileOpsTracker;
    using EventuallyPersistentEngine::getAutoShardCount;

    // Test hook called before creating an item that is BgFetched
    TestingHook<> preCreateItemHook;

private:
    std::unique_ptr<QuotaSharingManager> quotaSharingManager;
};
