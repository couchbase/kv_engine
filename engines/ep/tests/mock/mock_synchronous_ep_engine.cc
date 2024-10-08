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

#include "mock_synchronous_ep_engine.h"

#include "checkpoint_config.h"
#include "checkpoint_remover.h"
#include "dcp/dcpconnmap.h"
#include "dcp/flow-control-manager.h"
#include "environment.h"
#include "item.h"
#include "mock_dcp_conn_map.h"
#include "mock_ep_bucket.h"
#include "mock_ephemeral_bucket.h"
#include "objectregistry.h"
#include <executor/executorpool.h>
#include <fmt/format.h>
#include <platform/cb_arena_malloc.h>
#include <platform/cbassert.h>
#include <platform/dirutils.h>
#include <programs/engine_testapp/mock_server.h>
#include <string>

#include <ep_engine_group.h>
#include <memcached/server_core_iface.h>
#include <memory_tracker.h>
#include <quota_sharing_item_pager.h>

SynchronousEPEngine::SynchronousEPEngine(const cb::ArenaMallocClient& client,
                                         std::string_view extra_config)
    : EventuallyPersistentEngine(get_mock_server_api, client) {
    // Account any additional allocations in setting up SynchronousEPEngine
    // to the base class EPEngine's tracking.
    BucketAllocationGuard guard(this);

    // Default to a reduced number of vBuckets & shards to speed up test
    // setup / teardown (fewer VBucket & other related objects).
    // Tests which require additional vbuckets can specify that in
    // extra_config string.
    if (!configuration.parseConfiguration("max_vbuckets=4;max_num_shards=2")) {
        throw std::invalid_argument(
                "SynchronousEPEngine: Unable to set reduced max_vbuckets & "
                "max_num_shards");
    }

    // Merge any extra config into the main configuration.
    if (!extra_config.empty()) {
        if (!configuration.parseConfiguration(extra_config)) {
            throw std::invalid_argument(fmt::format(
                    "Unable to parse config string: {}", extra_config));
        }
    }

    name = "SynchronousEPEngine:" + configuration.getCouchBucket();

    auto& env = ::Environment::get();
    env.engineFileDescriptors = serverApi->core->getMaxEngineFileDescriptors();

    // workload is needed by EPStore's constructor (to construct the
    // VBucketMap).
    auto shards = configuration.getMaxNumShards();
    workload = std::make_unique<WorkLoadPolicy>(/*workers*/ 1, shards);

    // dcpConnMap_ is needed by EPStore's constructor.
    dcpConnMap_ = std::make_unique<MockDcpConnMap>(*this);

    // checkpointConfig is needed by CheckpointManager (via EPStore).
    checkpointConfig = std::make_unique<CheckpointConfig>(configuration);

    if (configuration.isDcpConsumerFlowControlEnabled()) {
        dcpFlowControlManager = std::make_unique<DcpFlowControlManager>(*this);
    }

    // Tests may need to create multiple failover table entries, so allow that
    maxFailoverEntries = configuration.getMaxFailoverEntries();

    if (configuration.isDataTrafficEnabled()) {
        enableTraffic(true);
    }

    maxItemSize = configuration.getMaxItemSize();

    setConflictResolutionMode(configuration.getConflictResolutionTypeString());

    allowSanitizeValueInDeletion =
            configuration.isAllowSanitizeValueInDeletion();

    stats.setLowWaterMarkPercent(configuration.getMemLowWatPercent());
    stats.setHighWaterMarkPercent(configuration.getMemHighWatPercent());

    configuration.addValueChangedListener(
            "dcp_consumer_buffer_ratio",
            std::make_unique<EpEngineValueChangeListener>(*this));
}

void SynchronousEPEngine::setKVBucket(std::unique_ptr<KVBucket> store) {
    cb_assert(kvBucket == nullptr);
    kvBucket = std::move(store);
}

void SynchronousEPEngine::setDcpConnMap(
        std::unique_ptr<DcpConnMap> dcpConnMap) {
    dcpConnMap_ = std::move(dcpConnMap);
}

void SynchronousEPEngine::setServerApi(ServerApi* api) {
    serverApi = api;
}

SynchronousEPEngineUniquePtr SynchronousEPEngine::build(
        const std::string& config) {
    auto client = cb::ArenaMalloc::registerClient();
    cb::ArenaMalloc::switchToClient(client);
    SynchronousEPEngineUniquePtr engine(
            new SynchronousEPEngine(client, config));

    // switch current thread to this new engine, so all sub-created objects
    // in this function are accounted in the buckets' mem_used, and we return
    // to the caller with the current thread associated with the built engine.
    ObjectRegistry::onSwitchThread(engine.get());

    // Similarly to EPEngine::initialize, create the data directory.
    const auto dbName = engine->getConfiguration().getDbname();
    if (dbName.empty()) {
        throw std::runtime_error(
                fmt::format("SynchronousEPEngine::build(): Invalid "
                            "configuration: dbname must be a non-empty value"));
    }
    try {
        std::filesystem::create_directories(dbName);
    } catch (const std::system_error& error) {
        throw std::runtime_error(
                fmt::format("SynchronousEPEngine::build(): Failed to create "
                            "data directory [{}]:{}",
                            dbName,
                            error.code().message()));
    }

    // Make sure the cached configuration parameters normally initialised in
    // EPEngine::initialise() are also initialised.
    engine->isCrossBucketHtQuotaSharing =
            engine->getConfiguration().isCrossBucketHtQuotaSharing();

    engine->memoryTracker = std::make_unique<StrictQuotaMemoryTracker>(*engine);

    engine->setKVBucket(
            engine->public_makeMockBucket(engine->getConfiguration()));
    engine->setCompressionMode(
            engine->getConfiguration().getCompressionModeString());

    engine->setMaxDataSize(engine->getConfiguration().getMaxSize());

    return engine;
}

void SynchronousEPEngine::destroy(bool force) {
    ObjectRegistry::onSwitchThread(nullptr);
    EventuallyPersistentEngine::destroy(force);
}

std::pair<cb::engine_errc, std::unique_ptr<Item>>
SynchronousEPEngine::createItem(const DocKeyView& key,
                                size_t nbytes,
                                uint32_t flags,
                                rel_time_t exptime,
                                const value_t& body,
                                uint8_t datatype,
                                uint64_t theCas,
                                int64_t bySeq,
                                Vbid vbid,
                                int64_t revSeq) {
    preCreateItemHook();
    return EventuallyPersistentEngine::createItem(key,
                                                  nbytes,
                                                  flags,
                                                  exptime,
                                                  body,
                                                  datatype,
                                                  theCas,
                                                  bySeq,
                                                  vbid,
                                                  revSeq);
}

QuotaSharingManager& SynchronousEPEngine::getQuotaSharingManager() {
    struct QuotaSharingManagerImpl : public QuotaSharingManager {
        QuotaSharingManagerImpl(SynchronousEPEngine& engine)
            : engine(engine), group(*engine.getServerApi()->bucket) {
        }

        EPEngineGroup& getGroup() override {
            return group;
        }

        ExTask getItemPager() override {
            if (!pager) {
                pager = std::make_shared<QuotaSharingItemPager>(
                        *engine.getServerApi()->bucket,
                        group,
                        engine.getTaskable(),
                        [n = engine.getConfiguration()
                                     .getConcurrentPagers()]() { return n; },
                        [n = std::chrono::milliseconds(
                                 engine.getConfiguration()
                                         .getPagerSleepTimeMs())]() {
                            return n;
                        });
                ExecutorPool::get()->cancel(pager->getId());
            }
            return pager;
        }

        SynchronousEPEngine& engine;
        EPEngineGroup group;
        ExTask pager{nullptr};
    };

    if (!quotaSharingManager) {
        quotaSharingManager = std::make_unique<QuotaSharingManagerImpl>(*this);
        quotaSharingManager->getGroup().add(*this);
    }

    return *quotaSharingManager;
}

void SynchronousEPEngine::initializeConnmap() {
    dcpConnMap_->initialize();
}

std::unique_ptr<KVBucket> SynchronousEPEngine::public_makeMockBucket(
        Configuration& config) {
    const auto bucketType = config.getBucketTypeString();
    if (bucketType == "persistent") {
        auto bucket = std::make_unique<testing::NiceMock<MockEPBucket>>(*this);
        bucket->initializeMockBucket();
        return bucket;
    }
    if (bucketType == "ephemeral") {
        EphemeralBucket::reconfigureForEphemeral(configuration);
        return std::make_unique<MockEphemeralBucket>(*this);
    }
    throw std::invalid_argument(bucketType +
                                " is not a recognized bucket "
                                "type");
}

cb::engine_errc SynchronousEPEngine::public_setWithMeta(
        Vbid vbucket,
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
        cb::const_byte_buffer emd) {
    return setWithMeta(vbucket,
                       key,
                       value,
                       itemMeta,
                       isDeleted,
                       datatype,
                       cas,
                       seqno,
                       cookie,
                       permittedVBStates,
                       checkConflicts,
                       allowExisting,
                       genBySeqno,
                       genCas,
                       emd);
}

DocKeyView SynchronousEPEngine::public_makeDocKey(
        CookieIface& cookie, const std::string& key) const {
    const auto buf = cb::const_byte_buffer{
            reinterpret_cast<const uint8_t*>(key.data()), key.size()};
    return makeDocKey(cookie, buf);
}
