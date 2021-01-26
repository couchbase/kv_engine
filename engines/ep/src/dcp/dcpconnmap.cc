/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

#include "dcpconnmap.h"

#include "bucket_logger.h"
#include "configuration.h"
#include "conn_notifier.h"
#include "conn_store.h"
#include "dcp/consumer.h"
#include "dcp/producer.h"
#include "ep_engine.h"
#include <daemon/tracing.h>
#include <memcached/server_cookie_iface.h>
#include <memcached/vbucket.h>
#include <phosphor/phosphor.h>
#include <statistics/cbstat_collector.h>

const uint32_t DcpConnMap::dbFileMem = 10 * 1024;
const uint16_t DcpConnMap::numBackfillsThreshold = 4096;
const uint8_t DcpConnMap::numBackfillsMemThreshold = 1;

class DcpConnMap::DcpConfigChangeListener : public ValueChangedListener {
public:
    explicit DcpConfigChangeListener(DcpConnMap& connMap);
    ~DcpConfigChangeListener() override {
    }
    void sizeValueChanged(const std::string& key, size_t value) override;
    void booleanValueChanged(const std::string& key, bool value) override;

private:
    DcpConnMap& myConnMap;
};

DcpConnMap::DcpConnMap(EventuallyPersistentEngine &e)
    : ConnMap(e),
      aggrDcpConsumerBufferSize(0) {
    backfills.numActiveSnoozing = 0;
    updateMaxActiveSnoozingBackfills(engine.getEpStats().getMaxDataSize());
    minCompressionRatioForProducer.store(
                    engine.getConfiguration().getDcpMinCompressionRatio());

    // Note: these allocations are deleted by ~Configuration
    auto& config = engine.getConfiguration();
    config.addValueChangedListener(
            "dcp_consumer_process_buffered_messages_yield_limit",
            std::make_unique<DcpConfigChangeListener>(*this));
    config.addValueChangedListener(
            "dcp_consumer_process_buffered_messages_batch_size",
            std::make_unique<DcpConfigChangeListener>(*this));
    config.addValueChangedListener(
            "dcp_idle_timeout",
            std::make_unique<DcpConfigChangeListener>(*this));
    config.addValueChangedListener(
            "allow_sanitize_value_in_deletion",
            std::make_unique<DcpConfigChangeListener>(*this));
}

DcpConnMap::~DcpConnMap() {
    EP_LOG_INFO("Deleted dcpConnMap_");
}

DcpConsumer* DcpConnMap::newConsumer(const void* cookie,
                                     const std::string& name,
                                     const std::string& consumerName) {
    std::string conn_name("eq_dcpq:");
    conn_name.append(name);

    auto consumer = makeConsumer(engine, cookie, conn_name, consumerName);
    EP_LOG_DEBUG("{} Connection created", consumer->logHeader());
    auto* rawPtr = consumer.get();

    // Get a write-handle!
    auto handle = connStore->getCookieToConnectionMapHandle();

    //  If we request a connection of the same name then
    //  mark the existing connection as "want to disconnect".
    const auto& connForName = handle->findConnHandlerByName(conn_name);
    if (connForName) {
        EP_LOG_INFO(
                "{} Disconnecting existing Dcp Consumer {} as it has the "
                "same name as a new connection {}",
                connForName->logHeader(),
                connForName->getCookie(),
                cookie);
        connForName->setDisconnect();
    }

    handle->addConnByCookie(cookie, std::move(consumer));
    return rawPtr;
}

std::shared_ptr<DcpConsumer> DcpConnMap::makeConsumer(
        EventuallyPersistentEngine& engine,
        const void* cookie,
        const std::string& connName,
        const std::string& consumerName) const {
    return std::make_shared<DcpConsumer>(
            engine, cookie, connName, consumerName);
}

bool DcpConnMap::isPassiveStreamConnected_UNLOCKED(Vbid vbucket) {
    auto handle = connStore->getCookieToConnectionMapHandle();
    for (const auto& cookieToConn : *handle) {
        auto* dcpConsumer =
                dynamic_cast<DcpConsumer*>(cookieToConn.second.get());
        if (dcpConsumer && dcpConsumer->isStreamPresent(vbucket)) {
            EP_LOG_DEBUG(
                    "({}) A DCP passive stream "
                    "is already exists for the vbucket in connection: {}",
                    vbucket,
                    dcpConsumer->logHeader());
            return true;
        }
    }
    return false;
}

ENGINE_ERROR_CODE DcpConnMap::addPassiveStream(ConnHandler& conn,
                                               uint32_t opaque,
                                               Vbid vbucket,
                                               uint32_t flags) {
    LockHolder lh(connsLock);
    /* Check if a stream (passive) for the vbucket is already present */
    if (isPassiveStreamConnected_UNLOCKED(vbucket)) {
        EP_LOG_WARN(
                "{} ({}) Failing to add passive stream, "
                "as one already exists for the vbucket!",
                conn.logHeader(),
                vbucket);
        return ENGINE_KEY_EEXISTS;
    }

    return conn.addStream(opaque, vbucket, flags);
}

DcpProducer* DcpConnMap::newProducer(const void* cookie,
                                     const std::string& name,
                                     uint32_t flags) {
    std::string conn_name("eq_dcpq:");
    conn_name.append(name);

    auto producer = std::make_shared<DcpProducer>(
            engine, cookie, conn_name, flags, true /*startTask*/);
    EP_LOG_DEBUG("{} Connection created", producer->logHeader());
    auto* result = producer.get();

    // Get a write-handle!
    auto handle = connStore->getCookieToConnectionMapHandle();

    // If we request a connection of the same name then mark the
    // existing connection as "want to disconnect" and pull it out from
    // the conn-map. Note (MB-36915): Just flag the connection here,
    // defer stream-shutdown to oldConn->setDisconnect below (ie, after
    // we release the connLock), potential deadlock by lock-inversion
    // with KVBucket::setVBucketState otherwise (on connLock /
    // vbstateLock).
    const auto& connForName = handle->findConnHandlerByName(conn_name);
    if (connForName) {
        EP_LOG_INFO(
                "{} Disconnecting existing Dcp Producer {} as it has the "
                "same name as a new connection {}",
                connForName->logHeader(),
                connForName->getCookie(),
                cookie);
        connForName->flagDisconnect();

        // Note: I thought that we need to 'map_.erase(oldCookie)'
        // here, the connection would stale in connMap forever
        // otherwise. But that is not the case. Memcached will call
        // down the disconnect-handle for oldCookie, which will be
        // correctly removed from conn-map. Any attempt to remove it
        // here will lead to issues described MB-36451.
    }

    handle->addConnByCookie(cookie, producer);
    return result;
}

void DcpConnMap::shutdownAllConnections() {
    EP_LOG_INFO("Shutting down dcp connections!");

    connNotifier->stop();
    manageConnections();

    // Take a copy of the connection map (under lock), then using the
    // copy iterate across closing all streams and cancelling any
    // tasks.
    // We do this so we don't hold the write lock of the map when calling
    // notifyPaused() on producer streams, as that would create a lock
    // cycle between the lock, worker thread lock and releaseLock.
    auto mapCopy =
            connStore->getCookieToConnectionMapHandle()->copyCookieToConn();

    closeStreams(mapCopy);
    cancelTasks(mapCopy);
}

void DcpConnMap::vbucketStateChanged(
        Vbid vbucket,
        vbucket_state_t state,
        bool closeInboundStreams,
        folly::SharedMutex::WriteHolder* vbstateLock) {
    LockHolder lh(connsLock);
    auto handle = connStore->getCookieToConnectionMapHandle();
    for (const auto& cookieToConn : *handle) {
        auto* producer = dynamic_cast<DcpProducer*>(cookieToConn.second.get());
        if (producer) {
            producer->closeStreamDueToVbStateChange(
                    vbucket, state, vbstateLock);
        } else if (closeInboundStreams) {
            static_cast<DcpConsumer*>(cookieToConn.second.get())
                    ->closeStreamDueToVbStateChange(vbucket, state);
        }
    }
}

void DcpConnMap::closeStreamsDueToRollback(Vbid vbucket) {
    auto handle = connStore->getCookieToConnectionMapHandle();
    for (const auto& cookieToConn : *handle) {
        auto* producer = dynamic_cast<DcpProducer*>(cookieToConn.second.get());
        if (producer) {
            producer->closeStreamDueToRollback(vbucket);
        }
    }
}

bool DcpConnMap::handleSlowStream(Vbid vbid, const CheckpointCursor* cursor) {
    bool ret = false;
    auto handle = connStore->getConnsForVBHandle(vbid);
    for (auto itr : handle) {
        auto* producer = dynamic_cast<DcpProducer*>(&itr.connHandler);
        if (producer && producer->handleSlowStream(vbid, cursor)) {
            return true;
        }
    }

    return ret;
}

void DcpConnMap::closeStreams(CookieToConnectionMap& map) {
    for (const auto& itr : map) {
        // Mark the connection as disconnected. This function is called during
        // the bucket shutdown path and if we don't do so then we could allow a
        // Producer to accept a racing StreamRequest during shutdown. When
        // memcached runs the connection again it will be disconnected anyways
        // as it will see that the bucket is being shut down.
        itr.second->flagDisconnect();
        auto producer = std::dynamic_pointer_cast<DcpProducer>(itr.second);
        if (producer) {
            producer->closeAllStreams();
            producer->cancelCheckpointCreatorTask();
            // The producer may be in EWOULDBLOCK (if it's idle), therefore
            // notify him to ensure the front-end connection can close the TCP
            // connection.
            producer->immediatelyNotify();
        } else {
            auto consumer = std::dynamic_pointer_cast<DcpConsumer>(itr.second);
            if (consumer) {
                consumer->closeAllStreams();
                // The consumer may be in EWOULDBLOCK (if it's idle), therefore
                // notify him to ensure the front-end connection can close the
                // TCP connection.
                consumer->immediatelyNotify();
            }
        }
    }
}

void DcpConnMap::cancelTasks(CookieToConnectionMap& map) {
    for (auto itr : map) {
        auto consumer = std::dynamic_pointer_cast<DcpConsumer>(itr.second);
        if (consumer) {
            consumer->cancelTask();
        }
    }
}

void DcpConnMap::disconnect(const void *cookie) {
    // Move the connection matching this cookie from the map_
    // data structure (under connsLock).
    std::shared_ptr<ConnHandler> conn;
    {
        auto handle = connStore->getCookieToConnectionMapHandle();
        auto connForCookie = handle->findConnHandlerByCookie(cookie);
        if (connForCookie) {
            conn = connForCookie;
            auto* epe = ObjectRegistry::getCurrentEngine();
            {
                NonBucketAllocationGuard guard;
                if (epe) {
                    auto conn_desc = epe->getServerApi()
                                             ->cookie->get_log_info(cookie)
                                             .second;
                    connForCookie->getLogger().info("Removing connection {}",
                                                    conn_desc);
                } else {
                    connForCookie->getLogger().info("Removing connection {}",
                                                    cookie);
                }
                ObjectRegistry::onSwitchThread(epe);
                // MB-36557: Just flag the connection as disconnected, defer
                // streams-shutdown (ie, close-stream + notify-connection) to
                // disconnectConn below (ie, after we release the connLock).
                // Potential deadlock by lock-inversion with
                // KVBucket::setVBucketState otherwise (on connLock /
                // vbstateLock).
                conn->flagDisconnect();
            }
            handle->removeConnByCookie(cookie);
        }
    }

    // Note we shutdown the stream *not* under the connsLock; this is
    // because as part of closing a DcpConsumer stream we need to
    // acquire PassiveStream::buffer.bufMutex; and that could deadlock
    // in EPBucket::setVBucketState, via
    // PassiveStream::processBufferedMessages.
    if (conn) {
        auto producer = std::dynamic_pointer_cast<DcpProducer>(conn);
        if (producer) {
            producer->closeAllStreams();
            producer->cancelCheckpointCreatorTask();
        } else {
            // Cancel consumer's processer task before closing all streams
            auto consumer = std::dynamic_pointer_cast<DcpConsumer>(conn);
            consumer->cancelTask();
            consumer->closeAllStreams();
            consumer->scheduleNotify();
        }
    }

    // Finished disconnecting the stream; add it to the
    // deadConnections list.
    if (conn) {
        LockHolder lh(connsLock);
        deadConnections.push_back(conn);
    }
}

void DcpConnMap::manageConnections() {
    std::list<std::shared_ptr<ConnHandler>> release;
    std::list<std::shared_ptr<ConnHandler>> toNotify;
    {
        LockHolder lh(connsLock);
        while (!deadConnections.empty()) {
            release.push_back(deadConnections.front());
            deadConnections.pop_front();
        }

        // Collect the list of connections that need to be signaled.
        auto handle = connStore->getCookieToConnectionMapHandle();
        for (const auto& cookieToConn : *handle) {
            const auto& conn = cookieToConn.second;
            if (conn && (conn->isPaused() || conn->doDisconnect()) &&
                conn->isReserved()) {
                /**
                 * Note: We want to send a notify even if we have sent one
                 * previously i.e. tp->sentNotify() == true.  The reason for this
                 * is manageConnections is used to notify idle connections once a
                 * second.  This results in the step function being invoked,
                 * which in turn may result in a dcp noop message being sent.
                 */
                toNotify.push_back(conn);
            }
        }
    }

    TRACE_LOCKGUARD_TIMED(releaseLock,
                          "mutex",
                          "DcpConnMap::manageConnections::releaseLock",
                          SlowMutexThreshold);

    for (auto& it : toNotify) {
        if (it.get() && it->isReserved()) {
            engine.scheduleDcpStep(it->getCookie());
        }
    }

    while (!release.empty()) {
        auto conn = release.front();
        conn->releaseReference();
        release.pop_front();
        auto prod = std::dynamic_pointer_cast<DcpProducer>(conn);
        if (prod) {
            removeVBConnections(*prod);
        }
    }
}

void DcpConnMap::removeVBConnections(DcpProducer& prod) {
    for (const auto vbid : prod.getVBVector()) {
        connStore->removeVBConnByVbid(vbid, prod.getCookie());
    }
}

void DcpConnMap::notifyVBConnections(Vbid vbid,
                                     uint64_t bySeqno,
                                     SyncWriteOperation syncWrite) {
    for (auto& vbConn : connStore->getConnsForVBHandle(vbid)) {
        auto* producer = dynamic_cast<DcpProducer*>(&vbConn.connHandler);
        if (producer) {
            producer->notifySeqnoAvailable(vbid, bySeqno, syncWrite);
        }
    }
}

void DcpConnMap::seqnoAckVBPassiveStream(Vbid vbid, int64_t seqno) {
    // Note: logically we should only have one Consumer per vBucket but
    // we may keep around old Consumers with either no PassiveStream for
    // this vBucket or a dead PassiveStream. We need to search the list of
    // ConnHandlers for the Consumer with the alive PassiveStream for this
    // vBucket.
    for (auto& vbConn : connStore->getConnsForVBHandle(vbid)) {
        auto* consumer = dynamic_cast<DcpConsumer*>(&vbConn.connHandler);
        if (consumer) {
            // Note: Sync Repl enabled at Consumer only if Producer supports it.
            //     This is to prevent that 6.5 Consumers send DCP_SEQNO_ACK to
            //     pre-6.5 Producers (e.g., topology change in a 6.5 cluster
            //     where a new pre-6.5 Active is elected).
            if (consumer->isSyncReplicationEnabled()) {
                consumer->seqnoAckStream(vbid, seqno);
            }
        }
    }
}

void DcpConnMap::notifyBackfillManagerTasks() {
    auto handle = connStore->getCookieToConnectionMapHandle();
    for (const auto& cookieToConn : *handle) {
        auto* producer = dynamic_cast<DcpProducer*>(cookieToConn.second.get());
        if (producer) {
            producer->notifyBackfillManager();
        }
    }
}

bool DcpConnMap::canAddBackfillToActiveQ()
{
    std::lock_guard<std::mutex> lh(backfills.mutex);
    if (backfills.numActiveSnoozing < backfills.maxActiveSnoozing) {
        ++backfills.numActiveSnoozing;
        return true;
    }
    return false;
}

void DcpConnMap::decrNumActiveSnoozingBackfills()
{
    {
        std::lock_guard<std::mutex> lh(backfills.mutex);
        if (backfills.numActiveSnoozing > 0) {
            --backfills.numActiveSnoozing;
            return;
        }
    }
    EP_LOG_WARN("ActiveSnoozingBackfills already zero!!!");
}

void DcpConnMap::updateMaxActiveSnoozingBackfills(size_t maxDataSize)
{
    double numBackfillsMemThresholdPercent =
                         static_cast<double>(numBackfillsMemThreshold)/100;
    size_t max = maxDataSize * numBackfillsMemThresholdPercent / dbFileMem;

    uint16_t newMaxActive;
    {
        std::lock_guard<std::mutex> lh(backfills.mutex);
        /* We must have atleast one active/snoozing backfill */
        backfills.maxActiveSnoozing =
                std::max(static_cast<size_t>(1),
                         std::min(max, static_cast<size_t>(numBackfillsThreshold)));
        newMaxActive = backfills.maxActiveSnoozing;
    }
    EP_LOG_DEBUG("Max active snoozing backfills set to {}", newMaxActive);
}

void DcpConnMap::addStats(const AddStatFn& add_stat, const void* c) {
    LockHolder lh(connsLock);
    add_casted_stat("ep_dcp_dead_conn_count", deadConnections.size(), add_stat,
                    c);
}

void DcpConnMap::updateMinCompressionRatioForProducers(float value) {
    minCompressionRatioForProducer.store(value);
}

float DcpConnMap::getMinCompressionRatio() {
    return minCompressionRatioForProducer.load();
}

DcpConnMap::DcpConfigChangeListener::DcpConfigChangeListener(DcpConnMap& connMap)
    : myConnMap(connMap){}

void DcpConnMap::DcpConfigChangeListener::sizeValueChanged(const std::string &key,
                                                           size_t value) {
    if (key == "dcp_consumer_process_buffered_messages_yield_limit") {
        myConnMap.consumerYieldConfigChanged(value);
    } else if (key == "dcp_consumer_process_buffered_messages_batch_size") {
        myConnMap.consumerBatchSizeConfigChanged(value);
    } else if (key == "dcp_idle_timeout") {
        myConnMap.idleTimeoutConfigChanged(value);
    }
}

void DcpConnMap::DcpConfigChangeListener::booleanValueChanged(
        const std::string& key, bool value) {
    if (key == "allow_sanitize_value_in_deletion") {
        myConnMap.consumerAllowSanitizeValueInDeletionConfigChanged(value);
    }
}

/*
 * Find all DcpConsumers and set the yield threshold
 */
void DcpConnMap::consumerYieldConfigChanged(size_t newValue) {
    auto handle = connStore->getCookieToConnectionMapHandle();
    for (const auto& cookieToConn : *handle) {
        auto* dcpConsumer =
                dynamic_cast<DcpConsumer*>(cookieToConn.second.get());
        if (dcpConsumer) {
            dcpConsumer->setProcessorYieldThreshold(newValue);
        }
    }
}

/*
 * Find all DcpConsumers and set the processor batchsize
 */
void DcpConnMap::consumerBatchSizeConfigChanged(size_t newValue) {
    auto handle = connStore->getCookieToConnectionMapHandle();
    for (const auto& cookieToConn : *handle) {
        auto* dcpConsumer =
                dynamic_cast<DcpConsumer*>(cookieToConn.second.get());
        if (dcpConsumer) {
            dcpConsumer->setProcessBufferedMessagesBatchSize(newValue);
        }
    }
}

void DcpConnMap::idleTimeoutConfigChanged(size_t newValue) {
    auto handle = connStore->getCookieToConnectionMapHandle();
    for (const auto& cookieToConn : *handle) {
        cookieToConn.second.get()->setIdleTimeout(
                std::chrono::seconds(newValue));
    }
}

void DcpConnMap::consumerAllowSanitizeValueInDeletionConfigChanged(
        bool newValue) {
    auto handle = connStore->getCookieToConnectionMapHandle();
    for (const auto& cookieToConn : *handle) {
        auto* consumer = dynamic_cast<DcpConsumer*>(cookieToConn.second.get());
        if (consumer) {
            consumer->setAllowSanitizeValueInDeletion(newValue);
        }
    }
}

std::shared_ptr<ConnHandler> DcpConnMap::findByName(const std::string& name) {
    auto handle = connStore->getCookieToConnectionMapHandle();
    for (const auto& cookieToConn : *handle) {
        // If the connection is NOT about to be disconnected
        // and the names match
        if (!cookieToConn.second->doDisconnect() &&
            cookieToConn.second->getName() == name) {
            return cookieToConn.second;
        }
    }
    return nullptr;
}

bool DcpConnMap::isConnections() {
    LockHolder lh(connsLock);
    return !connStore->getCookieToConnectionMapHandle()->empty();
}
