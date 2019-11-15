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
#include "dcp/consumer.h"
#include "dcp/producer.h"
#include "ep_engine.h"
#include "statwriter.h"
#include <daemon/tracing.h>
#include <memcached/server_cookie_iface.h>
#include <memcached/vbucket.h>
#include <phosphor/phosphor.h>

const uint32_t DcpConnMap::dbFileMem = 10 * 1024;
const uint16_t DcpConnMap::numBackfillsThreshold = 4096;
const uint8_t DcpConnMap::numBackfillsMemThreshold = 1;

class DcpConnMap::DcpConfigChangeListener : public ValueChangedListener {
public:
    DcpConfigChangeListener(DcpConnMap& connMap);
    virtual ~DcpConfigChangeListener() {
    }
    virtual void sizeValueChanged(const std::string& key, size_t value);

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
    engine.getConfiguration().addValueChangedListener(
            "dcp_consumer_process_buffered_messages_yield_limit",
            std::make_unique<DcpConfigChangeListener>(*this));
    engine.getConfiguration().addValueChangedListener(
            "dcp_consumer_process_buffered_messages_batch_size",
            std::make_unique<DcpConfigChangeListener>(*this));
    engine.getConfiguration().addValueChangedListener(
            "dcp_idle_timeout",
            std::make_unique<DcpConfigChangeListener>(*this));
}

DcpConnMap::~DcpConnMap() {
    EP_LOG_INFO("Deleted dcpConnMap_");
}

DcpConsumer* DcpConnMap::newConsumer(const void* cookie,
                                     const std::string& name,
                                     const std::string& consumerName) {
    LockHolder lh(connsLock);

    std::string conn_name("eq_dcpq:");
    conn_name.append(name);

    const auto& iter = map_.find(cookie);
    if (iter != map_.end()) {
        iter->second->setDisconnect();
        EP_LOG_INFO(
                "Failed to create Dcp Consumer because connection "
                "({}) already exists.",
                cookie);
        return nullptr;
    }

    /*
     *  If we request a connection of the same name then
     *  mark the existing connection as "want to disconnect".
     */
    for (const auto& cookieToConn : map_) {
        if (cookieToConn.second->getName() == conn_name) {
            EP_LOG_INFO(
                    "{} Disconnecting existing Dcp Consumer {} as it has the "
                    "same "
                    "name as a new connection {}",
                    cookieToConn.second->logHeader(),
                    cookieToConn.first,
                    cookie);
            cookieToConn.second->setDisconnect();
        }
    }

    auto consumer = makeConsumer(engine, cookie, conn_name, consumerName);
    EP_LOG_DEBUG("{} Connection created", consumer->logHeader());
    auto* rawPtr = consumer.get();
    map_[cookie] = std::move(consumer);
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
    for (const auto& cookieToConn : map_) {
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
    DcpProducer* result{nullptr};
    std::shared_ptr<ConnHandler> oldConn;
    {
        LockHolder lh(connsLock);

        std::string conn_name("eq_dcpq:");
        conn_name.append(name);

        const auto it = map_.find(cookie);
        if (it != map_.end()) {
            oldConn = it->second;
            oldConn->flagDisconnect();
            EP_LOG_INFO(
                    "Failed to create Dcp Producer because connection "
                    "({}) already exists.",
                    cookie);
        } else {
            // If we request a connection of the same name then mark the
            // existing connection as "want to disconnect" and pull it out from
            // the conn-map. Note (MB-36915): Just flag the connection here,
            // defer stream-shutdown to oldConn->setDisconnect below (ie, after
            // we release the connLock), potential deadlock by lock-inversion
            // with KVBucket::setVBucketState otherwise (on connLock /
            // vbstateLock).
            for (const auto& cookieToConn : map_) {
                if (cookieToConn.second->getName() == conn_name) {
                    const auto* oldCookie = cookieToConn.first;
                    oldConn = cookieToConn.second;
                    EP_LOG_INFO(
                            "{} Disconnecting existing Dcp Producer {} as it "
                            "has the "
                            "same "
                            "name as a new connection {}",
                            cookieToConn.second->logHeader(),
                            oldCookie,
                            cookie);
                    oldConn->flagDisconnect();

                    // Note: I thought that we need to 'map_.erase(oldCookie)'
                    // here, the connection would stale in connMap forever
                    // otherwise. But that is not the case. Memcached will call
                    // down the disconnect-handle for oldCookie, which will be
                    // correctly removed from conn-map. Any attempt to remove it
                    // here will lead to issues described MB-36451.
                }
            }

            auto producer = std::make_shared<DcpProducer>(
                    engine, cookie, conn_name, flags, true /*startTask*/);
            EP_LOG_DEBUG("{} Connection created", producer->logHeader());
            result = producer.get();
            map_[cookie] = std::move(producer);
        }
    }

    if (oldConn) {
        oldConn->setDisconnect();
    }

    return result;
}

void DcpConnMap::shutdownAllConnections() {
    EP_LOG_INFO("Shutting down dcp connections!");

    if (connNotifier_ != NULL) {
        connNotifier_->stop();
        manageConnections();
    }

    // Take a copy of the connection map (under lock), then using the
    // copy iterate across closing all streams and cancelling any
    // tasks.
    // We do this so we don't hold the connsLock when calling
    // notifyPaused() on producer streams, as that would create a lock
    // cycle between connLock, worker thread lock and releaseLock.
    CookieToConnectionMap mapCopy;
    {
        LockHolder lh(connsLock);
        mapCopy = map_;
    }

    closeStreams(mapCopy);
    cancelTasks(mapCopy);
}

void DcpConnMap::vbucketStateChanged(
        Vbid vbucket,
        vbucket_state_t state,
        bool closeInboundStreams,
        boost::optional<folly::SharedMutex::WriteHolder&> vbstateLock) {
    LockHolder lh(connsLock);
    for (const auto& cookieToConn : map_) {
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
    LockHolder lh(connsLock);
    for (const auto& cookieToConn : map_) {
        auto* producer = dynamic_cast<DcpProducer*>(cookieToConn.second.get());
        if (producer) {
            producer->closeStreamDueToRollback(vbucket);
        }
    }
}

bool DcpConnMap::handleSlowStream(Vbid vbid, const CheckpointCursor* cursor) {
    size_t lock_num = vbid.get() % vbConnLockNum;
    std::lock_guard<std::mutex> lh(vbConnLocks[lock_num]);

    for (const auto& weakPtr : vbConns[vbid.get()]) {
        auto connection = weakPtr.lock();
        if (!connection) {
            continue;
        }
        auto* producer = dynamic_cast<DcpProducer*>(connection.get());
        if (producer && producer->handleSlowStream(vbid, cursor)) {
            return true;
        }
    }
    return false;
}

void DcpConnMap::closeStreams(CookieToConnectionMap& map) {
    for (const auto& itr : map) {
        auto producer = dynamic_pointer_cast<DcpProducer>(itr.second);
        if (producer) {
            producer->closeAllStreams();
            producer->cancelCheckpointCreatorTask();
            // The producer may be in EWOULDBLOCK (if it's idle), therefore
            // notify him to ensure the front-end connection can close the TCP
            // connection.
            producer->immediatelyNotify();
        } else {
            auto consumer = dynamic_pointer_cast<DcpConsumer>(itr.second);
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
        auto consumer = dynamic_pointer_cast<DcpConsumer>(itr.second);
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
        LockHolder lh(connsLock);
        auto itr(map_.find(cookie));
        if (itr != map_.end()) {
            conn = itr->second;
            if (conn.get()) {
                auto* epe = ObjectRegistry::onSwitchThread(nullptr, true);
                if (epe) {
                    auto conn_desc =
                         epe->getServerApi()->cookie->get_log_info(cookie).second;
                    conn->getLogger().info("Removing connection {}", conn_desc);
                } else {
                    conn->getLogger().info("Removing connection {}", cookie);
                }
                ObjectRegistry::onSwitchThread(epe);
                // MB-36557: Just flag the connection as disconnected, defer
                // streams-shutdown (ie, close-stream + notify-connection) to
                // disconnectConn below (ie, after we release the connLock).
                // Potential deadlock by lock-inversion with
                // KVBucket::setVBucketState otherwise (on connLock /
                // vbstateLock).
                conn->flagDisconnect();
                map_.erase(itr);
            }
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
        for (const auto& cookieToConn : map_) {
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

    for (auto it = toNotify.begin(); it != toNotify.end(); ++it) {
        if ((*it).get() && (*it)->isReserved()) {
            engine.notifyIOComplete((*it)->getCookie(), ENGINE_SUCCESS);
        }
    }

    while (!release.empty()) {
        auto conn = release.front();
        conn->releaseReference();
        release.pop_front();
        auto prod = dynamic_pointer_cast<DcpProducer>(conn);
        if (prod) {
            removeVBConnections(*prod);
        }
    }
}

void DcpConnMap::removeVBConnections(DcpProducer& prod) {
    for (const auto vbid : prod.getVBVector()) {
        size_t lock_num = vbid.get() % vbConnLockNum;
        std::lock_guard<std::mutex> lh(vbConnLocks[lock_num]);
        auto& vb_conns = vbConns[vbid.get()];
        for (auto itr = vb_conns.begin(); itr != vb_conns.end();) {
            auto connection = (*itr).lock();
            if (!connection) {
                // ConnHandler no longer exists, cleanup.
                itr = vb_conns.erase(itr);
            } else if (prod.getCookie() == connection->getCookie()) {
                // Found conn with matching cookie, done.
                vb_conns.erase(itr);
                break;
            } else {
                ++itr;
            }
        }
    }
}

void DcpConnMap::notifyVBConnections(Vbid vbid,
                                     uint64_t bySeqno,
                                     SyncWriteOperation syncWrite) {
    size_t lock_num = vbid.get() % vbConnLockNum;
    std::lock_guard<std::mutex> lh(vbConnLocks[lock_num]);

    for (auto& weakPtr : vbConns[vbid.get()]) {
        auto connection = weakPtr.lock();
        if (!connection) {
            continue;
        }
        auto* producer = dynamic_cast<DcpProducer*>(connection.get());
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
    // vBucket. We will just ack all alive Consumers and allow the Consumer to
    // determine if it needs to do any work.
    size_t index = vbid.get() % vbConnLockNum;
    std::lock_guard<std::mutex> lg(vbConnLocks[index]);
    for (auto& weakPtr : vbConns[vbid.get()]) {
        auto connection = weakPtr.lock();
        if (!connection) {
            continue;
        }
        auto consumer = dynamic_pointer_cast<DcpConsumer>(connection);
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
    LockHolder lh(connsLock);
    for (const auto& cookieToConn : map_) {
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

/*
 * Find all DcpConsumers and set the yield threshold
 */
void DcpConnMap::consumerYieldConfigChanged(size_t newValue) {
    LockHolder lh(connsLock);
    for (const auto& cookieToConn : map_) {
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
    LockHolder lh(connsLock);
    for (const auto& cookieToConn : map_) {
        auto* dcpConsumer =
                dynamic_cast<DcpConsumer*>(cookieToConn.second.get());
        if (dcpConsumer) {
            dcpConsumer->setProcessBufferedMessagesBatchSize(newValue);
        }
    }
}

void DcpConnMap::idleTimeoutConfigChanged(size_t newValue) {
    LockHolder lh(connsLock);
    for (const auto& cookieToConn : map_) {
        cookieToConn.second.get()->setIdleTimeout(
                std::chrono::seconds(newValue));
    }
}

std::shared_ptr<ConnHandler> DcpConnMap::findByName(const std::string& name) {
    LockHolder lh(connsLock);
    for (const auto& cookieToConn : map_) {
        // If the connection is NOT about to be disconnected
        // and the names match
        if (!cookieToConn.second->doDisconnect() &&
            cookieToConn.second->getName() == name) {
            return cookieToConn.second;
        }
    }
    return nullptr;
}
