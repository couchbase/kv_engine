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

#include "dcpconnmap.h"

#include "bucket_logger.h"
#include "configuration.h"
#include "conn_store.h"
#include "dcp/consumer.h"
#include "dcp/producer.h"
#include "ep_engine.h"
#include "objectregistry.h"
#include <daemon/tracing.h>
#include <memcached/connection_iface.h>
#include <memcached/vbucket.h>
#include <phosphor/phosphor.h>
#include <statistics/cbstat_collector.h>

class DcpConnMap::DcpConfigChangeListener : public ValueChangedListener {
public:
    explicit DcpConfigChangeListener(DcpConnMap& connMap);
    ~DcpConfigChangeListener() override = default;
    void sizeValueChanged(std::string_view key, size_t value) override;
    void booleanValueChanged(std::string_view key, bool value) override;

private:
    DcpConnMap& myConnMap;
};

DcpConnMap::DcpConnMap(EventuallyPersistentEngine &e)
    : ConnMap(e),
      aggrDcpConsumerBufferSize(0) {
    minCompressionRatioForProducer.store(
                    engine.getConfiguration().getDcpMinCompressionRatio());

    // Note: these allocations are deleted by ~Configuration
    auto& config = engine.getConfiguration();
    config.addValueChangedListener(
            "dcp_idle_timeout",
            std::make_unique<DcpConfigChangeListener>(*this));
    config.addValueChangedListener(
            "allow_sanitize_value_in_deletion",
            std::make_unique<DcpConfigChangeListener>(*this));
}

DcpConnMap::~DcpConnMap() {
    EP_LOG_INFO_RAW("Deleted dcpConnMap_");
}

DcpConsumer* DcpConnMap::newConsumer(CookieIface& cookie,
                                     const std::string& name,
                                     const std::string& consumerName) {
    std::string conn_name("eq_dcpq:");
    conn_name.append(name);

    auto consumer = makeConsumer(engine, &cookie, conn_name, consumerName);
    cookie.getConnectionIface().setDcpConnHandler(
            consumer, engine.getArenaMallocClient());

    EP_LOG_DEBUG_CTX("Connection created", {"consumer", consumer->logHeader()});
    auto* rawPtr = consumer.get();

    // Get a write-handle!
    auto handle = connStore->getCookieToConnectionMapHandle();

    //  If we request a connection of the same name then
    //  mark the existing connection as "want to disconnect".
    const auto& connForName = handle->findConnHandlerByName(conn_name);
    if (connForName) {
        EP_LOG_INFO_CTX(
                "Disconnecting existing Dcp Consumer as it has the same name "
                "as a new connection",
                {"dcp", "consumer"},
                {"dcp_name", name},
                {"existing_cookie",
                 fmt::to_string(fmt::ptr(
                         static_cast<const void*>(connForName->getCookie())))},
                {"cookie",
                 fmt::to_string(fmt::ptr(static_cast<const void*>(&cookie)))});
        connForName->setDisconnect();
    }

    handle->addConnByCookie(&cookie, std::move(consumer));
    return rawPtr;
}

std::shared_ptr<DcpConsumer> DcpConnMap::makeConsumer(
        EventuallyPersistentEngine& engine,
        CookieIface* cookie,
        const std::string& connName,
        const std::string& consumerName) const {
    return std::make_shared<DcpConsumer>(
            engine, cookie, connName, consumerName);
}

bool DcpConnMap::isPassiveStreamConnected(Vbid vbucket) {
    auto handle = connStore->getCookieToConnectionMapHandle();
    for (const auto& cookieToConn : *handle) {
        auto* dcpConsumer =
                dynamic_cast<DcpConsumer*>(cookieToConn.second.get());
        if (dcpConsumer && dcpConsumer->isStreamPresent(vbucket)) {
            EP_LOG_DEBUG_CTX(
                    "A DCP passive stream already exists for the vbucket",
                    {"dcp", "consumer"},
                    {"dcp_name", dcpConsumer->getName()},
                    {"vb", vbucket});
            return true;
        }
    }
    return false;
}

cb::engine_errc DcpConnMap::addPassiveStream(ConnHandler& conn,
                                             uint32_t opaque,
                                             Vbid vbucket,
                                             cb::mcbp::DcpAddStreamFlag flags) {
    // Check if a stream (passive) for the vbucket is already present
    if (isPassiveStreamConnected(vbucket)) {
        EP_LOG_WARN_CTX(
                "Failing to add passive stream, as one already exists for the "
                "vbucket",
                {"dcp", "consumer"},
                {"dcp_name", conn.getName()},
                {"vb", vbucket});
        return cb::engine_errc::key_already_exists;
    }

    return conn.addStream(opaque, vbucket, flags);
}

DcpProducer* DcpConnMap::newProducer(CookieIface& cookie,
                                     const std::string& name,
                                     cb::mcbp::DcpOpenFlag flags) {
    std::string conn_name("eq_dcpq:");
    conn_name.append(name);

    auto producer = std::make_shared<DcpProducer>(
            engine, &cookie, conn_name, flags, true /*startTask*/);
    cookie.getConnectionIface().setDcpConnHandler(
            producer, engine.getArenaMallocClient());

    EP_LOG_DEBUG_CTX("Connection created",
                     {"dcp", "producer"},
                     {"dcp_name", producer->getName()}, );
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
        EP_LOG_INFO_CTX(
                "Disconnecting existing Dcp Producer as it has the same name "
                "as a new connection",
                {"dcp", "producer"},
                {"dcp_name", connForName->getName()},
                {"existing_cookie",
                 fmt::to_string(fmt::ptr(
                         static_cast<const void*>(connForName->getCookie())))},
                {"cookie",
                 fmt::to_string(fmt::ptr(static_cast<const void*>(&cookie)))});
        connForName->flagDisconnect();

        // Note: I thought that we need to 'map_.erase(oldCookie)'
        // here, the connection would stale in connMap forever
        // otherwise. But that is not the case. Memcached will call
        // down the disconnect-handle for oldCookie, which will be
        // correctly removed from conn-map. Any attempt to remove it
        // here will lead to issues described MB-36451.
    }

    handle->addConnByCookie(&cookie, std::move(producer));
    return result;
}

void DcpConnMap::shutdownAllConnections() {
    EP_LOG_INFO_RAW("Shutting down dcp connections!");

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
        std::unique_lock<folly::SharedMutex>* vbstateLock) {
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
            producer->scheduleNotify();
        } else {
            auto consumer = std::dynamic_pointer_cast<DcpConsumer>(itr.second);
            if (consumer) {
                consumer->closeAllStreams();
                // The consumer may be in EWOULDBLOCK (if it's idle), therefore
                // notify him to ensure the front-end connection can close the
                // TCP connection.
                consumer->scheduleNotify();
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

void DcpConnMap::disconnect(CookieIface* cookie) {
    std::shared_ptr<ConnHandler> conn;
    {
        auto handle = connStore->getCookieToConnectionMapHandle();
        conn = handle->findConnHandlerByCookie(cookie);
        if (conn) {
            auto* epe = ObjectRegistry::getCurrentEngine();
            if (epe) {
                conn->getLogger().infoWithContext(
                        "Removing connection",
                        {{"description",
                          cookie->getConnectionIface().getDescription()}});
            } else {
                conn->getLogger().infoWithContext(
                        "Removing connection",
                        {{"cookie",
                          fmt::to_string(fmt::ptr(
                                  static_cast<const void*>(cookie)))}});
            }
            // MB-36557: Just flag the connection as disconnected, defer
            // streams-shutdown (ie, close-stream + notify-connection) to
            // disconnectConn below (ie, after we release the connLock).
            // Potential deadlock by lock-inversion with
            // KVBucket::setVBucketState otherwise (on connLock /
            // vbstateLock).
            conn->flagDisconnect();

            handle->removeConnByCookie(cookie);
        }
    }

    // We do the minimal required work under ConnStore lock. All next steps can
    // be executed lock-free.
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

        // All streams closed, add to the deadConnections for releasing at
        // ConnManager.
        deadConnections.wlock()->emplace_back(std::move(conn));
    }
}

void DcpConnMap::manageConnections() {
    std::list<std::shared_ptr<ConnHandler>> release;
    deadConnections.wlock()->swap(release);

    for (auto& conn : release) {
        auto prod = std::dynamic_pointer_cast<DcpProducer>(conn);
        if (prod) {
            removeVBConnections(*prod);
        }
    }
}

void DcpConnMap::notifyConnections() {
    std::list<std::shared_ptr<ConnHandler>> toNotify;
    {
        // Collect the list of connections that need to be signaled.
        auto handle = connStore->getCookieToConnectionMapHandle();
        for (const auto& cookieToConn : *handle) {
            const auto& conn = cookieToConn.second;
            if (conn && (conn->isPaused() || conn->doDisconnect())) {
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

    for (auto& it : toNotify) {
        if (it.get()) {
            engine.scheduleDcpStep(*it->getCookie());
        }
    }
}

void DcpConnMap::removeVBConnections(DcpProducer& prod) {
    for (const auto vbid : prod.getVBVector()) {
        connStore->removeVBConnByVbid(vbid, prod.getCookie());
    }
}

void DcpConnMap::notifyVBConnections(Vbid vbid, queue_op op) {
    for (auto& vbConn : connStore->getConnsForVBHandle(vbid)) {
        auto* producer = dynamic_cast<DcpProducer*>(&vbConn.connHandler);
        if (producer) {
            producer->notifySeqnoAvailable(vbid, op);
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

void DcpConnMap::addStats(const AddStatFn& add_stat, CookieIface& c) {
    add_casted_stat("ep_dcp_dead_conn_count",
                    deadConnections.rlock()->size(),
                    add_stat,
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

void DcpConnMap::DcpConfigChangeListener::sizeValueChanged(std::string_view key,
                                                           size_t value) {
    if (key == "dcp_idle_timeout") {
        myConnMap.idleTimeoutConfigChanged(value);
    }
}

void DcpConnMap::DcpConfigChangeListener::booleanValueChanged(
        std::string_view key, bool value) {
    if (key == "allow_sanitize_value_in_deletion") {
        myConnMap.consumerAllowSanitizeValueInDeletionConfigChanged(value);
    }
}

void DcpConnMap::idleTimeoutConfigChanged(size_t newValue) {
    auto handle = connStore->getCookieToConnectionMapHandle();
    for (const auto& cookieToConn : *handle) {
        cookieToConn.second->setIdleTimeout(std::chrono::seconds(newValue));
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
    return !connStore->getCookieToConnectionMapHandle()->empty();
}

void DcpConnMap::setBackfillByteLimit(size_t bytes) {
    auto handle = connStore->getCookieToConnectionMapHandle();
    for (const auto& cookieToConn : *handle) {
        auto* producer = dynamic_cast<DcpProducer*>(cookieToConn.second.get());
        if (producer) {
            producer->setBackfillByteLimit(bytes);
        }
    }
}
