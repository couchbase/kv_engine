/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc
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

#pragma once

#include "conn_store_fwd.h"

#include "dcp/dcp-types.h"

#include <folly/Synchronized.h>

#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

class EventuallyPersistentEngine;
class Vbid;

/**
 * The purpose of this class is to manage the lifetime of ConnHandler objects
 * with respect to cookies and vBuckets. Logically a cookie owns a single
 * ConnHandler. This class owns the ConnHandler objects that are mapped to by
 * cookies using shared_ptrs. This class ensures that if we ever destroy a
 * ConnHandler then any references to it in vbToConns are also destroyed.
 *
 * ConnHandler objects are created as shared_ptr's by the DcpConnMap in the
 * newConsumer and newProducer functions. They are then added to the ConnStore
 * which stores a shared_ptr to the object in the CookieToConnection map inside
 * the CookieToConnMapHandle. This handle provides exclusive access to the
 * container so that it can be manipulated in a thread safe manner. The
 * DcpConnMap will remove ConnHandler objects from the ConnStore when a
 * ConnHandler is being disconnected. The ConnHandler objects will then be
 * cleaned up by a background task in the DcpConnMap.
 *
 * When Streams are added to connections (ConnHandlers) an entry mapping Vbid to
 * ConnHandler(s) is tracked so that we can iterate over all ConnHandlers that
 * a Vbid is aware of. As streams are created, a reference to the ConnHandler is
 * placed in the VbToConns map. This reference refers to a unique ConnHandler,
 * and should we have multiple streams per vBucket then a refCount is bumped.
 * This allows us to clean up the reference entry when all streams are closed.
 *
 * When a connection is disconnected, (a ConnHandler is removed from the
 * CookieToConnection map) we clean up the reference to the ConnHandler in the
 * VbToConns map to ensure that we do not try to access a stale reference after
 * the ConnHandler has been destroyed.
 */
class ConnStore {
public:
    /**
     * Element of the VBToConnsMap. We need to track a refCount for each of the
     * ConnHandlers in the map as we may have multiple occurrences of each when
     * we have multiple streams per vBucket per connections (Collections Stream
     * ID functionality). In this case, we can only remove the VBConn if the
     * refCount is 0.
     */
    struct VBConn {
        ConnHandler& connHandler;
        uint8_t refCount = 0;
    };

    using VBToConnsMap = std::vector<std::list<VBConn>>;

    ConnStore(EventuallyPersistentEngine& engine);

    /**
     * Locked iteration handle for consumers to iterate on one of the elements
     * of VBToConnsMap.
     *
     * @tparam Container Type of iterable container
     */
    template <typename Container>
    struct IterableHandle {
        IterableHandle(Container& container, std::unique_lock<std::mutex>&& lg)
            : container(container), lg(std::move(lg)) {
        }

        // Copy is not allowed as we cannot copy the lock
        IterableHandle(const IterableHandle&) = delete;
        IterableHandle& operator=(const IterableHandle&) = delete;

        IterableHandle(IterableHandle&& other) = default;
        IterableHandle& operator=(IterableHandle&& other) = default;

        typename Container::const_iterator begin() const {
            return container.cbegin();
        }

        typename Container::const_iterator end() const {
            return container.cend();
        }

    protected:
        Container& container;
        std::unique_lock<std::mutex> lg;
    };

    /**
     * Handle to access the CookieToConnection map. This is a separate struct so
     * that we can easily guard access with folly::Synchronized.
     */
    struct CookieToConnMapHandle {
        CookieToConnMapHandle(ConnStore& connStore) : connStore(connStore) {
        }

        CookieToConnectionMap::const_iterator begin() const {
            return cookieToConn.cbegin();
        }

        CookieToConnectionMap::const_iterator end() const {
            return cookieToConn.cend();
        }

        /**
         * Get the ConnHandler reference from the cookieToConn map for the given
         * cookie.
         *
         * @param cookie cookie to lookup
         * @return The ConnHandler or a nullptr
         */
        std::shared_ptr<ConnHandler> findConnHandlerByCookie(
                const void* cookie);

        /**
         * Get the ConnHandler reference from the cookieToConn map for the given
         * name.
         *
         * @param name name to lookup
         * @return The ConnHandler or a nullptr
         */
        std::shared_ptr<ConnHandler> findConnHandlerByName(
                const std::string& name);

        /**
         * Add an owning reference to a ConnHandler to the connMap for the given
         * cookie.
         *
         * @param cookie the cookie mapping to the ConnHandler
         * @param consumer reference to a ConnHandler
         * @throws runtime_error if a connection already exists for this cookie
         */
        void addConnByCookie(const void* cookie,
                             std::shared_ptr<ConnHandler> conn);

        /**
         * Remove the connection in connMap for the given cookie. Also removes
         * all of the instances of the associated ConnHandler from vbToConns.
         *
         * @param cookie the cookie mapping to the ConnHandler
         */
        void removeConnByCookie(const void* cookie);

        /**
         * @return a copy of cookieToConn so that users can call functions
         * without locking.
         */
        CookieToConnectionMap copyCookieToConn() const {
            return cookieToConn;
        }

        /**
         * @return true if cookieToConn is empty
         */
        bool empty() const {
            return cookieToConn.empty();
        }

    protected:
        /**
         * The connMap owns the ConnHandlers which are mapped to by cookie.
         */
        CookieToConnectionMap cookieToConn;
        ConnStore& connStore;
    };

    /**
     * Get a locked handle on the CookieToConnectionMap
     * @return folly::LockedPtr to the CookieToConnMapHandle (result of
     *         folly::Synchronized<>.wlock()). Auto because the type is too long
     */
    auto getCookieToConnectionMapHandle() {
        return cookieToConnHandle.wlock();
    }

    /**
     * Get a locked handle on the list of VBConns for a given VB.
     */
    IterableHandle<std::list<VBConn>> getConnsForVBHandle(Vbid vb);

    /**
     * Get the ConnHandler reference from the vbToConns map for the given name.
     *
     * @param vbid Vbid against which we should search
     * @param name name to lookup
     * @return True if the ConnHandler exists
     */
    bool doesVbConnExist(Vbid vbid, const std::string& name);

    /**
     * Get the ConnHandler reference from the vbToConns map for the given cookie
     *
     * @param vbid Vbid against which we should search
     * @param cookie cookie of the ConnHandler
     * @return True if the ConnHandler exists
     */
    bool doesVbConnExist(Vbid vbid, const void* cookie);

    /**
     * Add a reference to vbToConns for the given ConnHandler.
     *
     * @param vbid the vBucket that the ConnHandler serves
     * @param conn the connection
     */
    void addVBConnByVbid(Vbid vbid, ConnHandler& conn);

    /**
     * Remove the reference in vbToConns for the given cookie (ConnHandler).
     *
     * @param vbid the vBucket that the ConnHandler no longer serves
     * @param cookie the cookie mapping to the ConnHandler
     */
    void removeVBConnByVbid(Vbid vbid, const void* cookie);

protected:
    VBToConnsMap::value_type::iterator getVBToConnsItr(
            std::unique_lock<std::mutex>& lock,
            Vbid vbid,
            const ConnHandler& conn);
    VBToConnsMap::value_type::iterator getVBToConnsItr(
            std::unique_lock<std::mutex>& lock, Vbid vbid, const void* cookie);
    VBToConnsMap::value_type::iterator getVBToConnsItr(
            std::unique_lock<std::mutex>& lock,
            Vbid vbid,
            const std::string& name);
    template <class Predicate>
    VBToConnsMap::value_type::iterator getVBToConnsItr(
            std::unique_lock<std::mutex>& lock, Vbid vbid, Predicate p);

    /**
     * @return true if VbConn entry exists.
     */
    bool doesVbConnExistInner(Vbid vbid, VBToConnsMap::value_type::iterator);

    folly::Synchronized<CookieToConnMapHandle> cookieToConnHandle;

    /**
     * vbToConns is effectively a map of vBucket ID to list of ConnHandlers.
     * Each vBucket can map to multiple ConnHandlers (multiple DcpProducers/
     * DcpConsumer + DcpProducer if streaming from a replica). A reference to a
     * ConnHandler for any given vBucket must always be valid.
     */
    VBToConnsMap vbToConns;
    std::vector<std::mutex> vbConnLocks{32};
};
