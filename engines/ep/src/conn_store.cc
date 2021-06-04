/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "conn_store.h"

#include "connhandler.h"
#include "ep_engine.h"

ConnStore::ConnStore(EventuallyPersistentEngine& engine)
    : cookieToConnHandle(CookieToConnMapHandle(*this)),
      vbToConns(engine.getConfiguration().getMaxVbuckets()) {
}

ConnStore::IterableHandle<std::list<ConnStore::VBConn>>
ConnStore::getConnsForVBHandle(Vbid vb) {
    size_t index = vb.get() % vbConnLocks.size();
    return {vbToConns[vb.get()],
            std::unique_lock<std::mutex>(vbConnLocks[index])};
}

void ConnStore::addVBConnByVbid(Vbid vbid, ConnHandler& conn) {
    if (vbid.get() > vbToConns.size()) {
        throw std::out_of_range(
                "ConnStore::addVBConnByVbid attempting to add a "
                "vbConn to an invalid vbucket " +
                vbid.to_string());
    }

    size_t lock_num = vbid.get() % vbConnLocks.size();
    std::unique_lock<std::mutex> lh(vbConnLocks[lock_num]);

    auto& list = vbToConns[vbid.get()];
    auto itr = getVBToConnsItr(lh, vbid, conn);

    // For collections we will allow many streams for a given vBucket per
    // Producer. For this, bump the refCount of the VBConn with each new stream.
    if (itr == list.end()) {
        vbToConns[vbid.get()].emplace_back(VBConn{conn, 1});
    } else {
        itr->refCount++;
    }
}

void ConnStore::removeVBConnByVbid(Vbid vbid, const CookieIface* cookie) {
    if (vbid.get() > vbToConns.size()) {
        throw std::out_of_range(
                "ConnStore::removeVBConnByVbid attempting to remove "
                "a vbConn from an invalid vbucket " +
                vbid.to_string());
    }

    size_t lock_num = vbid.get() % vbConnLocks.size();
    std::unique_lock<std::mutex> lh(vbConnLocks[lock_num]);

    auto& list = vbToConns[vbid.get()];
    auto itr = getVBToConnsItr(lh, vbid, cookie);

    if (itr != list.end()) {
        // Decrement then check if we should remove the Conn from vbToConns map.
        if (--itr->refCount == 0) {
            list.erase(itr);
        }
    }
}

ConnStore::VBToConnsMap::value_type::iterator ConnStore::getVBToConnsItr(
        std::unique_lock<std::mutex>& lock,
        Vbid vbid,
        const ConnHandler& conn) {
    return getVBToConnsItr(lock, vbid, conn.getCookie());
}

ConnStore::VBToConnsMap::value_type::iterator ConnStore::getVBToConnsItr(
        std::unique_lock<std::mutex>& lock,
        Vbid vbid,
        const CookieIface* cookie) {
    return getVBToConnsItr(lock, vbid, [cookie](const VBConn& listConn) {
        return listConn.connHandler.getCookie() == cookie;
    });
}

ConnStore::VBToConnsMap::value_type::iterator ConnStore::getVBToConnsItr(
        std::unique_lock<std::mutex>& lock,
        Vbid vbid,
        const std::string& name) {
    return getVBToConnsItr(lock, vbid, [name](const VBConn& listConn) {
        return listConn.connHandler.getName() == name;
    });
}

template <class Predicate>
ConnStore::VBToConnsMap::value_type::iterator ConnStore::getVBToConnsItr(
        std::unique_lock<std::mutex>& lock, Vbid vbid, Predicate p) {
    auto& list = vbToConns[vbid.get()];
    return std::find_if(list.begin(), list.end(), p);
}

bool ConnStore::doesVbConnExist(Vbid vbid, const CookieIface* cookie) {
    size_t lock_num = vbid.get() % vbConnLocks.size();
    std::unique_lock<std::mutex> lh(vbConnLocks[lock_num]);
    auto itr = getVBToConnsItr(lh, vbid, cookie);

    return doesVbConnExistInner(vbid, itr);
}

bool ConnStore::doesVbConnExist(Vbid vbid, const std::string& name) {
    size_t lock_num = vbid.get() % vbConnLocks.size();
    std::unique_lock<std::mutex> lh(vbConnLocks[lock_num]);
    auto itr = getVBToConnsItr(lh, vbid, name);
    return doesVbConnExistInner(vbid, itr);
}

bool ConnStore::doesVbConnExistInner(
        Vbid vbid, ConnStore::VBToConnsMap::value_type::iterator itr) {
    auto& list = vbToConns[vbid.get()];
    if (itr == list.end()) {
        return false;
    }
    return true;
}

std::shared_ptr<ConnHandler>
ConnStore::CookieToConnMapHandle::findConnHandlerByCookie(
        const CookieIface* cookie) {
    auto itr = cookieToConn.find(cookie);
    if (itr == cookieToConn.end()) {
        return {};
    } else {
        return itr->second;
    }
}

std::shared_ptr<ConnHandler>
ConnStore::CookieToConnMapHandle::findConnHandlerByName(
        const std::string& name) {
    for (const auto& e : cookieToConn) {
        if (e.second->getName() == name) {
            return e.second;
        }
    }
    return {};
}

void ConnStore::CookieToConnMapHandle::addConnByCookie(
        const CookieIface* cookie, std::shared_ptr<ConnHandler> conn) {
    Expects(conn.get());

    auto existing = findConnHandlerByCookie(cookie);
    if (existing) {
        throw std::runtime_error(
                "ConnStore::addConnByCookie attempting to add a "
                "consumer but a connection already exists"
                "for this cookie");
    } else {
        cookieToConn[cookie] = conn;
    }
}

void ConnStore::CookieToConnMapHandle::removeConnByCookie(
        const CookieIface* cookie) {
    auto itr = cookieToConn.find(cookie);
    if (itr != cookieToConn.end()) {
        // Remove all ConnHandlers associated with this cookie from vbToConns
        for (size_t i = 0; i < connStore.vbToConns.size(); i++) {
            size_t lock_num = i % connStore.vbConnLocks.size();
            std::unique_lock<std::mutex> lh(connStore.vbConnLocks[lock_num]);

            auto& list = connStore.vbToConns[i];
            list.remove_if([cookie](VBConn listConn) {
                return cookie == listConn.connHandler.getCookie();
            });
        }
        cookieToConn.erase(itr);
    }
}
