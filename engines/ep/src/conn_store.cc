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

#include "conn_store.h"

#include "connhandler.h"
#include "ep_engine.h"

ConnStore::ConnStore(EventuallyPersistentEngine& engine)
    : cookieToConnHandle(*this),
      vbToConns(engine.getConfiguration().getMaxVbuckets()) {
}

ConnStore::IterableHandle<std::list<ConnStore::VBConn>>
ConnStore::getConnsForVBHandle(Vbid vb) {
    size_t index = vb.get() % vbConnLocks.size();
    return {vbToConns[vb.get()],
            std::unique_lock<std::mutex>(vbConnLocks[index])};
}

void ConnStore::addVBConnByVbid(Vbid vbid, std::shared_ptr<ConnHandler> conn) {
    Expects(conn.get());

    if (vbid.get() > vbToConns.size()) {
        throw std::out_of_range(
                "ConnStore::addVBConnByVbid attempting to add a "
                "vbConn to an invalid vbucket " +
                vbid.to_string());
    }

    size_t lock_num = vbid.get() % vbConnLocks.size();
    std::unique_lock<std::mutex> lh(vbConnLocks[lock_num]);

    auto& list = vbToConns[vbid.get()];
    auto itr = getVBToConnsItr(lh, vbid, *conn.get());

    // For collections we will allow many streams for a given vBucket per
    // Producer. For this, bump the refCount of the VBConn with each new stream.
    if (itr == list.end()) {
        vbToConns[vbid.get()].emplace_back(VBConn{std::move(conn), 1});
    } else {
        itr->refCount++;
    }
}

void ConnStore::removeVBConnByVbid(Vbid vbid, const void* cookie) {
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
        std::unique_lock<std::mutex>& lock, Vbid vbid, const void* cookie) {
    return getVBToConnsItr(lock, vbid, [cookie](const VBConn& listConn) {
        auto locked = listConn.connHandler.lock();
        if (!locked) {
            return false;
        }
        return locked->getCookie() == cookie;
    });
}

ConnStore::VBToConnsMap::value_type::iterator ConnStore::getVBToConnsItr(
        std::unique_lock<std::mutex>& lock,
        Vbid vbid,
        const std::string& name) {
    return getVBToConnsItr(lock, vbid, [name](const VBConn& listConn) {
        auto locked = listConn.connHandler.lock();
        if (!locked) {
            return false;
        }
        return locked->getName() == name;
    });
}

template <class Predicate>
ConnStore::VBToConnsMap::value_type::iterator ConnStore::getVBToConnsItr(
        std::unique_lock<std::mutex>& lock, Vbid vbid, Predicate p) {
    auto& list = vbToConns[vbid.get()];
    return std::find_if(list.begin(), list.end(), p);
}

bool ConnStore::doesVbConnExist(Vbid vbid, const void* cookie) {
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

    auto locked = itr->connHandler.lock();
    if (!locked) {
        // ConnHandler no longer exists, cleanup. This /shouldn't/ happen but
        // it's pulled from the current DcpConnMap code and will be (@TODO)
        // removed when we convert the shared_ptr to a reference.
        list.erase(itr);
        return false;
    }

    return true;
}

std::shared_ptr<ConnHandler>
ConnStore::CookieToConnMapHandle::findConnHandlerByCookie(const void* cookie) {
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
        const void* cookie, std::shared_ptr<ConnHandler> conn) {
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

void ConnStore::CookieToConnMapHandle::removeConnByCookie(const void* cookie) {
    auto itr = cookieToConn.find(cookie);
    if (itr != cookieToConn.end()) {
        // Remove all ConnHandlers associated with this cookie from vbToConns
        for (size_t i = 0; i < connStore.vbToConns.size(); i++) {
            size_t lock_num = i % connStore.vbConnLocks.size();
            std::unique_lock<std::mutex> lh(connStore.vbConnLocks[lock_num]);

            auto& list = connStore.vbToConns[i];
            list.remove_if([cookie](VBConn listConn) {
                auto locked = listConn.connHandler.lock();
                // Remove any connections that we cannot lock
                if (!locked) {
                    return true;
                }

                return cookie == locked->getCookie();
            });
        }
        cookieToConn.erase(itr);
    }
}
