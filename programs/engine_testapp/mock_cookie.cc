/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "mock_cookie.h"
#include "mock_server.h"

#include <mcbp/protocol/status.h>

MockCookie::CheckPrivilegeFunction MockCookie::checkPrivilegeFunction;

MockCookie::MockCookie(EngineIface* e) : engine(e) {
    mock_register_cookie(*this);
}

MockCookie::~MockCookie() {
    mock_unregister_cookie(*this);
}

MockCookie* create_mock_cookie(EngineIface* engine) {
    return new MockCookie(engine);
}

void destroy_mock_cookie(CookieIface* cookie) {
    if (cookie == nullptr) {
        return;
    }

    auto* c = dynamic_cast<MockCookie*>(cookie);
    if (c == nullptr) {
        throw std::runtime_error(
                "destroy_mock_cookie: Provided cookie is not a MockCookie");
    }

    c->disconnect();
    if (c->decrementRefcount() == 0) {
        delete c;
    }
}

void MockCookie::setEwouldblock(bool ewouldblock) {
    handle_ewouldblock = ewouldblock;
}

void MockCookie::setMutationExtrasHandling(bool enable) {
    handle_mutation_extras = enable;
}

bool MockCookie::isMutationExtrasSupported() const {
    return handle_mutation_extras;
}

void MockCookie::setDatatypeSupport(protocol_binary_datatype_t datatypes) {
    enabled_datatypes = std::bitset<8>(datatypes);
}

bool MockCookie::isDatatypeSupported(
        protocol_binary_datatype_t datatype) const {
    std::bitset<8> in(datatype);
    return (enabled_datatypes & in) == in;
}

void MockCookie::setCollectionsSupport(bool enable) {
    handle_collections_support = enable;
}

bool MockCookie::isCollectionsSupported() const {
    return handle_collections_support;
}

std::mutex& MockCookie::getMutex() {
    return mutex;
}

void MockCookie::lock() {
    mutex.lock();
}

void MockCookie::unlock() {
    mutex.unlock();
}

void MockCookie::disconnect() {
    if (engine) {
        engine->disconnect(*this);
    }
}

cb::rbac::PrivilegeAccess MockCookie::testPrivilege(
        cb::rbac::Privilege privilege,
        std::optional<ScopeID> sid,
        std::optional<CollectionID> cid) const {
    if (checkPrivilegeFunction) {
        return checkPrivilegeFunction(*this, privilege, sid, cid);
    }

    return cb::rbac::PrivilegeAccessOk;
}

MockCookie* cookie_to_mock_cookie(const CookieIface* cookie) {
    auto* ret =
            const_cast<MockCookie*>(dynamic_cast<const MockCookie*>(cookie));
    if (ret == nullptr) {
        throw std::runtime_error(
                "cookie_to_mock_cookie(): provided cookie is not a MockCookie");
    }
    return ret;
}

MockCookie& cookie_to_mock_cookie(const CookieIface& cookie) {
    return *cookie_to_mock_cookie(&cookie);
}
