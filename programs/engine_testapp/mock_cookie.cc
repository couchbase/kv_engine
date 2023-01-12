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

void MockConnection::scheduleDcpStep() {
    if (userScheduleDcpStep) {
        userScheduleDcpStep();
    } else {
        throw std::runtime_error(
                "MockConnection::scheduleDcpStep no user callback specified");
    }
}

void MockConnection::setUserScheduleDcpStep(std::function<void()> func) {
    userScheduleDcpStep = std::move(func);
}
std::string_view MockConnection::getDescription() const {
    using namespace std::string_view_literals;
    return "[you - me]"sv;
}

MockCookie::CheckPrivilegeFunction MockCookie::checkPrivilegeFunction;
MockCookie::CheckForPrivilegeAtLeastInOneCollectionFunction
        MockCookie::checkForPrivilegeAtLeastInOneCollectionFunction;

MockCookie::MockCookie(EngineIface* e) : engine(e) {
    mock_register_cookie(*this);
    connection = std::make_unique<MockConnection>();
    // Restore the old behavior by default until we get the tests
    // fixed
    connection->setUserScheduleDcpStep(
            [this]() { notifyIoComplete(cb::engine_errc::success); });
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

cb::rbac::PrivilegeAccess MockCookie::checkPrivilege(
        cb::rbac::Privilege privilege,
        std::optional<ScopeID> sid,
        std::optional<CollectionID> cid) {
    return testPrivilege(privilege, sid, cid);
}

cb::rbac::PrivilegeAccess MockCookie::checkForPrivilegeAtLeastInOneCollection(
        cb::rbac::Privilege privilege) const {
    if (checkForPrivilegeAtLeastInOneCollectionFunction) {
        return checkForPrivilegeAtLeastInOneCollectionFunction(*this,
                                                               privilege);
    }
    return cb::rbac::PrivilegeAccessOk;
}

void MockCookie::setCurrentCollectionInfo(ScopeID sid,
                                          CollectionID cid,
                                          uint64_t manifestUid,
                                          bool metered) {
    // do nothing
}

ConnectionIface& MockCookie::getConnectionIface() {
    return *connection;
}

void MockCookie::notifyIoComplete(cb::engine_errc status) {
    if (userNotifyIoComplete) {
        userNotifyIoComplete(status);
    } else {
        mock_notify_io_complete(*this, status);
    }
}

bool MockCookie::checkThrottle(size_t, size_t) {
    return false;
}

bool MockCookie::sendResponse(cb::engine_errc,
                              std::string_view,
                              std::string_view) {
    // do nothing
    return true;
}

MockCookie* cookie_to_mock_cookie(CookieIface* cookie) {
    return &asMockCookie(*cookie);
}

MockCookie& asMockCookie(CookieIface& cookie) {
    auto* mc = dynamic_cast<MockCookie*>(&cookie);
    if (mc == nullptr) {
        throw std::runtime_error(
                "asMockCookie(): provided cookie is not a MockCookie");
    }
    return *mc;
}
