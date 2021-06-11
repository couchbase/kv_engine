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

/// mock_server_cookie_mutex to guard references, and object deletion in
/// case references becomes zero. Not put into the API as people shouldn't
/// mess around with it!!
extern std::mutex mock_server_cookie_mutex;

MockCookie::MockCookie(EngineIface* e) : engine(e) {
}

MockCookie::~MockCookie() {
    if (engine) {
        engine->disconnect(*this);
    }
}

void MockCookie::validate() const {
    if (magic != MAGIC) {
        throw std::runtime_error("MockCookie::validate(): Invalid magic");
    }
}

CookieIface* create_mock_cookie(EngineIface* engine) {
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

    std::lock_guard<std::mutex> guard(mock_server_cookie_mutex);
    c->validate();
    c->disconnect();
    if (c->references == 0) {
        delete c;
    }
}

void mock_set_ewouldblock_handling(const CookieIface* cookie, bool enable) {
    auto* c = cookie_to_mock_cookie(cookie);
    c->handle_ewouldblock = enable;
}

void mock_set_mutation_extras_handling(const CookieIface* cookie, bool enable) {
    auto* c = cookie_to_mock_cookie(cookie);
    c->handle_mutation_extras = enable;
}

void mock_set_datatype_support(const CookieIface* cookie,
                               protocol_binary_datatype_t datatypes) {
    auto* c = cookie_to_mock_cookie(cookie);
    c->enabled_datatypes = std::bitset<8>(datatypes);
}

void mock_set_collections_support(const CookieIface* cookie, bool enable) {
    auto* c = cookie_to_mock_cookie(cookie);
    c->handle_collections_support = enable;
}

bool mock_is_collections_supported(const CookieIface* cookie) {
    auto* c = cookie_to_mock_cookie(cookie);
    return c->handle_collections_support;
}

void lock_mock_cookie(const CookieIface* cookie) {
    auto* c = cookie_to_mock_cookie(cookie);
    c->mutex.lock();
}

void unlock_mock_cookie(const CookieIface* cookie) {
    auto* c = cookie_to_mock_cookie(cookie);
    c->mutex.unlock();
}

void waitfor_mock_cookie(const CookieIface* cookie) {
    auto* c = cookie_to_mock_cookie(cookie);

    std::unique_lock<std::mutex> lock(c->mutex, std::adopt_lock);
    if (!lock.owns_lock()) {
        throw std::logic_error("waitfor_mock_cookie: cookie should be locked!");
    }

    c->cond.wait(lock, [&c] {
        return c->num_processed_notifications != c->num_io_notifications;
    });
    c->num_processed_notifications = c->num_io_notifications;

    lock.release();
}

void disconnect_all_mock_connections() {
    // Currently does nothing; we don't track mock_connstructs
}

int get_number_of_mock_cookie_references(const CookieIface* cookie) {
    if (cookie == nullptr) {
        return -1;
    }
    std::lock_guard<std::mutex> guard(mock_server_cookie_mutex);
    auto* c = cookie_to_mock_cookie(cookie);
    return c->references;
}

size_t get_number_of_mock_cookie_io_notifications(const CookieIface* cookie) {
    auto* c = cookie_to_mock_cookie(cookie);
    return c->num_io_notifications;
}

MockCookie* cookie_to_mock_cookie(const CookieIface* cookie) {
    auto* ret = dynamic_cast<const MockCookie*>(cookie);
    ret->validate();
    return const_cast<MockCookie*>(ret);
}
