/*
 *     Copyright 2019 Couchbase, Inc.
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
#include "mock_cookie.h"
#include "mock_server.h"

/// mock_server_cookie_mutex to guard references, and object deletion in
/// case references becomes zero. Not put into the API as people shouldn't
/// mess around with it!!
extern std::mutex mock_server_cookie_mutex;

void MockCookie::validate() const {
    if (magic != MAGIC) {
        throw std::runtime_error("MockCookie::validate(): Invalid magic");
    }
}

const void* create_mock_cookie() {
    return new MockCookie();
}

static void disconnect_mock_connection(struct MockCookie* c) {
    // mock_server_cookie_mutex already held in calling function
    c->references--;
    mock_perform_callbacks(ON_DISCONNECT, nullptr, c);
}

void destroy_mock_cookie(const void* cookie) {
    if (cookie == nullptr) {
        return;
    }
    std::lock_guard<std::mutex> guard(mock_server_cookie_mutex);
    auto* c = cookie_to_mock_cookie(cookie);
    disconnect_mock_connection(c);
    if (c->references == 0) {
        delete c;
    }
}

void mock_set_ewouldblock_handling(const void* cookie, bool enable) {
    auto* c = cookie_to_mock_cookie(cookie);
    c->handle_ewouldblock = enable;
}

void mock_set_mutation_extras_handling(const void* cookie, bool enable) {
    auto* c = cookie_to_mock_cookie(cookie);
    c->handle_mutation_extras = enable;
}

void mock_set_datatype_support(const void* cookie,
                               protocol_binary_datatype_t datatypes) {
    auto* c = cookie_to_mock_cookie(cookie);
    c->enabled_datatypes = std::bitset<8>(datatypes);
}

void mock_set_collections_support(const void* cookie, bool enable) {
    auto* c = cookie_to_mock_cookie(cookie);
    c->handle_collections_support = enable;
}

void lock_mock_cookie(const void* cookie) {
    auto* c = cookie_to_mock_cookie(cookie);
    c->mutex.lock();
}

void unlock_mock_cookie(const void* cookie) {
    auto* c = cookie_to_mock_cookie(cookie);
    c->mutex.unlock();
}

void waitfor_mock_cookie(const void* cookie) {
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

int get_number_of_mock_cookie_references(const void* cookie) {
    if (cookie == nullptr) {
        return -1;
    }
    std::lock_guard<std::mutex> guard(mock_server_cookie_mutex);
    auto* c = cookie_to_mock_cookie(cookie);
    return c->references;
}

size_t get_number_of_mock_cookie_io_notifications(const void* cookie) {
    auto* c = cookie_to_mock_cookie(cookie);
    return c->num_io_notifications;
}

MockCookie* cookie_to_mock_cookie(const void* cookie) {
    auto* ret = reinterpret_cast<const MockCookie*>(cookie);
    ret->validate();
    return const_cast<MockCookie*>(ret);
}
