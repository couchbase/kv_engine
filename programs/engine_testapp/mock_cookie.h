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
#pragma once

#include <memcached/engine.h>
#include <memcached/engine_testapp.h>
#include <memcached/tracer.h>

#include <platform/compression/buffer.h>
#include <atomic>
#include <bitset>
#include <condition_variable>
#include <mutex>
#include <string>

class DcpConnHandlerIface;

struct MockCookie : cb::tracing::Traceable {
    /**
     * Create a new cookie which isn't bound to an engine. This cookie won't
     * notify the engine when it disconnects.
     */
    MockCookie() : MockCookie(nullptr){};

    /**
     * Create a new cookie which is bound to the provided engine.
     *
     * @param e the engine to notify (or nullptr if no engine is to be
     *          notified
     */
    explicit MockCookie(EngineIface* e);

    ~MockCookie() override;

    const uint64_t magic{MAGIC};
    void* engine_data{};
    int sfd{};
    ENGINE_ERROR_CODE status{ENGINE_SUCCESS};
    int nblocks{0}; /* number of ewouldblocks */
    bool handle_ewouldblock{true};
    bool handle_mutation_extras{true};
    std::bitset<8> enabled_datatypes;
    bool handle_collections_support{false};
    std::mutex mutex;
    std::condition_variable cond;
    int references{1};
    uint64_t num_io_notifications{};
    uint64_t num_processed_notifications{};
    std::string authenticatedUser{"nobody"};
    in_port_t parent_port{666};
    DcpConnHandlerIface* connHandlerIface = nullptr;

    void validate() const;

    /// decrement the ref count and signal the bucket that we're disconnecting
    void disconnect() {
        references--;
        if (engine) {
            engine->disconnect(static_cast<const void*>(this));
        }
    }

    cb::compression::Buffer inflated_payload;

protected:
    static const uint64_t MAGIC = 0xbeefcafecafebeefULL;
    EngineIface* engine;
};

cb::tracing::Traceable* create_mock_cookie(EngineIface* engine = nullptr);

void destroy_mock_cookie(cb::tracing::Traceable* cookie);

void mock_set_ewouldblock_handling(const void* cookie, bool enable);

void mock_set_mutation_extras_handling(const void* cookie, bool enable);

void mock_set_collections_support(const void* cookie, bool enable);

bool mock_is_collections_supported(const void* cookie);

void mock_set_datatype_support(const void* cookie,
                               protocol_binary_datatype_t datatypes);

void lock_mock_cookie(const void* cookie);

void unlock_mock_cookie(const void* cookie);

void waitfor_mock_cookie(const void* cookie);

void disconnect_all_mock_connections();

int get_number_of_mock_cookie_references(const void* cookie);

size_t get_number_of_mock_cookie_io_notifications(const void* cookie);

MockCookie* cookie_to_mock_cookie(const void* cookie);
