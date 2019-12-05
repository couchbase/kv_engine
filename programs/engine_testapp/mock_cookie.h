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
#include <memcached/server_callback_iface.h>
#include <memcached/tracer.h>

#include <atomic>
#include <bitset>
#include <condition_variable>
#include <mutex>
#include <string>

struct MockCookie : cb::tracing::Traceable {
    const uint64_t magic{MAGIC};
    void* engine_data{};
    bool connected{};
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

    void validate() const;

protected:
    static const uint64_t MAGIC = 0xbeefcafecafebeefULL;
};

const void* create_mock_cookie();

void destroy_mock_cookie(const void* cookie);

void mock_set_ewouldblock_handling(const void* cookie, bool enable);

void mock_set_mutation_extras_handling(const void* cookie, bool enable);

void mock_set_collections_support(const void* cookie, bool enable);

void mock_set_datatype_support(const void* cookie,
                               protocol_binary_datatype_t datatypes);

void lock_mock_cookie(const void* cookie);

void unlock_mock_cookie(const void* cookie);

void waitfor_mock_cookie(const void* cookie);

void disconnect_all_mock_connections();

int get_number_of_mock_cookie_references(const void* cookie);

size_t get_number_of_mock_cookie_io_notifications(const void* cookie);

MockCookie* cookie_to_mock_object(const void* cookie);
