/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <memcached/cookie_iface.h>
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

struct MockCookie : public CookieIface {
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
    cb::engine_errc status{cb::engine_errc::success};
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
            engine->disconnect(*this);
        }
    }

    cb::compression::Buffer inflated_payload;

protected:
    static const uint64_t MAGIC = 0xbeefcafecafebeefULL;
    EngineIface* engine;
};

CookieIface* create_mock_cookie(EngineIface* engine = nullptr);

void destroy_mock_cookie(CookieIface* cookie);

void mock_set_ewouldblock_handling(const CookieIface* cookie, bool enable);

void mock_set_mutation_extras_handling(const CookieIface* cookie, bool enable);

void mock_set_collections_support(const CookieIface* cookie, bool enable);

bool mock_is_collections_supported(const CookieIface* cookie);

void mock_set_datatype_support(const CookieIface* cookie,
                               protocol_binary_datatype_t datatypes);

void lock_mock_cookie(const CookieIface* cookie);

void unlock_mock_cookie(const CookieIface* cookie);

void waitfor_mock_cookie(const CookieIface* cookie);

void disconnect_all_mock_connections();

int get_number_of_mock_cookie_references(const CookieIface* cookie);

size_t get_number_of_mock_cookie_io_notifications(const CookieIface* cookie);

MockCookie* cookie_to_mock_cookie(const CookieIface* cookie);
