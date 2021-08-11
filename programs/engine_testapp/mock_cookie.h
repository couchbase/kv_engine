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

#include <platform/compress.h>
#include <platform/compression/buffer.h>
#include <atomic>
#include <bitset>
#include <condition_variable>
#include <mutex>
#include <string>

class DcpConnHandlerIface;

class MockCookie : public CookieIface {
public:
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

    Tenant* getTenant() override {
        return nullptr;
    }

    const Tenant* getTenant() const override {
        return nullptr;
    }

    /// Is the current cookie blocked?
    bool isEwouldblock() const {
        return handle_ewouldblock;
    }

    /// Set the ewouldblock status for the cookie
    void setEwouldblock(bool ewouldblock);

    // The source code was initially written in C which didn't have the
    // concept of shared pointers so the current code use a manual
    // reference counting. If the engine wants to keep a reference to the
    // cookie it must bump the reference count to avoid the core to reuse
    // the cookie leaving the engine with a dangling pointer.

    /// Get the current reference count
    uint8_t getRefcount() {
        return references;
    }

    /// Add a reference to the cookie
    /// returns the incremented ref count
    uint8_t incrementRefcount() {
        return ++references;
    }

    /// Release a reference to the cookie
    /// returns the decremented ref count
    uint8_t decrementRefcount() {
        return --references;
    }

    void* getEngineStorage() const override {
        return engine_data;
    }

    void setEngineStorage(void* value) override {
        engine_data = value;
    }

    void setConHandler(DcpConnHandlerIface* handler) {
        connHandlerIface = handler;
    }
    DcpConnHandlerIface* getConHandler() const {
        return connHandlerIface;
    }

    void setMutationExtrasHandling(bool enable);
    bool isMutationExtrasSupported() const override;

    void setDatatypeSupport(protocol_binary_datatype_t datatypes);
    bool isDatatypeSupported(
            protocol_binary_datatype_t datatype) const override;

    void setCollectionsSupport(bool enable);
    bool isCollectionsSupported() const override;

    uint32_t getConnectionId() const override {
        return sfd;
    }

    std::mutex& getMutex();
    void lock();
    void unlock();
    void wait();

    /// decrement the ref count and signal the bucket that we're disconnecting
    void disconnect();

    std::string_view getInflatedInputPayload() const override {
        return {inflated_payload.data(), inflated_payload.size()};
    }

    uint64_t getNumIoNotifications() const {
        return num_io_notifications;
    }
    void handleIoComplete(cb::engine_errc completeStatus);

    void setStatus(cb::engine_errc newStatus);
    cb::engine_errc getStatus() const;

    std::string getAuthedUser() const {
        return authenticatedUser;
    }

    in_port_t getParentPort() const {
        return parent_port;
    }

    void waitForNotifications(std::unique_lock<std::mutex>& lock);

    using CheckPrivilegeFunction = std::function<cb::rbac::PrivilegeAccess(
            const CookieIface&,
            cb::rbac::Privilege,
            std::optional<ScopeID>,
            std::optional<CollectionID>)>;
    static void setCheckPrivilegeFunction(CheckPrivilegeFunction func) {
        checkPrivilegeFunction = std::move(func);
    }

    cb::rbac::PrivilegeAccess testPrivilege(
            cb::rbac::Privilege privilege,
            std::optional<ScopeID> sid,
            std::optional<CollectionID> cid) const override;

protected:
    static CheckPrivilegeFunction checkPrivilegeFunction;

    void* engine_data{nullptr};
    uint32_t sfd{};
    cb::engine_errc status{cb::engine_errc::success};
    bool handle_ewouldblock{true};
    bool handle_mutation_extras{true};
    std::bitset<8> enabled_datatypes;
    bool handle_collections_support{false};
    std::mutex mutex;
    std::condition_variable cond;
    std::atomic<uint8_t> references{1};
    uint64_t num_io_notifications{};
    uint64_t num_processed_notifications{};
    std::string authenticatedUser{"nobody"};
    in_port_t parent_port{666};
    DcpConnHandlerIface* connHandlerIface = nullptr;

    cb::compression::Buffer inflated_payload;
    EngineIface* engine = nullptr;
};

MockCookie* create_mock_cookie(EngineIface* engine = nullptr);

void destroy_mock_cookie(CookieIface* cookie);

MockCookie* cookie_to_mock_cookie(const CookieIface* cookie);
MockCookie& cookie_to_mock_cookie(const CookieIface& cookie);
