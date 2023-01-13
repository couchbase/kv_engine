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

#include <memcached/connection_iface.h>
#include <memcached/cookie_iface.h>
#include <memcached/engine_error.h>
#include <memcached/rbac/privilege_database.h>
#include <memcached/tracer.h>
#include <memcached/types.h>
#include <platform/compression/buffer.h>
#include <platform/socket.h>
#include <atomic>
#include <bitset>
#include <mutex>
#include <string>

class DcpConnHandlerIface;
struct EngineIface;

class MockConnection : public ConnectionIface {
public:
    MockConnection()
        : ConnectionIface({{"ip", "127.0.0.1"}, {"port", 665}},
                          {{"ip", "127.0.0.1"}, {"port", 666}}) {
    }
    void scheduleDcpStep() override;
    void setUserScheduleDcpStep(std::function<void()> func);
    std::function<void()> userScheduleDcpStep;

    ConnectionPriority getPriority() const override {
        return priority;
    }

    void setPriority(ConnectionPriority value) override {
        priority = value;
    }

    std::string_view getDescription() const override;
    const cb::rbac::UserIdent& getUser() const override;

protected:
    ConnectionPriority priority{ConnectionPriority::Medium};
    cb::rbac::UserIdent user{"nobody", cb::rbac::Domain::Local};
};

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

    ConnectionIface& getConnectionIface() override;

    // Get the actual connection we've got
    MockConnection& getConnection() {
        return *connection;
    };

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

    void notifyIoComplete(cb::engine_errc status) override;

    std::mutex& getMutex();
    void lock();
    void unlock();

    /// decrement the ref count and signal the bucket that we're disconnecting
    void disconnect();

    std::string_view getInflatedInputPayload() const override {
        return {inflated_payload.data(), inflated_payload.size()};
    }

    bool isValidJson(std::string_view view) override;

    uint32_t getPrivilegeContextRevision() override;

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

    cb::rbac::PrivilegeAccess checkPrivilege(
            cb::rbac::Privilege privilege,
            std::optional<ScopeID> sid,
            std::optional<CollectionID> cid) override;

    using CheckForPrivilegeAtLeastInOneCollectionFunction =
            std::function<cb::rbac::PrivilegeAccess(const CookieIface&,
                                                    cb::rbac::Privilege)>;
    static void setCheckForPrivilegeAtLeastInOneCollectionFunction(
            CheckForPrivilegeAtLeastInOneCollectionFunction func) {
        checkForPrivilegeAtLeastInOneCollectionFunction = std::move(func);
    }
    cb::rbac::PrivilegeAccess checkForPrivilegeAtLeastInOneCollection(
            cb::rbac::Privilege privilege) const override;

    void setCurrentCollectionInfo(ScopeID sid,
                                  CollectionID cid,
                                  uint64_t manifestUid,
                                  bool metered) override;
    bool checkThrottle(size_t, size_t) override;
    bool sendResponse(cb::engine_errc status,
                      std::string_view extras,
                      std::string_view value) override;
    void setErrorContext(std::string message) override {
        error_context = std::move(message);
    }
    std::string getErrorContext() const override {
        return error_context;
    }

    /// An alternative function to call for notifyIoComplete
    void setUserNotifyIoComplete(std::function<void(cb::engine_errc)> func) {
        userNotifyIoComplete = std::move(func);
    }

protected:
    static CheckPrivilegeFunction checkPrivilegeFunction;
    static CheckForPrivilegeAtLeastInOneCollectionFunction
            checkForPrivilegeAtLeastInOneCollectionFunction;

    uint32_t sfd{};
    bool handle_ewouldblock{true};
    bool handle_mutation_extras{true};
    std::bitset<8> enabled_datatypes;
    bool handle_collections_support{false};
    std::mutex mutex;
    std::atomic<uint8_t> references{1};
    std::string error_context;
    cb::compression::Buffer inflated_payload;
    EngineIface* engine = nullptr;
    std::unique_ptr<MockConnection> connection;
    std::function<void(cb::engine_errc)> userNotifyIoComplete;
};

MockCookie* create_mock_cookie(EngineIface* engine = nullptr);

void destroy_mock_cookie(CookieIface* cookie);

MockCookie* cookie_to_mock_cookie(CookieIface* cookie);
MockCookie& asMockCookie(CookieIface& cookie);
