/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
/*
 * Helper for wrapping a ServerCookieIface to override specific methods for
 * a particular test.
 */
#pragma once

#include "programs/engine_testapp/mock_server.h"
#include <memcached/server_cookie_iface.h>

/**
 * Tests may need to mock around with the notify_io_complete
 * method. Previously we copied in a new notify_io_complete method, but
 * we can't do that as the cookie interface contains virtual pointers.
 * An easier approach is to create a class which just wraps the server
 * API and we may subclass this class to override whatever method we want
 *
 * The constructor installs itself as the mock server cookie interface,
 * and the destructor reinstalls the original server cookie interface.
 */
class WrappedServerCookieIface : public ServerCookieIface {
public:
    WrappedServerCookieIface() : wrapped(get_mock_server_api()->cookie) {
        get_mock_server_api()->cookie = this;
    }

    ~WrappedServerCookieIface() override {
        get_mock_server_api()->cookie = wrapped;
    }
    void setDcpConnHandler(const CookieIface& cookie,
                           DcpConnHandlerIface* handler) override {
        wrapped->setDcpConnHandler(cookie, handler);
    }
    DcpConnHandlerIface* getDcpConnHandler(const CookieIface& cookie) override {
        return wrapped->getDcpConnHandler(cookie);
    }
    void setDcpFlowControlBufferSize(const CookieIface& cookie,
                                     std::size_t size) override {
        wrapped->setDcpFlowControlBufferSize(cookie, size);
    }
    void notify_io_complete(const CookieIface& cookie,
                            cb::engine_errc status) override {
        return wrapped->notify_io_complete(cookie, status);
    }
    void scheduleDcpStep(const CookieIface& cookie) override {
        wrapped->scheduleDcpStep(cookie);
    }
    void reserve(const CookieIface& cookie) override {
        wrapped->reserve(cookie);
    }
    void release(const CookieIface& cookie) override {
        wrapped->release(cookie);
    }
    void set_priority(const CookieIface& cookie,
                      ConnectionPriority priority) override {
        return wrapped->set_priority(cookie, priority);
    }
    ConnectionPriority get_priority(const CookieIface& cookie) override {
        return wrapped->get_priority(cookie);
    }
    uint64_t get_connection_id(const CookieIface& cookie) override {
        return wrapped->get_connection_id(cookie);
    }
    cb::rbac::PrivilegeAccess check_privilege(
            const CookieIface& cookie,
            cb::rbac::Privilege privilege,
            std::optional<ScopeID> sid,
            std::optional<CollectionID> cid) override {
        return wrapped->check_privilege(cookie, privilege, sid, cid);
    }
    cb::rbac::PrivilegeAccess check_for_privilege_at_least_in_one_collection(
            const CookieIface& cookie, cb::rbac::Privilege privilege) override {
        return wrapped->check_for_privilege_at_least_in_one_collection(
                cookie, privilege);
    }
    uint32_t get_privilege_context_revision(
            const CookieIface& cookie) override {
        return wrapped->get_privilege_context_revision(cookie);
    }
    cb::mcbp::Status engine_error2mcbp(const CookieIface& cookie,
                                       cb::engine_errc code) override {
        return wrapped->engine_error2mcbp(cookie, code);
    }
    std::pair<uint32_t, std::string> get_log_info(
            const CookieIface& cookie) override {
        return wrapped->get_log_info(cookie);
    }
    std::string get_authenticated_user(const CookieIface& cookie) override {
        return wrapped->get_authenticated_user(cookie);
    }
    in_port_t get_connected_port(const CookieIface& cookie) override {
        return wrapped->get_connected_port(cookie);
    }
    void set_error_context(CookieIface& cookie,
                           std::string_view message) override {
        wrapped->set_error_context(cookie, message);
    }
    void set_error_json_extras(CookieIface& cookie,
                               const nlohmann::json& json) override {
        wrapped->set_error_json_extras(cookie, json);
    }
    void set_unknown_collection_error_context(CookieIface& cookie,
                                              uint64_t manifestUid) override {
        wrapped->set_unknown_collection_error_context(cookie, manifestUid);
    }

    bool is_valid_json(CookieIface& cookie, std::string_view view) override {
        return wrapped->is_valid_json(cookie, view);
    }
    void send_response(const CookieIface& cookie,
                       cb::engine_errc status,
                       std::string_view view) override {
        wrapped->send_response(cookie, status, view);
    }

protected:
    ServerCookieIface* wrapped;
};
