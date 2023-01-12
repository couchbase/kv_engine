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
    void setDcpConnHandler(CookieIface& cookie,
                           DcpConnHandlerIface* handler) override {
        wrapped->setDcpConnHandler(cookie, handler);
    }
    DcpConnHandlerIface* getDcpConnHandler(CookieIface& cookie) override {
        return wrapped->getDcpConnHandler(cookie);
    }
    void setDcpFlowControlBufferSize(CookieIface& cookie,
                                     std::size_t size) override {
        wrapped->setDcpFlowControlBufferSize(cookie, size);
    }
    void reserve(CookieIface& cookie) override {
        wrapped->reserve(cookie);
    }
    void release(CookieIface& cookie) override {
        wrapped->release(cookie);
    }
    uint32_t get_privilege_context_revision(CookieIface& cookie) override {
        return wrapped->get_privilege_context_revision(cookie);
    }
    std::string get_authenticated_user(CookieIface& cookie) override {
        return wrapped->get_authenticated_user(cookie);
    }
    in_port_t get_connected_port(CookieIface& cookie) override {
        return wrapped->get_connected_port(cookie);
    }
    bool is_valid_json(CookieIface& cookie, std::string_view view) override {
        return wrapped->is_valid_json(cookie, view);
    }

protected:
    ServerCookieIface* wrapped;
};
