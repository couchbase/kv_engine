/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "connection.h"
#include "cookie.h"
#include "doc_pre_expiry.h"
#include "enginemap.h"
#include "environment.h"
#include "front_end_thread.h"
#include "mcaudit.h"
#include "memcached.h"
#include "server_core_api.h"
#include "tracing.h"
#include <memcached/engine.h>
#include <memcached/rbac/privileges.h>
#include <memcached/server_bucket_iface.h>
#include <memcached/server_cookie_iface.h>
#include <memcached/server_document_iface.h>
#include <phosphor/phosphor.h>
#include <utilities/engine_errc_2_mcbp.h>

static Cookie& getCookie(const CookieIface& void_cookie) {
    auto& cookie = dynamic_cast<const Cookie&>(void_cookie);
    return const_cast<Cookie&>(cookie);
}

struct ServerBucketApi : public ServerBucketIface {
    unique_engine_ptr createBucket(
            const std::string& module,
            ServerApi* (*get_server_api)()) const override {
        auto type = module_to_bucket_type(module);
        if (type == BucketType::Unknown) {
            return {};
        }

        try {
            return new_engine_instance(type, get_server_api);
        } catch (const std::exception&) {
            return {};
        }
    }
};

struct ServerDocumentApi : public ServerDocumentIface {
    cb::engine_errc pre_link(CookieIface& void_cookie,
                             item_info& info) override {
        // Sanity check that people aren't calling the method with a bogus
        // cookie
        auto& cookie = dynamic_cast<Cookie&>(void_cookie);

        auto* context = cookie.getCommandContext();
        if (context != nullptr) {
            return context->pre_link_document(info);
        }

        return cb::engine_errc::success;
    }

    std::string pre_expiry(const item_info& itm_info) override {
        return document_pre_expiry(itm_info);
    }
    void audit_document_access(
            CookieIface& void_cookie,
            cb::audit::document::Operation operation) override {
        auto& cookie = dynamic_cast<Cookie&>(void_cookie);
        cb::audit::document::add(cookie, operation);
    }
};

struct ServerCookieApi : public ServerCookieIface {
    void setDcpConnHandler(const CookieIface& cookie,
                           DcpConnHandlerIface* handler) override {
        getCookie(cookie).getConnection().setDcpConnHandlerIface(handler);
    }

    DcpConnHandlerIface* getDcpConnHandler(const CookieIface& cookie) override {
        return getCookie(cookie).getConnection().getDcpConnHandlerIface();
    }

    void setDcpFlowControlBufferSize(const CookieIface& cookie,
                                     std::size_t size) override {
        getCookie(cookie).getConnection().setDcpFlowControlBufferSize(size);
    }

    void notify_io_complete(const CookieIface& cookie,
                            cb::engine_errc status) override {
        notifyIoComplete(getCookie(cookie), status);
    }

    void scheduleDcpStep(const CookieIface& cookie) override {
        ::scheduleDcpStep(getCookie(cookie));
    }

    void reserve(const CookieIface& void_cookie) override {
        getCookie(void_cookie).incrementRefcount();
    }

    void release(const CookieIface& void_cookie) override {
        auto& cookie = getCookie(void_cookie);
        auto& connection = cookie.getConnection();
        connection.getThread().eventBase.runInEventBaseThreadAlwaysEnqueue(
                [&cookie]() {
                    TRACE_LOCKGUARD_TIMED(
                            cookie.getConnection().getThread().mutex,
                            "mutex",
                            "release",
                            SlowMutexThreshold);
                    cookie.decrementRefcount();
                    cookie.getConnection().triggerCallback();
                });
    }

    void set_priority(const CookieIface& cookie,
                      ConnectionPriority priority) override {
        getCookie(cookie).getConnection().setPriority(priority);
    }

    ConnectionPriority get_priority(const CookieIface& void_cookie) override {
        auto& cookie = dynamic_cast<const Cookie&>(void_cookie);
        return cookie.getConnection().getPriority();
    }

    uint64_t get_connection_id(const CookieIface& void_cookie) override {
        auto& cookie = dynamic_cast<const Cookie&>(void_cookie);
        return uint64_t(&cookie.getConnection());
    }

    cb::rbac::PrivilegeAccess check_privilege(
            const CookieIface& cookie,
            cb::rbac::Privilege privilege,
            std::optional<ScopeID> sid,
            std::optional<CollectionID> cid) override {
        return getCookie(cookie).checkPrivilege(privilege, sid, cid);
    }
    cb::rbac::PrivilegeAccess check_for_privilege_at_least_in_one_collection(
            const CookieIface& cookie, cb::rbac::Privilege privilege) override {
        return getCookie(cookie).checkForPrivilegeAtLeastInOneCollection(
                privilege);
    }
    uint32_t get_privilege_context_revision(
            const CookieIface& cookie) override {
        return getCookie(cookie).getPrivilegeContext().getGeneration();
    }
    cb::mcbp::Status engine_error2mcbp(const CookieIface& void_cookie,
                                       cb::engine_errc code) override {
        auto& cookie = dynamic_cast<const Cookie&>(void_cookie);
        auto& connection = cookie.getConnection();

        code = connection.remapErrorCode(code);
        if (code == cb::engine_errc::disconnect) {
            throw cb::engine_error(
                    cb::engine_errc::disconnect,
                    "engine_error2mcbp: " + std::to_string(connection.getId()) +
                            ": Disconnect client");
        }

        return cb::mcbp::to_status(cb::engine_errc(code));
    }

    std::pair<uint32_t, std::string> get_log_info(
            const CookieIface& void_cookie) override {
        auto& cookie = dynamic_cast<const Cookie&>(void_cookie);
        return std::make_pair(cookie.getConnectionId(),
                              cookie.getConnection().getDescription());
    }

    std::string get_authenticated_user(const CookieIface& cookie) override {
        return getCookie(cookie).getConnection().getUser().name;
    }

    in_port_t get_connected_port(const CookieIface& cookie) override {
        return getCookie(cookie).getConnection().getParentPort();
    }

    void set_error_context(CookieIface& cookie,
                           std::string_view message) override {
        getCookie(cookie).setErrorContext(std::string{message});
    }

    void set_error_json_extras(CookieIface& cookie,
                               const nlohmann::json& json) override {
        getCookie(cookie).setErrorJsonExtras(json);
    }

    void set_unknown_collection_error_context(CookieIface& cookie,
                                              uint64_t manifestUid) override {
        getCookie(cookie).setUnknownCollectionErrorContext(manifestUid);
    }

    bool is_valid_json(CookieIface& cookieIface,
                       std::string_view view) override {
        auto& cookie = getCookie(cookieIface);
        return cookie.getConnection().getThread().isValidJson(cookie, view);
    }

    void send_response(const CookieIface& cookieIface,
                       cb::engine_errc status,
                       std::string_view view) override {
        getCookie(cookieIface)
                .sendResponse(status, {}, {}, view, cb::mcbp::Datatype::Raw, 0);
    }

    void execution_complete(const CookieIface& cookieIface) override {
        auto& cookie = getCookie(cookieIface);
        executionComplete(cookie);
    }
};

class ServerApiImpl : public ServerApi {
public:
    ServerApiImpl() : ServerApi() {
        core = &core_api;
        cookie = &server_cookie_api;
        document = &document_api;
        bucket = &bucket_api;
    }

protected:
    ServerCoreApi core_api;
    ServerCookieApi server_cookie_api;
    ServerDocumentApi document_api;
    ServerBucketApi bucket_api;
};

/**
 * Callback the engines may call to get the public server interface
 * @return pointer to a structure containing the interface. The client should
 *         know the layout and perform the proper casts.
 */
ServerApi* get_server_api() {
    static ServerApiImpl rv;
    return &rv;
}
