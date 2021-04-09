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
#include "log_macros.h"
#include "mcaudit.h"
#include "memcached.h"
#include "server_core_api.h"
#include "settings.h"
#include "tracing.h"
#include <memcached/engine.h>
#include <memcached/rbac/privileges.h>
#include <memcached/server_bucket_iface.h>
#include <memcached/server_cookie_iface.h>
#include <memcached/server_document_iface.h>
#include <memcached/server_log_iface.h>
#include <phosphor/phosphor.h>
#include <utilities/engine_errc_2_mcbp.h>
#include <gsl/gsl>

static Cookie& getCookie(gsl::not_null<const void*> void_cookie) {
    auto* ccookie = reinterpret_cast<const Cookie*>(void_cookie.get());
    return *const_cast<Cookie*>(ccookie);
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

struct ServerLogApi : public ServerLogIface {
    spdlog::logger* get_spdlogger() override {
        return cb::logger::get();
    }

    void register_spdlogger(std::shared_ptr<spdlog::logger> logger) override {
        cb::logger::registerSpdLogger(logger);
    }

    void unregister_spdlogger(const std::string& name) override {
        cb::logger::unregisterSpdLogger(name);
    }

    void set_level(spdlog::level::level_enum severity) override {
        switch (severity) {
        case spdlog::level::level_enum::trace:
            Settings::instance().setVerbose(2);
            break;
        case spdlog::level::level_enum::debug:
            Settings::instance().setVerbose(1);
            break;
        default:
            Settings::instance().setVerbose(0);
            break;
        }
    }
};

struct ServerDocumentApi : public ServerDocumentIface {
    cb::engine_errc pre_link(gsl::not_null<const void*> void_cookie,
                             item_info& info) override {
        // Sanity check that people aren't calling the method with a bogus
        // cookie
        auto* cookie =
                reinterpret_cast<Cookie*>(const_cast<void*>(void_cookie.get()));

        auto* context = cookie->getCommandContext();
        if (context != nullptr) {
            return context->pre_link_document(info);
        }

        return cb::engine_errc::success;
    }

    std::string pre_expiry(const item_info& itm_info) override {
        return document_pre_expiry(itm_info);
    }
    void audit_document_access(
            gsl::not_null<const void*> void_cookie,
            cb::audit::document::Operation operation) override {
        auto* cookie =
                reinterpret_cast<Cookie*>(const_cast<void*>(void_cookie.get()));
        cb::audit::document::add(*cookie, operation);
    }
};

struct ServerCookieApi : public ServerCookieIface {
    void setDcpConnHandler(gsl::not_null<const void*> cookie,
                           DcpConnHandlerIface* handler) override {
        getCookie(cookie).getConnection().setDcpConnHandlerIface(handler);
    }

    DcpConnHandlerIface* getDcpConnHandler(
            gsl::not_null<const void*> cookie) override {
        return getCookie(cookie).getConnection().getDcpConnHandlerIface();
    }

    void setDcpFlowControlBufferSize(gsl::not_null<const void*> cookie,
                                     std::size_t size) override {
        getCookie(cookie).getConnection().setDcpFlowControlBufferSize(size);
    }

    void store_engine_specific(gsl::not_null<const void*> void_cookie,
                               void* engine_data) override {
        const auto* cc = reinterpret_cast<const Cookie*>(void_cookie.get());
        auto* cookie = const_cast<Cookie*>(cc);
        cookie->setEngineStorage(engine_data);
    }

    void* get_engine_specific(gsl::not_null<const void*> void_cookie) override {
        auto* cookie = reinterpret_cast<const Cookie*>(void_cookie.get());
        return cookie->getEngineStorage();
    }

    bool is_datatype_supported(gsl::not_null<const void*> void_cookie,
                               protocol_binary_datatype_t datatype) override {
        auto* cookie = reinterpret_cast<const Cookie*>(void_cookie.get());
        return cookie->getConnection().isDatatypeEnabled(datatype);
    }

    bool is_mutation_extras_supported(
            gsl::not_null<const void*> void_cookie) override {
        auto* cookie = reinterpret_cast<const Cookie*>(void_cookie.get());
        return cookie->getConnection().isSupportsMutationExtras();
    }

    bool is_collections_supported(
            gsl::not_null<const void*> void_cookie) override {
        auto* cookie = reinterpret_cast<const Cookie*>(void_cookie.get());
        return cookie->getConnection().isCollectionsSupported();
    }

    cb::mcbp::ClientOpcode get_opcode_if_ewouldblock_set(
            gsl::not_null<const void*> void_cookie) override {
        auto* cookie = reinterpret_cast<const Cookie*>(void_cookie.get());

        cb::mcbp::ClientOpcode opcode = cb::mcbp::ClientOpcode::Invalid;
        if (cookie->isEwouldblock()) {
            try {
                opcode =
                        cb::mcbp::ClientOpcode(cookie->getHeader().getOpcode());
            } catch (...) {
                // Don't barf out if the header isn't there
            }
        }
        return opcode;
    }

    void notify_io_complete(gsl::not_null<const void*> cookie,
                            cb::engine_errc status) override {
        notifyIoComplete(getCookie(cookie), status);
    }

    void scheduleDcpStep(gsl::not_null<const void*> cookie) override {
        ::scheduleDcpStep(getCookie(cookie));
    }

    void reserve(gsl::not_null<const void*> void_cookie) override {
        getCookie(void_cookie).incrementRefcount();
    }

    void release(gsl::not_null<const void*> void_cookie) override {
        auto& cookie = getCookie(void_cookie);
        auto& connection = cookie.getConnection();
        auto& thr = connection.getThread();

        TRACE_LOCKGUARD_TIMED(thr.mutex,
                              "mutex",
                              "release_cookie::threadLock",
                              SlowMutexThreshold);

        // Releasing the reference to the object may cause it to change
        // state. (NOTE: the release call shall never be called from the
        // worker threads), so put the connection in the pool of pending
        // IO and have the system retry the operation for the connection
        cookie.decrementRefcount();

        // kick the thread in the butt
        if (thr.notification.push(&connection)) {
            notify_thread(thr);
        }
    }

    void set_priority(gsl::not_null<const void*> cookie,
                      ConnectionPriority priority) override {
        getCookie(cookie).getConnection().setPriority(priority);
    }

    ConnectionPriority get_priority(
            gsl::not_null<const void*> void_cookie) override {
        auto* cookie = reinterpret_cast<const Cookie*>(void_cookie.get());
        return cookie->getConnection().getPriority();
    }

    uint64_t get_connection_id(
            gsl::not_null<const void*> void_cookie) override {
        auto* cookie = reinterpret_cast<const Cookie*>(void_cookie.get());
        return uint64_t(&cookie->getConnection());
    }

    cb::rbac::PrivilegeAccess check_privilege(
            gsl::not_null<const void*> cookie,
            cb::rbac::Privilege privilege,
            std::optional<ScopeID> sid,
            std::optional<CollectionID> cid) override {
        return getCookie(cookie).checkPrivilege(privilege, sid, cid);
    }

    cb::rbac::PrivilegeAccess test_privilege(
            gsl::not_null<const void*> cookie,
            cb::rbac::Privilege privilege,
            std::optional<ScopeID> sid,
            std::optional<CollectionID> cid) override {
        return getCookie(cookie).testPrivilege(privilege, sid, cid);
    }

    uint32_t get_privilege_context_revision(
            gsl::not_null<const void*> cookie) override {
        return getCookie(cookie).getPrivilegeContext().getGeneration();
    }
    cb::mcbp::Status engine_error2mcbp(gsl::not_null<const void*> void_cookie,
                                       cb::engine_errc code) override {
        const auto* cookie = reinterpret_cast<const Cookie*>(void_cookie.get());
        auto& connection = cookie->getConnection();

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
            gsl::not_null<const void*> void_cookie) override {
        auto* cookie = reinterpret_cast<const Cookie*>(void_cookie.get());
        return std::make_pair(cookie->getConnection().getId(),
                              cookie->getConnection().getDescription());
    }

    std::string get_authenticated_user(
            gsl::not_null<const void*> cookie) override {
        return getCookie(cookie).getConnection().getUser().name;
    }

    in_port_t get_connected_port(gsl::not_null<const void*> cookie) override {
        return getCookie(cookie).getConnection().getParentPort();
    }

    void set_error_context(gsl::not_null<void*> cookie,
                           std::string_view message) override {
        getCookie(cookie).setErrorContext(std::string{message});
    }

    void set_error_json_extras(gsl::not_null<void*> cookie,
                               const nlohmann::json& json) override {
        getCookie(cookie).setErrorJsonExtras(json);
    }

    void set_unknown_collection_error_context(gsl::not_null<void*> cookie,
                                              uint64_t manifestUid) override {
        getCookie(cookie).setUnknownCollectionErrorContext(manifestUid);
    }

    std::string_view get_inflated_payload(gsl::not_null<const void*> cookie,
                                          const cb::mcbp::Request&) override {
        return getCookie(cookie).getInflatedInputPayload();
    }
};

class ServerApiImpl : public ServerApi {
public:
    ServerApiImpl() : ServerApi() {
        core = &core_api;
        log = &server_log_api;
        cookie = &server_cookie_api;
        document = &document_api;
        bucket = &bucket_api;
    }

protected:
    ServerCoreApi core_api;
    ServerCookieApi server_cookie_api;
    ServerLogApi server_log_api;
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
