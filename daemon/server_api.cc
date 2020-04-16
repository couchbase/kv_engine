/*
 *     Copyright 2017 Couchbase, Inc.
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

#include "connection.h"
#include "cookie.h"
#include "doc_pre_expiry.h"
#include "enginemap.h"
#include "environment.h"
#include "front_end_thread.h"
#include "log_macros.h"
#include "mc_time.h"
#include "mcaudit.h"
#include "memcached.h"
#include "session_cas.h"
#include "settings.h"
#include "tracing.h"
#include <memcached/engine.h>
#include <memcached/rbac/privileges.h>
#include <memcached/server_bucket_iface.h>
#include <memcached/server_cookie_iface.h>
#include <memcached/server_core_iface.h>
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
            SERVER_HANDLE_V1* (*get_server_api)()) const override {
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

struct ServerCoreApi : public ServerCoreIface {
    rel_time_t get_current_time() override {
        return mc_time_get_current_time();
    }

    rel_time_t realtime(rel_time_t exptime) override {
        return mc_time_convert_to_real_time(exptime);
    }

    time_t abstime(rel_time_t exptime) override {
        return mc_time_convert_to_abs_time(exptime);
    }

    time_t limit_abstime(time_t t, std::chrono::seconds limit) override {
        return mc_time_limit_abstime(t, limit);
    }

    int parse_config(const char* str,
                     config_item* items,
                     FILE* error) override {
        return ::parse_config(str, items, error);
    }

    ThreadPoolConfig getThreadPoolSizes() override {
        auto& instance = Settings::instance();
        return ThreadPoolConfig(instance.getNumReaderThreads(),
                                instance.getNumWriterThreads());
    }

    size_t getMaxEngineFileDescriptors() override {
        return environment.engine_file_descriptors;
    }

    bool isCollectionsEnabled() const override {
        return Settings::instance().isCollectionsEnabled();
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
    ENGINE_ERROR_CODE pre_link(gsl::not_null<const void*> void_cookie,
                               item_info& info) override {
        // Sanity check that people aren't calling the method with a bogus
        // cookie
        auto* cookie =
                reinterpret_cast<Cookie*>(const_cast<void*>(void_cookie.get()));

        auto* context = cookie->getCommandContext();
        if (context != nullptr) {
            return context->pre_link_document(info);
        }

        return ENGINE_SUCCESS;
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

    bool validate_session_cas(uint64_t cas) override {
        return session_cas.increment_session_counter(cas);
    }

    void decrement_session_ctr() override {
        session_cas.decrement_session_counter();
    }

    void notify_io_complete(gsl::not_null<const void*> cookie,
                            ENGINE_ERROR_CODE status) override {
        ::notify_io_complete(cookie, status);
    }

    ENGINE_ERROR_CODE reserve(gsl::not_null<const void*> void_cookie) override {
        getCookie(void_cookie).incrementRefcount();
        return ENGINE_SUCCESS;
    }

    ENGINE_ERROR_CODE release(gsl::not_null<const void*> void_cookie) override {
        auto& cookie = getCookie(void_cookie);
        auto& connection = cookie.getConnection();
        int notify;
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
        notify = add_conn_to_pending_io_list(
                &connection, nullptr, ENGINE_SUCCESS);

        // kick the thread in the butt
        if (notify) {
            notify_thread(thr);
        }

        return ENGINE_SUCCESS;
    }

    void set_priority(gsl::not_null<const void*> void_cookie,
                      CONN_PRIORITY priority) override {
        auto* cookie = reinterpret_cast<const Cookie*>(void_cookie.get());

        auto* c = &cookie->getConnection();
        switch (priority) {
        case CONN_PRIORITY_HIGH:
            c->setPriority(Connection::Priority::High);
            return;
        case CONN_PRIORITY_MED:
            c->setPriority(Connection::Priority::Medium);
            return;
        case CONN_PRIORITY_LOW:
            c->setPriority(Connection::Priority::Low);
            return;
        }

        LOG_WARNING(
                "{}: ServerCookieApi::set_priority: priority (which is {}) is "
                "not a "
                "valid CONN_PRIORITY - closing connection {}",
                c->getId(),
                priority,
                c->getDescription());
        c->shutdown();
    }

    CONN_PRIORITY get_priority(
            gsl::not_null<const void*> void_cookie) override {
        auto* cookie = reinterpret_cast<const Cookie*>(void_cookie.get());

        auto& conn = cookie->getConnection();
        const auto priority = conn.getPriority();
        switch (priority) {
        case Connection::Priority::High:
            return CONN_PRIORITY_HIGH;
        case Connection::Priority::Medium:
            return CONN_PRIORITY_MED;
        case Connection::Priority::Low:
            return CONN_PRIORITY_LOW;
        }

        LOG_WARNING(
                "{}: ServerCookieApi::get_priority: priority (which is {}) is "
                "not a "
                "valid CONN_PRIORITY. {}",
                conn.getId(),
                int(priority),
                conn.getDescription());
        return CONN_PRIORITY_MED;
    }

    bucket_id_t get_bucket_id(gsl::not_null<const void*> cookie) override {
        return bucket_id_t(getCookie(cookie).getConnection().getBucketIndex());
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
                                       ENGINE_ERROR_CODE code) override {
        const auto* cookie = reinterpret_cast<const Cookie*>(void_cookie.get());
        auto& connection = cookie->getConnection();

        code = connection.remapErrorCode(code);
        if (code == ENGINE_DISCONNECT) {
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

class ServerApi : public SERVER_HANDLE_V1 {
public:
    ServerApi() : server_handle_v1_t() {
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
SERVER_HANDLE_V1* get_server_api() {
    static ServerApi rv;
    return &rv;
}
