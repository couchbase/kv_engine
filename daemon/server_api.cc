/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "buckets.h"
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

    std::optional<AssociatedBucketHandle> tryAssociateBucket(
            EngineIface* engine) const override {
        auto* bucket = BucketManager::instance().tryAssociateBucket(engine);
        if (!bucket) {
            return {};
        }

        return AssociatedBucketHandle(engine, [bucket](EngineIface*) {
            BucketManager::instance().disassociateBucket(bucket);
        });
    }
};

struct ServerDocumentApi : public ServerDocumentIface {
    cb::engine_errc pre_link(CookieIface& void_cookie,
                             item_info& info) override {
        // Sanity check that people aren't calling the method with a bogus
        // cookie
        auto& cookie = asCookie(void_cookie);
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
        auto& cookie = asCookie(void_cookie);
        cb::audit::document::add(cookie, operation, cookie.getRequestKey());
    }

    void document_expired(const EngineIface& engine, size_t nbytes) override {
        BucketManager::instance().forEach([&engine, nbytes](Bucket& bucket) {
            if (bucket.type != BucketType::ClusterConfigOnly &&
                &engine == &bucket.getEngine()) {
                bucket.documentExpired(nbytes);
                return false;
            }
            return true;
        });
    }
};

struct ServerCookieApi : public ServerCookieIface {
    void setDcpConnHandler(CookieIface& cookie,
                           DcpConnHandlerIface* handler) override {
        asCookie(cookie).getConnection().setDcpConnHandlerIface(handler);
    }

    DcpConnHandlerIface* getDcpConnHandler(CookieIface& cookie) override {
        return asCookie(cookie).getConnection().getDcpConnHandlerIface();
    }

    void setDcpFlowControlBufferSize(CookieIface& cookie,
                                     std::size_t size) override {
        asCookie(cookie).getConnection().setDcpFlowControlBufferSize(size);
    }

    void reserve(CookieIface& void_cookie) override {
        asCookie(void_cookie).incrementRefcount();
    }

    void release(CookieIface& void_cookie) override {
        auto& cookie = asCookie(void_cookie);
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

    uint32_t get_privilege_context_revision(CookieIface& cookie) override {
        return asCookie(cookie).getPrivilegeContext().getGeneration();
    }

    bool is_valid_json(CookieIface& cookieIface,
                       std::string_view view) override {
        auto& cookie = asCookie(cookieIface);
        return cookie.getConnection().getThread().isValidJson(cookie, view);
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
