/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
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

#include "config.h"
#include "nobucket.h"

#include <memcached/dcp.h>

#include <cstdlib>
#include <stdexcept>
#include <string>

/**
 * The NoBucket is a bucket that just returns "ENGINE_NO_BUCKET" for "all"
 * operations. The main purpose for having this bucket is to reduce the
 * code complexity so we don't have to had checks all over the place to
 * see if a client is connected to a bucket or not (we may just associate
 * the connection to this bucket and it'll handle the appropriate
 * command.
 */
class NoBucket : public ENGINE_HANDLE_V1, public dcp_interface {
public:
    NoBucket() {
        dcp_interface::mutation = dcp_mutation;
        dcp_interface::deletion = dcp_deletion;
        dcp_interface::deletion_v2 = dcp_deletion_v2;
        dcp_interface::expiration = dcp_expiration;
        dcp_interface::flush = dcp_flush;
        dcp_interface::set_vbucket_state = dcp_set_vbucket_state;
        dcp_interface::system_event = dcp_system_event;
        ENGINE_HANDLE_V1::collections.set_manifest = collections_set_manifest;
        ENGINE_HANDLE_V1::collections.get_manifest = collections_get_manifest;
    };

    ENGINE_ERROR_CODE initialize(const char* config_str) override {
        return ENGINE_SUCCESS;
    }

    void destroy(bool) override {
        delete this;
    }

    cb::EngineErrorItemPair allocate(gsl::not_null<const void*>,
                                     const DocKey&,
                                     const size_t,
                                     const int,
                                     const rel_time_t,
                                     uint8_t,
                                     uint16_t) override {
        return cb::makeEngineErrorItemPair(cb::engine_errc::no_bucket);
    }

    std::pair<cb::unique_item_ptr, item_info> allocate_ex(
            gsl::not_null<const void*> cookie,
            const DocKey& key,
            size_t nbytes,
            size_t priv_nbytes,
            int flags,
            rel_time_t exptime,
            uint8_t datatype,
            uint16_t vbucket) override {
        throw cb::engine_error(cb::engine_errc::no_bucket, "no bucket");
    }

    ENGINE_ERROR_CODE remove(gsl::not_null<const void*>,
                             const DocKey&,
                             uint64_t&,
                             uint16_t,
                             mutation_descr_t&) override {
        return ENGINE_NO_BUCKET;
    }

    void release(gsl::not_null<item*>) override {
        throw std::logic_error(
                "NoBucket::item_release: no items should have"
                " been allocated from this engine");
    }

    cb::EngineErrorItemPair get(gsl::not_null<const void*>,
                                const DocKey&,
                                uint16_t,
                                DocStateFilter) override {
        return cb::makeEngineErrorItemPair(cb::engine_errc::no_bucket);
    }

    cb::EngineErrorItemPair get_if(
            gsl::not_null<const void*>,
            const DocKey&,
            uint16_t,
            std::function<bool(const item_info&)>) override {
        return cb::makeEngineErrorItemPair(cb::engine_errc::no_bucket);
    }

    cb::EngineErrorMetadataPair get_meta(gsl::not_null<const void*> cookie,
                                         const DocKey& key,
                                         uint16_t vbucket) override {
        return cb::EngineErrorMetadataPair(cb::engine_errc::no_bucket, {});
    }

    cb::EngineErrorItemPair get_locked(gsl::not_null<const void*>,
                                       const DocKey&,
                                       uint16_t,
                                       uint32_t) override {
        return cb::makeEngineErrorItemPair(cb::engine_errc::no_bucket);
    }

    ENGINE_ERROR_CODE unlock(gsl::not_null<const void*>,
                             const DocKey&,
                             uint16_t,
                             uint64_t) override {
        return ENGINE_NO_BUCKET;
    }

    cb::EngineErrorItemPair get_and_touch(gsl::not_null<const void*> cookie,
                                          const DocKey&,
                                          uint16_t,
                                          uint32_t) override {
        return cb::makeEngineErrorItemPair(cb::engine_errc::no_bucket);
    }

    ENGINE_ERROR_CODE store(gsl::not_null<const void*>,
                            gsl::not_null<item*>,
                            uint64_t&,
                            ENGINE_STORE_OPERATION,
                            DocumentState) override {
        return ENGINE_NO_BUCKET;
    }

    cb::EngineErrorCasPair store_if(gsl::not_null<const void*>,
                                    gsl::not_null<item*>,
                                    uint64_t,
                                    ENGINE_STORE_OPERATION,
                                    cb::StoreIfPredicate,
                                    DocumentState) override {
        return {cb::engine_errc::no_bucket, 0};
    }

    ENGINE_ERROR_CODE flush(gsl::not_null<const void*>) override {
        return ENGINE_NO_BUCKET;
    }

    ENGINE_ERROR_CODE get_stats(gsl::not_null<const void*>,
                                cb::const_char_buffer key,
                                ADD_STAT) override {
        return ENGINE_NO_BUCKET;
    }

    void reset_stats(gsl::not_null<const void*> cookie) override {
    }

    ENGINE_ERROR_CODE unknown_command(
            const void*,
            gsl::not_null<protocol_binary_request_header*>,
            ADD_RESPONSE,
            DocNamespace) override {
        return ENGINE_NO_BUCKET;
    }

    void item_set_cas(gsl::not_null<item*>, uint64_t) override {
        throw std::logic_error(
                "NoBucket::item_set_cas: no items should have"
                " been allocated from this engine");
    }

    void item_set_datatype(gsl::not_null<item*>,
                           protocol_binary_datatype_t) override {
        throw std::logic_error(
                "NoBucket::item_set_datatype: no items should have"
                " been allocated from this engine");
    }

    bool get_item_info(gsl::not_null<const item*>,
                       gsl::not_null<item_info*>) override {
        throw std::logic_error(
                "NoBucket::get_item_info: no items should have"
                " been allocated from this engine");
    }

    // DcpIface implementation ////////////////////////////////////////////////

    ENGINE_ERROR_CODE step(
            gsl::not_null<const void*>,
            gsl::not_null<struct dcp_message_producers*>) override {
        return ENGINE_NO_BUCKET;
    }

    ENGINE_ERROR_CODE open(gsl::not_null<const void*>,
                           uint32_t,
                           uint32_t,
                           uint32_t,
                           cb::const_char_buffer,
                           cb::const_byte_buffer) override {
        return ENGINE_NO_BUCKET;
    }

    ENGINE_ERROR_CODE add_stream(gsl::not_null<const void*>,
                                 uint32_t,
                                 uint16_t,
                                 uint32_t) override {
        return ENGINE_NO_BUCKET;
    }

    ENGINE_ERROR_CODE close_stream(gsl::not_null<const void*>,
                                   uint32_t,
                                   uint16_t) override {
        return ENGINE_NO_BUCKET;
    }

    ENGINE_ERROR_CODE stream_req(gsl::not_null<const void*>,
                                 uint32_t,
                                 uint32_t,
                                 uint16_t,
                                 uint64_t,
                                 uint64_t,
                                 uint64_t,
                                 uint64_t,
                                 uint64_t,
                                 uint64_t*,
                                 dcp_add_failover_log) override {
        return ENGINE_NO_BUCKET;
    }

    ENGINE_ERROR_CODE get_failover_log(
            gsl::not_null<const void*>,
            uint32_t,
            uint16_t,
            ENGINE_ERROR_CODE (*)(vbucket_failover_t*,
                                  size_t,
                                  gsl::not_null<const void*>)) override {
        return ENGINE_NO_BUCKET;
    }

    ENGINE_ERROR_CODE stream_end(gsl::not_null<const void*>,
                                 uint32_t,
                                 uint16_t,
                                 uint32_t) override {
        return ENGINE_NO_BUCKET;
    }

    ENGINE_ERROR_CODE snapshot_marker(gsl::not_null<const void*>,
                                      uint32_t,
                                      uint16_t,
                                      uint64_t,
                                      uint64_t,
                                      uint32_t) override {
        return ENGINE_NO_BUCKET;
    }

private:
    /**
     * Convert the ENGINE_HANDLE to the underlying class type
     *
     * @param handle the handle as provided by the frontend
     * @return the actual no bucket object
     */
    static NoBucket* get_handle(ENGINE_HANDLE* handle) {
        return reinterpret_cast<NoBucket*>(handle);
    }

    static bool set_item_info(gsl::not_null<ENGINE_HANDLE*>,
                              gsl::not_null<item*>,
                              gsl::not_null<const item_info*>) {
        throw std::logic_error(
                "NoBucket::set_item_info: no items should have"
                " been allocated from this engine");
    }

    static ENGINE_ERROR_CODE dcp_mutation(gsl::not_null<ENGINE_HANDLE*>,
                                          gsl::not_null<const void*>,
                                          uint32_t,
                                          const DocKey&,
                                          cb::const_byte_buffer,
                                          size_t,
                                          uint8_t,
                                          uint64_t,
                                          uint16_t,
                                          uint32_t,
                                          uint64_t,
                                          uint64_t,
                                          uint32_t,
                                          uint32_t,
                                          cb::const_byte_buffer,
                                          uint8_t) {
        return ENGINE_NO_BUCKET;
    }

    static ENGINE_ERROR_CODE dcp_deletion(gsl::not_null<ENGINE_HANDLE*>,
                                          gsl::not_null<const void*>,
                                          uint32_t,
                                          const DocKey&,
                                          cb::const_byte_buffer,
                                          size_t,
                                          uint8_t,
                                          uint64_t,
                                          uint16_t,
                                          uint64_t,
                                          uint64_t,
                                          cb::const_byte_buffer) {
        return ENGINE_NO_BUCKET;
    }

    static ENGINE_ERROR_CODE dcp_deletion_v2(gsl::not_null<ENGINE_HANDLE*>,
                                             gsl::not_null<const void*>,
                                             uint32_t,
                                             const DocKey&,
                                             cb::const_byte_buffer,
                                             size_t,
                                             uint8_t,
                                             uint64_t,
                                             uint16_t,
                                             uint64_t,
                                             uint64_t,
                                             uint32_t) {
        return ENGINE_NO_BUCKET;
    }

    static ENGINE_ERROR_CODE dcp_expiration(gsl::not_null<ENGINE_HANDLE*>,
                                            gsl::not_null<const void*>,
                                            uint32_t,
                                            const DocKey&,
                                            cb::const_byte_buffer,
                                            size_t,
                                            uint8_t,
                                            uint64_t,
                                            uint16_t,
                                            uint64_t,
                                            uint64_t,
                                            cb::const_byte_buffer) {
        return ENGINE_NO_BUCKET;
    }

    static ENGINE_ERROR_CODE dcp_flush(gsl::not_null<ENGINE_HANDLE*>,
                                       gsl::not_null<const void*>,
                                       uint32_t,
                                       uint16_t) {
        return ENGINE_NO_BUCKET;
    }

    static ENGINE_ERROR_CODE dcp_set_vbucket_state(
            gsl::not_null<ENGINE_HANDLE*>,
            gsl::not_null<const void*>,
            uint32_t,
            uint16_t,
            vbucket_state_t) {
        return ENGINE_NO_BUCKET;
    }

    static ENGINE_ERROR_CODE dcp_system_event(
            gsl::not_null<ENGINE_HANDLE*> handle,
            gsl::not_null<const void*> cookie,
            uint32_t opaque,
            uint16_t vbucket,
            mcbp::systemevent::id event,
            uint64_t bySeqno,
            cb::const_byte_buffer key,
            cb::const_byte_buffer eventData) {
        return ENGINE_NO_BUCKET;
    }

    static cb::engine_error collections_set_manifest(
            gsl::not_null<ENGINE_HANDLE*> handle, cb::const_char_buffer json) {
        return {cb::engine_errc::no_bucket,
                "nobucket::collections_set_manifest"};
    }

    static cb::EngineErrorStringPair collections_get_manifest(
            gsl::not_null<ENGINE_HANDLE*> handle) {
        return {cb::engine_errc::no_bucket,
                "nobucket::collections_get_manifest"};
    }
};

MEMCACHED_PUBLIC_API
ENGINE_ERROR_CODE create_no_bucket_instance(GET_SERVER_API get_server_api,
                                            ENGINE_HANDLE** handle) {
    try {
        NoBucket* engine = new NoBucket();
        *handle = reinterpret_cast<ENGINE_HANDLE*>(engine);
    } catch (std::bad_alloc& e) {
        auto logger = get_server_api()->log->get_logger();
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "NoBucket: failed to create engine: %s", e.what());
        return ENGINE_FAILED;
    }

    return ENGINE_SUCCESS;
}

MEMCACHED_PUBLIC_API
void destroy_engine(void) {
    // Empty
}
