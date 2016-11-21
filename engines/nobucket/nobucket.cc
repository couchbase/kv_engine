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
class NoBucket : public ENGINE_HANDLE_V1 {
public:
    NoBucket() {
        memset(this, 0, sizeof(*this));
        ENGINE_HANDLE_V1::interface.interface = 1;
        ENGINE_HANDLE_V1::get_info = get_info;
        ENGINE_HANDLE_V1::initialize = initialize;
        ENGINE_HANDLE_V1::destroy = destroy;
        ENGINE_HANDLE_V1::allocate = item_allocate;
        ENGINE_HANDLE_V1::remove = item_delete;
        ENGINE_HANDLE_V1::release = item_release;
        ENGINE_HANDLE_V1::get = get;
        ENGINE_HANDLE_V1::get_stats = get_stats;
        ENGINE_HANDLE_V1::reset_stats = reset_stats;
        ENGINE_HANDLE_V1::store = store;
        ENGINE_HANDLE_V1::flush = flush;
        ENGINE_HANDLE_V1::unknown_command = unknown_command;
        ENGINE_HANDLE_V1::tap_notify = tap_notify;
        ENGINE_HANDLE_V1::get_tap_iterator = get_tap_iterator;
        ENGINE_HANDLE_V1::item_set_cas = item_set_cas;
        ENGINE_HANDLE_V1::get_item_info = get_item_info;
        ENGINE_HANDLE_V1::set_item_info = set_item_info;
        ENGINE_HANDLE_V1::dcp.step = dcp_step;
        ENGINE_HANDLE_V1::dcp.open = dcp_open;
        ENGINE_HANDLE_V1::dcp.add_stream = dcp_add_stream;
        ENGINE_HANDLE_V1::dcp.close_stream = dcp_close_stream;
        ENGINE_HANDLE_V1::dcp.get_failover_log = dcp_get_failover_log;
        ENGINE_HANDLE_V1::dcp.stream_req = dcp_stream_req;
        ENGINE_HANDLE_V1::dcp.stream_end = dcp_stream_end;
        ENGINE_HANDLE_V1::dcp.snapshot_marker = dcp_snapshot_marker;
        ENGINE_HANDLE_V1::dcp.mutation = dcp_mutation;
        ENGINE_HANDLE_V1::dcp.deletion = dcp_deletion;
        ENGINE_HANDLE_V1::dcp.expiration = dcp_expiration;
        ENGINE_HANDLE_V1::dcp.flush = dcp_flush;
        ENGINE_HANDLE_V1::dcp.set_vbucket_state = dcp_set_vbucket_state;
        info.description = "Disconnect engine v1.0";
    };

private:
    engine_info info;

    /**
     * Convert the ENGINE_HANDLE to the underlying class type
     *
     * @param handle the handle as provided by the frontend
     * @return the actual no bucket object
     */
    static NoBucket* get_handle(ENGINE_HANDLE* handle) {
        return reinterpret_cast<NoBucket*>(handle);
    }

    static const engine_info* get_info(ENGINE_HANDLE* handle) {
        return &get_handle(handle)->info;
    }

    static ENGINE_ERROR_CODE initialize(ENGINE_HANDLE*, const char*) {
        return ENGINE_SUCCESS;
    }

    static void destroy(ENGINE_HANDLE* handle, const bool) {
        delete get_handle(handle);
    }

    static ENGINE_ERROR_CODE item_allocate(ENGINE_HANDLE*, const void*,
                                           item**, const DocKey&, const size_t,
                                           const int, const rel_time_t,
                                           uint8_t, uint16_t) {
        return ENGINE_NO_BUCKET;
    }

    static ENGINE_ERROR_CODE item_delete(ENGINE_HANDLE*, const void*,
                                         const DocKey&, uint64_t*, uint16_t,
                                         mutation_descr_t*) {
        return ENGINE_NO_BUCKET;
    }

    static void item_release(ENGINE_HANDLE*, const void*, item*) {
        throw std::logic_error("NoBucket::item_release: no items should have"
                                   " been allocated from this engine");
    }

    static ENGINE_ERROR_CODE get(ENGINE_HANDLE*, const void*, item**,
                                 const DocKey&, uint16_t) {
        return ENGINE_NO_BUCKET;
    }

    static ENGINE_ERROR_CODE get_stats(ENGINE_HANDLE*, const void*,
                                       const char*, int, ADD_STAT) {
        return ENGINE_SUCCESS;
    }

    static ENGINE_ERROR_CODE store(ENGINE_HANDLE*, const void*, item*,
                                   uint64_t*, ENGINE_STORE_OPERATION) {
        return ENGINE_NO_BUCKET;
    }

    static ENGINE_ERROR_CODE flush(ENGINE_HANDLE*, const void*, time_t) {
        return ENGINE_NO_BUCKET;
    }

    static void reset_stats(ENGINE_HANDLE*, const void*) {
    }

    static ENGINE_ERROR_CODE unknown_command(ENGINE_HANDLE*, const void*,
                                             protocol_binary_request_header*,
                                             ADD_RESPONSE, DocNamespace) {
        return ENGINE_NO_BUCKET;
    }


    static void item_set_cas(ENGINE_HANDLE*, const void*, item*, uint64_t) {
        throw std::logic_error("NoBucket::item_set_cas: no items should have"
                                   " been allocated from this engine");
    }

    static bool get_item_info(ENGINE_HANDLE*, const void*, const item*,
                              item_info*) {
        throw std::logic_error("NoBucket::get_item_info: no items should have"
                                   " been allocated from this engine");
    }

    static bool set_item_info(ENGINE_HANDLE*, const void*, item*,
                              const item_info*) {
        throw std::logic_error("NoBucket::set_item_info: no items should have"
                                   " been allocated from this engine");
    }

    static ENGINE_ERROR_CODE tap_notify(ENGINE_HANDLE*, const void*, void*,
                                        uint16_t, uint8_t, uint16_t,
                                        tap_event_t, uint32_t, const void*,
                                        size_t, uint32_t, uint32_t, uint64_t,
                                        uint8_t, const void*, size_t,
                                        uint16_t) {
        return ENGINE_NO_BUCKET;
    }

    static TAP_ITERATOR get_tap_iterator(ENGINE_HANDLE*, const void*,
                                         const void*, size_t, uint32_t,
                                         const void*, size_t) {
        return nullptr;
    }

    static ENGINE_ERROR_CODE dcp_step(ENGINE_HANDLE*, const void*,
                                      struct dcp_message_producers*) {
        return ENGINE_NO_BUCKET;
    }

    static ENGINE_ERROR_CODE dcp_open(ENGINE_HANDLE*, const void*, uint32_t,
                                      uint32_t, uint32_t, void*, uint16_t) {
        return ENGINE_NO_BUCKET;
    }

    static ENGINE_ERROR_CODE dcp_add_stream(ENGINE_HANDLE*, const void*,
                                            uint32_t, uint16_t, uint32_t) {
        return ENGINE_NO_BUCKET;
    }

    static ENGINE_ERROR_CODE dcp_close_stream(ENGINE_HANDLE*, const void*,
                                              uint32_t, uint16_t) {
        return ENGINE_NO_BUCKET;
    }

    static ENGINE_ERROR_CODE dcp_stream_req(ENGINE_HANDLE*, const void*,
                                            uint32_t, uint32_t, uint16_t,
                                            uint64_t, uint64_t, uint64_t,
                                            uint64_t, uint64_t, uint64_t*,
                                            dcp_add_failover_log) {
        return ENGINE_NO_BUCKET;
    }

    static ENGINE_ERROR_CODE dcp_get_failover_log(ENGINE_HANDLE*, const void*,
                                                  uint32_t, uint16_t,
                                                  ENGINE_ERROR_CODE (* )(
                                                      vbucket_failover_t*,
                                                      size_t, const void*)) {
        return ENGINE_NO_BUCKET;
    }

    static ENGINE_ERROR_CODE dcp_stream_end(ENGINE_HANDLE*, const void*,
                                            uint32_t, uint16_t, uint32_t) {
        return ENGINE_NO_BUCKET;
    }

    static ENGINE_ERROR_CODE dcp_snapshot_marker(ENGINE_HANDLE*, const void*,
                                                 uint32_t, uint16_t, uint64_t,
                                                 uint64_t, uint32_t) {
        return ENGINE_NO_BUCKET;
    }

    static ENGINE_ERROR_CODE dcp_mutation(ENGINE_HANDLE*, const void*, uint32_t,
                                          const void*, uint16_t, const void*,
                                          uint32_t, uint64_t, uint16_t,
                                          uint32_t, uint8_t, uint64_t, uint64_t,
                                          uint32_t, uint32_t, const void*,
                                          uint16_t, uint8_t) {
        return ENGINE_NO_BUCKET;
    }

    static ENGINE_ERROR_CODE dcp_deletion(ENGINE_HANDLE*, const void*, uint32_t,
                                          const void*, uint16_t, uint64_t,
                                          uint16_t, uint64_t, uint64_t,
                                          const void*, uint16_t) {
        return ENGINE_NO_BUCKET;
    }

    static ENGINE_ERROR_CODE dcp_expiration(ENGINE_HANDLE*, const void*,
                                            uint32_t, const void*, uint16_t,
                                            uint64_t, uint16_t, uint64_t,
                                            uint64_t, const void*, uint16_t) {
        return ENGINE_NO_BUCKET;
    }

    static ENGINE_ERROR_CODE dcp_flush(ENGINE_HANDLE*, const void*, uint32_t,
                                       uint16_t) {
        return ENGINE_NO_BUCKET;
    }

    static ENGINE_ERROR_CODE dcp_set_vbucket_state(ENGINE_HANDLE*, const void*,
                                                   uint32_t, uint16_t,
                                                   vbucket_state_t) {
        return ENGINE_NO_BUCKET;
    }
};

MEMCACHED_PUBLIC_API
ENGINE_ERROR_CODE create_no_bucket_instance(uint64_t interface,
                                            GET_SERVER_API get_server_api,
                                            ENGINE_HANDLE **handle)
{
    if (interface != 1) {
        return ENGINE_ENOTSUP;
    }

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
