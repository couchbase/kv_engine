/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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

#include "mcbp_executors.h"

#include "buckets.h"
#include "config_parse.h"
#include "debug_helpers.h"
#include "ioctl.h"
#include "mc_time.h"
#include "mcaudit.h"
#include "mcbp.h"
#include "mcbp_privileges.h"
#include "mcbp_topkeys.h"
#include "protocol/mcbp/appendprepend_context.h"
#include "protocol/mcbp/arithmetic_context.h"
#include "protocol/mcbp/audit_configure_context.h"
#include "protocol/mcbp/create_remove_bucket_command_context.h"
#include "protocol/mcbp/dcp_deletion.h"
#include "protocol/mcbp/dcp_expiration.h"
#include "protocol/mcbp/dcp_mutation.h"
#include "protocol/mcbp/dcp_system_event_executor.h"
#include "protocol/mcbp/engine_wrapper.h"
#include "protocol/mcbp/executors.h"
#include "protocol/mcbp/flush_command_context.h"
#include "protocol/mcbp/gat_context.h"
#include "protocol/mcbp/get_context.h"
#include "protocol/mcbp/get_locked_context.h"
#include "protocol/mcbp/get_meta_context.h"
#include "protocol/mcbp/mutation_context.h"
#include "protocol/mcbp/rbac_reload_command_context.h"
#include "protocol/mcbp/remove_context.h"
#include "protocol/mcbp/sasl_auth_command_context.h"
#include "protocol/mcbp/sasl_refresh_command_context.h"
#include "protocol/mcbp/stats_context.h"
#include "protocol/mcbp/unlock_context.h"
#include "sasl_tasks.h"
#include "session_cas.h"
#include "subdocument.h"

#include <utilities/protocol2text.h>

std::array<bool, 0x100>&  topkey_commands = get_mcbp_topkeys();
std::array<mcbp_package_execute, 0x100>& executors = get_mcbp_executors();

/**
 * Triggers topkeys_update (i.e., increments topkeys stats) if called by a
 * valid operation.
 */
void update_topkeys(const DocKey& key, McbpConnection* c) {

    if (topkey_commands[c->binary_header.request.opcode]) {
        if (all_buckets[c->getBucketIndex()].topkeys != nullptr) {
            all_buckets[c->getBucketIndex()].topkeys->updateKey(key.data(),
                                                                key.size(),
                                                                mc_time_get_current_time());
        }
    }
}

static void process_bin_get(Cookie& cookie) {
    cookie.obtainContext<GetCommandContext>(cookie).drive();
}

static void process_bin_get_meta(Cookie& cookie) {
    cookie.obtainContext<GetMetaCommandContext>(cookie).drive();
}

static void get_locked_executor(Cookie& cookie) {
    cookie.obtainContext<GetLockedCommandContext>(cookie).drive();
}

static void unlock_executor(Cookie& cookie) {
    cookie.obtainContext<UnlockCommandContext>(cookie).drive();
}

static void gat_executor(Cookie& cookie) {
    cookie.obtainContext<GatCommandContext>(cookie).drive();
}


static ENGINE_ERROR_CODE default_unknown_command(
    EXTENSION_BINARY_PROTOCOL_DESCRIPTOR*,
    ENGINE_HANDLE*,
    const void* void_cookie,
    protocol_binary_request_header* request,
    ADD_RESPONSE response) {

    auto* cookie = reinterpret_cast<const Cookie*>(void_cookie);
    return bucket_unknown_command(&cookie->getConnection(), request, response);
}

struct request_lookup {
    EXTENSION_BINARY_PROTOCOL_DESCRIPTOR* descriptor;
    BINARY_COMMAND_CALLBACK callback;
};

static struct request_lookup request_handlers[0x100];

typedef void (* RESPONSE_HANDLER)(McbpConnection*);

/**
 * A map between the response packets op-code and the function to handle
 * the response message.
 */
static std::array<RESPONSE_HANDLER, 0x100> response_handlers;

void setup_mcbp_lookup_cmd(
    EXTENSION_BINARY_PROTOCOL_DESCRIPTOR* descriptor,
    uint8_t cmd,
    BINARY_COMMAND_CALLBACK new_handler) {
    request_handlers[cmd].descriptor = descriptor;
    request_handlers[cmd].callback = new_handler;
}

static void process_bin_unknown_packet(Cookie& cookie) {
    auto* req = reinterpret_cast<protocol_binary_request_header*>(
            cookie.getPacketAsVoidPtr());
    auto& connection = cookie.getConnection();

    ENGINE_ERROR_CODE ret = cookie.getAiostat();
    cookie.setAiostat(ENGINE_SUCCESS);
    cookie.setEwouldblock(false);

    if (ret == ENGINE_SUCCESS) {
        struct request_lookup* rq = request_handlers + req->request.opcode;
        ret = rq->callback(rq->descriptor,
                           connection.getBucketEngineAsV0(),
                           static_cast<const void*>(&cookie),
                           req,
                           mcbp_response_handler);
    }

    ret = cookie.getConnection().remapErrorCode(ret);
    switch (ret) {
    case ENGINE_SUCCESS: {
        if (cookie.getDynamicBuffer().getRoot() != nullptr) {
            // We assume that if the underlying engine returns a success then
            // it is sending a success to the client.
            ++connection.getBucket()
                      .responseCounters[PROTOCOL_BINARY_RESPONSE_SUCCESS];
            mcbp_write_and_free(&connection, &cookie.getDynamicBuffer());
        } else {
            connection.setState(McbpStateMachine::State::new_cmd);
        }
        update_topkeys(DocKey(req->bytes + sizeof(*req) + req->request.extlen,
                              ntohs(req->request.keylen),
                              connection.getDocNamespace()),
                       &connection);
        break;
    }
    case ENGINE_EWOULDBLOCK:
        cookie.setEwouldblock(true);
        break;
    case ENGINE_DISCONNECT:
        connection.setState(McbpStateMachine::State::closing);
        break;
    default:
        // Release the dynamic buffer.. it may be partial..
        cookie.clearDynamicBuffer();
        cookie.sendResponse(cb::engine_errc(ret));
    }
}

/**
 * We received a noop response.. just ignore it
 */
static void process_bin_noop_response(McbpConnection* c) {
    c->setState(McbpStateMachine::State::new_cmd);
}

static void add_set_replace_executor(Cookie& cookie,
                                     ENGINE_STORE_OPERATION store_op) {
    auto& connection = cookie.getConnection();
    auto* req = reinterpret_cast<protocol_binary_request_set*>(
            cookie.getPacketAsVoidPtr());
    cookie.obtainContext<MutationCommandContext>(connection, req, store_op)
            .drive();
}

static void add_executor(Cookie& cookie) {
    auto& req = cookie.getRequest(Cookie::PacketContent::Full);
    auto& connection = cookie.getConnection();
    connection.setNoReply(req.isQuiet());
    add_set_replace_executor(cookie, OPERATION_ADD);
}

static void set_executor(Cookie& cookie) {
    auto& req = cookie.getRequest(Cookie::PacketContent::Full);
    auto& connection = cookie.getConnection();
    connection.setNoReply(req.isQuiet());
    add_set_replace_executor(cookie, OPERATION_SET);
}

static void replace_executor(Cookie& cookie) {
    auto& req = cookie.getRequest(Cookie::PacketContent::Full);
    auto& connection = cookie.getConnection();
    connection.setNoReply(req.isQuiet());
    add_set_replace_executor(cookie, OPERATION_REPLACE);
}

static void append_prepend_executor(
        Cookie& cookie, const AppendPrependCommandContext::Mode mode) {
    auto& connection = cookie.getConnection();
    auto* req = reinterpret_cast<protocol_binary_request_append*>(
            cookie.getPacketAsVoidPtr());
    cookie.obtainContext<AppendPrependCommandContext>(connection, req, mode)
            .drive();
}

static void append_executor(Cookie& cookie) {
    auto& req = cookie.getRequest(Cookie::PacketContent::Full);
    auto& connection = cookie.getConnection();
    connection.setNoReply(req.isQuiet());
    append_prepend_executor(cookie, AppendPrependCommandContext::Mode::Append);
}

static void prepend_executor(Cookie& cookie) {
    auto& req = cookie.getRequest(Cookie::PacketContent::Full);
    auto& connection = cookie.getConnection();
    connection.setNoReply(req.isQuiet());
    append_prepend_executor(cookie, AppendPrependCommandContext::Mode::Prepend);
}

static void get_executor(Cookie& cookie) {
    auto& req = cookie.getRequest(Cookie::PacketContent::Full);
    auto& connection = cookie.getConnection();
    connection.setNoReply(req.isQuiet());
    process_bin_get(cookie);
}

static void get_meta_executor(Cookie& cookie) {
    auto& connection = cookie.getConnection();
    switch (connection.getCmd()) {
    case PROTOCOL_BINARY_CMD_GET_META:
        // McbpConnection::noreply already set to false in constructor
        break;
    case PROTOCOL_BINARY_CMD_GETQ_META:
        connection.setNoReply(true);
        break;
    default:
        LOG_WARNING(&connection,
                    "%u: get_meta_executor: cmd (which is %d) is not a valid "
                    "GET_META "
                    "variant - closing connection",
                    connection.getId(),
                    connection.getCmd());
        connection.setState(McbpStateMachine::State::closing);
        return;
    }

    process_bin_get_meta(cookie);
}

static void stat_executor(Cookie& cookie) {
    cookie.obtainContext<StatsCommandContext>(cookie).drive();
}

static void isasl_refresh_executor(Cookie& cookie) {
    cookie.obtainContext<SaslRefreshCommandContext>(cookie).drive();
}

static void ssl_certs_refresh_executor(Cookie& cookie) {
    // MB-22464 - We don't cache the SSL certificates in memory
    cookie.sendResponse(cb::mcbp::Status::Success);
}

static void verbosity_executor(Cookie& cookie) {
    auto* req = reinterpret_cast<protocol_binary_request_verbosity*>(
            cookie.getPacketAsVoidPtr());
    uint32_t level = (uint32_t)ntohl(req->message.body.level);
    if (level > MAX_VERBOSITY_LEVEL) {
        level = MAX_VERBOSITY_LEVEL;
    }
    settings.setVerbose(static_cast<int>(level));
    perform_callbacks(ON_LOG_LEVEL, NULL, NULL);
    cookie.sendResponse(cb::mcbp::Status::Success);
}

static void version_executor(Cookie& cookie) {
    mcbp_write_response(&cookie.getConnection(),
                        get_server_version(),
                        0,
                        0,
                        (uint32_t)strlen(get_server_version()));
}

static void quit_executor(Cookie& cookie) {
    cookie.sendResponse(cb::mcbp::Status::Success);
    cookie.getConnection().setWriteAndGo(McbpStateMachine::State::closing);
}

static void quitq_executor(Cookie& cookie) {
    cookie.getConnection().setState(McbpStateMachine::State::closing);
}

static void sasl_list_mech_executor(Cookie& cookie) {
    auto& connection = cookie.getConnection();
    if (!connection.isSaslAuthEnabled()) {
        cookie.sendResponse(cb::mcbp::Status::NotSupported);
        return;
    }

    if (connection.isSslEnabled() && settings.has.ssl_sasl_mechanisms) {
        const auto& mechs = settings.getSslSaslMechanisms();
        mcbp_write_response(&connection, mechs.data(), 0, 0, mechs.size());
    } else if (!connection.isSslEnabled() && settings.has.sasl_mechanisms) {
        const auto& mechs = settings.getSaslMechanisms();
        mcbp_write_response(&connection, mechs.data(), 0, 0, mechs.size());
    } else {
        /*
         * The administrator did not configure any SASL mechanisms.
         * Go ahead and use whatever we've got in cbsasl
         */
        const char* result_string = nullptr;
        unsigned int string_length = 0;

        auto ret = cbsasl_listmech(connection.getSaslConn(),
                                   nullptr,
                                   nullptr,
                                   " ",
                                   nullptr,
                                   &result_string,
                                   &string_length,
                                   nullptr);

        if (ret == CBSASL_OK) {
            mcbp_write_response(
                    &connection, (char*)result_string, 0, 0, string_length);
        } else {
            /* Perhaps there's a better error for this... */
            LOG_WARNING(&connection,
                        "%u: Failed to list SASL mechanisms: %s",
                        connection.getId(),
                        cbsasl_strerror(connection.getSaslConn(), ret));
            cookie.sendResponse(cb::mcbp::Status::AuthError);
        }
    }
}

static void sasl_auth_executor(Cookie& cookie) {
    cookie.obtainContext<SaslAuthCommandContext>(cookie).drive();
}

static void noop_executor(Cookie& cookie) {
    cookie.sendResponse(cb::mcbp::Status::Success);
}

static void flush_executor(Cookie& cookie) {
    cookie.obtainContext<FlushCommandContext>(cookie).drive();
}

static void delete_executor(Cookie& cookie) {
    cookie.obtainContext<RemoveCommandContext>(
                  cookie, cookie.getRequest(Cookie::PacketContent::Full))
            .drive();
}

static void arithmetic_executor(Cookie& cookie) {
    auto& connection = cookie.getConnection();
    auto* req = reinterpret_cast<protocol_binary_request_incr*>(
            cookie.getPacketAsVoidPtr());
    cookie.obtainContext<ArithmeticCommandContext>(connection, *req).drive();
}

static void arithmeticq_executor(Cookie& cookie) {
    auto& connection = cookie.getConnection();
    connection.setNoReply(true);
    arithmetic_executor(cookie);
}

static void set_ctrl_token_executor(Cookie& cookie) {
    auto& connection = cookie.getConnection();
    auto* req = reinterpret_cast<protocol_binary_request_set_ctrl_token*>(
            cookie.getPacketAsVoidPtr());
    uint64_t casval = ntohll(req->message.header.request.cas);
    uint64_t newval = ntohll(req->message.body.new_cas);
    uint64_t value;

    auto ret = session_cas.cas(newval, casval, value);
    mcbp_response_handler(NULL,
                          0,
                          NULL,
                          0,
                          NULL,
                          0,
                          PROTOCOL_BINARY_RAW_BYTES,
                          engine_error_2_mcbp_protocol_error(ret),
                          value,
                          static_cast<const void*>(&cookie));

    mcbp_write_and_free(&connection, &cookie.getDynamicBuffer());
}

static void get_ctrl_token_executor(Cookie& cookie) {
    auto& connection = cookie.getConnection();
    mcbp_response_handler(NULL,
                          0,
                          NULL,
                          0,
                          NULL,
                          0,
                          PROTOCOL_BINARY_RAW_BYTES,
                          PROTOCOL_BINARY_RESPONSE_SUCCESS,
                          session_cas.getCasValue(),
                          static_cast<const void*>(&cookie));
    mcbp_write_and_free(&connection, &cookie.getDynamicBuffer());
}

static void ioctl_get_executor(Cookie& cookie) {
    auto& connection = cookie.getConnection();
    auto* req = reinterpret_cast<protocol_binary_request_ioctl_set*>(
            cookie.getPacketAsVoidPtr());
    ENGINE_ERROR_CODE ret = cookie.getAiostat();
    cookie.setAiostat(ENGINE_SUCCESS);
    cookie.setEwouldblock(false);

    std::string value;
    if (ret == ENGINE_SUCCESS) {
        const auto* key_ptr =
                reinterpret_cast<const char*>(req->bytes + sizeof(req->bytes));
        size_t keylen = ntohs(req->message.header.request.keylen);
        const std::string key(key_ptr, keylen);

        ret = ioctl_get_property(&connection, key, value);
    }

    ret = connection.remapErrorCode(ret);
    switch (ret) {
    case ENGINE_SUCCESS:
        try {
            if (mcbp_response_handler(nullptr,
                                      0,
                                      nullptr,
                                      0,
                                      value.data(),
                                      value.size(),
                                      PROTOCOL_BINARY_RAW_BYTES,
                                      PROTOCOL_BINARY_RESPONSE_SUCCESS,
                                      0,
                                      static_cast<const void*>(&cookie))) {
                mcbp_write_and_free(&connection, &cookie.getDynamicBuffer());
            } else {
                cookie.sendResponse(cb::mcbp::Status::Enomem);
            }
        } catch (const std::exception& e) {
            LOG_WARNING(&connection,
                        "ioctl_get_executor: Failed to format response: %s",
                        e.what());
            cookie.sendResponse(cb::mcbp::Status::Enomem);
        }
        break;
    case ENGINE_EWOULDBLOCK:
        cookie.setAiostat(ENGINE_EWOULDBLOCK);
        cookie.setEwouldblock(true);
        break;
    case ENGINE_DISCONNECT:
        connection.setState(McbpStateMachine::State::closing);
        break;
    default:
        cookie.sendResponse(cb::mcbp::to_status(cb::engine_errc(ret)));
    }
}

static void ioctl_set_executor(Cookie& cookie) {
    auto& connection = cookie.getConnection();
    auto* req = reinterpret_cast<protocol_binary_request_ioctl_set*>(
            cookie.getPacketAsVoidPtr());

    const auto* key_ptr =
            reinterpret_cast<const char*>(req->bytes + sizeof(req->bytes));
    size_t keylen = ntohs(req->message.header.request.keylen);
    const std::string key(key_ptr, keylen);

    const char* val_ptr = key_ptr + keylen;
    size_t vallen = ntohl(req->message.header.request.bodylen) - keylen;
    const std::string value(val_ptr, vallen);

    ENGINE_ERROR_CODE status = ioctl_set_property(&connection, key, value);

    cookie.sendResponse(cb::mcbp::to_status(cb::engine_errc(status)));
}

static void config_validate_executor(Cookie& cookie) {
    auto& connection = cookie.getConnection();
    const char* val_ptr = NULL;
    cJSON* errors = NULL;
    auto* req = reinterpret_cast<protocol_binary_request_ioctl_set*>(
            cookie.getPacketAsVoidPtr());

    size_t keylen = ntohs(req->message.header.request.keylen);
    size_t vallen = ntohl(req->message.header.request.bodylen) - keylen;

    /* Key not yet used, must be zero length. */
    if (keylen != 0) {
        cookie.sendResponse(cb::mcbp::Status::Einval);
        return;
    }

    /* must have non-zero length config */
    if (vallen == 0 || vallen > CONFIG_VALIDATE_MAX_LENGTH) {
        cookie.sendResponse(cb::mcbp::Status::Einval);
        return;
    }

    val_ptr = (const char*)(req->bytes + sizeof(req->bytes)) + keylen;

    /* null-terminate value, and convert to integer */
    try {
        std::string val_buffer(val_ptr, vallen);
        errors = cJSON_CreateArray();

        if (validate_proposed_config_changes(val_buffer.c_str(), errors)) {
            cookie.sendResponse(cb::mcbp::Status::Success);
        } else {
            /* problem(s). Send the errors back to the client. */
            char* error_string = cJSON_PrintUnformatted(errors);
            if (mcbp_response_handler(NULL,
                                      0,
                                      NULL,
                                      0,
                                      error_string,
                                      (uint32_t)strlen(error_string),
                                      PROTOCOL_BINARY_RAW_BYTES,
                                      PROTOCOL_BINARY_RESPONSE_EINVAL,
                                      0,
                                      static_cast<const void*>(&cookie))) {
                mcbp_write_and_free(&connection, &cookie.getDynamicBuffer());
            } else {
                cookie.sendResponse(cb::mcbp::Status::Enomem);
            }
            cJSON_Free(error_string);
        }

        cJSON_Delete(errors);
    } catch (const std::bad_alloc&) {
        LOG_WARNING(&connection,
                    "%u: Failed to allocate buffer of size %" PRIu64
                    " to validate config. Shutting down connection",
                    connection.getId(),
                    vallen + 1);
        connection.setState(McbpStateMachine::State::closing);
        return;
    }

}

static void config_reload_executor(Cookie& cookie) {
    // We need to audit that the privilege debug mode changed and
    // in order to do that we need the "connection" object so we can't
    // do this by using the common "changed_listener"-interface.
    const bool old_priv_debug = settings.isPrivilegeDebug();
    reload_config_file();
    if (settings.isPrivilegeDebug() != old_priv_debug) {
        audit_set_privilege_debug_mode(&cookie.getConnection(),
                                       settings.isPrivilegeDebug());
    }
    cookie.sendResponse(cb::mcbp::Status::Success);
}

static void audit_config_reload_executor(Cookie& cookie) {
    cookie.obtainContext<AuditConfigureCommandContext>(cookie).drive();
}

static void audit_put_executor(Cookie& cookie) {
    auto* req = reinterpret_cast<const protocol_binary_request_audit_put*>(
            cookie.getPacketAsVoidPtr());
    const void* payload = req->bytes + sizeof(req->message.header) +
                          req->message.header.request.extlen;

    const size_t payload_length = ntohl(req->message.header.request.bodylen) -
                                  req->message.header.request.extlen;

    if (mc_audit_event(ntohl(req->message.body.id), payload, payload_length)) {
        cookie.sendResponse(cb::mcbp::Status::Success);
    } else {
        cookie.sendResponse(cb::mcbp::Status::Einternal);
    }
}

static void create_remove_bucket_executor(Cookie& cookie) {
    cookie.obtainContext<CreateRemoveBucketCommandContext>(cookie).drive();
}

static void get_errmap_executor(Cookie& cookie) {
    auto const* req = reinterpret_cast<protocol_binary_request_get_errmap*>(
            cookie.getPacketAsVoidPtr());
    uint16_t version = ntohs(req->message.body.version);
    auto const& ss = settings.getErrorMap(version);
    if (ss.empty()) {
        cookie.sendResponse(cb::mcbp::Status::KeyEnoent);
    } else {
        mcbp_response_handler(NULL,
                              0,
                              NULL,
                              0,
                              ss.data(),
                              ss.size(),
                              PROTOCOL_BINARY_RAW_BYTES,
                              PROTOCOL_BINARY_RESPONSE_SUCCESS,
                              0,
                              static_cast<const void*>(&cookie));
        mcbp_write_and_free(&cookie.getConnection(),
                            &cookie.getDynamicBuffer());
    }
}

static void shutdown_executor(Cookie& cookie) {
    if (session_cas.increment_session_counter(cookie.getRequest().getCas())) {
        shutdown_server();
        session_cas.decrement_session_counter();
        cookie.sendResponse(cb::mcbp::Status::Success);
    } else {
        cookie.sendResponse(cb::mcbp::Status::KeyEexists);
    }
}

std::array<mcbp_package_execute, 0x100>& get_mcbp_executors() {
    static std::array<mcbp_package_execute, 0x100> executors;
    std::fill(executors.begin(), executors.end(), nullptr);

    executors[PROTOCOL_BINARY_CMD_DCP_OPEN] = dcp_open_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_ADD_STREAM] = dcp_add_stream_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_CLOSE_STREAM] = dcp_close_stream_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER] = dcp_snapshot_marker_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_DELETION] = dcp_deletion_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_EXPIRATION] = dcp_expiration_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_FLUSH] = dcp_flush_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_GET_FAILOVER_LOG] = dcp_get_failover_log_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_MUTATION] = dcp_mutation_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_SET_VBUCKET_STATE] = dcp_set_vbucket_state_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_NOOP] = dcp_noop_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_BUFFER_ACKNOWLEDGEMENT] = dcp_buffer_acknowledgement_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_CONTROL] = dcp_control_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_STREAM_END] = dcp_stream_end_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_STREAM_REQ] = dcp_stream_req_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_SYSTEM_EVENT] = dcp_system_event_executor;
    executors[PROTOCOL_BINARY_CMD_SUBDOC_GET] = subdoc_get_executor;
    executors[PROTOCOL_BINARY_CMD_SUBDOC_EXISTS] = subdoc_exists_executor;
    executors[PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD] = subdoc_dict_add_executor;
    executors[PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT] = subdoc_dict_upsert_executor;
    executors[PROTOCOL_BINARY_CMD_SUBDOC_DELETE] = subdoc_delete_executor;
    executors[PROTOCOL_BINARY_CMD_SUBDOC_REPLACE] = subdoc_replace_executor;
    executors[PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST] = subdoc_array_push_last_executor;
    executors[PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST] = subdoc_array_push_first_executor;
    executors[PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT] = subdoc_array_insert_executor;
    executors[PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE] = subdoc_array_add_unique_executor;
    executors[PROTOCOL_BINARY_CMD_SUBDOC_COUNTER] = subdoc_counter_executor;
    executors[PROTOCOL_BINARY_CMD_SUBDOC_MULTI_LOOKUP] = subdoc_multi_lookup_executor;
    executors[PROTOCOL_BINARY_CMD_SUBDOC_MULTI_MUTATION] = subdoc_multi_mutation_executor;
    executors[PROTOCOL_BINARY_CMD_SUBDOC_GET_COUNT] = subdoc_get_count_executor;

    executors[PROTOCOL_BINARY_CMD_COLLECTIONS_SET_MANIFEST] =
            collections_set_manifest_executor;

    return executors;
}

static void rbac_refresh_executor(Cookie& cookie) {
    cookie.obtainContext<RbacReloadCommandContext>(cookie).drive();
}

static void no_support_executor(Cookie& cookie) {
    cookie.sendResponse(cb::mcbp::Status::NotSupported);
}

using HandlerFunction = std::function<void(Cookie&)>;
std::array<HandlerFunction, 0x100> handlers;

void initialize_protocol_handlers() {
    for (auto& handler : handlers) {
        handler = process_bin_unknown_packet;
    }

    handlers[PROTOCOL_BINARY_CMD_ISASL_REFRESH] = isasl_refresh_executor;
    handlers[PROTOCOL_BINARY_CMD_SSL_CERTS_REFRESH] =
            ssl_certs_refresh_executor;
    handlers[PROTOCOL_BINARY_CMD_VERBOSITY] = verbosity_executor;
    handlers[PROTOCOL_BINARY_CMD_HELLO] = process_hello_packet_executor;
    handlers[PROTOCOL_BINARY_CMD_VERSION] = version_executor;
    handlers[PROTOCOL_BINARY_CMD_QUIT] = quit_executor;
    handlers[PROTOCOL_BINARY_CMD_QUITQ] = quitq_executor;
    handlers[PROTOCOL_BINARY_CMD_SASL_LIST_MECHS] = sasl_list_mech_executor;
    handlers[PROTOCOL_BINARY_CMD_SASL_AUTH] = sasl_auth_executor;
    handlers[PROTOCOL_BINARY_CMD_SASL_STEP] = sasl_auth_executor;
    handlers[PROTOCOL_BINARY_CMD_NOOP] = noop_executor;
    handlers[PROTOCOL_BINARY_CMD_FLUSH] = flush_executor;
    handlers[PROTOCOL_BINARY_CMD_FLUSHQ] = flush_executor;
    handlers[PROTOCOL_BINARY_CMD_SETQ] = set_executor;
    handlers[PROTOCOL_BINARY_CMD_SET] = set_executor;
    handlers[PROTOCOL_BINARY_CMD_ADDQ] = add_executor;
    handlers[PROTOCOL_BINARY_CMD_ADD] = add_executor;
    handlers[PROTOCOL_BINARY_CMD_REPLACEQ] = replace_executor;
    handlers[PROTOCOL_BINARY_CMD_REPLACE] = replace_executor;
    handlers[PROTOCOL_BINARY_CMD_APPENDQ] = append_executor;
    handlers[PROTOCOL_BINARY_CMD_APPEND] = append_executor;
    handlers[PROTOCOL_BINARY_CMD_PREPENDQ] = prepend_executor;
    handlers[PROTOCOL_BINARY_CMD_PREPEND] = prepend_executor;
    handlers[PROTOCOL_BINARY_CMD_GET] = get_executor;
    handlers[PROTOCOL_BINARY_CMD_GETQ] = get_executor;
    handlers[PROTOCOL_BINARY_CMD_GETK] = get_executor;
    handlers[PROTOCOL_BINARY_CMD_GETKQ] = get_executor;
    handlers[PROTOCOL_BINARY_CMD_GET_META] = get_meta_executor;
    handlers[PROTOCOL_BINARY_CMD_GETQ_META] = get_meta_executor;
    handlers[PROTOCOL_BINARY_CMD_GAT] = gat_executor;
    handlers[PROTOCOL_BINARY_CMD_GATQ] = gat_executor;
    handlers[PROTOCOL_BINARY_CMD_TOUCH] = gat_executor;
    handlers[PROTOCOL_BINARY_CMD_DELETE] = delete_executor;
    handlers[PROTOCOL_BINARY_CMD_DELETEQ] = delete_executor;
    handlers[PROTOCOL_BINARY_CMD_STAT] = stat_executor;
    handlers[PROTOCOL_BINARY_CMD_INCREMENT] = arithmetic_executor;
    handlers[PROTOCOL_BINARY_CMD_INCREMENTQ] = arithmeticq_executor;
    handlers[PROTOCOL_BINARY_CMD_DECREMENT] = arithmetic_executor;
    handlers[PROTOCOL_BINARY_CMD_DECREMENTQ] = arithmeticq_executor;
    handlers[PROTOCOL_BINARY_CMD_GET_CMD_TIMER] = get_cmd_timer_executor;
    handlers[PROTOCOL_BINARY_CMD_SET_CTRL_TOKEN] = set_ctrl_token_executor;
    handlers[PROTOCOL_BINARY_CMD_GET_CTRL_TOKEN] = get_ctrl_token_executor;
    handlers[PROTOCOL_BINARY_CMD_IOCTL_GET] = ioctl_get_executor;
    handlers[PROTOCOL_BINARY_CMD_IOCTL_SET] = ioctl_set_executor;
    handlers[PROTOCOL_BINARY_CMD_CONFIG_VALIDATE] = config_validate_executor;
    handlers[PROTOCOL_BINARY_CMD_CONFIG_RELOAD] = config_reload_executor;
    handlers[PROTOCOL_BINARY_CMD_AUDIT_PUT] = audit_put_executor;
    handlers[PROTOCOL_BINARY_CMD_AUDIT_CONFIG_RELOAD] =
            audit_config_reload_executor;
    handlers[PROTOCOL_BINARY_CMD_SHUTDOWN] = shutdown_executor;
    handlers[PROTOCOL_BINARY_CMD_CREATE_BUCKET] = create_remove_bucket_executor;
    handlers[PROTOCOL_BINARY_CMD_LIST_BUCKETS] = list_bucket_executor;
    handlers[PROTOCOL_BINARY_CMD_DELETE_BUCKET] = create_remove_bucket_executor;
    handlers[PROTOCOL_BINARY_CMD_SELECT_BUCKET] = select_bucket_executor;
    handlers[PROTOCOL_BINARY_CMD_GET_ERROR_MAP] = get_errmap_executor;
    handlers[PROTOCOL_BINARY_CMD_GET_LOCKED] = get_locked_executor;
    handlers[PROTOCOL_BINARY_CMD_UNLOCK_KEY] = unlock_executor;
    handlers[PROTOCOL_BINARY_CMD_DROP_PRIVILEGE] = drop_privilege_executor;
    handlers[PROTOCOL_BINARY_CMD_RBAC_REFRESH] = rbac_refresh_executor;
    handlers[PROTOCOL_BINARY_CMD_GET_CLUSTER_CONFIG] =
            get_cluster_config_executor;
    handlers[PROTOCOL_BINARY_CMD_SET_CLUSTER_CONFIG] =
            set_cluster_config_executor;

    handlers[PROTOCOL_BINARY_CMD_TAP_CONNECT] = no_support_executor;
    handlers[PROTOCOL_BINARY_CMD_TAP_MUTATION] = no_support_executor;
    handlers[PROTOCOL_BINARY_CMD_TAP_DELETE] = no_support_executor;
    handlers[PROTOCOL_BINARY_CMD_TAP_FLUSH] = no_support_executor;
    handlers[PROTOCOL_BINARY_CMD_TAP_OPAQUE] = no_support_executor;
    handlers[PROTOCOL_BINARY_CMD_TAP_VBUCKET_SET] = no_support_executor;
    handlers[PROTOCOL_BINARY_CMD_TAP_CHECKPOINT_START] = no_support_executor;
    handlers[PROTOCOL_BINARY_CMD_TAP_CHECKPOINT_END] = no_support_executor;
}

static void process_bin_dcp_response(McbpConnection* c) {
    ENGINE_ERROR_CODE ret = ENGINE_DISCONNECT;

    c->enableDatatype(cb::mcbp::Feature::SNAPPY);
    c->enableDatatype(cb::mcbp::Feature::JSON);

    if (c->getBucketEngine()->dcp.response_handler != NULL) {
        auto* header = reinterpret_cast<protocol_binary_response_header*>(
                c->getCookieObject().getPacketAsVoidPtr());
        ret = c->getBucketEngine()->dcp.response_handler
            (c->getBucketEngineAsV0(), c->getCookie(), header);
        ret = c->remapErrorCode(ret);
    }

    if (ret == ENGINE_DISCONNECT) {
        c->setState(McbpStateMachine::State::closing);
    } else {
        c->setState(McbpStateMachine::State::ship_log);
    }
}


void initialize_mbcp_lookup_map(void) {
    int ii;
    for (ii = 0; ii < 0x100; ++ii) {
        request_handlers[ii].descriptor = NULL;
        request_handlers[ii].callback = default_unknown_command;
    }

    response_handlers[PROTOCOL_BINARY_CMD_NOOP] = process_bin_noop_response;

    response_handlers[PROTOCOL_BINARY_CMD_DCP_OPEN] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_ADD_STREAM] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_CLOSE_STREAM] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_STREAM_REQ] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_GET_FAILOVER_LOG] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_STREAM_END] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_MUTATION] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_DELETION] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_EXPIRATION] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_FLUSH] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_SET_VBUCKET_STATE] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_NOOP] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_BUFFER_ACKNOWLEDGEMENT] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_CONTROL] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_SYSTEM_EVENT] =
            process_bin_dcp_response;
}

/**
 * Check if the current packet use an invalid datatype value. It may be
 * considered invalid for two reasons:
 *
 *    1) it is using an unknown value
 *    2) The connected client has not enabled the datatype
 *    3) The bucket has disabled the datatype
 *
 * @param c - the connected client
 * @return true if the packet is considered invalid in this context,
 *         false otherwise
 */
static bool invalid_datatype(McbpConnection* c) {
    return !c->isDatatypeEnabled(c->binary_header.request.datatype);
}

static protocol_binary_response_status validate_bin_header(McbpConnection* c) {
    if (c->binary_header.request.bodylen >=
        (c->binary_header.request.keylen + c->binary_header.request.extlen)) {
        return PROTOCOL_BINARY_RESPONSE_SUCCESS;
    } else {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }
}

static void execute_request_packet(McbpConnection* c) {
    static McbpPrivilegeChains privilegeChains;
    protocol_binary_response_status result;

    char* packet =
            static_cast<char*>(c->getCookieObject().getPacketAsVoidPtr());
    auto opcode = static_cast<protocol_binary_command>(c->binary_header.request.opcode);
    auto executor = executors[opcode];
    auto& cookie = c->getCookieObject();

    const auto res = privilegeChains.invoke(opcode, c->getCookieObject());
    switch (res) {
    case cb::rbac::PrivilegeAccess::Fail:
        LOG_WARNING(c,
                    "%u %s: no access to command %s",
                    c->getId(), c->getDescription().c_str(),
                    memcached_opcode_2_text(opcode));
        audit_command_access_failed(c);

        if (c->remapErrorCode(ENGINE_EACCESS) == ENGINE_DISCONNECT) {
            c->setState(McbpStateMachine::State::closing);
            return;
        } else {
            cookie.sendResponse(cb::mcbp::Status::Eaccess);
        }

        return;
    case cb::rbac::PrivilegeAccess::Ok:
        result = validate_bin_header(c);
        if (result == PROTOCOL_BINARY_RESPONSE_SUCCESS) {
            result = c->validateCommand(opcode);
        }

        if (result != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
            LOG_NOTICE(c,
                       "%u: Invalid format specified for %s - %d - "
                           "closing connection",
                       c->getId(), memcached_opcode_2_text(opcode), result);
            audit_invalid_packet(c);
            cookie.sendResponse(cb::mcbp::Status(result));
            c->setWriteAndGo(McbpStateMachine::State::closing);
            return;
        }

        if (executor != NULL) {
            executor(c, packet);
        } else {
            handlers[opcode](cookie);
        }
        return;
    case cb::rbac::PrivilegeAccess::Stale:
        if (c->remapErrorCode(ENGINE_AUTH_STALE) == ENGINE_DISCONNECT) {
            c->setState(McbpStateMachine::State::closing);
        } else {
            cookie.sendResponse(cb::mcbp::Status::AuthStale);
        }
        return;
    }

    LOG_WARNING(c,
                "%u: execute_request_packet: res (which is %d) is not a valid "
                "AuthResult - closing connection",
                res);
    c->setState(McbpStateMachine::State::closing);
}

/**
 * We've received a response packet. Parse and execute it
 *
 * @param c The connection receiving the packet
 */
static void execute_response_packet(McbpConnection* c) {
    auto handler = response_handlers[c->binary_header.request.opcode];
    if (handler) {
        handler(c);
    } else {
        LOG_NOTICE(c,
                   "%u: Unsupported response packet received with opcode: %02x",
                   c->getId(),
                   c->binary_header.request.opcode);
        c->setState(McbpStateMachine::State::closing);
    }
}

static cb::mcbp::Status validate_packet_execusion_constraints(
        McbpConnection* c) {
    if (invalid_datatype(c)) {
        c->getCookieObject().setErrorContext("Invalid datatype provided");
        return cb::mcbp::Status::Einval;
    }

    /* binprot supports 16bit keys, but internals are still 8bit */
    if (c->binary_header.request.keylen > KEY_MAX_LENGTH) {
        c->getCookieObject().setErrorContext("Invalid key length");
        return cb::mcbp::Status::Einval;
    }

    /*
     * Protect ourself from someone trying to kill us by sending insanely
     * large packets.
     */
    if (c->binary_header.request.bodylen > settings.getMaxPacketSize()) {
        c->getCookieObject().setErrorContext("Packet is too big");
        return cb::mcbp::Status::Einval;
    }

    return cb::mcbp::Status::Success;
}

void mcbp_execute_packet(McbpConnection* c) {
    if (c->binary_header.request.magic == PROTOCOL_BINARY_RES) {
        execute_response_packet(c);
    } else {
        // We've already verified that the packet is a legal packet
        // so it must be a request
        execute_request_packet(c);
    }
}

void try_read_mcbp_command(McbpConnection* c) {
    if (c == nullptr) {
        throw std::logic_error(
                "try_read_mcbp_command: Internal error, connection is not "
                "mcbp");
    }

    auto input = c->read->rdata();
    if (input.size() < sizeof(cb::mcbp::Request)) {
        throw std::logic_error(
                "try_read_mcbp_command: header not present (got " +
                std::to_string(c->read->rsize()) + " of " +
                std::to_string(sizeof(cb::mcbp::Request)) + ")");
    }
    auto& cookie = c->getCookieObject();
    cookie.setPacket(
            Cookie::PacketContent::Header,
            cb::const_byte_buffer{input.data(), sizeof(cb::mcbp::Request)});

    if (settings.getVerbose() > 1) {
        /* Dump the packet before we convert it to host order */
        char buffer[1024];
        ssize_t nw;
        nw = bytes_to_output_string(buffer,
                                    sizeof(buffer),
                                    c->getId(),
                                    true,
                                    "Read binary protocol data:",
                                    (const char*)input.data(),
                                    sizeof(cb::mcbp::Request));
        if (nw != -1) {
            LOG_DEBUG(c, "%s", buffer);
        }
    }

    // Create a host-local-byte-order-copy of the header
    std::memcpy(
            c->binary_header.bytes, input.data(), sizeof(cb::mcbp::Request));
    c->binary_header.request.keylen = ntohs(c->binary_header.request.keylen);
    c->binary_header.request.bodylen = ntohl(c->binary_header.request.bodylen);
    c->binary_header.request.vbucket = ntohs(c->binary_header.request.vbucket);
    c->binary_header.request.cas = ntohll(c->binary_header.request.cas);

    if (c->binary_header.request.magic != PROTOCOL_BINARY_REQ &&
        !(c->binary_header.request.magic == PROTOCOL_BINARY_RES &&
          response_handlers[c->binary_header.request.opcode])) {
        if (c->binary_header.request.magic != PROTOCOL_BINARY_RES) {
            LOG_WARNING(c, "%u: Invalid magic: %x, closing connection",
                        c->getId(), c->binary_header.request.magic);
        } else {
            LOG_WARNING(c,
                        "%u: Unsupported response packet received: %u, "
                            "closing connection",
                        c->getId(),
                        (unsigned int)c->binary_header.request.opcode);

        }
        c->setState(McbpStateMachine::State::closing);
        return;
    }

    if (c->getBucketEngine() == nullptr) {
        throw std::logic_error(
                "try_read_mcbp_command: Not connected to a bucket");
    }

    c->addMsgHdr(true);
    c->setCmd(c->binary_header.request.opcode);
    /* clear the returned cas value */
    cookie.setCas(0);
    c->setNoReply(false);
    c->setStart(ProcessClock::now());
    MEMCACHED_PROCESS_COMMAND_START(
            c->getId(), input.data(), sizeof(cb::mcbp::Request));

    auto reason = validate_packet_execusion_constraints(c);
    if (reason != cb::mcbp::Status::Success) {
        cookie.sendResponse(reason);
        c->setWriteAndGo(McbpStateMachine::State::closing);
        return;
    }

    if (c->isPacketAvailable()) {
        // we've got the entire packet spooled up, just go execute
        cookie.setPacket(Cookie::PacketContent::Full,
                         cb::const_byte_buffer{
                                 input.data(),
                                 sizeof(cb::mcbp::Request) +
                                         c->binary_header.request.bodylen});
        c->setState(McbpStateMachine::State::execute);
    } else {
        // we need to allocate more memory!!
        try {
            size_t needed = sizeof(cb::mcbp::Request) +
                            c->binary_header.request.bodylen;
            c->read->ensureCapacity(needed - c->read->rsize());
            cookie.setPacket(Cookie::PacketContent::Header,
                             cb::const_byte_buffer{c->read->rdata().data(),
                                                   sizeof(cb::mcbp::Request)});
        } catch (const std::bad_alloc&) {
            LOG_WARNING(c,
                        "%u: Failed to grow buffer.. closing connection",
                        c->getId());
            c->setState(McbpStateMachine::State::closing);
            return;
        }
        c->setState(McbpStateMachine::State::read_packet_body);
    }
}
