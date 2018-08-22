/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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
#include "protocol/mcbp/get_active_external_users_command_context.h"
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

#include <mcbp/protocol/header.h>
#include <nlohmann/json.hpp>
#include <platform/string.h>
#include <utilities/protocol2text.h>

std::array<bool, 0x100>&  topkey_commands = get_mcbp_topkeys();

/**
 * Triggers topkeys_update (i.e., increments topkeys stats) if called by a
 * valid operation.
 */
void update_topkeys(const Cookie& cookie) {
    const auto opcode = cookie.getHeader().getOpcode();
    if (topkey_commands[opcode]) {
        const auto index = cookie.getConnection().getBucketIndex();
        const auto key = cookie.getRequestKey();
        if (all_buckets[index].topkeys != nullptr) {
            all_buckets[index].topkeys->updateKey(
                    key.data(), key.size(), mc_time_get_current_time());
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

/**
 * The handler function is used to handle and incomming packet (command or
 * response).
 * Each handler is provided with a Cookie object which contains all
 * of the context information about the command/response.
 *
 * When called the entire packet is available.
 */
using HandlerFunction = std::function<void(Cookie&)>;

/**
 * A map between the request packets op-code and the function to handle
 * the request message
 */
std::array<HandlerFunction, 0x100> handlers;

/**
 * A map between the response packets op-code and the function to handle
 * the response message.
 */
std::array<HandlerFunction, 0x100> response_handlers;

static void process_bin_unknown_packet(Cookie& cookie) {
    auto& connection = cookie.getConnection();

    auto ret = cookie.swapAiostat(ENGINE_SUCCESS);

    if (ret == ENGINE_SUCCESS) {
        ret = bucket_unknown_command(cookie, mcbp_response_handler);
    }

    ret = cookie.getConnection().remapErrorCode(ret);
    switch (ret) {
    case ENGINE_SUCCESS: {
        if (cookie.getDynamicBuffer().getRoot() != nullptr) {
            // We assume that if the underlying engine returns a success then
            // it is sending a success to the client.
            ++connection.getBucket()
                      .responseCounters[PROTOCOL_BINARY_RESPONSE_SUCCESS];
            cookie.sendDynamicBuffer();
        } else {
            connection.setState(StateMachine::State::new_cmd);
        }
        update_topkeys(cookie);
        break;
    }
    case ENGINE_EWOULDBLOCK:
        cookie.setEwouldblock(true);
        break;
    case ENGINE_DISCONNECT:
        connection.setState(StateMachine::State::closing);
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
static void process_bin_noop_response(Cookie& cookie) {
    cookie.getConnection().setState(StateMachine::State::new_cmd);
}

static void add_set_replace_executor(Cookie& cookie,
                                     ENGINE_STORE_OPERATION store_op) {
    cookie.obtainContext<MutationCommandContext>(
                  cookie,
                  cookie.getRequest(Cookie::PacketContent::Full),
                  store_op)
            .drive();
}

static void add_executor(Cookie& cookie) {
    add_set_replace_executor(cookie, OPERATION_ADD);
}

static void set_executor(Cookie& cookie) {
    add_set_replace_executor(cookie, OPERATION_SET);
}

static void replace_executor(Cookie& cookie) {
    add_set_replace_executor(cookie, OPERATION_REPLACE);
}

static void append_prepend_executor(Cookie& cookie) {
    const auto& req = cookie.getRequest(Cookie::PacketContent::Full);
    cookie.obtainContext<AppendPrependCommandContext>(cookie, req).drive();
}

static void get_executor(Cookie& cookie) {
    process_bin_get(cookie);
}

static void get_meta_executor(Cookie& cookie) {
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
    const std::string version{get_server_version()};
    cookie.sendResponse(cb::mcbp::Status::Success,
                        {},
                        {},
                        version,
                        cb::mcbp::Datatype::Raw,
                        0);
}

static void quit_executor(Cookie& cookie) {
    cookie.sendResponse(cb::mcbp::Status::Success);
    auto& connection = cookie.getConnection();
    LOG_DEBUG("{}: quit_executor - closing connection {}",
              connection.getId(),
              connection.getDescription());
    connection.setWriteAndGo(StateMachine::State::closing);
}

static void quitq_executor(Cookie& cookie) {
    auto& connection = cookie.getConnection();
    LOG_DEBUG("{}: quitq_executor - closing connection {}",
              connection.getId(),
              connection.getDescription());
    connection.setState(StateMachine::State::closing);
}

static void sasl_list_mech_executor(Cookie& cookie) {
    auto& connection = cookie.getConnection();
    if (!connection.isSaslAuthEnabled()) {
        cookie.sendResponse(cb::mcbp::Status::NotSupported);
        return;
    }

    if (connection.isSslEnabled() && settings.has.ssl_sasl_mechanisms) {
        const auto& mechs = settings.getSslSaslMechanisms();
        cookie.sendResponse(cb::mcbp::Status::Success,
                            {},
                            {},
                            mechs,
                            cb::mcbp::Datatype::Raw,
                            0);
    } else if (!connection.isSslEnabled() && settings.has.sasl_mechanisms) {
        const auto& mechs = settings.getSaslMechanisms();
        cookie.sendResponse(cb::mcbp::Status::Success,
                            {},
                            {},
                            mechs,
                            cb::mcbp::Datatype::Raw,
                            0);
    } else {
        /*
         * The administrator did not configure any SASL mechanisms.
         * Go ahead and use whatever we've got in cbsasl
         */
        const auto mechs = cb::sasl::server::listmech();
        cookie.sendResponse(cb::mcbp::Status::Success,
                            {},
                            {},
                            mechs,
                            cb::mcbp::Datatype::Raw,
                            0);
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
    const auto& req = cookie.getRequest(Cookie::PacketContent::Full);
    cookie.obtainContext<ArithmeticCommandContext>(cookie, req).drive();
}

static void set_ctrl_token_executor(Cookie& cookie) {
    auto* req = reinterpret_cast<protocol_binary_request_set_ctrl_token*>(
            cookie.getPacketAsVoidPtr());
    uint64_t casval = ntohll(req->message.header.request.cas);
    uint64_t newval = ntohll(req->message.body.new_cas);
    uint64_t value;

    auto ret = cb::engine_errc(session_cas.cas(newval, casval, value));

    // The contract in the protocol description for set-ctrl-token is
    // to include the CAS value in the response even for failures
    // (there is a unit test which enforce this)
    cookie.setCas(value);
    cookie.sendResponse(cb::mcbp::to_status(ret),
                        {},
                        {},
                        {},
                        cb::mcbp::Datatype::Raw,
                        value);
}

static void get_ctrl_token_executor(Cookie& cookie) {
    cookie.sendResponse(cb::mcbp::Status::Success,
                        {},
                        {},
                        {},
                        cb::mcbp::Datatype::Raw,
                        session_cas.getCasValue());
}

static void ioctl_get_executor(Cookie& cookie) {
    auto& connection = cookie.getConnection();
    auto* req = reinterpret_cast<protocol_binary_request_ioctl_set*>(
            cookie.getPacketAsVoidPtr());
    auto ret = cookie.swapAiostat(ENGINE_SUCCESS);

    std::string value;
    if (ret == ENGINE_SUCCESS) {
        const auto* key_ptr =
                reinterpret_cast<const char*>(req->bytes + sizeof(req->bytes));
        size_t keylen = ntohs(req->message.header.request.keylen);
        const std::string key(key_ptr, keylen);
        ret = ioctl_get_property(cookie, key, value);
    }

    auto remapErr = connection.remapErrorCode(ret);
    switch (remapErr) {
    case ENGINE_SUCCESS:
        cookie.sendResponse(cb::mcbp::Status::Success,
                            {},
                            {},
                            {value.data(), value.size()},
                            cb::mcbp::Datatype::Raw,
                            0);
        break;
    case ENGINE_EWOULDBLOCK:
        cookie.setEwouldblock(true);
        break;
    case ENGINE_DISCONNECT:
        if (ret == ENGINE_DISCONNECT) {
            LOG_WARNING(
                    "{}: ioctl_get_executor - ioctl_get_property returned "
                    "ENGINE_DISCONNECT - closing connection {}",
                    connection.getId(),
                    connection.getDescription());
        }
        connection.setState(StateMachine::State::closing);
        break;
    default:
        cookie.sendResponse(cb::mcbp::to_status(cb::engine_errc(remapErr)));
    }
}

static void ioctl_set_executor(Cookie& cookie) {
    auto ret = cookie.swapAiostat(ENGINE_SUCCESS);

    auto& connection = cookie.getConnection();
    if (ret == ENGINE_SUCCESS) {
        auto* req = reinterpret_cast<protocol_binary_request_ioctl_set*>(
                cookie.getPacketAsVoidPtr());

        const auto* key_ptr =
                reinterpret_cast<const char*>(req->bytes + sizeof(req->bytes));
        size_t keylen = ntohs(req->message.header.request.keylen);
        const std::string key(key_ptr, keylen);

        const char* val_ptr = key_ptr + keylen;
        size_t vallen = ntohl(req->message.header.request.bodylen) - keylen;
        const std::string value(val_ptr, vallen);

        ret = ioctl_set_property(cookie, key, value);
    }
    auto remapErr = connection.remapErrorCode(ret);

    switch (remapErr) {
    case ENGINE_EWOULDBLOCK:
        cookie.setEwouldblock(true);
        break;
    case ENGINE_DISCONNECT:
        if (ret == ENGINE_DISCONNECT) {
            LOG_WARNING(
                    "{}: ioctl_set_executor - ioctl_set_property returned "
                    "ENGINE_DISCONNECT - closing connection {}",
                    connection.getId(),
                    connection.getDescription());
        }
        connection.setState(StateMachine::State::closing);
        break;
    default:
        cookie.sendResponse(cb::mcbp::to_status(cb::engine_errc(remapErr)));
    }
}

static void config_validate_executor(Cookie& cookie) {
    const auto& request = cookie.getRequest(Cookie::PacketContent::Full);
    const auto value = request.getValue();

    // the config validator needs a null-terminated string...
    std::string val_buffer(reinterpret_cast<const char*>(value.data()),
                           value.size());
    unique_cJSON_ptr errors(cJSON_CreateArray());

    if (validate_proposed_config_changes(val_buffer.c_str(), errors.get())) {
        cookie.sendResponse(cb::mcbp::Status::Success);
        return;
    }

    // problem(s). Send the errors back to the client.
    cookie.setErrorContext(to_string(errors, false));
    cookie.sendResponse(cb::mcbp::Status::Einval);
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
    const auto& request = cookie.getRequest(Cookie::PacketContent::Full);
    // The packet validator ensured that this is 4 bytes long
    const auto extras = request.getExtdata();
    const uint32_t id = *reinterpret_cast<const uint32_t*>(extras.data());

    if (mc_audit_event(ntohl(id), request.getValue())) {
        cookie.sendResponse(cb::mcbp::Status::Success);
    } else {
        cookie.sendResponse(cb::mcbp::Status::Ebusy);
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
        cookie.sendResponse(cb::mcbp::Status::Success,
                            {},
                            {},
                            {ss.data(), ss.size()},
                            cb::mcbp::Datatype::JSON,
                            0);
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

static void update_user_permissions_executor(Cookie& cookie) {
    auto& request = cookie.getRequest(Cookie::PacketContent::Full);
    auto key = request.getKey();
    auto value = request.getValue();
    cb::rbac::updateUser(
            std::string{reinterpret_cast<const char*>(key.data()), key.size()},
            std::string{reinterpret_cast<const char*>(value.data()),
                        value.size()});
    cookie.sendResponse(cb::mcbp::Status::Success);
}

static void rbac_refresh_executor(Cookie& cookie) {
    cookie.obtainContext<RbacReloadCommandContext>(cookie).drive();
}

static void auth_provider_executor(Cookie& cookie) {
    if (!settings.isExternalAuthServiceEnabled()) {
        cookie.setErrorContext(
                "Support for external authentication service is disabled");
        cookie.sendResponse(cb::mcbp::Status::NotSupported);
        return;
    }

    auto& connection = cookie.getConnection();
    if (connection.isDuplexSupported()) {
        // To ease the integration with ns_server we'll just tell it
        // that we accepted this connection for auth requests, but
        // we won't use it
        cookie.sendResponse(cb::mcbp::Status::Success);
    } else {
        cookie.setErrorContext("Connection is not in duplex mode");
        cookie.sendResponse(cb::mcbp::Status::Einval);
    }
}

static void get_active_external_users_executor(Cookie& cookie) {
    cookie.obtainContext<GetActiveExternalUsersCommandContext>(cookie).drive();
}

static void no_support_executor(Cookie& cookie) {
    cookie.sendResponse(cb::mcbp::Status::NotSupported);
}

static void process_bin_dcp_response(Cookie& cookie) {
    auto& c = cookie.getConnection();

    c.enableDatatype(cb::mcbp::Feature::JSON);

    auto* dcp = c.getBucket().getDcpIface();
    if (!dcp) {
        LOG_WARNING(
                "{}: process_bin_dcp_response - DcpIface is nullptr - "
                "closing connection {}",
                c.getId(),
                c.getDescription());
        c.setState(StateMachine::State::closing);
        return;
    }

    auto packet = cookie.getPacket(Cookie::PacketContent::Full);
    const auto* header =
            reinterpret_cast<const protocol_binary_response_header*>(
                    packet.data());

    auto ret = dcp->response_handler(&cookie, header);
    auto remapErr = c.remapErrorCode(ret);

    if (remapErr == ENGINE_DISCONNECT) {
        if (ret == ENGINE_DISCONNECT) {
            LOG_WARNING(
                    "{}: process_bin_dcp_response - response_handler returned "
                    "ENGINE_DISCONNECT - closing connection {}",
                    c.getId(),
                    c.getDescription());
        }
        c.setState(StateMachine::State::closing);
    } else {
        c.setState(StateMachine::State::ship_log);
    }
}

void initialize_mbcp_lookup_map() {
    response_handlers[PROTOCOL_BINARY_CMD_NOOP] = process_bin_noop_response;

    response_handlers[PROTOCOL_BINARY_CMD_DCP_OPEN] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_ADD_STREAM] =
            process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_CLOSE_STREAM] =
            process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_STREAM_REQ] =
            process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_GET_FAILOVER_LOG] =
            process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_STREAM_END] =
            process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER] =
            process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_MUTATION] =
            process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_DELETION] =
            process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_EXPIRATION] =
            process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_SET_VBUCKET_STATE] =
            process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_NOOP] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_BUFFER_ACKNOWLEDGEMENT] =
            process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_CONTROL] =
            process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_SYSTEM_EVENT] =
            process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_GET_ERROR_MAP] =
            process_bin_dcp_response;

    for (auto& handler : handlers) {
        handler = process_bin_unknown_packet;
    }

    handlers[PROTOCOL_BINARY_CMD_DCP_OPEN] = dcp_open_executor;
    handlers[PROTOCOL_BINARY_CMD_DCP_ADD_STREAM] = dcp_add_stream_executor;
    handlers[PROTOCOL_BINARY_CMD_DCP_CLOSE_STREAM] = dcp_close_stream_executor;
    handlers[PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER] =
            dcp_snapshot_marker_executor;
    handlers[PROTOCOL_BINARY_CMD_DCP_DELETION] = dcp_deletion_executor;
    handlers[PROTOCOL_BINARY_CMD_DCP_EXPIRATION] = dcp_expiration_executor;
    handlers[PROTOCOL_BINARY_CMD_DCP_GET_FAILOVER_LOG] =
            dcp_get_failover_log_executor;
    handlers[PROTOCOL_BINARY_CMD_DCP_MUTATION] = dcp_mutation_executor;
    handlers[PROTOCOL_BINARY_CMD_DCP_SET_VBUCKET_STATE] =
            dcp_set_vbucket_state_executor;
    handlers[PROTOCOL_BINARY_CMD_DCP_NOOP] = dcp_noop_executor;
    handlers[PROTOCOL_BINARY_CMD_DCP_BUFFER_ACKNOWLEDGEMENT] =
            dcp_buffer_acknowledgement_executor;
    handlers[PROTOCOL_BINARY_CMD_DCP_CONTROL] = dcp_control_executor;
    handlers[PROTOCOL_BINARY_CMD_DCP_STREAM_END] = dcp_stream_end_executor;
    handlers[PROTOCOL_BINARY_CMD_DCP_STREAM_REQ] = dcp_stream_req_executor;
    handlers[PROTOCOL_BINARY_CMD_DCP_SYSTEM_EVENT] = dcp_system_event_executor;
    handlers[PROTOCOL_BINARY_CMD_COLLECTIONS_SET_MANIFEST] =
            collections_set_manifest_executor;
    handlers[PROTOCOL_BINARY_CMD_COLLECTIONS_GET_MANIFEST] =
            collections_get_manifest_executor;
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
    handlers[PROTOCOL_BINARY_CMD_APPENDQ] = append_prepend_executor;
    handlers[PROTOCOL_BINARY_CMD_APPEND] = append_prepend_executor;
    handlers[PROTOCOL_BINARY_CMD_PREPENDQ] = append_prepend_executor;
    handlers[PROTOCOL_BINARY_CMD_PREPEND] = append_prepend_executor;
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
    handlers[PROTOCOL_BINARY_CMD_INCREMENTQ] = arithmetic_executor;
    handlers[PROTOCOL_BINARY_CMD_DECREMENT] = arithmetic_executor;
    handlers[PROTOCOL_BINARY_CMD_DECREMENTQ] = arithmetic_executor;
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
    handlers[PROTOCOL_BINARY_CMD_GET_FAILOVER_LOG] =
            dcp_get_failover_log_executor;
    handlers[PROTOCOL_BINARY_CMD_DROP_PRIVILEGE] = drop_privilege_executor;
    handlers[uint8_t(cb::mcbp::ClientOpcode::UpdateUserPermissions)] =
            update_user_permissions_executor;
    handlers[PROTOCOL_BINARY_CMD_RBAC_REFRESH] = rbac_refresh_executor;
    handlers[uint8_t(cb::mcbp::ClientOpcode::AuthProvider)] =
            auth_provider_executor;
    handlers[uint8_t(cb::mcbp::ClientOpcode::GetActiveExternalUsers)] =
            get_active_external_users_executor;
    handlers[PROTOCOL_BINARY_CMD_GET_CLUSTER_CONFIG] =
            get_cluster_config_executor;
    handlers[PROTOCOL_BINARY_CMD_SET_CLUSTER_CONFIG] =
            set_cluster_config_executor;

    handlers[PROTOCOL_BINARY_CMD_SUBDOC_GET] = subdoc_get_executor;
    handlers[PROTOCOL_BINARY_CMD_SUBDOC_EXISTS] = subdoc_exists_executor;
    handlers[PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD] = subdoc_dict_add_executor;
    handlers[PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT] =
            subdoc_dict_upsert_executor;
    handlers[PROTOCOL_BINARY_CMD_SUBDOC_DELETE] = subdoc_delete_executor;
    handlers[PROTOCOL_BINARY_CMD_SUBDOC_REPLACE] = subdoc_replace_executor;
    handlers[PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST] =
            subdoc_array_push_last_executor;
    handlers[PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST] =
            subdoc_array_push_first_executor;
    handlers[PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT] =
            subdoc_array_insert_executor;
    handlers[PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE] =
            subdoc_array_add_unique_executor;
    handlers[PROTOCOL_BINARY_CMD_SUBDOC_COUNTER] = subdoc_counter_executor;
    handlers[PROTOCOL_BINARY_CMD_SUBDOC_MULTI_LOOKUP] =
            subdoc_multi_lookup_executor;
    handlers[PROTOCOL_BINARY_CMD_SUBDOC_MULTI_MUTATION] =
            subdoc_multi_mutation_executor;
    handlers[PROTOCOL_BINARY_CMD_SUBDOC_GET_COUNT] = subdoc_get_count_executor;

    handlers[PROTOCOL_BINARY_CMD_TAP_CONNECT] = no_support_executor;
    handlers[PROTOCOL_BINARY_CMD_TAP_MUTATION] = no_support_executor;
    handlers[PROTOCOL_BINARY_CMD_TAP_DELETE] = no_support_executor;
    handlers[PROTOCOL_BINARY_CMD_TAP_FLUSH] = no_support_executor;
    handlers[PROTOCOL_BINARY_CMD_TAP_OPAQUE] = no_support_executor;
    handlers[PROTOCOL_BINARY_CMD_TAP_VBUCKET_SET] = no_support_executor;
    handlers[PROTOCOL_BINARY_CMD_TAP_CHECKPOINT_START] = no_support_executor;
    handlers[PROTOCOL_BINARY_CMD_TAP_CHECKPOINT_END] = no_support_executor;

    handlers[PROTOCOL_BINARY_CMD_ADJUST_TIMEOFDAY] = adjust_timeofday_executor;
}

void execute_request_packet(Cookie& cookie, const cb::mcbp::Request& request) {
    auto* c = &cookie.getConnection();

    static McbpPrivilegeChains privilegeChains;
    protocol_binary_response_status result;

    const auto opcode = request.opcode;
    const auto res = privilegeChains.invoke(opcode, cookie);
    switch (res) {
    case cb::rbac::PrivilegeAccess::Fail:
        LOG_WARNING("{} {}: no access to command {}",
                    c->getId(),
                    c->getDescription(),
                    memcached_opcode_2_text(opcode));
        audit_command_access_failed(cookie);

        if (c->remapErrorCode(ENGINE_EACCESS) == ENGINE_DISCONNECT) {
            c->setState(StateMachine::State::closing);
            return;
        } else {
            cookie.sendResponse(cb::mcbp::Status::Eaccess);
        }

        return;
    case cb::rbac::PrivilegeAccess::Ok:
        if (request.isValid()) {
            // The framing of the packet is valid...
            // Verify that the actual command is legal
            auto& bucket = cookie.getConnection().getBucket();
            result = bucket.validatorChains.invoke(opcode, cookie);
        } else {
            result = PROTOCOL_BINARY_RESPONSE_EINVAL;
        }

        if (result != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
            // Log the mcbp header and extras
            const auto& header = cookie.getHeader();
            std::stringstream ss;
            ss << header;

            // If extras, do a raw dump (a corrupt extlen could mean 255 bytes)
            if (header.getExtlen()) {
                auto packet = cookie.getPacket();
                ss << ", rawextras:";
                for (uint8_t index = 0; index < header.getExtlen(); index++) {
                    const auto* byte =
                            packet.data() + sizeof(cb::mcbp::Header) + index;
                    // don't exceed the packet buffer
                    if (byte < packet.data() + packet.size()) {
                        ss << std::hex << int(*byte);
                    } else {
                        break;
                    }
                }
            }

            LOG_WARNING(
                    "{}: Invalid format specified for {} - {} - "
                    "closing connection packet:{} ",
                    c->getId(),
                    memcached_opcode_2_text(opcode),
                    result,
                    ss.str());
            audit_invalid_packet(cookie);
            cookie.sendResponse(cb::mcbp::Status(result));
            c->setWriteAndGo(StateMachine::State::closing);
            return;
        }

        handlers[opcode](cookie);
        return;
    case cb::rbac::PrivilegeAccess::Stale:
        if (c->remapErrorCode(ENGINE_AUTH_STALE) == ENGINE_DISCONNECT) {
            c->setState(StateMachine::State::closing);
        } else {
            cookie.sendResponse(cb::mcbp::Status::AuthStale);
        }
        return;
    }

    LOG_WARNING(
            "{}: execute_request_packet: res (which is {}) is not a valid "
            "AuthResult - closing connection",
            c->getId(),
            uint32_t(res));
    c->setState(StateMachine::State::closing);
}

static void execute_client_response_packet(Cookie& cookie,
                                           const cb::mcbp::Response& response) {
    auto handler = response_handlers[response.opcode];
    if (handler) {
        handler(cookie);
    } else {
        auto& c = cookie.getConnection();
        LOG_INFO("{}: Unsupported response packet received with opcode: {:x}",
                 c.getId(),
                 uint32_t(response.opcode));
        c.setState(StateMachine::State::closing);
    }
}

static void execute_server_response_packet(Cookie& cookie,
                                           const cb::mcbp::Response& response) {
    auto& c = cookie.getConnection();
    c.setState(StateMachine::State::new_cmd);

    switch (response.getServerOpcode()) {
    case cb::mcbp::ServerOpcode::ClustermapChangeNotification:
    case cb::mcbp::ServerOpcode::AuthRequest:
        // ignore
        return;
    }

    LOG_INFO(
            "{}: Ignoring unsupported server response packet received with "
            "opcode: {:x}",
            c.getId(),
            uint32_t(response.opcode));
}

/**
 * We've received a response packet. Parse and execute it
 *
 * @param cookie the current command context
 * @param response the actual response packet
 */
void execute_response_packet(Cookie& cookie,
                             const cb::mcbp::Response& response) {
    switch (response.getMagic()) {
    case cb::mcbp::Magic::ClientResponse:
    case cb::mcbp::Magic::AltClientResponse:
        execute_client_response_packet(cookie, response);
        return;
    case cb::mcbp::Magic::ServerResponse:
        execute_server_response_packet(cookie, response);
        return;
    case cb::mcbp::Magic::ClientRequest:
    case cb::mcbp::Magic::ServerRequest:;
    }

    throw std::logic_error(
            "execute_response_packet: provided packet is not a response");
}

static cb::mcbp::Status validate_packet_execusion_constraints(Cookie& cookie) {
    /*
     * Check if the current packet use an invalid datatype value. It may be
     * considered invalid for two reasons:
     *
     *    1) it is using an unknown value
     *    2) The connected client has not enabled the datatype
     *    3) The bucket has disabled the datatype
     *
     */
    const auto& header = cookie.getHeader();
    if (!cookie.getConnection().isDatatypeEnabled(header.getDatatype())) {
        cookie.setErrorContext("Invalid datatype provided");
        return cb::mcbp::Status::Einval;
    }

    /* binprot supports 16bit keys, but internals are still 8bit */
    if (header.getKeylen() > KEY_MAX_LENGTH) {
        cookie.setErrorContext("Key length exceed " +
                               std::to_string(KEY_MAX_LENGTH));
        return cb::mcbp::Status::Einval;
    }

    /*
     * Protect ourself from someone trying to kill us by sending insanely
     * large packets.
     */
    if (header.getBodylen() > settings.getMaxPacketSize()) {
        cookie.setErrorContext("Packet is too big");
        return cb::mcbp::Status::Einval;
    }

    return cb::mcbp::Status::Success;
}

void try_read_mcbp_command(Connection& c) {
    auto input = c.read->rdata();
    if (input.size() < sizeof(cb::mcbp::Request)) {
        throw std::logic_error(
                "try_read_mcbp_command: header not present (got " +
                std::to_string(c.read->rsize()) + " of " +
                std::to_string(sizeof(cb::mcbp::Request)) + ")");
    }
    auto& cookie = c.getCookieObject();
    cookie.initialize(
            cb::const_byte_buffer{input.data(), sizeof(cb::mcbp::Request)},
            c.isTracingEnabled());

    const auto& header = cookie.getHeader();
    if (settings.getVerbose() > 1) {
        try {
            LOG_TRACE(">{} Read command {}", c.getId(), header.toJSON().dump());
        } catch (const std::exception&) {
            // Failed to decode the header.. do a raw multiline dump
            // instead
            char buffer[1024];
            ssize_t nw;
            nw = bytes_to_output_string(buffer,
                                        sizeof(buffer),
                                        c.getId(),
                                        true,
                                        "Read binary protocol data:",
                                        (const char*)input.data(),
                                        sizeof(cb::mcbp::Request));
            if (nw != -1) {
                LOG_TRACE("{}", buffer);
            }
        }
    }

    if (!header.isValid()) {
        LOG_WARNING(
                "{}: Invalid packet format detected (magic: {:x}), closing "
                "connection",
                c.getId(),
                header.getMagic());
        audit_invalid_packet(cookie);
        c.setState(StateMachine::State::closing);
        return;
    }

    c.addMsgHdr(true);

    if (header.isRequest()) {
        auto reason = validate_packet_execusion_constraints(cookie);
        if (reason != cb::mcbp::Status::Success) {
            cookie.sendResponse(reason);
            LOG_WARNING(
                    "{}: try_read_mcbp_command - "
                    "validate_packet_execusion_constraints returned {} - "
                    "closing "
                    "connection {}",
                    c.getId(),
                    std::to_string(static_cast<uint16_t>(reason)),
                    c.getDescription());
            c.setWriteAndGo(StateMachine::State::closing);
            return;
        }
    }

    if (c.isPacketAvailable()) {
        // we've got the entire packet spooled up, just go execute
        cookie.setPacket(Cookie::PacketContent::Full,
                         cb::const_byte_buffer{input.data(),
                                               sizeof(cb::mcbp::Request) +
                                                       header.getBodylen()});
        c.setState(StateMachine::State::execute);
    } else {
        // we need to allocate more memory!!
        try {
            size_t needed = sizeof(cb::mcbp::Request) + header.getBodylen();
            c.read->ensureCapacity(needed - c.read->rsize());
            // ensureCapacity may have reallocated the buffer.. make sure
            // that the packet in the cookie points to the correct address
            cookie.setPacket(Cookie::PacketContent::Header,
                             cb::const_byte_buffer{c.read->rdata().data(),
                                                   sizeof(cb::mcbp::Request)});
        } catch (const std::bad_alloc&) {
            LOG_WARNING("{}: Failed to grow buffer.. closing connection",
                        c.getId());
            c.setState(StateMachine::State::closing);
            return;
        }
        c.setState(StateMachine::State::read_packet_body);
    }
}
