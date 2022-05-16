/*
 * Portions Copyright (c) 2010-Present Couchbase
 * Portions Copyright (c) 2008 Danga Interactive
 *
 * Use of this software is governed by the Apache License, Version 2.0 and
 * BSD 3 Clause included in the files licenses/APL2.txt and
 * licenses/BSD-3-Clause-Danga-Interactive.txt
 */
#include "mcbp_executors.h"

#include "cmdline.h"
#include "config_parse.h"
#include "error_map_manager.h"
#include "external_auth_manager_thread.h"
#include "ioctl.h"
#include "mc_time.h"
#include "mcaudit.h"
#include "mcbp.h"
#include "mcbp_privileges.h"
#include "network_interface_manager.h"
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
#include "protocol/mcbp/ifconfig_context.h"
#include "protocol/mcbp/mutation_context.h"
#include "protocol/mcbp/rbac_reload_command_context.h"
#include "protocol/mcbp/remove_context.h"
#include "protocol/mcbp/sasl_refresh_command_context.h"
#include "protocol/mcbp/sasl_start_command_context.h"
#include "protocol/mcbp/sasl_step_command_context.h"
#include "protocol/mcbp/session_validated_command_context.h"
#include "protocol/mcbp/settings_reload_command_context.h"
#include "protocol/mcbp/stats_context.h"
#include "protocol/mcbp/unlock_context.h"
#include "session_cas.h"
#include "settings.h"
#include "ssl_utils.h"
#include "subdocument.h"
#include <logger/logger.h>
#include <mcbp/protocol/header.h>
#include <nlohmann/json.hpp>
#include <utilities/engine_errc_2_mcbp.h>

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

static void ifconfig_executor(Cookie& cookie) {
    cookie.obtainContext<IfconfigCommandContext>(cookie).drive();
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

    auto ret = cookie.swapAiostat(cb::engine_errc::success);

    if (ret == cb::engine_errc::success) {
        ret = bucket_unknown_command(cookie, mcbpResponseHandlerFn);
    }

    ret = cookie.getConnection().remapErrorCode(ret);
    switch (ret) {
    case cb::engine_errc::success: {
        break;
    }
    case cb::engine_errc::would_block:
        cookie.setEwouldblock(true);
        break;
    case cb::engine_errc::disconnect:
        connection.shutdown();
        break;
    default:
        cookie.sendResponse(cb::engine_errc(ret));
    }
}

/**
 * We received a noop response.. just ignore it
 */
static void process_bin_noop_response(Cookie& cookie) {
    // do nothing
}

static void add_set_replace_executor(Cookie& cookie, StoreSemantics store_op) {
    cookie.obtainContext<MutationCommandContext>(
                  cookie, cookie.getRequest(), store_op)
            .drive();
}

static void add_executor(Cookie& cookie) {
    add_set_replace_executor(cookie, StoreSemantics::Add);
}

static void set_executor(Cookie& cookie) {
    add_set_replace_executor(cookie, StoreSemantics::Set);
}

static void replace_executor(Cookie& cookie) {
    add_set_replace_executor(cookie, StoreSemantics::Replace);
}

static void append_prepend_executor(Cookie& cookie) {
    const auto& req = cookie.getRequest();
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
    cookie.setErrorContext("Use ifconfig tls instead");
    cookie.sendResponse(cb::mcbp::Status::NotSupported);
}

static void verbosity_executor(Cookie& cookie) {
#define MAX_VERBOSITY_LEVEL 2
    using cb::mcbp::request::VerbosityPayload;
    auto extras = cookie.getRequest().getExtdata();
    auto* payload = reinterpret_cast<const VerbosityPayload*>(extras.data());
    int level = payload->getLevel();
    if (level < 0 || level > MAX_VERBOSITY_LEVEL) {
        level = MAX_VERBOSITY_LEVEL;
    }
    Settings::instance().setVerbose(level);
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
    connection.shutdown();
    connection.setTerminationReason("Client sent QUIT");
}

static void quitq_executor(Cookie& cookie) {
    auto& connection = cookie.getConnection();
    LOG_DEBUG("{}: quitq_executor - closing connection {}",
              connection.getId(),
              connection.getDescription());
    connection.shutdown();
    connection.setTerminationReason("Client sent QUIT");
}

static void sasl_list_mech_executor(Cookie& cookie) {
    auto& connection = cookie.getConnection();
    if (!connection.isSaslAuthEnabled()) {
        cookie.sendResponse(cb::mcbp::Status::NotSupported);
        return;
    }

    if (connection.isSslEnabled() &&
        Settings::instance().has.ssl_sasl_mechanisms) {
        const auto& mechs = Settings::instance().getSslSaslMechanisms();
        cookie.sendResponse(cb::mcbp::Status::Success,
                            {},
                            {},
                            mechs,
                            cb::mcbp::Datatype::Raw,
                            0);
    } else if (!connection.isSslEnabled() &&
               Settings::instance().has.sasl_mechanisms) {
        const auto& mechs = Settings::instance().getSaslMechanisms();
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
    cookie.obtainContext<SaslStartCommandContext>(cookie).drive();
}

static void sasl_step_executor(Cookie& cookie) {
    cookie.obtainContext<SaslStepCommandContext>(cookie).drive();
}

static void noop_executor(Cookie& cookie) {
    cookie.sendResponse(cb::mcbp::Status::Success);
}

static void flush_executor(Cookie& cookie) {
    cookie.obtainContext<FlushCommandContext>(cookie).drive();
}

static void delete_executor(Cookie& cookie) {
    cookie.obtainContext<RemoveCommandContext>(cookie, cookie.getRequest())
            .drive();
}

static void arithmetic_executor(Cookie& cookie) {
    const auto& req = cookie.getRequest();
    cookie.obtainContext<ArithmeticCommandContext>(cookie, req).drive();
}

static void set_ctrl_token_executor(Cookie& cookie) {
    using cb::mcbp::request::SetCtrlTokenPayload;
    auto& req = cookie.getRequest();
    auto extras = req.getExtdata();
    auto* payload = reinterpret_cast<const SetCtrlTokenPayload*>(extras.data());
    auto newval = payload->getCas();
    const auto casval = req.getCas();
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
    auto ret = cookie.swapAiostat(cb::engine_errc::success);
    cb::mcbp::Datatype datatype = cb::mcbp::Datatype::Raw;
    std::string value;
    if (ret == cb::engine_errc::success) {
        auto& req = cookie.getRequest();
        auto key_data = req.getKey();
        const std::string key(reinterpret_cast<const char*>(key_data.data()),
                              key_data.size());
        ret = ioctl_get_property(cookie, key, value, datatype);
    }

    auto remapErr = connection.remapErrorCode(ret);
    switch (remapErr) {
    case cb::engine_errc::success:
        cookie.sendResponse(cb::mcbp::Status::Success,
                            {},
                            {},
                            {value.data(), value.size()},
                            datatype,
                            0);
        break;
    case cb::engine_errc::would_block:
        cookie.setEwouldblock(true);
        break;
    case cb::engine_errc::disconnect:
        if (ret == cb::engine_errc::disconnect) {
            LOG_WARNING(
                    "{}: ioctl_get_executor - ioctl_get_property returned "
                    "cb::engine_errc::disconnect - closing connection {}",
                    connection.getId(),
                    connection.getDescription());
            connection.setTerminationReason(
                    "ioctl_get_executor forced disconnect");
        }
        connection.shutdown();
        break;
    default:
        cookie.sendResponse(cb::mcbp::to_status(cb::engine_errc(remapErr)));
    }
}

static void ioctl_set_executor(Cookie& cookie) {
    auto ret = cookie.swapAiostat(cb::engine_errc::success);

    auto& connection = cookie.getConnection();
    if (ret == cb::engine_errc::success) {
        auto& req = cookie.getRequest();
        auto key_data = req.getKey();
        auto val_data = req.getValue();
        const std::string key(reinterpret_cast<const char*>(key_data.data()),
                              key_data.size());
        const std::string value(reinterpret_cast<const char*>(val_data.data()),
                                val_data.size());

        ret = ioctl_set_property(cookie, key, value);
    }
    auto remapErr = connection.remapErrorCode(ret);

    switch (remapErr) {
    case cb::engine_errc::would_block:
        cookie.setEwouldblock(true);
        break;
    case cb::engine_errc::disconnect:
        if (ret == cb::engine_errc::disconnect) {
            LOG_WARNING(
                    "{}: ioctl_set_executor - ioctl_set_property returned "
                    "cb::engine_errc::disconnect - closing connection {}",
                    connection.getId(),
                    connection.getDescription());
            connection.setTerminationReason(
                    "ioctl_set_executor forced disconnect");
        }
        connection.shutdown();
        break;
    default:
        cookie.sendResponse(cb::mcbp::to_status(cb::engine_errc(remapErr)));
    }
}

static void config_validate_executor(Cookie& cookie) {
    const auto& request = cookie.getRequest();
    const auto value = request.getValue();

    // the config validator needs a null-terminated string...
    std::string val_buffer(reinterpret_cast<const char*>(value.data()),
                           value.size());

    auto errors = validate_proposed_config_changes(val_buffer.c_str());
    if (!errors) {
        cookie.sendResponse(cb::mcbp::Status::Success);
        return;
    }

    // problem(s). Send the errors back to the client.
    cookie.setErrorContext(errors->dump());
    cookie.sendResponse(cb::mcbp::Status::Einval);
}

static void config_reload_executor(Cookie& cookie) {
    cookie.obtainContext<SettingsReloadCommandContext>(cookie).drive();
}

static void audit_config_reload_executor(Cookie& cookie) {
    cookie.obtainContext<AuditConfigureCommandContext>(cookie).drive();
}

static void audit_put_executor(Cookie& cookie) {
    const auto& request = cookie.getRequest();
    // The packet validator ensured that this is 4 bytes long
    const auto extras = request.getExtdata();
    const uint32_t id = *reinterpret_cast<const uint32_t*>(extras.data());

    if (mc_audit_event(cookie, ntohl(id), request.getValue())) {
        cookie.sendResponse(cb::mcbp::Status::Success);
    } else {
        cookie.sendResponse(cb::mcbp::Status::Ebusy);
    }
}

static void create_remove_bucket_executor(Cookie& cookie) {
    cookie.obtainContext<CreateRemoveBucketCommandContext>(cookie).drive();
}

static void get_errmap_executor(Cookie& cookie) {
    auto value = cookie.getRequest().getValue();
    auto* req = reinterpret_cast<const cb::mcbp::request::GetErrmapPayload*>(
            value.data());
    auto errormap = ErrorMapManager::instance().getErrorMap(req->getVersion());
    if (errormap.empty()) {
        cookie.sendResponse(cb::mcbp::Status::KeyEnoent);
    } else {
        cookie.sendResponse(cb::mcbp::Status::Success,
                            {},
                            {},
                            errormap,
                            cb::mcbp::Datatype::JSON,
                            0);
    }
}

static void shutdown_executor(Cookie& cookie) {
    if (session_cas.increment_session_counter(cookie.getRequest().getCas())) {
        session_cas.decrement_session_counter();
        cookie.sendResponse(cb::mcbp::Status::Success);
        LOG_INFO("{} Shutdown server requested", cookie.getConnectionId());
        shutdown_server();
    } else {
        cookie.sendResponse(cb::mcbp::Status::KeyEexists);
    }
}

static void set_bucket_compute_unit_throttle_limits_executor(Cookie& cookie) {
    if (!isServerlessDeployment()) {
        cookie.sendResponse(cb::mcbp::Status::NotSupported);
        return;
    }
    std::string name(cookie.getRequestKey().getBuffer());
    using cb::mcbp::request::SetBucketComputeUnitThrottleLimitPayload;
    auto& req = cookie.getRequest();
    auto extras = req.getExtdata();
    auto* payload =
            reinterpret_cast<const SetBucketComputeUnitThrottleLimitPayload*>(
                    extras.data());
    bool found = false;
    BucketManager::instance().forEach(
            [id = cookie.getConnectionId(), &name, &found, payload](
                    auto& bucket) -> bool {
                if (strcmp(bucket.name, name.c_str()) == 0) {
                    bucket.setThrottleLimit(id, payload->getLimit());
                    found = true;
                    return false;
                }
                return true;
            });

    if (found) {
        cookie.sendResponse(cb::mcbp::Status::Success);
    } else {
        cookie.sendResponse(cb::mcbp::Status::KeyEnoent);
    }
}

static void set_bucket_data_limit_exceeded_executor(Cookie& cookie) {
    if (!isServerlessDeployment()) {
        cookie.sendResponse(cb::mcbp::Status::NotSupported);
        return;
    }
    std::string name(cookie.getRequestKey().getBuffer());
    using cb::mcbp::request::SetBucketDataLimitExceededPayload;
    auto& req = cookie.getRequest();
    auto extras = req.getExtdata();
    auto* payload =
            reinterpret_cast<const SetBucketDataLimitExceededPayload*>(
                    extras.data());
    bool found = false;
    BucketManager::instance().forEach(
            [&cookie, &name, &found, payload](auto& bucket) -> bool {
                if (strcmp(bucket.name, name.c_str()) == 0) {
                    if (bucket.bucket_quota_exceeded != payload->isEnabled()) {
                        LOG_INFO("{} {}able client document ingress",
                                 cookie.getConnectionId(),
                                 payload->isEnabled() ? "En" : "Dis");
                        bucket.bucket_quota_exceeded = payload->isEnabled();
                    }
                    found = true;
                    return false;
                }
                return true;
            });

    if (found) {
        cookie.sendResponse(cb::mcbp::Status::Success);
    } else {
        cookie.sendResponse(cb::mcbp::Status::KeyEnoent);
    }
}

static void update_user_permissions_executor(Cookie& cookie) {
    auto& request = cookie.getRequest();
    auto value = request.getValue();
    auto status = cb::mcbp::Status::Success;

    try {
        cb::rbac::updateExternalUser(
                std::string{reinterpret_cast<const char*>(value.data()),
                            value.size()});
    } catch (const nlohmann::json::exception& error) {
        cookie.setErrorContext(error.what());
        status = cb::mcbp::Status::Einval;
        LOG_WARNING(
                R"({}: update_user_permissions_executor: Failed to parse provided JSON: {})",
                cookie.getConnectionId(),
                error.what());
    } catch (const std::runtime_error& error) {
        cookie.setErrorContext(error.what());
        status = cb::mcbp::Status::Einval;
        LOG_WARNING(
                R"({}: update_user_permissions_executor: An error occurred while updating user: {})",
                cookie.getConnectionId(),
                error.what());
    }

    cookie.sendResponse(status);
}

static void set_param_executor(Cookie& cookie) {
    cookie.obtainContext<SetParameterCommandContext>(cookie).drive();
}

static void get_vbucket_executor(Cookie& cookie) {
    cookie.obtainContext<GetVbucketCommandContext>(cookie).drive();
}

static void set_vbucket_executor(Cookie& cookie) {
    cookie.obtainContext<SetVbucketCommandContext>(cookie).drive();
}

static void delete_vbucket_executor(Cookie& cookie) {
    cookie.obtainContext<DeleteVbucketCommandContext>(cookie).drive();
}

static void compact_db_executor(Cookie& cookie) {
    cookie.obtainContext<CompactDatabaseCommandContext>(cookie).drive();
}

static void rbac_refresh_executor(Cookie& cookie) {
    cookie.obtainContext<RbacReloadCommandContext>(cookie).drive();
}

static void auth_provider_executor(Cookie& cookie) {
    if (!Settings::instance().isExternalAuthServiceEnabled()) {
        cookie.setErrorContext(
                "Support for external authentication service is disabled");
        cookie.sendResponse(cb::mcbp::Status::NotSupported);
        return;
    }

    auto& connection = cookie.getConnection();
    if (connection.isDuplexSupported()) {
        externalAuthManager->add(connection);
        cookie.sendResponse(cb::mcbp::Status::Success);
        LOG_INFO("{}: Registered as authentication provider: {}",
                 connection.getId(),
                 connection.getDescription());
    } else {
        cookie.setErrorContext("Connection is not in duplex mode");
        cookie.sendResponse(cb::mcbp::Status::Einval);
    }
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
        c.shutdown();
        c.setTerminationReason("Connected engine does not support DCP");
        return;
    }

    auto ret = dcp->response_handler(cookie, cookie.getHeader().getResponse());
    auto remapErr = c.remapErrorCode(ret);

    if (remapErr == cb::engine_errc::disconnect) {
        if (ret == cb::engine_errc::disconnect) {
            LOG_WARNING(
                    "{}: process_bin_dcp_response - response_handler returned "
                    "cb::engine_errc::disconnect - closing connection {}",
                    c.getId(),
                    c.getDescription());
            c.setTerminationReason(
                    "process_bin_dcp_response forced disconnect");
        }
        c.shutdown();
    }
}

static void setup_response_handler(cb::mcbp::ClientOpcode opcode,
                                   HandlerFunction function) {
    response_handlers[std::underlying_type<cb::mcbp::ClientOpcode>::type(
            opcode)] = std::move(function);
}

static void setup_handler(cb::mcbp::ClientOpcode opcode,
                          HandlerFunction function) {
    handlers[std::underlying_type<cb::mcbp::ClientOpcode>::type(opcode)] =
            std::move(function);
}

void initialize_mbcp_lookup_map() {
    setup_response_handler(cb::mcbp::ClientOpcode::Noop,
                           process_bin_noop_response);
    setup_response_handler(cb::mcbp::ClientOpcode::DcpOpen,
                           process_bin_dcp_response);
    setup_response_handler(cb::mcbp::ClientOpcode::DcpAddStream,
                           process_bin_dcp_response);
    setup_response_handler(cb::mcbp::ClientOpcode::DcpCloseStream,
                           process_bin_dcp_response);
    setup_response_handler(cb::mcbp::ClientOpcode::DcpStreamReq,
                           process_bin_dcp_response);
    setup_response_handler(cb::mcbp::ClientOpcode::DcpGetFailoverLog,
                           process_bin_dcp_response);
    setup_response_handler(cb::mcbp::ClientOpcode::DcpStreamEnd,
                           process_bin_dcp_response);
    setup_response_handler(cb::mcbp::ClientOpcode::DcpSnapshotMarker,
                           process_bin_dcp_response);
    setup_response_handler(cb::mcbp::ClientOpcode::DcpMutation,
                           process_bin_dcp_response);
    setup_response_handler(cb::mcbp::ClientOpcode::DcpDeletion,
                           process_bin_dcp_response);
    setup_response_handler(cb::mcbp::ClientOpcode::DcpExpiration,
                           process_bin_dcp_response);
    setup_response_handler(cb::mcbp::ClientOpcode::DcpSetVbucketState,
                           process_bin_dcp_response);
    setup_response_handler(cb::mcbp::ClientOpcode::DcpNoop,
                           process_bin_dcp_response);
    setup_response_handler(cb::mcbp::ClientOpcode::DcpBufferAcknowledgement,
                           process_bin_dcp_response);
    setup_response_handler(cb::mcbp::ClientOpcode::DcpControl,
                           process_bin_dcp_response);
    setup_response_handler(cb::mcbp::ClientOpcode::DcpSystemEvent,
                           process_bin_dcp_response);
    setup_response_handler(cb::mcbp::ClientOpcode::DcpPrepare,
                           process_bin_dcp_response);
    setup_response_handler(cb::mcbp::ClientOpcode::DcpCommit,
                           process_bin_dcp_response);
    setup_response_handler(cb::mcbp::ClientOpcode::DcpAbort,
                           process_bin_dcp_response);
    setup_response_handler(cb::mcbp::ClientOpcode::DcpSeqnoAcknowledged,
                           process_bin_dcp_response);
    setup_response_handler(cb::mcbp::ClientOpcode::GetErrorMap,
                           process_bin_dcp_response);

    for (auto& handler : handlers) {
        handler = process_bin_unknown_packet;
    }

    setup_handler(cb::mcbp::ClientOpcode::DcpOpen, dcp_open_executor);
    setup_handler(cb::mcbp::ClientOpcode::DcpAddStream,
                  dcp_add_stream_executor);
    setup_handler(cb::mcbp::ClientOpcode::DcpCloseStream,
                  dcp_close_stream_executor);
    setup_handler(cb::mcbp::ClientOpcode::DcpSnapshotMarker,
                  dcp_snapshot_marker_executor);
    setup_handler(cb::mcbp::ClientOpcode::DcpDeletion, dcp_deletion_executor);
    setup_handler(cb::mcbp::ClientOpcode::DcpExpiration,
                  dcp_expiration_executor);
    setup_handler(cb::mcbp::ClientOpcode::DcpGetFailoverLog,
                  dcp_get_failover_log_executor);
    setup_handler(cb::mcbp::ClientOpcode::DcpMutation, dcp_mutation_executor);
    setup_handler(cb::mcbp::ClientOpcode::DcpSetVbucketState,
                  dcp_set_vbucket_state_executor);
    setup_handler(cb::mcbp::ClientOpcode::DcpNoop, dcp_noop_executor);
    setup_handler(cb::mcbp::ClientOpcode::DcpBufferAcknowledgement,
                  dcp_buffer_acknowledgement_executor);
    setup_handler(cb::mcbp::ClientOpcode::DcpControl, dcp_control_executor);
    setup_handler(cb::mcbp::ClientOpcode::DcpStreamEnd,
                  dcp_stream_end_executor);
    setup_handler(cb::mcbp::ClientOpcode::DcpStreamReq,
                  dcp_stream_req_executor);
    setup_handler(cb::mcbp::ClientOpcode::DcpSystemEvent,
                  dcp_system_event_executor);
    setup_handler(cb::mcbp::ClientOpcode::DcpPrepare, dcp_prepare_executor);
    setup_handler(cb::mcbp::ClientOpcode::DcpSeqnoAcknowledged,
                  dcp_seqno_acknowledged_executor);
    setup_handler(cb::mcbp::ClientOpcode::DcpCommit, dcp_commit_executor);
    setup_handler(cb::mcbp::ClientOpcode::DcpAbort, dcp_abort_executor);

    setup_handler(cb::mcbp::ClientOpcode::CollectionsSetManifest,
                  collections_set_manifest_executor);
    setup_handler(cb::mcbp::ClientOpcode::CollectionsGetManifest,
                  collections_get_manifest_executor);
    setup_handler(cb::mcbp::ClientOpcode::CollectionsGetID,
                  collections_get_collection_id_executor);
    setup_handler(cb::mcbp::ClientOpcode::CollectionsGetScopeID,
                  collections_get_scope_id_executor);

    setup_handler(cb::mcbp::ClientOpcode::IsaslRefresh, isasl_refresh_executor);
    setup_handler(cb::mcbp::ClientOpcode::SslCertsRefresh,
                  ssl_certs_refresh_executor);
    setup_handler(cb::mcbp::ClientOpcode::Verbosity, verbosity_executor);
    setup_handler(cb::mcbp::ClientOpcode::Hello, process_hello_packet_executor);
    setup_handler(cb::mcbp::ClientOpcode::Version, version_executor);
    setup_handler(cb::mcbp::ClientOpcode::Quit, quit_executor);
    setup_handler(cb::mcbp::ClientOpcode::Quitq, quitq_executor);
    setup_handler(cb::mcbp::ClientOpcode::SaslListMechs,
                  sasl_list_mech_executor);
    setup_handler(cb::mcbp::ClientOpcode::SaslAuth, sasl_auth_executor);
    setup_handler(cb::mcbp::ClientOpcode::SaslStep, sasl_step_executor);
    setup_handler(cb::mcbp::ClientOpcode::Noop, noop_executor);
    setup_handler(cb::mcbp::ClientOpcode::Flush, flush_executor);
    setup_handler(cb::mcbp::ClientOpcode::Flushq, flush_executor);
    setup_handler(cb::mcbp::ClientOpcode::Setq, set_executor);
    setup_handler(cb::mcbp::ClientOpcode::Set, set_executor);
    setup_handler(cb::mcbp::ClientOpcode::Addq, add_executor);
    setup_handler(cb::mcbp::ClientOpcode::Add, add_executor);
    setup_handler(cb::mcbp::ClientOpcode::Replaceq, replace_executor);
    setup_handler(cb::mcbp::ClientOpcode::Replace, replace_executor);
    setup_handler(cb::mcbp::ClientOpcode::Appendq, append_prepend_executor);
    setup_handler(cb::mcbp::ClientOpcode::Append, append_prepend_executor);
    setup_handler(cb::mcbp::ClientOpcode::Prependq, append_prepend_executor);
    setup_handler(cb::mcbp::ClientOpcode::Prepend, append_prepend_executor);
    setup_handler(cb::mcbp::ClientOpcode::Get, get_executor);
    setup_handler(cb::mcbp::ClientOpcode::Getq, get_executor);
    setup_handler(cb::mcbp::ClientOpcode::Getk, get_executor);
    setup_handler(cb::mcbp::ClientOpcode::Getkq, get_executor);
    setup_handler(cb::mcbp::ClientOpcode::GetMeta, get_meta_executor);
    setup_handler(cb::mcbp::ClientOpcode::GetqMeta, get_meta_executor);
    setup_handler(cb::mcbp::ClientOpcode::Gat, gat_executor);
    setup_handler(cb::mcbp::ClientOpcode::Gatq, gat_executor);
    setup_handler(cb::mcbp::ClientOpcode::Touch, gat_executor);
    setup_handler(cb::mcbp::ClientOpcode::Delete, delete_executor);
    setup_handler(cb::mcbp::ClientOpcode::Deleteq, delete_executor);
    setup_handler(cb::mcbp::ClientOpcode::Stat, stat_executor);
    setup_handler(cb::mcbp::ClientOpcode::Increment, arithmetic_executor);
    setup_handler(cb::mcbp::ClientOpcode::Incrementq, arithmetic_executor);
    setup_handler(cb::mcbp::ClientOpcode::Decrement, arithmetic_executor);
    setup_handler(cb::mcbp::ClientOpcode::Decrementq, arithmetic_executor);
    setup_handler(cb::mcbp::ClientOpcode::GetCmdTimer, get_cmd_timer_executor);
    setup_handler(cb::mcbp::ClientOpcode::SetCtrlToken,
                  set_ctrl_token_executor);
    setup_handler(cb::mcbp::ClientOpcode::GetCtrlToken,
                  get_ctrl_token_executor);
    setup_handler(cb::mcbp::ClientOpcode::IoctlGet, ioctl_get_executor);
    setup_handler(cb::mcbp::ClientOpcode::IoctlSet, ioctl_set_executor);
    setup_handler(cb::mcbp::ClientOpcode::ConfigValidate,
                  config_validate_executor);
    setup_handler(cb::mcbp::ClientOpcode::ConfigReload, config_reload_executor);
    setup_handler(cb::mcbp::ClientOpcode::AuditPut, audit_put_executor);
    setup_handler(cb::mcbp::ClientOpcode::AuditConfigReload,
                  audit_config_reload_executor);
    setup_handler(cb::mcbp::ClientOpcode::Shutdown, shutdown_executor);
    setup_handler(cb::mcbp::ClientOpcode::SetBucketComputeUnitThrottleLimits,
                  set_bucket_compute_unit_throttle_limits_executor);
    setup_handler(cb::mcbp::ClientOpcode::SetBucketDataLimitExceeded,
                  set_bucket_data_limit_exceeded_executor);
    setup_handler(cb::mcbp::ClientOpcode::CreateBucket,
                  create_remove_bucket_executor);
    setup_handler(cb::mcbp::ClientOpcode::ListBuckets, list_bucket_executor);
    setup_handler(cb::mcbp::ClientOpcode::DeleteBucket,
                  create_remove_bucket_executor);
    setup_handler(cb::mcbp::ClientOpcode::SelectBucket, select_bucket_executor);
    setup_handler(cb::mcbp::ClientOpcode::GetErrorMap, get_errmap_executor);
    setup_handler(cb::mcbp::ClientOpcode::GetLocked, get_locked_executor);
    setup_handler(cb::mcbp::ClientOpcode::UnlockKey, unlock_executor);
    setup_handler(cb::mcbp::ClientOpcode::GetFailoverLog,
                  dcp_get_failover_log_executor);
    setup_handler(cb::mcbp::ClientOpcode::DropPrivilege,
                  drop_privilege_executor);
    setup_handler(cb::mcbp::ClientOpcode::UpdateExternalUserPermissions,
                  update_user_permissions_executor);
    setup_handler(cb::mcbp::ClientOpcode::RbacRefresh, rbac_refresh_executor);
    setup_handler(cb::mcbp::ClientOpcode::AuthProvider, auth_provider_executor);
    setup_handler(cb::mcbp::ClientOpcode::GetClusterConfig,
                  get_cluster_config_executor);
    setup_handler(cb::mcbp::ClientOpcode::SetClusterConfig,
                  set_cluster_config_executor);

    setup_handler(cb::mcbp::ClientOpcode::SubdocGet, subdoc_get_executor);
    setup_handler(cb::mcbp::ClientOpcode::SubdocExists, subdoc_exists_executor);
    setup_handler(cb::mcbp::ClientOpcode::SubdocDictAdd,
                  subdoc_dict_add_executor);
    setup_handler(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                  subdoc_dict_upsert_executor);
    setup_handler(cb::mcbp::ClientOpcode::SubdocDelete, subdoc_delete_executor);
    setup_handler(cb::mcbp::ClientOpcode::SubdocReplace,
                  subdoc_replace_executor);
    setup_handler(cb::mcbp::ClientOpcode::SubdocArrayPushLast,
                  subdoc_array_push_last_executor);
    setup_handler(cb::mcbp::ClientOpcode::SubdocArrayPushFirst,
                  subdoc_array_push_first_executor);
    setup_handler(cb::mcbp::ClientOpcode::SubdocArrayInsert,
                  subdoc_array_insert_executor);
    setup_handler(cb::mcbp::ClientOpcode::SubdocArrayAddUnique,
                  subdoc_array_add_unique_executor);
    setup_handler(cb::mcbp::ClientOpcode::SubdocCounter,
                  subdoc_counter_executor);
    setup_handler(cb::mcbp::ClientOpcode::SubdocMultiLookup,
                  subdoc_multi_lookup_executor);
    setup_handler(cb::mcbp::ClientOpcode::SubdocMultiMutation,
                  subdoc_multi_mutation_executor);
    setup_handler(cb::mcbp::ClientOpcode::SubdocGetCount,
                  subdoc_get_count_executor);
    setup_handler(cb::mcbp::ClientOpcode::SubdocReplaceBodyWithXattr,
                  subdoc_replace_body_with_xattr_executor);

    setup_handler(cb::mcbp::ClientOpcode::AdjustTimeofday,
                  adjust_timeofday_executor);

    setup_handler(cb::mcbp::ClientOpcode::SetParam, set_param_executor);
    setup_handler(cb::mcbp::ClientOpcode::GetVbucket, get_vbucket_executor);
    setup_handler(cb::mcbp::ClientOpcode::SetVbucket, set_vbucket_executor);
    setup_handler(cb::mcbp::ClientOpcode::DelVbucket, delete_vbucket_executor);
    setup_handler(cb::mcbp::ClientOpcode::CompactDb, compact_db_executor);
    setup_handler(cb::mcbp::ClientOpcode::Ifconfig, ifconfig_executor);
    setup_handler(cb::mcbp::ClientOpcode::RangeScanCreate,
                  range_scan_create_executor);
    setup_handler(cb::mcbp::ClientOpcode::RangeScanContinue,
                  range_scan_continue_executor);
    setup_handler(cb::mcbp::ClientOpcode::RangeScanCancel,
                  range_scan_cancel_executor);
}

static cb::engine_errc getEngineErrorCode(
        const cb::rbac::PrivilegeAccess& access) {
    switch (access.getStatus()) {
    case cb::rbac::PrivilegeAccess::Status::Ok:
        return cb::engine_errc::success;
    case cb::rbac::PrivilegeAccess::Status::Fail:
        return cb::engine_errc::no_access;
    case cb::rbac::PrivilegeAccess::Status::FailNoPrivileges:
        // No scope specific commands are being checked here, privilege fail
        // with no scope, collection privs is
        // cb::engine_errc::unknown_collection
        return cb::engine_errc::unknown_collection;
    }
    throw std::invalid_argument(
            "getEngineErrorCode(PrivilegeAccess) unknown status:" +
            std::to_string(uint32_t(access.getStatus())));
}

static cb::mcbp::Status getStatusCode(const cb::rbac::PrivilegeAccess& access) {
    switch (access.getStatus()) {
    case cb::rbac::PrivilegeAccess::Status::Ok:
        return cb::mcbp::Status::Success;
    case cb::rbac::PrivilegeAccess::Status::Fail:
        return cb::mcbp::Status::Eaccess;
    case cb::rbac::PrivilegeAccess::Status::FailNoPrivileges:
        // No scope specific commands are being checked here, privilege fail
        // with no scope, collection privs is UnknownCollection
        return cb::mcbp::Status::UnknownCollection;
    }
    throw std::invalid_argument(
            "getStatusCode(PrivilegeAccess) unknown status" +
            std::to_string(uint32_t(access.getStatus())));
}

void execute_client_request_packet(Cookie& cookie,
                                   const cb::mcbp::Request& request) {
    auto* c = &cookie.getConnection();

    static McbpPrivilegeChains privilegeChains;

    const auto opcode = request.getClientOpcode();
    auto res = cb::rbac::PrivilegeAccessOk;
    if (!cookie.isAuthorized()) {
        res = privilegeChains.invoke(opcode, cookie);
    }

    if (res.failed()) {
        if (c->remapErrorCode(getEngineErrorCode(res)) ==
            cb::engine_errc::disconnect) {
            c->shutdown();
        } else {
            auto status = getStatusCode(res);
            if (status == cb::mcbp::Status::UnknownCollection) {
                cookie.setUnknownCollectionErrorContext();
            }

            cookie.sendResponse(status);
        }
        return;
    }

    cookie.setAuthorized();
    handlers[std::underlying_type<cb::mcbp::ClientOpcode>::type(opcode)](
            cookie);
}

void execute_request_packet(Cookie& cookie, const cb::mcbp::Request& request) {
    switch (request.getMagic()) {
    case cb::mcbp::Magic::AltClientRequest:
    case cb::mcbp::Magic::ClientRequest:
        execute_client_request_packet(cookie, request);
        return;
    case cb::mcbp::Magic::ServerRequest:
        throw std::runtime_error(
                "execute_request_packet: processing server requests is not "
                "(yet) supported");
    case cb::mcbp::Magic::AltClientResponse:
        break;
    case cb::mcbp::Magic::ClientResponse:
        break;
    case cb::mcbp::Magic::ServerResponse:
        break;
    }

    throw std::logic_error(
            "execute_request_packet: provided packet is not a request");
}

static void execute_client_response_packet(Cookie& cookie,
                                           const cb::mcbp::Response& response) {
    const auto opcode = response.getClientOpcode();
    auto handler = response_handlers[uint8_t(opcode)];
    if (handler) {
        handler(cookie);
    } else {
        auto& c = cookie.getConnection();
        LOG_WARNING(
                "{}: Unsupported response packet received with opcode: {:#x} "
                "({})",
                c.getId(),
                uint32_t(opcode),
                is_valid_opcode(opcode) ? to_string(opcode)
                                        : "<invalid opcode>");
        c.shutdown();
        c.setTerminationReason("Unsupported response packet received");
    }
}

static void execute_server_response_packet(Cookie& cookie,
                                           const cb::mcbp::Response& response) {
    auto& c = cookie.getConnection();
    const auto opcode = response.getServerOpcode();
    switch (opcode) {
    case cb::mcbp::ServerOpcode::ClustermapChangeNotification:
    case cb::mcbp::ServerOpcode::ActiveExternalUsers:
        // ignore
        return;
    case cb::mcbp::ServerOpcode::Authenticate:
    case cb::mcbp::ServerOpcode::GetAuthorization:
        externalAuthManager->responseReceived(response);
        return;
    }

    LOG_INFO(
            "{}: Ignoring unsupported server response packet received with "
            "opcode: {:#x} ({})",
            c.getId(),
            uint32_t(opcode),
            is_valid_opcode(opcode) ? to_string(opcode) : "<invalid opcode>");
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
    case cb::mcbp::Magic::AltClientRequest:
    case cb::mcbp::Magic::ServerRequest:;
    }

    throw std::logic_error(
            "execute_response_packet: provided packet is not a response");
}
