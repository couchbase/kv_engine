/*
 * Portions Copyright (c) 2010-Present Couchbase
 * Portions Copyright (c) 2008 Danga Interactive
 *
 * Use of this software is governed by the Apache License, Version 2.0 and
 * BSD 3 Clause included in the files licenses/APL2.txt and
 * licenses/BSD-3-Clause-Danga-Interactive.txt
 */
#include "mcbp_executors.h"

#include "buckets.h"
#include "config_parse.h"
#include "error_map_manager.h"
#include "external_auth_manager_thread.h"
#include "ioctl.h"
#include "mc_time.h"
#include "mcaudit.h"
#include "mcbp.h"
#include "mcbp_privileges.h"
#include "protocol/mcbp/appendprepend_context.h"
#include "protocol/mcbp/arithmetic_context.h"
#include "protocol/mcbp/audit_configure_context.h"
#include "protocol/mcbp/bucket_management_command_context.h"
#include "protocol/mcbp/dcp_deletion.h"
#include "protocol/mcbp/dcp_expiration.h"
#include "protocol/mcbp/dcp_mutation.h"
#include "protocol/mcbp/dcp_system_event_executor.h"
#include "protocol/mcbp/delete_fusion_namespace_command_context.h"
#include "protocol/mcbp/engine_wrapper.h"
#include "protocol/mcbp/executors.h"
#include "protocol/mcbp/gat_context.h"
#include "protocol/mcbp/get_context.h"
#include "protocol/mcbp/get_file_fragment_context.h"
#include "protocol/mcbp/get_fusion_namespaces_command_context.h"
#include "protocol/mcbp/get_fusion_storage_snapshot_command_context.h"
#include "protocol/mcbp/get_locked_context.h"
#include "protocol/mcbp/get_meta_context.h"
#include "protocol/mcbp/get_vbucket_command_context.h"
#include "protocol/mcbp/getex_context.h"
#include "protocol/mcbp/ifconfig_context.h"
#include "protocol/mcbp/ioctl_command_context.h"
#include "protocol/mcbp/mount_fusion_vbucket_command_context.h"
#include "protocol/mcbp/mutation_context.h"
#include "protocol/mcbp/observe_context.h"
#include "protocol/mcbp/prepare_snapshot_context.h"
#include "protocol/mcbp/prune_encryption_keys_context.h"
#include "protocol/mcbp/rbac_reload_command_context.h"
#include "protocol/mcbp/release_fusion_storage_snapshot_command_context.h"
#include "protocol/mcbp/release_snapshot_context.h"
#include "protocol/mcbp/remove_context.h"
#include "protocol/mcbp/sasl_refresh_command_context.h"
#include "protocol/mcbp/sasl_start_command_context.h"
#include "protocol/mcbp/sasl_step_command_context.h"
#include "protocol/mcbp/set_active_encryption_keys_context.h"
#include "protocol/mcbp/set_chronicle_auth_token_command_context.h"
#include "protocol/mcbp/set_cluster_config_command_context.h"
#include "protocol/mcbp/set_param_command_context.h"
#include "protocol/mcbp/set_vbucket_command_context.h"
#include "protocol/mcbp/settings_reload_command_context.h"
#include "protocol/mcbp/single_state_steppable_context.h"
#include "protocol/mcbp/stats_context.h"
#include "protocol/mcbp/sync_fusion_logstore_command_context.h"
#include "protocol/mcbp/unknown_packet_command_context.h"
#include "protocol/mcbp/unmount_fusion_vbucket_command_context.h"
#include "settings.h"
#include "subdocument.h"

#include <logger/logger.h>
#include <mcbp/protocol/header.h>
#include <nlohmann/json.hpp>
#include <serverless/config.h>
#include <utilities/engine_errc_2_mcbp.h>
#include <utilities/fusion_support.h>
#include <utilities/throttle_utilities.h>

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

using cb::engine::Feature;

static void process_bin_get_meta(Cookie& cookie) {
    cookie.obtainContext<GetMetaCommandContext>(cookie).drive();
}

static void get_locked_executor(Cookie& cookie) {
    cookie.obtainContext<GetLockedCommandContext>(cookie).drive();
}

static void unlock_executor(Cookie& cookie) {
    cookie.obtainContext<SingleStateCommandContext>(cookie, [](Cookie& c) {
              return bucket_unlock(c,
                                   c.getRequestKey(),
                                   c.getRequest().getVBucket(),
                                   c.getRequest().getCas());
          }).drive();
}

static void gat_executor(Cookie& cookie) {
    cookie.obtainContext<GatCommandContext>(cookie).drive();
}

static void evict_key_executor(Cookie& cookie) {
    cookie.obtainContext<SingleStateCommandContext>(cookie, [](Cookie& c) {
              return bucket_evict_key(
                      c, c.getRequestKey(), c.getRequest().getVBucket());
          }).drive();
}

static void seqno_persistence_executor(Cookie& cookie) {
    cookie.obtainContext<SingleStateCommandContext>(cookie, [](Cookie& c) {
              auto data = c.getHeader().getExtdata();
              auto seqno =
                      ntohll(*reinterpret_cast<const uint64_t*>(data.data()));
              return bucket_wait_for_seqno_persistence(
                      c, seqno, c.getRequest().getVBucket());
          }).drive();
}

static void ifconfig_executor(Cookie& cookie) {
    cookie.obtainContext<IfconfigCommandContext>(cookie).drive();
}

static void start_persistence_executor(Cookie& cookie) {
    cookie.obtainContext<SingleStateCommandContext>(cookie, [](Cookie& c) {
              return bucket_start_persistence(c);
          }).drive();
}

static void stop_persistence_executor(Cookie& cookie) {
    cookie.obtainContext<SingleStateCommandContext>(cookie, [](Cookie& c) {
              return bucket_stop_persistence(c);
          }).drive();
}

static void observe_executor(Cookie& cookie) {
    cookie.obtainContext<ObserveCommandContext>(cookie).drive();
}

static void enable_traffic_control_mode_executor(Cookie& cookie) {
    cookie.obtainContext<SingleStateCommandContext>(cookie, [](Cookie& c) {
              return bucket_set_traffic_control_mode(
                      c, TrafficControlMode::Enabled);
          }).drive();
}

static void disable_traffic_control_mode_executor(Cookie& cookie) {
    cookie.obtainContext<SingleStateCommandContext>(cookie, [](Cookie& c) {
              return bucket_set_traffic_control_mode(
                      c, TrafficControlMode::Disabled);
          }).drive();
}

static void process_bin_unknown_packet(Cookie& cookie) {
    cookie.obtainContext<UnknownPacketCommandContext>(cookie).drive();
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
    cookie.obtainContext<GetCommandContext>(cookie).drive();
}

static void getex_executor(Cookie& cookie) {
    cookie.obtainContext<GetExCommandContext>(cookie).drive();
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

static void collections_set_manifest_executor(Cookie& cookie) {
    cookie.obtainContext<SingleStateCommandContext>(cookie, [](Cookie& c) {
              return c.getConnection()
                      .getBucketEngine()
                      .set_collection_manifest(c,
                                               c.getRequest().getValueString());
          }).drive();
}

static void verbosity_executor(Cookie& cookie) {
    constexpr int MAX_VERBOSITY_LEVEL = 2;
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

static void get_file_fragment_executor(Cookie& cookie) {
    if (cookie.getConnection().getBucket().supports(Feature::Persistence)) {
        cookie.obtainContext<GetFileFragmentContext>(cookie).drive();
    } else {
        cookie.sendResponse(cb::mcbp::Status::NotSupported);
    }
}

static void prepare_snapshot_executor(Cookie& cookie) {
    if (cookie.getConnection().getBucket().supports(Feature::Persistence)) {
        cookie.obtainContext<PrepareSnapshotContext>(cookie).drive();
    } else {
        cookie.sendResponse(cb::mcbp::Status::NotSupported);
    }
}
static void release_snapshot_executor(Cookie& cookie) {
    if (cookie.getConnection().getBucket().supports(Feature::Persistence)) {
        cookie.obtainContext<ReleaseSnapshotContext>(cookie).drive();
    } else {
        cookie.sendResponse(cb::mcbp::Status::NotSupported);
    }
}
static void download_snapshot_executor(Cookie& cookie) {
    if (cookie.getConnection().getBucket().supports(Feature::Persistence)) {
        cookie.obtainContext<SingleStateCommandContext>(cookie, [](Cookie& c) {
                  return c.getConnection().getBucketEngine().download_snapshot(
                          c,
                          c.getRequest().getVBucket(),
                          c.getRequest().getValueString());
              }).drive();
    } else {
        cookie.sendResponse(cb::mcbp::Status::NotSupported);
    }
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
    connection.shutdown();
    connection.setTerminationReason("Client sent QUIT");
}

static void quitq_executor(Cookie& cookie) {
    auto& connection = cookie.getConnection();
    connection.shutdown();
    connection.setTerminationReason("Client sent QUIT");
}

static void sasl_list_mech_executor(Cookie& cookie) {
    auto mech = cookie.getConnection().getSaslMechanisms();
    if (mech.empty()) {
        cookie.sendResponse(cb::mcbp::Status::NotSupported);
    } else {
        cookie.sendResponse(cb::mcbp::Status::Success,
                            {},
                            {},
                            mech,
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

static void delete_executor(Cookie& cookie) {
    cookie.obtainContext<RemoveCommandContext>(cookie, cookie.getRequest())
            .drive();
}

static void arithmetic_executor(Cookie& cookie) {
    const auto& req = cookie.getRequest();
    cookie.obtainContext<ArithmeticCommandContext>(cookie, req).drive();
}

static void set_ctrl_token_executor(Cookie& cookie) {
    cookie.sendResponse(cb::mcbp::Status::NotSupported);
}

static void get_ctrl_token_executor(Cookie& cookie) {
    cookie.sendResponse(cb::mcbp::Status::NotSupported);
}

static void ioctl_executor(Cookie& cookie) {
    cookie.obtainContext<IoctlCommandContext>(cookie).drive();
}

static void config_validate_executor(Cookie& cookie) {
    const auto& request = cookie.getRequest();
    auto errors = validate_proposed_config_changes(request.getValueString());
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

    cookie.sendResponse(mc_audit_event(cookie, ntohl(id), request.getValue()));
}

static void create_remove_bucket_executor(Cookie& cookie) {
    cookie.obtainContext<BucketManagementCommandContext>(cookie).drive();
}

static void pause_bucket_executor(Cookie& cookie) {
    cookie.obtainContext<BucketManagementCommandContext>(cookie).drive();
}

static void resume_bucket_executor(Cookie& cookie) {
    cookie.obtainContext<BucketManagementCommandContext>(cookie).drive();
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
    cookie.sendResponse(cb::mcbp::Status::Success);
    LOG_INFO_CTX("Shutdown server requested",
                 {"conn_id", cookie.getConnectionId()});
    shutdown_server();
}

static void set_bucket_throttle_properties_executor(Cookie& cookie) {
    std::string name(cookie.getRequestKey().getBuffer());
    cb::throttle::SetThrottleLimitPayload limits =
            nlohmann::json::parse(cookie.getHeader().getValueString());

    bool found = false;
    BucketManager::instance().forEach([&name, &found, &limits](
                                              auto& bucket) -> bool {
        if (bucket.name == name) {
            bucket.setThrottleLimits(limits.reserved, limits.hard_limit);
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

static void set_node_throttle_properties_executor(Cookie& cookie) {
    const cb::throttle::SetNodeThrottleLimitPayload limits =
            nlohmann::json::parse(cookie.getHeader().getValueString());
    auto& instance = cb::serverless::Config::instance();
    if (limits.capacity) {
        instance.nodeCapacity = limits.capacity.value();
    }
    if (limits.default_throttle_reserved_units) {
        instance.defaultThrottleReservedUnits =
                limits.default_throttle_reserved_units.value();
    }
    if (limits.default_throttle_hard_limit) {
        instance.defaultThrottleHardLimit =
                limits.default_throttle_hard_limit.value();
    }
    cookie.sendResponse(cb::mcbp::Status::Success);
}

static void set_active_encryption_key_executor(Cookie& cookie) {
    cookie.obtainContext<SetActiveEncryptionKeysContext>(cookie).drive();
}

static void prune_encryption_keys_executor(Cookie& cookie) {
    cookie.obtainContext<PruneEncryptionKeysContext>(cookie).drive();
}

static void set_bucket_data_limit_exceeded_executor(Cookie& cookie) {
    std::string name(cookie.getRequestKey().getBuffer());
    using cb::mcbp::request::SetBucketDataLimitExceededPayload;
    auto& req = cookie.getRequest();
    auto extras = req.getExtdata();
    auto* payload = reinterpret_cast<const SetBucketDataLimitExceededPayload*>(
            extras.data());
    using cb::mcbp::Status;
    auto status = Status::KeyEnoent;
    BucketManager::instance().forEach([&cookie, &name, &status, payload](
                                              auto& bucket) -> bool {
        if (bucket.name == name) {
            std::lock_guard<std::mutex> guard(bucket.mutex);
            if (bucket.data_ingress_status != payload->getStatus()) {
                bool access = cookie.getConnection().isNodeSupervisor();
                if (!access) {
                    // we may only progress if:
                    // previous is success and next is BucketSizeLimitExceeded
                    // previous is BucketSizeLimitExceeded and next is Success
                    access = (bucket.data_ingress_status == Status::Success &&
                              payload->getStatus() ==
                                      Status::BucketSizeLimitExceeded) ||
                             (bucket.data_ingress_status ==
                                      Status::BucketSizeLimitExceeded &&
                              payload->getStatus() == Status::Success);
                }

                if (!access) {
                    LOG_WARNING_CTX(
                            "The regulator can't set client document ingress "
                            "as the node supervisor locked the value",
                            {"conn_id", cookie.getConnectionId()},
                            {"bucket", name},
                            {"from", bucket.data_ingress_status.load()},
                            {"to", payload->getStatus()});
                    status = Status::Locked;
                    return false;
                }

                if (payload->getStatus() == Status::Success) {
                    LOG_INFO_CTX("Enable client document ingress for bucket",
                                 {"conn_id", cookie.getConnectionId()},
                                 {"bucket", name});
                } else {
                    LOG_INFO_CTX("Disable client document ingress for bucket",
                                 {"conn_id", cookie.getConnectionId()},
                                 {"bucket", name},
                                 {"status", payload->getStatus()});
                }
                bucket.data_ingress_status = payload->getStatus();
            }
            status = Status::Success;
            return false;
        }
        return true;
    });

    cookie.sendResponse(status);
}

static void update_user_permissions_executor(Cookie& cookie) {
    auto& request = cookie.getRequest();
    auto value = request.getValueString();
    auto status = cb::mcbp::Status::Success;

    try {
        cb::rbac::updateExternalUser(value);
    } catch (const nlohmann::json::exception& error) {
        cookie.setErrorContext(error.what());
        status = cb::mcbp::Status::Einval;
        LOG_WARNING_CTX(
                "update_user_permissions_executor: Failed to parse provided "
                "JSON",
                {"conn_id", cookie.getConnectionId()},
                {"error", error.what()});
    } catch (const std::runtime_error& error) {
        cookie.setErrorContext(error.what());
        status = cb::mcbp::Status::Einval;
        LOG_WARNING_CTX(
                "update_user_permissions_executor: An error occurred while "
                "updating user permissions",
                {"conn_id", cookie.getConnectionId()},
                {"error", error.what()});
    }

    cookie.sendResponse(status);
}

static void set_param_executor(Cookie& cookie) {
    cookie.obtainContext<SetParamCommandContext>(cookie).drive();
}

static void get_vbucket_executor(Cookie& cookie) {
    cookie.obtainContext<GetVbucketCommandContext>(cookie).drive();
}

static void set_vbucket_executor(Cookie& cookie) {
    cookie.obtainContext<SetVbucketCommandContext>(cookie).drive();
}

static void delete_vbucket_executor(Cookie& cookie) {
    cookie.obtainContext<SingleStateCommandContext>(cookie, [](Cookie& c) {
              using namespace std::string_view_literals;
              const auto& req = c.getRequest();
              auto value = req.getValueString();
              return bucket_delete_vbucket(
                      c, req.getVBucket(), value == "async=0"sv);
          }).drive();
}

static void compact_db_executor(Cookie& cookie) {
    cookie.obtainContext<SingleStateCommandContext>(cookie, [](Cookie& c) {
              return bucket_compact_database(c);
          }).drive();
}

static void rbac_refresh_executor(Cookie& cookie) {
    cookie.obtainContext<RbacReloadCommandContext>(cookie).drive();
}

static void set_cluster_config_executor(Cookie& cookie) {
    cookie.obtainContext<SetClusterConfigCommandContext>(cookie).drive();
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
        LOG_INFO_CTX("Registered as authentication provider",
                     {"conn_id", connection.getId()},
                     {"description", connection.getDescription()});
    } else {
        cookie.setErrorContext("Connection is not in duplex mode");
        cookie.sendResponse(cb::mcbp::Status::Einval);
    }
}

static void get_fusion_storage_snapshot_executor(Cookie& cookie) {
    if (cookie.getConnection().getBucket().supports(Feature::Fusion)) {
        cookie.obtainContext<GetFusionStorageSnapshotCommandContext>(cookie)
                .drive();

    } else {
        cookie.sendResponse(cb::mcbp::Status::NotSupported);
    }
}

static void release_fusion_storage_snapshot_executor(Cookie& cookie) {
    if (cookie.getConnection().getBucket().supports(Feature::Fusion)) {
        cookie.obtainContext<ReleaseFusionStorageSnapshotCommandContext>(cookie)
                .drive();
    } else {
        cookie.sendResponse(cb::mcbp::Status::NotSupported);
    }
}

static void mount_fusion_vbucket_executor(Cookie& cookie) {
    if (cookie.getConnection().getBucket().supports(Feature::Fusion)) {
        cookie.obtainContext<MountFusionVbucketCommandContext>(cookie).drive();
    } else {
        cookie.sendResponse(cb::mcbp::Status::NotSupported);
    }
}

static void unmount_vbucket_executor(Cookie& cookie) {
    if (cookie.getConnection().getBucket().supports(Feature::Fusion)) {
        cookie.obtainContext<UnmountFusionVbucketCommandContext>(cookie)
                .drive();
    } else {
        cookie.sendResponse(cb::mcbp::Status::NotSupported);
    }
}

static void sync_fusion_logstore_executor(Cookie& cookie) {
    if (cookie.getConnection().getBucket().supports(Feature::Fusion)) {
        cookie.obtainContext<SyncFusionLogstoreCommandContext>(cookie).drive();
    } else {
        cookie.sendResponse(cb::mcbp::Status::NotSupported);
    }
}

static void start_fusion_uploader_executor(Cookie& cookie) {
    if (!cookie.getConnection().getBucket().supports(Feature::Fusion)) {
        cookie.sendResponse(cb::mcbp::Status::NotSupported);
        return;
    }

    cookie.obtainContext<SingleStateCommandContext>(cookie, [](Cookie& c) {
              try {
                  const auto& req = c.getRequest();
                  const auto args = nlohmann::json::parse(req.getValueString());
                  const std::string term = args["term"];
                  auto& engine = c.getConnection().getBucketEngine();
                  return engine.startFusionUploader(req.getVBucket(),
                                                    std::stoull(term));
              } catch (const std::exception& e) {
                  const auto& conn = c.getConnection();
                  LOG_WARNING_CTX("start_fusion_uploader_executor",
                                  {"conn_id", conn.getId()},
                                  {"description", conn.getDescription()},
                                  {"error", e.what()});
                  c.setErrorContext(e.what());
                  return cb::engine_errc::failed;
              }
          }).drive();
}

static void stop_fusion_uploader_executor(Cookie& cookie) {
    if (!cookie.getConnection().getBucket().supports(Feature::Fusion)) {
        cookie.sendResponse(cb::mcbp::Status::NotSupported);
        return;
    }

    cookie.obtainContext<SingleStateCommandContext>(cookie, [](Cookie& c) {
              try {
                  const auto& req = c.getRequest();
                  auto& engine = c.getConnection().getBucketEngine();
                  return engine.stopFusionUploader(req.getVBucket());
              } catch (const std::exception& e) {
                  const auto& conn = c.getConnection();
                  LOG_WARNING_CTX("stop_fusion_uploader_executor",
                                  {"conn_id", conn.getId()},
                                  {"description", conn.getDescription()},
                                  {"error", e.what()});
                  c.setErrorContext(e.what());
                  return cb::engine_errc::failed;
              }
          }).drive();
}

static void set_chronicle_auth_token_executor(Cookie& cookie) {
    if (!cookie.getConnection().getBucket().supports(Feature::Persistence)) {
        cookie.sendResponse(cb::mcbp::Status::NotSupported);
        return;
    }
    cookie.obtainContext<SetChronicleAuthTokenCommandContext>(cookie).drive();
}

static void delete_fusion_namespace_executor(Cookie& cookie) {
    if (isFusionSupportEnabled()) {
        cookie.obtainContext<DeleteFusionNamespaceCommandContext>(cookie)
                .drive();
    } else {
        cookie.sendResponse(cb::mcbp::Status::NotSupported);
    }
}

static void get_fusion_namespaces_executor(Cookie& cookie) {
    if (isFusionSupportEnabled()) {
        cookie.obtainContext<GetFusionNamespacesCommandContext>(cookie).drive();
    } else {
        cookie.sendResponse(cb::mcbp::Status::NotSupported);
    }
}

static void process_bin_noop_response(Cookie&) {
    // do nothing
}

static void process_bin_dcp_response(Cookie& cookie) {
    auto& c = cookie.getConnection();

    c.enableDatatype(cb::mcbp::Feature::JSON);

    auto* dcp = c.getBucket().getDcpIface();
    if (!dcp) {
        LOG_WARNING_CTX(
                "process_bin_dcp_response - DcpIface is nullptr - closing "
                "connection",
                {"conn_id", c.getId()},
                {"description", c.getDescription()});
        c.shutdown();
        c.setTerminationReason("Connected engine does not support DCP");
        return;
    }

    auto ret = dcp->response_handler(cookie, cookie.getHeader().getResponse());
    auto remapErr = c.remapErrorCode(ret);

    if (remapErr == cb::engine_errc::disconnect) {
        if (ret == cb::engine_errc::disconnect) {
            LOG_WARNING_CTX(
                    "process_bin_dcp_response - response_handler returned "
                    "cb::engine_errc::disconnect - closing connection",
                    {"conn_id", c.getId()},
                    {"description", c.getDescription()});
            c.setTerminationReason(
                    "process_bin_dcp_response forced disconnect");
        }
        c.shutdown();
    }
}

static void setup_response_handler(cb::mcbp::ClientOpcode opcode,
                                   HandlerFunction function) {
    response_handlers[std::underlying_type_t<cb::mcbp::ClientOpcode>(opcode)] =
            std::move(function);
}

static void setup_handler(cb::mcbp::ClientOpcode opcode,
                          HandlerFunction function) {
    handlers[std::underlying_type_t<cb::mcbp::ClientOpcode>(opcode)] =
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
    setup_handler(cb::mcbp::ClientOpcode::GetEx, getex_executor);
    setup_handler(cb::mcbp::ClientOpcode::GetExReplica, getex_executor);
    setup_handler(cb::mcbp::ClientOpcode::Get, get_executor);
    setup_handler(cb::mcbp::ClientOpcode::GetReplica, get_executor);
    setup_handler(cb::mcbp::ClientOpcode::GetRandomKey, get_executor);
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
    setup_handler(cb::mcbp::ClientOpcode::IoctlGet, ioctl_executor);
    setup_handler(cb::mcbp::ClientOpcode::IoctlSet, ioctl_executor);
    setup_handler(cb::mcbp::ClientOpcode::ConfigValidate,
                  config_validate_executor);
    setup_handler(cb::mcbp::ClientOpcode::ConfigReload, config_reload_executor);
    setup_handler(cb::mcbp::ClientOpcode::AuditPut, audit_put_executor);
    setup_handler(cb::mcbp::ClientOpcode::AuditConfigReload,
                  audit_config_reload_executor);
    setup_handler(cb::mcbp::ClientOpcode::Shutdown, shutdown_executor);
    setup_handler(cb::mcbp::ClientOpcode::SetBucketThrottleProperties,
                  set_bucket_throttle_properties_executor);
    setup_handler(cb::mcbp::ClientOpcode::SetBucketDataLimitExceeded,
                  set_bucket_data_limit_exceeded_executor);
    setup_handler(cb::mcbp::ClientOpcode::SetNodeThrottleProperties,
                  set_node_throttle_properties_executor);
    setup_handler(cb::mcbp::ClientOpcode::SetActiveEncryptionKeys,
                  set_active_encryption_key_executor);
    setup_handler(cb::mcbp::ClientOpcode::PruneEncryptionKeys,
                  prune_encryption_keys_executor);
    setup_handler(cb::mcbp::ClientOpcode::CreateBucket,
                  create_remove_bucket_executor);
    setup_handler(cb::mcbp::ClientOpcode::ListBuckets, list_bucket_executor);
    setup_handler(cb::mcbp::ClientOpcode::DeleteBucket,
                  create_remove_bucket_executor);
    setup_handler(cb::mcbp::ClientOpcode::SelectBucket, select_bucket_executor);
    setup_handler(cb::mcbp::ClientOpcode::PauseBucket, pause_bucket_executor);
    setup_handler(cb::mcbp::ClientOpcode::ResumeBucket, resume_bucket_executor);
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

    setup_handler(cb::mcbp::ClientOpcode::GetFusionStorageSnapshot,
                  get_fusion_storage_snapshot_executor);
    setup_handler(cb::mcbp::ClientOpcode::ReleaseFusionStorageSnapshot,
                  release_fusion_storage_snapshot_executor);
    setup_handler(cb::mcbp::ClientOpcode::MountFusionVbucket,
                  mount_fusion_vbucket_executor);
    setup_handler(cb::mcbp::ClientOpcode::UnmountFusionVbucket,
                  unmount_vbucket_executor);
    setup_handler(cb::mcbp::ClientOpcode::SyncFusionLogstore,
                  sync_fusion_logstore_executor);
    setup_handler(cb::mcbp::ClientOpcode::StartFusionUploader,
                  start_fusion_uploader_executor);
    setup_handler(cb::mcbp::ClientOpcode::StopFusionUploader,
                  stop_fusion_uploader_executor);
    setup_handler(cb::mcbp::ClientOpcode::SetChronicleAuthToken,
                  set_chronicle_auth_token_executor);
    setup_handler(cb::mcbp::ClientOpcode::DeleteFusionNamespace,
                  delete_fusion_namespace_executor);
    setup_handler(cb::mcbp::ClientOpcode::GetFusionNamespaces,
                  get_fusion_namespaces_executor);

    setup_handler(cb::mcbp::ClientOpcode::StartPersistence,
                  start_persistence_executor);
    setup_handler(cb::mcbp::ClientOpcode::StopPersistence,
                  stop_persistence_executor);
    setup_handler(cb::mcbp::ClientOpcode::SeqnoPersistence,
                  seqno_persistence_executor);
    setup_handler(cb::mcbp::ClientOpcode::EnableTraffic,
                  enable_traffic_control_mode_executor);
    setup_handler(cb::mcbp::ClientOpcode::DisableTraffic,
                  disable_traffic_control_mode_executor);
    setup_handler(cb::mcbp::ClientOpcode::EvictKey, evict_key_executor);
    setup_handler(cb::mcbp::ClientOpcode::Observe, observe_executor);
    setup_handler(cb::mcbp::ClientOpcode::GetFileFragment,
                  get_file_fragment_executor);
    setup_handler(cb::mcbp::ClientOpcode::PrepareSnapshot,
                  prepare_snapshot_executor);
    setup_handler(cb::mcbp::ClientOpcode::ReleaseSnapshot,
                  release_snapshot_executor);
    setup_handler(cb::mcbp::ClientOpcode::DownloadSnapshot,
                  download_snapshot_executor);
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
    handlers[std::underlying_type_t<cb::mcbp::ClientOpcode>(opcode)](cookie);
}

void execute_client_response_packet(Cookie& cookie,
                                    const cb::mcbp::Response& response) {
    auto& c = cookie.getConnection();
    if (!c.isAuthenticated()) {
        c.shutdown();
        c.setTerminationReason("Not authenticated");
        return;
    }

    const auto opcode = response.getClientOpcode();
    auto handler = response_handlers[uint8_t(opcode)];
    if (handler) {
        handler(cookie);
    } else {
        auto& c = cookie.getConnection();
        LOG_WARNING_CTX(
                "Unsupported response packet received",
                {"conn_id", c.getId()},
                {"opcode_number", fmt::format("{:#x}", uint32_t(opcode))},
                {"opcode",
                 is_valid_opcode(opcode) ? to_string(opcode)
                                         : "<invalid opcode>"});
        c.shutdown();
        c.setTerminationReason("Unsupported response packet received");
    }
}

void execute_server_response_packet(Cookie& cookie,
                                    const cb::mcbp::Response& response) {
    auto& c = cookie.getConnection();
    if (!c.isAuthenticated()) {
        c.shutdown();
        c.setTerminationReason("Not authenticated");
        return;
    }

    const auto opcode = response.getServerOpcode();
    switch (opcode) {
    case cb::mcbp::ServerOpcode::ClustermapChangeNotification:
    case cb::mcbp::ServerOpcode::ActiveExternalUsers:
        // ignore
        return;
    case cb::mcbp::ServerOpcode::Authenticate:
    case cb::mcbp::ServerOpcode::GetAuthorization:
        if (cookie.checkPrivilege(cb::rbac::Privilege::NodeSupervisor)
                    .success()) {
            externalAuthManager->responseReceived(response);
        }
    }
}
