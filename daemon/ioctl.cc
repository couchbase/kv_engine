/*
 *     Copyright 2014-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "ioctl.h"

#include "bucket_manager.h"
#include "buckets.h"
#include "connection.h"
#include "cookie.h"
#include "external_auth_manager_thread.h"
#include "front_end_thread.h"
#include "settings.h"
#include "top_keys_controller.h"
#include "tracing.h"
#include "utilities/string_utilities.h"
#include <logger/logger.h>
#include <mcbp/mcbp.h>
#include <memcached/io_control.h>
#include <memcached/util.h>
#include <nlohmann/json.hpp>
#include <platform/cb_arena_malloc.h>
#include <platform/split_string.h>
#include <serverless/config.h>
#include <algorithm>

/*
 * Implement ioctl-style memcached commands (ioctl_get / ioctl_set).
 */

/**
 * Function interface for ioctl_get callbacks
 */
using GetCallbackFunc =
        std::function<cb::engine_errc(Cookie& cookie,
                                      const StrToStrMap& arguments,
                                      std::string& value,
                                      cb::mcbp::Datatype& datatype)>;

/**
 * Function interface for ioctl_set callbacks
 */
using SetCallbackFunc =
        std::function<cb::engine_errc(Cookie& cookie,
                                      const StrToStrMap& arguments,
                                      const std::string& value)>;

/**
 * Callback for calling allocator specific memory release
 */
static cb::engine_errc setReleaseFreeMemory(Cookie& cookie,
                                            const StrToStrMap&,
                                            const std::string& value) {
    cb::ArenaMalloc::releaseMemory();
    auto& c = cookie.getConnection();
    LOG_INFO_CTX("IOCTL_SET: release_free_memory called",
                 {"conn_id", c.getId()});
    return cb::engine_errc::success;
}

static cb::engine_errc setJemallocProfActive(Cookie& cookie,
                                             const StrToStrMap&,
                                             const std::string& value) {
    bool enable;
    if (value == "true") {
        enable = true;
    } else if (value == "false") {
        enable = false;
    } else {
        return cb::engine_errc::invalid_arguments;
    }

    int res = cb::ArenaMalloc::setProperty(
            "prof.active", &enable, sizeof(enable));
    auto& c = cookie.getConnection();
    LOG_INFO_CTX("IOCTL_SET: setJemallocProfActive called",
                 {"conn_id", c.getId()},
                 {"description", c.getDescription()},
                 {"value", value},
                 {"result", (res == 0) ? "success" : "failure"});

    return (res == 0) ? cb::engine_errc::success
                      : cb::engine_errc::invalid_arguments;
}

static cb::engine_errc setJemallocProfDump(Cookie& cookie,
                                           const StrToStrMap&,
                                           const std::string&) {
    int res = cb::ArenaMalloc::setProperty("prof.dump", nullptr, 0);
    auto& c = cookie.getConnection();
    LOG_INFO_CTX("IOCTL_SET: setJemallocProfDump called",
                 {"conn_id", c.getId()},
                 {"description", c.getDescription()},
                 {"result", (res == 0) ? "success" : "failure"});

    return (res == 0) ? cb::engine_errc::success
                      : cb::engine_errc::invalid_arguments;
}

cb::engine_errc ioctlGetMcbpSla(Cookie& cookie,
                                const StrToStrMap& arguments,
                                std::string& value,
                                cb::mcbp::Datatype& datatype) {
    if (!arguments.empty() || !value.empty()) {
        return cb::engine_errc::invalid_arguments;
    }

    value = cb::mcbp::sla::to_json().dump();
    datatype = cb::mcbp::Datatype::JSON;
    return cb::engine_errc::success;
}

/**
 * The topkeys.stop ioctl command is used to stop an ongoing key sampling
 * collection and retrieve the collected statistics. It accepts the following
 * query parameters:
 *
 * Optional Parameters
 *
 * `uuid`
 *   - Type: UUID string (RFC 4122 format)
 *   - Default: Empty (uses the currently running session)
 *   - Description: The UUID of the tracing session to stop. This UUID was
 *                  returned by the corresponding topkeys.start command
 *   - Constraints:
 *      + Must be a valid UUID string if provided
 *      + Malformed UUID string results in invalid_arguments error
 *      + If UUID doesn't match the running session, returns KeyEexists error
 *                with the correct UUID
 *   - Example: uuid=550e8400-e29b-41d4-a716-446655440000
 *
 * `limit`
 *   - Type: Integer (positive)
 *   - Default: 100
 *   - Description: Maximum number of top keys to return per bucket/collection
 *                  combination. This limits the size of the returned JSON but
 *                  does not affect the data already collected
 *   - Constraints:
 *      + Invalid values (non-numeric) are silently ignored, and the default
 *                (100) is used instead
 *      + The limit only affects how many keys are returned from already-
 *                collected data
 *   - Example: limit=10
 *
 * Response
 *
 * On success, returns a JSON object containing the collected key access
 * statistics:
 *
 *     {
 *       "keys": {
 *         "bucket-name": {
 *           "cid:0x0": { "key1": 42, "key2": 15, "key3": 8 },
 *           "cid:0x9": { "collection-key1": 12, "collection-key2": 5 }
 *         }
 *       },
 *       "num_keys_omitted": 150,
 *       "shards": 4,
 *       "topkey": {
 *         "bucket": "bucket-name",
 *         "collection": "cid:0x0",
 *         "count": 42,
 *         "key": "key1"
 *       },
 *       "num_keys_collected": 250
 *     }
 *
 * Response Fields:
 *   - keys: Hierarchical object with bucket names as top-level keys,
 *           collection IDs as second-level keys, and key names as third-level
 *           keys with access counts as values
 *   - num_keys_omitted: Number of unique keys that exceeded the collection
 *                       limit during sampling (if non-zero, consider increasing
 *                       the limit parameter in topkeys.start)
 *   - shards: Number of shards used for collection (from the topkeys.start
 *              command)
 *   - topkey: The single most accessed key with its bucket, collection, and
 *             access count
 *   - num_keys_collected: Total number of unique keys sampled during the
 *                        session
 *
 * Error Conditions
 *   - KeyEnoent: No topkeys collection is currently running
 *   - KeyEexists: A collection is running but with a different UUID
 *                 (response includes the correct UUID in error JSON extras)
 *   - invalid_arguments: UUID parameter is malformed or cannot be parsed
 *   - failed: Internal error when serializing the response JSON (data is
 *             discarded)
 *
 * @param cookie The cookie representing the client connection and request
 *               context
 * @param args The query parameters provided with the ioctl command, parsed
 *             into a map
 * @param value The data to send back to the client in the response body
 *               (output parameter)
 * @param datatype The datatype of the response (output parameter), typically
 *                set to JSON if the result is a JSON string
 * @return Engine status code.
 */
static cb::engine_errc ioctlGetTopkeysStop(Cookie& cookie,
                                           const StrToStrMap& args,
                                           std::string& value,
                                           cb::mcbp::Datatype& datatype) {
    std::size_t limit = 100;
    if (args.contains("limit")) {
        try {
            limit = std::stoul(args.find("limit")->second);
        } catch (const std::exception&) {
            // Ignore the exception and return the default limit instead
            // (we've already performed the tracing so there isn't really
            // any good reason to just discard the data because the client
            // provided an invalid value for limit. The limit is only used
            // to return *less* data than we've already collected.
        }
    }

    cb::uuid::uuid_t uuid;
    if (args.contains("uuid")) {
        try {
            uuid = cb::uuid::from_string(args.find("uuid")->second);
        } catch (const std::exception& exception) {
            LOG_ERROR_CTX("Failed to parse uuid",
                          {"conn_id", cookie.getConnection().getId()},
                          {"error", exception.what()});
            return cb::engine_errc::invalid_arguments;
        }
    }

    auto [status, json] =
            cb::trace::topkeys::Controller::instance().stop(uuid, limit);
    if (status == cb::engine_errc::success) {
        try {
            value = json.dump();
        } catch (const std::exception& exception) {
            LOG_ERROR_CTX(
                    "Failed to get trace data. Trace data will be discarded",
                    {"conn_id", cookie.getConnection().getId()},
                    {"error", exception.what()});
            return cb::engine_errc::failed;
        }
        datatype = cb::mcbp::Datatype::JSON;
        return cb::engine_errc::success;
    }

    if (json.is_object()) {
        cookie.setErrorJsonExtras(json);
    }
    cookie.setErrorContext("Failed to start topkeys collection");
    return status;
}

cb::engine_errc ioctl_get_property(Cookie& cookie,
                                   const std::string& key,
                                   std::string& value,
                                   cb::mcbp::Datatype& datatype) {
    datatype = cb::mcbp::Datatype::Raw;
    std::pair<std::string, StrToStrMap> request;

    try {
        request = decode_query(key);
    } catch (const std::invalid_argument&) {
        return cb::engine_errc::invalid_arguments;
    }

    auto& manager = cb::ioctl::Manager::getInstance();
    auto* id = manager.lookup(request.first);
    if (id) {
        switch (id->id) {
        case cb::ioctl::Id::Sla:
            return ioctlGetMcbpSla(cookie, request.second, value, datatype);
        case cb::ioctl::Id::TraceConfig:
            return ioctlGetTracingConfig(
                    cookie, request.second, value, datatype);
        case cb::ioctl::Id::TraceStatus:
            return ioctlGetTracingStatus(
                    cookie, request.second, value, datatype);
        case cb::ioctl::Id::TraceDumpList:
            return ioctlGetTracingList(cookie, request.second, value, datatype);
        case cb::ioctl::Id::TraceDumpBegin:
            return ioctlGetTracingBeginDump(
                    cookie, request.second, value, datatype);
        case cb::ioctl::Id::TraceDumpGet:
            return ioctlGetTraceDump(cookie, request.second, value, datatype);

        case cb::ioctl::Id::ExternalAuthLogging:
            if (externalAuthManager) {
                value = externalAuthManager->isLoggingEnabled() ? "true"
                                                                : "false";
                datatype = cb::mcbp::Datatype::JSON;
                return cb::engine_errc::success;
            }
            return cb::engine_errc::not_supported;

        case cb::ioctl::Id::TopkeysStop:
            return ioctlGetTopkeysStop(cookie, request.second, value, datatype);

        case cb::ioctl::Id::JemallocProfActive: // may only be used with Set
        case cb::ioctl::Id::JemallocProfDump: // may only be used with Set
        case cb::ioctl::Id::ReleaseFreeMemory: // may only be used with Set
        case cb::ioctl::Id::ServerlessMaxConnectionsPerBucket: // set only
        case cb::ioctl::Id::ServerlessReadUnitSize: // set only
        case cb::ioctl::Id::ServerlessWriteUnitSize: // set only
        case cb::ioctl::Id::TraceDumpClear: // may only be used with Set
        case cb::ioctl::Id::TraceStart: // may only be used with Set
        case cb::ioctl::Id::TraceStop: // may only be used with Set
        case cb::ioctl::Id::TopkeysStart:
        case cb::ioctl::Id::enum_max:
            break;
        }
    }
    return cb::engine_errc::invalid_arguments;
}

static cb::engine_errc ioctlSetMcbpSla(Cookie& cookie,
                                       const StrToStrMap&,
                                       const std::string& value) {
    try {
        cb::mcbp::sla::reconfigure(nlohmann::json::parse(value));
        LOG_INFO_CTX("SLA configuration changed",
                     {"to", cb::mcbp::sla::to_json()});
    } catch (const std::exception& e) {
        cookie.getEventId();
        auto& c = cookie.getConnection();
        LOG_WARNING_CTX("Failed to set MCBP SLA",
                        {"conn_id", c.getId()},
                        {"event_id", cookie.getEventId()},
                        {"error", e.what()});
        return cb::engine_errc::invalid_arguments;
    }

    return cb::engine_errc::success;
}

static cb::engine_errc ioctlSetServerlessMaxConnectionsPerBucket(
        Cookie& cookie, const StrToStrMap&, const std::string& value) {
    if (cb::serverless::isEnabled()) {
        try {
            auto& config = cb::serverless::Config::instance();
            auto val = std::stoul(value);
            if (val < 100) {
                cookie.setErrorContext(
                        "Maximum number of connections cannot be below 100");
                return cb::engine_errc::invalid_arguments;
            }
            config.maxConnectionsPerBucket.store(val,
                                                 std::memory_order_release);
            LOG_INFO_CTX("Set maximum connections to a bucket", {"to", val});
        } catch (const std::exception&) {
            cookie.setErrorContext(
                    "Failed to convert the provided value to an integer");
            return cb::engine_errc::invalid_arguments;
        }

        return cb::engine_errc::success;
    }

    std::string reason{
            cb::ioctl::Manager::getInstance()
                    .lookup(cb::ioctl::Id::ServerlessMaxConnectionsPerBucket)
                    .key};
    reason.append(" may only be used on serverless deployments");
    cookie.setErrorContext(std::move(reason));
    return cb::engine_errc::invalid_arguments;
}

static cb::engine_errc ioctlSetServerlessUnitSize(Cookie& cookie,
                                                  cb::ioctl::Id id,
                                                  const std::string& value) {
    if (cb::serverless::isEnabled()) {
        try {
            auto& config = cb::serverless::Config::instance();
            auto val = std::stoul(value);
            if (id == cb::ioctl::Id::ServerlessReadUnitSize) {
                LOG_INFO_CTX(
                        "Change RCU size",
                        {"from",
                         config.readUnitSize.load(std::memory_order_acquire)},
                        {"to", val});
                config.readUnitSize.store(val, std::memory_order_release);
            } else if (id == cb::ioctl::Id::ServerlessWriteUnitSize) {
                LOG_INFO_CTX(
                        "Change WCU size",
                        {"from",
                         config.writeUnitSize.load(std::memory_order_acquire)},
                        {"to", val});
                config.writeUnitSize.store(val, std::memory_order_release);
            } else {
                LOG_WARNING_RAW(
                        "ioctlSetServerlessUnitSize: Internal error, "
                        "called for unknown id. request ignored");
                cookie.setErrorContext("Internal error");
                return cb::engine_errc::invalid_arguments;
            }
        } catch (const std::exception&) {
            cookie.setErrorContext(
                    "Failed to convert the provided value to an integer");
            return cb::engine_errc::invalid_arguments;
        }

        return cb::engine_errc::success;
    }

    std::string reason{cb::ioctl::Manager::getInstance().lookup(id).key};
    reason.append(" may only be used on serverless deployments");
    cookie.setErrorContext(std::move(reason));
    return cb::engine_errc::invalid_arguments;
}

/**
 * The topkeys.start ioctl command is used to start collecting key access
 * statistics. It accepts the following query parameters:
 *
 * Required Parameters
 *
 * None - all parameters are optional with sensible defaults.
 *
 * Optional Parameters
 *
 * `limit`
 *   - Type: Integer (positive)
 *   - Default: 10000
 *   - Description: Maximum number of unique keys to collect during the sampling
 *                  period
 *   - Constraints: Cannot be zero
 *   - Example: limit=10000
 *
 * `shards`
 *   - Type: Integer (positive)
 *   - Default: numWorkerThreads * 4 (typically 4x the number of worker threads)
 *   - Description: Number of parallel shards to use for collecting statistics.
 *                  More shards reduce contention but increase memory usage
 *   - Constraints: Cannot be zero
 *   - Example: shards=4
 *
 * `expected_duration`
 *   - Type: Integer (seconds, positive)
 *   - Default: 60
 *   - Description: Expected duration of the sampling in seconds. Internally
 *                  multiplied by 1.3 (adding 30% buffer) and capped at 60
 *                  seconds minimum. If the collection runs longer than this
 *                  duration, it will be stopped and the collected data dropped
 *   - Constraints: Cannot be zero
 *   -Example: expected_duration=60
 *
 * `bucket_filter`
 *   - Type: Comma-separated list of bucket names
 *   - Default: Empty (monitor all buckets)
 *   - Description: Filter to only monitor specific buckets. Bucket names must
 *                  exist on the server
 *   - Constraints: Unknown bucket names result in no_such_key error
 *   - Example: bucket_filter=default,mybucket
 *
 * `collection_filter`
 *   - Type: Comma-separated list of collection IDs (hex or decimal)
 *   - Default: Empty (monitor all collections in the filtered bucket)
 *   - Description: Filter to only monitor specific collections within a single
 *                  bucket
 *   - Constraints:
 *      + Requires bucket_filter to be set with exactly one bucket
 *      + Results in invalid_arguments error if bucket filter has 0 or more
 *                than 1 bucket
 *      + Collection IDs must be valid unsigned integers
 *   - Example: collection_filter=9,10 (filters on collections 0x9 and 0x10)
 *
 * Response
 *
 * On success, returns a JSON object containing the UUID of the tracing session:
 *
 *     { "uuid": "550e8400-e29b-41d4-a716-446655440000" }
 *
 * On failure, returns an appropriate error status with optional JSON extras
 * containing the UUID of an existing session.
 *
 * @param cookie The cookie representing the client connection and request
 *               context
 * @param args The query parameters provided with the ioctl command, parsed into
 *             a map
 * @param result The data to send back to the client in the response body
 *               (output parameter)
 * @param datatype The datatype of the response (output parameter), typically
 *                set to JSON if the result is a JSON string
 * @return Engine status code.
 */
static cb::engine_errc ioctlSetTopkeysStart(Cookie& cookie,
                                            const StrToStrMap& args,
                                            const std::string&,
                                            std::string& result,
                                            cb::mcbp::Datatype& datatype) {
    std::size_t limit = 10000;
    if (args.contains("limit")) {
        try {
            limit = std::stoul(args.find("limit")->second);
            if (limit == 0) {
                cookie.setErrorContext("limit cannot be zero");
                return cb::engine_errc::invalid_arguments;
            }
        } catch (const std::exception&) {
            cookie.setErrorContext("Failed to parse limit argument");
            return cb::engine_errc::invalid_arguments;
        }
    }

    std::size_t shards = Settings::instance().getNumWorkerThreads() * 4;
    if (args.contains("shards")) {
        try {
            shards = std::stoul(args.find("shards")->second);
            if (shards == 0) {
                cookie.setErrorContext("Shards cannot be zero");
                return cb::engine_errc::invalid_arguments;
            }
        } catch (const std::exception&) {
            cookie.setErrorContext("Failed to parse shards argument");
            return cb::engine_errc::invalid_arguments;
        }
    }

    std::size_t expected_duration = 60;
    if (args.contains("expected_duration")) {
        try {
            std::size_t val =
                    std::stoul(args.find("expected_duration")->second) * 1.3;
            expected_duration = std::min(val, expected_duration);
            if (val == 0) {
                cookie.setErrorContext("expected_duration cannot be zero");
                return cb::engine_errc::invalid_arguments;
            }
        } catch (const std::exception&) {
            cookie.setErrorContext(
                    "Failed to parse expected_duration argument");
            return cb::engine_errc::invalid_arguments;
        }
    }

    std::vector<std::size_t> bucket_filter;
    if (args.contains("bucket_filter")) {
        std::unordered_map<std::string, std::size_t> bucketnames;

        for (std::size_t idx = 0; idx < cb::limits::TotalBuckets; ++idx) {
            auto name = BucketManager::instance().getName(idx);
            if (!name.empty()) {
                bucketnames.insert({std::move(name), idx});
            }
        }

        auto filter = args.find("bucket_filter")->second;
        try {
            auto parts = cb::string::split(filter, ',');
            for (const auto& part : parts) {
                std::string bucket(part);
                if (bucketnames.contains(bucket)) {
                    bucket_filter.push_back(bucketnames[bucket]);
                } else {
                    cookie.setErrorContext(
                            fmt::format("Unknown bucket {}", bucket));
                    return cb::engine_errc::no_such_key;
                }
            }
        } catch (const std::exception& exception) {
            LOG_WARNING_CTX("Failed to parse bucket filter",
                            {"error", exception.what()});
            return cb::engine_errc::failed;
        }
        std::ranges::sort(bucket_filter);
    }

    std::vector<CollectionIDType> collection_filter;
    if (args.contains("collection_filter")) {
        if (bucket_filter.size() != 1) {
            cookie.setErrorContext(
                    "The bucket filter must include 1 bucket in order to "
                    "filter on collections within the bucket");
            return cb::engine_errc::invalid_arguments;
        }
        auto filter = args.find("collection_filter")->second;
        try {
            auto parts = cb::string::split(filter, ',');
            collection_filter.reserve(parts.size());
            for (const auto& part : parts) {
                CollectionIDType collection;
                if (safe_strtoul(part, collection)) {
                    collection_filter.emplace_back(collection);
                } else {
                    cookie.setErrorContext(fmt::format(
                            "Failed to parse collection id: {}", part));
                    return cb::engine_errc::invalid_arguments;
                }
            }
        } catch (const std::exception& exception) {
            LOG_WARNING_CTX("Failed to parse collection filter",
                            {"error", exception.what()});
            return cb::engine_errc::failed;
        }
        std::ranges::sort(collection_filter);
    }

    const auto [status, uuid] =
            cb::trace::topkeys::Controller::instance().create(
                    limit,
                    shards,
                    std::chrono::seconds(expected_duration),
                    bucket_filter,
                    collection_filter);
    nlohmann::json json = {{"uuid", to_string(uuid)}};
    if (status == cb::engine_errc::success) {
        result = json.dump();
        datatype = cb::mcbp::Datatype::JSON;
    } else {
        cookie.setErrorJsonExtras(json);
        cookie.setErrorContext("Failed to start topkeys collection");
    }
    return status;
}

cb::engine_errc ioctl_set_property(Cookie& cookie,
                                   const std::string& key,
                                   const std::string& value,
                                   std::string& result,
                                   cb::mcbp::Datatype& datatype) {
    std::pair<std::string, StrToStrMap> request;

    try {
        request = decode_query(key);
    } catch (const std::invalid_argument&) {
        return cb::engine_errc::invalid_arguments;
    }

    result.clear();
    datatype = cb::mcbp::Datatype::Raw;

    auto& manager = cb::ioctl::Manager::getInstance();
    auto* id = manager.lookup(request.first);
    if (id) {
        switch (id->id) {
        case cb::ioctl::Id::JemallocProfActive:
            return setJemallocProfActive(cookie, request.second, value);
        case cb::ioctl::Id::JemallocProfDump:
            return setJemallocProfDump(cookie, request.second, value);
        case cb::ioctl::Id::ReleaseFreeMemory:
            return setReleaseFreeMemory(cookie, request.second, value);
        case cb::ioctl::Id::Sla:
            return ioctlSetMcbpSla(cookie, request.second, value);
        case cb::ioctl::Id::ServerlessMaxConnectionsPerBucket:
            return ioctlSetServerlessMaxConnectionsPerBucket(
                    cookie, request.second, value);
        case cb::ioctl::Id::ServerlessReadUnitSize:
        case cb::ioctl::Id::ServerlessWriteUnitSize:
            return ioctlSetServerlessUnitSize(cookie, id->id, value);
        case cb::ioctl::Id::TraceConfig:
            return ioctlSetTracingConfig(cookie, request.second, value);
        case cb::ioctl::Id::TraceStart:
            return ioctlSetTracingStart(cookie, request.second, value);
        case cb::ioctl::Id::TraceStop:
            return ioctlSetTracingStop(cookie, request.second, value);
        case cb::ioctl::Id::TraceDumpClear:
            return ioctlSetTracingClearDump(cookie, request.second, value);

        case cb::ioctl::Id::ExternalAuthLogging:
            if (externalAuthManager) {
                if (value == "true") {
                    externalAuthManager->setLoggingEnabled(true);
                } else if (value == "false") {
                    externalAuthManager->setLoggingEnabled(false);
                } else {
                    return cb::engine_errc::invalid_arguments;
                }
                return cb::engine_errc::success;
            }
            return cb::engine_errc::not_supported;
        case cb::ioctl::Id::TopkeysStart:
            return ioctlSetTopkeysStart(
                    cookie, request.second, value, result, datatype);

        case cb::ioctl::Id::TraceDumpBegin: // may only be used with Get
        case cb::ioctl::Id::TraceDumpGet: // may only be used with Get
        case cb::ioctl::Id::TraceDumpList: // may only be used with Get
        case cb::ioctl::Id::TraceStatus: // may only be used with Get
        case cb::ioctl::Id::TopkeysStop:

        case cb::ioctl::Id::enum_max:
            break;
        }
    }

    return cb::engine_errc::invalid_arguments;
}
