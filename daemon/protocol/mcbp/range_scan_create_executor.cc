/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "engine_wrapper.h"
#include "executors.h"

#include <daemon/cookie.h>
#include <mcbp/protocol/request.h>
#include <memcached/range_scan.h>
#include <memcached/range_scan_id.h>
#include <memcached/range_scan_optional_configuration.h>
#include <nlohmann/json.hpp>
#include <platform/base64.h>
#include <spdlog/fmt/fmt.h>
#include <utilities/json_utilities.h>

static cb::rangescan::KeyOnly getKeyOnly(const nlohmann::json& jsonObject) {
    auto rv = cb::rangescan::KeyOnly::No;
    auto keyOnly = cb::getOptionalJsonObject(
            jsonObject, "key_only", nlohmann::json::value_t::boolean);
    if (keyOnly) {
        rv = keyOnly.value().get<bool>() ? cb::rangescan::KeyOnly::Yes
                                         : cb::rangescan::KeyOnly::No;
    }
    return rv;
}

static CollectionID getCollectionID(const nlohmann::json& jsonObject) {
    auto collection = cb::getOptionalJsonObject(
            jsonObject, "collection", nlohmann::json::value_t::string);

    if (!collection) {
        return CollectionID::Default;
    }
    return CollectionID(collection.value().get<std::string>());
}

static cb::rangescan::SamplingConfiguration getSamplingConfig(
        const nlohmann::json& samplingConfig) {
    auto samples = cb::getJsonObject(samplingConfig,
                                     "samples",
                                     nlohmann::json::value_t::number_unsigned,
                                     "getSamplingConfig");
    auto seed = cb::getJsonObject(samplingConfig,
                                  "seed",
                                  nlohmann::json::value_t::number_unsigned,
                                  "getSamplingConfig");
    return cb::rangescan::SamplingConfiguration{samples.get<size_t>(),
                                                seed.get<uint32_t>()};
}

static cb::rangescan::SnapshotRequirements getSnapshotRequirements(
        const nlohmann::json& snapshotRequirements) {
    auto vbUuid = cb::getJsonObject(snapshotRequirements,
                                    "vb_uuid",
                                    nlohmann::json::value_t::number_unsigned,
                                    "getSnapshotRequirements");
    auto seqno = cb::getJsonObject(snapshotRequirements,
                                   "seqno",
                                   nlohmann::json::value_t::number_unsigned,
                                   "getSnapshotRequirements");
    auto seqnoExists =
            cb::getOptionalJsonObject(snapshotRequirements,
                                      "seqno_exists",
                                      nlohmann::json::value_t::boolean);
    auto timeoutMs =
            cb::getOptionalJsonObject(snapshotRequirements,
                                      "timeout_ms",
                                      nlohmann::json::value_t::number_unsigned);

    cb::rangescan::SnapshotRequirements rv;
    rv.vbUuid = vbUuid.get<uint64_t>();
    rv.seqno = seqno.get<uint64_t>();
    if (seqnoExists) {
        rv.seqnoMustBeInSnapshot = seqnoExists.value().get<bool>();
    }
    if (timeoutMs) {
        rv.timeout =
                std::chrono::milliseconds(timeoutMs.value().get<uint32_t>());
    }
    return rv;
}

/**
 * Return a std::string and the KeyType from the "range" object, this can be
 * re-used to find start/e_start end/e_end.
 *
 * @param range The range JSON object
 * @param key first key to lookup (start/end)
 * @param eKey second key to lookup (e_start/e_end)
 */
static std::pair<std::string, cb::rangescan::KeyType> getRange(
        const nlohmann::json& range,
        const std::string& key,
        const std::string& eKey) {
    auto value1 = cb::getOptionalJsonObject(
            range, key, nlohmann::json::value_t::string);
    auto value2 = cb::getOptionalJsonObject(
            range, eKey, nlohmann::json::value_t::string);

    // both defined is failure, none defined is failure
    if (value1 && value2) {
        throw std::invalid_argument(
                fmt::format("range included both {} and {}", key, eKey));
    }
    if (!value1 && !value2) {
        throw std::invalid_argument(
                fmt::format("range did not include {} or {}", key, eKey));
    }

    if (value1) {
        return {value1.value().get<std::string>(),
                cb::rangescan::KeyType::Inclusive};
    }
    return {value2.value().get<std::string>(),
            cb::rangescan::KeyType::Exclusive};
}

static std::pair<cb::engine_errc, cb::rangescan::Id> createRangeScan(
        Cookie& cookie) {
    const auto& req = cookie.getRequest();

    // let it throw
    nlohmann::json parsed = nlohmann::json::parse(req.getValueString());

    auto range = cb::getOptionalJsonObject(
            parsed, "range", nlohmann::json::value_t::object);
    auto samplingConfigJSON = cb::getOptionalJsonObject(
            parsed, "sampling", nlohmann::json::value_t::object);
    auto snapshotReqsJSON = cb::getOptionalJsonObject(
            parsed, "snapshot_requirements", nlohmann::json::value_t::object);

    if (range && samplingConfigJSON) {
        return {cb::engine_errc::invalid_arguments, {}};
    }

    // Define the complete range, which may get overridden
    std::string start{"\0", 1};
    std::string end{"\xFF"};
    cb::rangescan::KeyType startType = cb::rangescan::KeyType::Inclusive;
    cb::rangescan::KeyType endType = cb::rangescan::KeyType::Inclusive;
    if (range) {
        try {
            std::tie(start, startType) =
                    getRange(range.value(), "start", "excl_start");
            std::tie(end, endType) = getRange(range.value(), "end", "excl_end");
        } catch (const std::exception& e) {
            cookie.setErrorContext(e.what());
            return {cb::engine_errc::invalid_arguments, {}};
        }

        // And now get the 'raw' key encoding from the base64 encoding
        start = cb::base64::decode(start);
        end = cb::base64::decode(end);
    }

    std::optional<cb::rangescan::SnapshotRequirements> snapshotReqs;
    if (snapshotReqsJSON) {
        snapshotReqs = getSnapshotRequirements(snapshotReqsJSON.value());
    }
    std::optional<cb::rangescan::SamplingConfiguration> samplingConfig;
    if (samplingConfigJSON) {
        samplingConfig = getSamplingConfig(samplingConfigJSON.value());
    }
    return createRangeScan(cookie,
                           req.getVBucket(),
                           getCollectionID(parsed),
                           cb::rangescan::KeyView{start, startType},
                           cb::rangescan::KeyView{end, endType},
                           getKeyOnly(parsed),
                           snapshotReqs,
                           samplingConfig);
}

void range_scan_create_executor(Cookie& cookie) {
    std::pair<cb::engine_errc, cb::rangescan::Id> status;
    status.first = cookie.swapAiostat(cb::engine_errc::success);

    if (status.first == cb::engine_errc::success) {
        status = createRangeScan(cookie);
    }

    if (status.first != cb::engine_errc::success) {
        handle_executor_status(cookie, status.first);
    } else {
        // Success - we have an id to return
        cookie.getConnection().sendResponse(
                cookie,
                cb::mcbp::Status::Success,
                {},
                {},
                {reinterpret_cast<const char*>(status.second.data),
                 status.second.size()},
                PROTOCOL_BINARY_RAW_BYTES,
                nullptr);
    }
}
