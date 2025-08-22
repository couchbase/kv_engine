/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "bucket_config_command_context.h"

#include "daemon/concurrency_semaphores.h"
#include "daemon/enginemap.h"
#include "logger/logger.h"
#include "mcbp/engine_wrapper.h"
#include "memcached/bucket_type.h"
#include "memcached/configuration_iface.h"
#include <memcached/config_parser.h>

BucketConfigCommandContext::BucketConfigCommandContext(Cookie& cookie)
    : BackgroundThreadCommandContext(
              cookie,
              TaskId::Core_BucketConfigValidationTask,
              "Bucket config validation",
              ConcurrencySemaphores::instance().bucket_config_validation) {
}

cb::engine_errc BucketConfigCommandContext::execute() {
    const auto& request = cookie.getRequest();
    std::string value(request.getValueString());
    std::string config;

    // Check if (optional) config was included after the value.
    auto marker = value.find('\0');
    if (marker != std::string::npos) {
        config = value.substr(marker + 1);
        value.resize(marker);
    }

    ParameterMap parameters;
    cb::config::tokenize(
            config, [&](auto key, auto val) { parameters.emplace(key, val); });

    ParameterValidationMap validation;

    switch (cookie.getRequest().getClientOpcode()) {
    case cb::mcbp::ClientOpcode::ValidateBucketConfig:
        try {
            std::unique_ptr<ConfigurationIface> configuration;
            auto type = module_to_bucket_type(value);
            configuration = create_bucket_configuration(type);
            Expects(configuration);
            validation = configuration->validateParameters(parameters);
        } catch (const std::exception& e) {
            response = "Failed to validate bucket configuration: " +
                       std::string(e.what());
            return cb::engine_errc::invalid_arguments;
        }
        break;
    default:
        return cb::engine_errc::not_supported;
    }

    datatype = cb::mcbp::Datatype::JSON;
    response = nlohmann::json(validation).dump();

    return cb::engine_errc::success;
}
