/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "executors.h"

#include <daemon/cookie.h>
#include <daemon/settings.h>
#include <logger/logger.h>
#include <memcached/protocol_binary.h>
#include <nlohmann/json.hpp>

#include <daemon/buckets.h>
#include <daemon/opentelemetry.h>
#include <mcbp/protocol/status.h>
#include <set>

// We can't use a set of enums that easily in an unordered_set.. just use an
// ordered for now..
using FeatureSet = std::set<cb::mcbp::Feature>;

/**
 * Try to see if the provided vector of features contais a certain feature
 *
 * @param features The vector to search
 * @param feature The feature to check for
 * @return true if it contains the feature, false otherwise
 */
bool containsFeature(const FeatureSet& features, cb::mcbp::Feature feature) {
    return features.find(feature) != features.end();
}

/**
 * Convert the input array of requested features into the Feature set which
 * don't include any illegal / unsupported features or any duplicates.
 *
 * In addition to that we'll also make sure that all dependent features is
 * enabled (and that we don't request features which are mutually exclusive)
 *
 * @param requested The set to populate with the requested features
 * @param input The input array
 */
void buildRequestVector(FeatureSet& requested, cb::sized_buffer<const uint16_t> input) {
    for (const auto& value : input) {
        const uint16_t in = ntohs(value);
        const auto feature = cb::mcbp::Feature(in);

        switch (feature) {
        case cb::mcbp::Feature::Invalid:
        case cb::mcbp::Feature::Invalid2:
        case cb::mcbp::Feature::TLS:
            // known, but we don't support them
            break;
        case cb::mcbp::Feature::TCPNODELAY:
        case cb::mcbp::Feature::TCPDELAY:
        case cb::mcbp::Feature::MUTATION_SEQNO:
        case cb::mcbp::Feature::XATTR:
        case cb::mcbp::Feature::JSON:
        case cb::mcbp::Feature::SNAPPY:
        case cb::mcbp::Feature::XERROR:
        case cb::mcbp::Feature::SELECT_BUCKET:
        case cb::mcbp::Feature::Collections:
        case cb::mcbp::Feature::OpenTracing:
        case cb::mcbp::Feature::PreserveTtl:
        case cb::mcbp::Feature::Duplex:
        case cb::mcbp::Feature::ClustermapChangeNotification:
        case cb::mcbp::Feature::UnorderedExecution:
        case cb::mcbp::Feature::Tracing:
        case cb::mcbp::Feature::AltRequestSupport:
        case cb::mcbp::Feature::SyncReplication:
        case cb::mcbp::Feature::VAttr:
        case cb::mcbp::Feature::PiTR:
        case cb::mcbp::Feature::SubdocCreateAsDeleted:
        case cb::mcbp::Feature::SubdocDocumentMacroSupport:
        case cb::mcbp::Feature::SubdocReplaceBodyWithXattr:
        case cb::mcbp::Feature::ReportUnitUsage:
        case cb::mcbp::Feature::NonBlockingThrottlingMode:
        case cb::mcbp::Feature::SubdocReplicaRead:

            // This isn't very optimal, but we've only got a handfull of elements ;)
            if (!containsFeature(requested, feature)) {
                requested.insert(feature);
            }

            break;
        }
    }

    // Run through the requested array and make sure we don't have
    // illegal combinations
    for (const auto& feature : requested) {
        switch (cb::mcbp::Feature(feature)) {
        case cb::mcbp::Feature::Invalid:
        case cb::mcbp::Feature::TLS:
        case cb::mcbp::Feature::MUTATION_SEQNO:
        case cb::mcbp::Feature::XATTR:
        case cb::mcbp::Feature::XERROR:
        case cb::mcbp::Feature::SELECT_BUCKET:
        case cb::mcbp::Feature::Invalid2:
        case cb::mcbp::Feature::SNAPPY:
        case cb::mcbp::Feature::JSON:
        case cb::mcbp::Feature::Tracing:
        case cb::mcbp::Feature::AltRequestSupport:
        case cb::mcbp::Feature::SyncReplication:
        case cb::mcbp::Feature::Duplex:
        case cb::mcbp::Feature::UnorderedExecution:
        case cb::mcbp::Feature::Collections:
        case cb::mcbp::Feature::OpenTracing:
        case cb::mcbp::Feature::PreserveTtl:
        case cb::mcbp::Feature::PiTR:
        case cb::mcbp::Feature::SubdocCreateAsDeleted:
        case cb::mcbp::Feature::SubdocReplaceBodyWithXattr:
        case cb::mcbp::Feature::ReportUnitUsage:
        case cb::mcbp::Feature::NonBlockingThrottlingMode:
        case cb::mcbp::Feature::SubdocReplicaRead:
            // No other dependency
            break;

        case cb::mcbp::Feature::TCPNODELAY:
            // cannot co-exist with TCPDELAY
            if (containsFeature(requested, cb::mcbp::Feature::TCPDELAY)) {
                throw std::invalid_argument("TCPNODELAY cannot co-exist with TCPDELAY");
            }
            break;
        case cb::mcbp::Feature::TCPDELAY:
            // cannot co-exist with TCPNODELAY
            if (containsFeature(requested, cb::mcbp::Feature::TCPNODELAY)) {
                throw std::invalid_argument("TCPDELAY cannot co-exist with TCPNODELAY");
            }
            break;
        case cb::mcbp::Feature::ClustermapChangeNotification:
            // Needs duplex
            if (!containsFeature(requested, cb::mcbp::Feature::Duplex)) {
                throw std::invalid_argument(to_string(feature) +
                                            " needs Duplex");
            }
            break;
        case cb::mcbp::Feature::SubdocDocumentMacroSupport:
        case cb::mcbp::Feature::VAttr:
            // Needs XATTR
            if (!containsFeature(requested, cb::mcbp::Feature::XATTR)) {
                throw std::invalid_argument(to_string(feature) +
                                            " needs XATTR");
            }
            break;
        }
    }
}

void process_hello_packet_executor(Cookie& cookie) {
    auto& connection = cookie.getConnection();
    auto& req = cookie.getRequest();
    std::string log_buffer;
    log_buffer.reserve(512);
    log_buffer.append("HELO ");

    auto keybuf = req.getKey();
    std::string_view key{reinterpret_cast<const char*>(keybuf.data()),
                         keybuf.size()};
    auto valuebuf = req.getValue();
    const cb::sized_buffer<const uint16_t> input{
            reinterpret_cast<const uint16_t*>(valuebuf.data()),
            valuebuf.size() / 2};

    std::vector<uint16_t> out;

    // We can't switch bucket if we've got multiple commands in flight
    if (connection.getNumberOfCookies() > 1) {
        LOG_INFO(
                "{}: {} Changing options via HELO is not possible with "
                "multiple "
                "commands in flight",
                connection.getId(),
                connection.getDescription());
        cookie.sendResponse(cb::mcbp::Status::NotSupported);
        return;
    }

    FeatureSet requested;
    try {
        buildRequestVector(requested, input);
    } catch (const std::invalid_argument& e) {
        LOG_INFO("{}: {} Invalid combination of options: {}",
                 connection.getId(),
                 connection.getDescription(),
                 e.what());
        cookie.setErrorContext(e.what());
        cookie.sendResponse(cb::mcbp::Status::Einval);
        return;
    }

    /*
     * Disable all features the hello packet may enable, so that
     * the client can toggle features on/off during a connection
     */
    connection.disableAllDatatypes();
    connection.setSupportsMutationExtras(false);
    connection.setXerrorSupport(false);
    connection.setCollectionsSupported(false);
    connection.setDuplexSupported(false);
    connection.setClustermapChangeNotificationSupported(false);
    connection.setTracingEnabled(false);
    connection.setAllowUnorderedExecution(false);
    connection.setReportUnitUsage(false);
    connection.setNonBlockingThrottlingMode(false);

    if (!key.empty()) {
        if (key.front() == '{') {
            // This may be JSON
            nlohmann::json json;
            try {
                json = nlohmann::json::parse(key);
                auto obj = json.find("i");
                if (obj != json.end() && (*obj).is_string()) {
                    try {
                        connection.setConnectionId(obj->get<std::string>());
                    } catch (const std::exception& exception) {
                        LOG_INFO("{}: Failed to parse connection uuid: {}",
                                 connection.getId(),
                                 exception.what());
                    }
                }
                obj = json.find("a");
                if (obj != json.end() && obj->is_string()) {
                    connection.setAgentName(obj->get<std::string>());
                }
            } catch (const nlohmann::json::exception&) {
                connection.setAgentName(key);
            }
        } else {
            connection.setAgentName(key);
        }

        log_buffer.append("[");
        log_buffer.append(key.data(), key.size());
        log_buffer.append("] ");
    }

    for (const auto& feature : requested) {
        bool added = false;

        switch (feature) {
        case cb::mcbp::Feature::Invalid:
        case cb::mcbp::Feature::Invalid2:
        case cb::mcbp::Feature::TLS:
            // Not implemented
            LOG_INFO("{}: {} requested unsupported feature {}",
                     connection.getId(),
                     connection.getDescription(),
                     to_string(feature));
            break;
        case cb::mcbp::Feature::TCPNODELAY:
        case cb::mcbp::Feature::TCPDELAY:
            connection.setTcpNoDelay(feature == cb::mcbp::Feature::TCPNODELAY);
            added = true;
            break;

        case cb::mcbp::Feature::MUTATION_SEQNO:
            connection.setSupportsMutationExtras(true);
            added = true;
            break;
        case cb::mcbp::Feature::XATTR:
            if (Settings::instance().isXattrEnabled() ||
                connection.isInternal()) {
                connection.enableDatatype(cb::mcbp::Feature::XATTR);
                added = true;
            }
            break;
        case cb::mcbp::Feature::JSON:
            if (Settings::instance().isDatatypeJsonEnabled()) {
                connection.enableDatatype(cb::mcbp::Feature::JSON);
                added = true;
            }
            break;
        case cb::mcbp::Feature::SNAPPY:
            if (Settings::instance().isDatatypeSnappyEnabled()) {
                connection.enableDatatype(cb::mcbp::Feature::SNAPPY);
                added = true;
            }
            break;
        case cb::mcbp::Feature::XERROR:
            connection.setXerrorSupport(true);
            added = true;
            break;
        case cb::mcbp::Feature::SELECT_BUCKET:
            // The select bucket is only informative ;-)
            added = true;
            break;
        case cb::mcbp::Feature::AltRequestSupport:
            // The AltRequestSupport is only informative
            added = true;
            break;
        case cb::mcbp::Feature::SyncReplication:
            // The SyncReplication is only informative
            added = true;
            break;
        case cb::mcbp::Feature::OpenTracing:
            if (OpenTelemetry::isEnabled()) {
                added = true;
            }
            break;
        case cb::mcbp::Feature::Collections: {
            auto& bucket = connection.getBucket();
            // Abort if the engine cannot support collections
            if (bucket.supports(cb::engine::Feature::Collections)) {
                connection.setCollectionsSupported(true);
                added = true;
            }
        } break;
        case cb::mcbp::Feature::Duplex:
            connection.setDuplexSupported(true);
            added = true;
            break;
        case cb::mcbp::Feature::ClustermapChangeNotification:
            connection.setClustermapChangeNotificationSupported(true);
            added = true;
            break;
        case cb::mcbp::Feature::UnorderedExecution:
            if (connection.isDCP()) {
                LOG_INFO(
                        "{}: {} Unordered execution is not supported for "
                        "DCP connections",
                        connection.getId(),
                        connection.getDescription());
            } else {
                connection.setAllowUnorderedExecution(true);
                added = true;
            }
            break;

        case cb::mcbp::Feature::Tracing:
            if (Settings::instance().isTracingEnabled()) {
                connection.setTracingEnabled(true);
                added = true;
                break;
            } else {
                LOG_INFO("{}: {} Request for [disabled] Tracing feature",
                         connection.getId(),
                         connection.getDescription());
            }
            break;

        case cb::mcbp::Feature::ReportUnitUsage:
            connection.setReportUnitUsage(true);
            added = true;
            break;
        case cb::mcbp::Feature::NonBlockingThrottlingMode:
            connection.setNonBlockingThrottlingMode(true);
            added = true;
            break;
        case cb::mcbp::Feature::PreserveTtl:
        case cb::mcbp::Feature::VAttr:
        case cb::mcbp::Feature::SubdocDocumentMacroSupport:
        case cb::mcbp::Feature::PiTR:
        case cb::mcbp::Feature::SubdocCreateAsDeleted:
        case cb::mcbp::Feature::SubdocReplaceBodyWithXattr:
        case cb::mcbp::Feature::SubdocReplicaRead:
            // Informative features don't need special handling
            added = true;
            break;
        } // end switch

        if (added) {
            out.push_back(htons(uint16_t(feature)));
            log_buffer.append(to_string(feature));
            log_buffer.append(", ");
        }
    }

    cookie.sendResponse(
            cb::mcbp::Status::Success,
            {},
            {},
            {reinterpret_cast<const char*>(out.data()), 2 * out.size()},
            cb::mcbp::Datatype::Raw,
            0);

    // Trim off the trailing whitespace (and potentially comma)
    log_buffer.resize(log_buffer.size() - 1);
    if (log_buffer.back() == ',') {
        log_buffer.resize(log_buffer.size() - 1);
    }

    LOG_INFO("{}: {} {}",
             connection.getId(),
             log_buffer,
             connection.getDescription());
}
