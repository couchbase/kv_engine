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
#include "platform/json_log.h"

#include <daemon/cookie.h>
#include <daemon/settings.h>
#include <logger/logger.h>
#include <memcached/protocol_binary.h>
#include <nlohmann/json.hpp>

#include <daemon/buckets.h>
#include <mcbp/protocol/status.h>
#include <serverless/config.h>

#include <set>

using cb::mcbp::Feature;
// We can't use a set of enums that easily in an unordered_set.. just use an
// ordered for now..
using FeatureSet = std::set<Feature>;

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
void buildRequestVector(FeatureSet& requested,
                        cb::sized_buffer<const uint16_t> input) {
    for (const auto& value : input) {
        const uint16_t in = ntohs(value);
        const auto feature = static_cast<Feature>(in);

        switch (feature) {
        case Feature::Invalid:
        case Feature::Invalid2:
        case Feature::Invalid3:
        case Feature::TLS:
            // known, but we don't support them
            break;
        case Feature::TcpNoDelay:
        case Feature::TCPDELAY_Unsupported:
        case Feature::MUTATION_SEQNO:
        case Feature::XATTR:
        case Feature::JSON:
        case Feature::SNAPPY:
        case Feature::XERROR:
        case Feature::SELECT_BUCKET:
        case Feature::Collections:
        case Feature::SnappyEverywhere:
        case Feature::PreserveTtl:
        case Feature::Duplex:
        case Feature::ClustermapChangeNotification:
        case Feature::UnorderedExecution:
        case Feature::Tracing:
        case Feature::AltRequestSupport:
        case Feature::SyncReplication:
        case Feature::VAttr:
        case Feature::SubdocCreateAsDeleted:
        case Feature::SubdocDocumentMacroSupport:
        case Feature::SubdocReplaceBodyWithXattr:
        case Feature::ReportUnitUsage:
        case Feature::NonBlockingThrottlingMode:
        case Feature::SubdocReplicaRead:
        case Feature::GetClusterConfigWithKnownVersion:
        case Feature::DedupeNotMyVbucketClustermap:
        case Feature::ClustermapChangeNotificationBrief:
        case Feature::SubdocAllowsAccessOnMultipleXattrKeys:
        case Feature::SubdocBinaryXattr:
        case Feature::RangeScanIncludeXattr:
        case Feature::SubdocAllowReplicaReadOnDeletedDocs:
        case Feature::GetRandomKeyIncludeXattr:

            // This isn't very optimal, but we've only got a handfull of
            // elements ;)
            if (!requested.contains(feature)) {
                requested.insert(feature);
            }

            break;
        }
    }

    // Run through the requested array and make sure we don't have
    // illegal combinations
    for (const auto& feature : requested) {
        switch (Feature(feature)) {
        case Feature::Invalid:
        case Feature::Invalid3:
        case Feature::TLS:
        case Feature::MUTATION_SEQNO:
        case Feature::XATTR:
        case Feature::XERROR:
        case Feature::SELECT_BUCKET:
        case Feature::Invalid2:
        case Feature::SNAPPY:
        case Feature::JSON:
        case Feature::Tracing:
        case Feature::AltRequestSupport:
        case Feature::SyncReplication:
        case Feature::Duplex:
        case Feature::UnorderedExecution:
        case Feature::Collections:
        case Feature::SnappyEverywhere:
        case Feature::PreserveTtl:
        case Feature::SubdocCreateAsDeleted:
        case Feature::SubdocReplaceBodyWithXattr:
        case Feature::ReportUnitUsage:
        case Feature::NonBlockingThrottlingMode:
        case Feature::SubdocReplicaRead:
        case Feature::GetClusterConfigWithKnownVersion:
        case Feature::DedupeNotMyVbucketClustermap:
        case Feature::SubdocAllowsAccessOnMultipleXattrKeys:
        case Feature::SubdocBinaryXattr:
        case Feature::TcpNoDelay:
        case Feature::TCPDELAY_Unsupported:
        case Feature::SubdocAllowReplicaReadOnDeletedDocs:
            // No other dependency
            break;

        case Feature::ClustermapChangeNotification:
        case Feature::ClustermapChangeNotificationBrief:
            // Needs duplex
            if (!requested.contains(Feature::Duplex)) {
                throw std::invalid_argument(
                        fmt::format("{} needs {}", feature, Feature::Duplex));
            }
            break;
        case Feature::SubdocDocumentMacroSupport:
        case Feature::VAttr:
        case Feature::RangeScanIncludeXattr:
            // Needs XATTR
            if (!requested.contains(Feature::XATTR)) {
                throw std::invalid_argument(
                        fmt::format("{} needs {}", feature, Feature::XATTR));
            }
            break;
        case Feature::GetRandomKeyIncludeXattr:
            // Needs XATTR and JSON and SnappyEverywhere
            if (!requested.contains(Feature::XATTR) ||
                !requested.contains(Feature::JSON) ||
                !requested.contains(Feature::SnappyEverywhere)) {
                throw std::invalid_argument(fmt::format(
                        "{} needs XATTR, JSON and SnappyEverywhere", feature));
            }
            break;
        }
    }

    // Make sure that we only enable the "brief" version if the client
    // asked for both
    if (requested.contains(Feature::ClustermapChangeNotificationBrief)) {
        requested.erase(Feature::ClustermapChangeNotification);
    }
}

void process_hello_packet_executor(Cookie& cookie) {
    auto& connection = cookie.getConnection();
    auto& req = cookie.getRequest();

    std::string_view key = req.getKeyString();
    auto valuebuf = req.getValue();
    const cb::sized_buffer<const uint16_t> input{
            reinterpret_cast<const uint16_t*>(valuebuf.data()),
            valuebuf.size() / 2};

    std::vector<uint16_t> out;

    // We can't switch bucket if we've got multiple commands in flight
    if (connection.getNumberOfCookies() > 1) {
        LOG_INFO_CTX(
                "Changing options via HELO is not possible with multiple "
                "commands in flight",
                {"conn_id", connection.getId()},
                {"description", connection.getDescription()});
        cookie.sendResponse(cb::mcbp::Status::NotSupported);
        return;
    }

    FeatureSet requested;
    try {
        buildRequestVector(requested, input);
    } catch (const std::invalid_argument& e) {
        LOG_INFO_CTX("Invalid combination of options",
                     {"conn_id", connection.getId()},
                     {"description", connection.getDescription()},
                     {"error", e.what()});
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
    connection.setClustermapChangeNotification(
            ClustermapChangeNotification::None);
    connection.setTracingEnabled(false);
    connection.setAllowUnorderedExecution(false);
    connection.setReportUnitUsage(false);
    connection.setNonBlockingThrottlingMode(false);
    connection.setDedupeNmvbMaps(false);
    connection.setSupportsSnappyEverywhere(false);

    // The connection key as a JSON object.
    cb::logger::Json client;
    if (!key.empty()) {
        if (key.front() == '{') {
            // This may be JSON
            try {
                client = cb::logger::Json::parse(key);
                auto obj = client.find("i");
                if (obj != client.end() && (*obj).is_string()) {
                    try {
                        connection.setConnectionId(obj->get<std::string>());
                    } catch (const std::exception& exception) {
                        LOG_INFO_CTX("Failed to parse connection uuid",
                                     {"conn_id", connection.getId()},
                                     {"error", exception.what()});
                    }
                }
                obj = client.find("a");
                if (obj != client.end() && obj->is_string()) {
                    connection.setAgentName(obj->get<std::string>());
                }
            } catch (const nlohmann::json::exception&) {
                client = {{"a", key}};
                connection.setAgentName(key);
            }
        } else {
            client = {{"a", key}};
            connection.setAgentName(key);
        }
    }

    // JSON array of enabled features.
    cb::logger::Json features = cb::logger::Json::array();
    for (const auto& feature : requested) {
        bool added = false;

        switch (feature) {
        case Feature::TCPDELAY_Unsupported:
        case Feature::Invalid:
        case Feature::Invalid2:
        case Feature::Invalid3:
        case Feature::TLS:
            break;

        case Feature::MUTATION_SEQNO:
            connection.setSupportsMutationExtras(true);
            added = true;
            break;
        case Feature::XATTR:
            if (Settings::instance().isXattrEnabled() ||
                connection.isInternal()) {
                connection.enableDatatype(Feature::XATTR);
                added = true;
            }
            break;
        case Feature::JSON:
            if (Settings::instance().isDatatypeJsonEnabled()) {
                connection.enableDatatype(Feature::JSON);
                added = true;
            }
            break;
        case Feature::SNAPPY:
            if (Settings::instance().isDatatypeSnappyEnabled()) {
                connection.enableDatatype(Feature::SNAPPY);
                added = true;
            }
            break;
        case Feature::XERROR:
            connection.setXerrorSupport(true);
            added = true;
            break;
        case Feature::Collections: {
            auto& bucket = connection.getBucket();
            // Abort if the engine cannot support collections
            if (bucket.supports(cb::engine::Feature::Collections)) {
                connection.setCollectionsSupported(true);
                added = true;
            }
        } break;
        case Feature::DedupeNotMyVbucketClustermap:
            connection.setDedupeNmvbMaps(true);
            added = true;
            break;
        case Feature::SnappyEverywhere:
            if (Settings::instance().isDatatypeSnappyEnabled()) {
                connection.setSupportsSnappyEverywhere(true);
                connection.enableDatatype(Feature::SNAPPY);
                added = true;
            }
            break;
        case Feature::Duplex:
            connection.setDuplexSupported(true);
            added = true;
            break;
        case Feature::ClustermapChangeNotificationBrief:
            if (Settings::instance().isClustermapPushNotificationsEnabled()) {
                connection.setClustermapChangeNotification(
                        ClustermapChangeNotification::Brief);
                added = true;
            }
            break;
        case Feature::ClustermapChangeNotification:
            if (Settings::instance().isClustermapPushNotificationsEnabled()) {
                connection.setClustermapChangeNotification(
                        ClustermapChangeNotification::Full);
                added = true;
            }
            break;
        case Feature::UnorderedExecution:
            if (connection.isDCP()) {
                LOG_INFO_CTX(
                        "Unordered execution is not supported for DCP "
                        "connections",
                        {"conn_id", connection.getId()},
                        {"description", connection.getDescription()});
            } else {
                connection.setAllowUnorderedExecution(true);
                added = true;
            }
            break;

        case Feature::Tracing:
            if (Settings::instance().isTracingEnabled()) {
                connection.setTracingEnabled(true);
                added = true;
            }
            break;

        case Feature::ReportUnitUsage:
            if (cb::serverless::isEnabled()) {
                connection.setReportUnitUsage(true);
                added = true;
            }
            break;
        case Feature::NonBlockingThrottlingMode:
            connection.setNonBlockingThrottlingMode(true);
            added = true;
            break;
        case Feature::TcpNoDelay:
        case Feature::PreserveTtl:
        case Feature::VAttr:
        case Feature::SubdocDocumentMacroSupport:
        case Feature::SubdocCreateAsDeleted:
        case Feature::SubdocReplaceBodyWithXattr:
        case Feature::SubdocReplicaRead:
        case Feature::GetClusterConfigWithKnownVersion:
        case Feature::SubdocAllowsAccessOnMultipleXattrKeys:
        case Feature::SubdocBinaryXattr:
        case Feature::SELECT_BUCKET:
        case Feature::AltRequestSupport:
        case Feature::SyncReplication:
        case Feature::RangeScanIncludeXattr:
        case Feature::SubdocAllowReplicaReadOnDeletedDocs:
        case Feature::GetRandomKeyIncludeXattr:
            // Informative features don't need special handling
            added = true;
            break;
        } // end switch

        if (added) {
            out.push_back(htons(uint16_t(feature)));
            features.push_back(feature);
        }
    }

    cookie.sendResponse(
            cb::mcbp::Status::Success,
            {},
            {},
            {reinterpret_cast<const char*>(out.data()), 2 * out.size()},
            cb::mcbp::Datatype::Raw,
            0);

    LOG_INFO_CTX("HELO",
                 {"conn_id", connection.getId()},
                 {"client", std::move(client)},
                 {"features", std::move(features)},
                 {"description", connection.getDescription()});
}
