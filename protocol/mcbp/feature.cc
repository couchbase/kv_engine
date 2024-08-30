/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <fmt/format.h>
#include <mcbp/protocol/feature.h>
#include <nlohmann/json.hpp>
#include <string>

std::string cb::mcbp::format_as(Feature feature) {
    switch (feature) {
    case Feature::Invalid:
        return "Invalid";
    case Feature::Invalid2:
        return "Invalid2";
    case Feature::Invalid3:
        return "Invalid3";
    case Feature::TLS:
        return "TLS";
    case Feature::TCPNODELAY_Unsupported:
        return "TCP nodelay";
    case Feature::MUTATION_SEQNO:
        return "Mutation seqno";
    case Feature::TCPDELAY_Unsupported:
        return "TCP delay";
    case Feature::XATTR:
        return "XATTR";
    case Feature::XERROR:
        return "XERROR";
    case Feature::SELECT_BUCKET:
        return "Select bucket";
    case Feature::SNAPPY:
        return "Snappy";
    case Feature::JSON:
        return "JSON";
    case Feature::Duplex:
        return "Duplex";
    case Feature::ClustermapChangeNotification:
        return "Clustermap change notification";
    case Feature::UnorderedExecution:
        return "Unordered execution";
    case Feature::Tracing:
        return "Tracing";
    case Feature::AltRequestSupport:
        return "AltRequestSupport";
    case Feature::SyncReplication:
        return "SyncReplication";
    case Feature::Collections:
        return "Collections";
    case Feature::SnappyEverywhere:
        return "SnappyEverywhere";
    case Feature::PreserveTtl:
        return "PreserveTtl";
    case Feature::VAttr:
        return "VAttr";
    case Feature::SubdocCreateAsDeleted:
        return "SubdocCreateAsDeleted";
    case Feature::SubdocDocumentMacroSupport:
        return "SubdocDocumentMacroSupport";
    case Feature::SubdocReplaceBodyWithXattr:
        return "SubdocReplaceBodyWithXattr";
    case Feature::ReportUnitUsage:
        return "ReportUnitUsage";
    case Feature::NonBlockingThrottlingMode:
        return "NonBlockingThrottlingMode";
    case Feature::SubdocReplicaRead:
        return "SubdocReplicaRead";
    case Feature::GetClusterConfigWithKnownVersion:
        return "GetClusterConfigWithKnownVersion";
    case Feature::DedupeNotMyVbucketClustermap:
        return "DedupeNotMyVbucketClustermap";
    case Feature::ClustermapChangeNotificationBrief:
        return "ClustermapChangeNotificationBrief";
    case Feature::SubdocAllowsAccessOnMultipleXattrKeys:
        return "SubdocAllowsAccessOnMultipleXattrKeys";
    case Feature::SubdocBinaryXattr:
        return "SubdocBinaryXattr";
    case cb::mcbp::Feature::RangeScanIncludeXattr:
        return "RangeScanIncludeXattr";
    }

    return fmt::format("unknown_{:#x}", static_cast<uint16_t>(feature));
}

void cb::mcbp::to_json(nlohmann::json& json, const Feature& feature) {
    json = format_as(feature);
}
