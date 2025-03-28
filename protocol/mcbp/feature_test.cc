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
#include <folly/portability/GTest.h>
#include <mcbp/protocol/feature.h>
#include <stdexcept>

const std::map<cb::mcbp::Feature, std::string> featureBlueprint = {
        {{cb::mcbp::Feature::Invalid, "Invalid"},
         {cb::mcbp::Feature::Invalid2, "Invalid2"},
         {cb::mcbp::Feature::Invalid3, "Invalid3"},
         {cb::mcbp::Feature::TLS, "TLS"},
         {cb::mcbp::Feature::TcpNoDelay, "TCP nodelay"},
         {cb::mcbp::Feature::MUTATION_SEQNO, "Mutation seqno"},
         {cb::mcbp::Feature::TCPDELAY_Unsupported, "TCP delay"},
         {cb::mcbp::Feature::XATTR, "XATTR"},
         {cb::mcbp::Feature::XERROR, "XERROR"},
         {cb::mcbp::Feature::SELECT_BUCKET, "Select bucket"},
         {cb::mcbp::Feature::Collections, "Collections"},
         {cb::mcbp::Feature::SnappyEverywhere, "SnappyEverywhere"},
         {cb::mcbp::Feature::SNAPPY, "Snappy"},
         {cb::mcbp::Feature::JSON, "JSON"},
         {cb::mcbp::Feature::Duplex, "Duplex"},
         {cb::mcbp::Feature::ClustermapChangeNotification,
          "Clustermap change notification"},
         {cb::mcbp::Feature::UnorderedExecution, "Unordered execution"},
         {cb::mcbp::Feature::Tracing, "Tracing"},
         {cb::mcbp::Feature::AltRequestSupport, "AltRequestSupport"},
         {cb::mcbp::Feature::SyncReplication, "SyncReplication"},
         {cb::mcbp::Feature::PreserveTtl, "PreserveTtl"},
         {cb::mcbp::Feature::VAttr, "VAttr"},
         {cb::mcbp::Feature::SubdocCreateAsDeleted, "SubdocCreateAsDeleted"},
         {cb::mcbp::Feature::SubdocDocumentMacroSupport,
          "SubdocDocumentMacroSupport"},
         {cb::mcbp::Feature::SubdocReplaceBodyWithXattr,
          "SubdocReplaceBodyWithXattr"},
         {cb::mcbp::Feature::ReportUnitUsage, "ReportUnitUsage"},
         {cb::mcbp::Feature::NonBlockingThrottlingMode,
          "NonBlockingThrottlingMode"},
         {cb::mcbp::Feature::SubdocReplicaRead, "SubdocReplicaRead"},
         {cb::mcbp::Feature::GetClusterConfigWithKnownVersion,
          "GetClusterConfigWithKnownVersion"},
         {cb::mcbp::Feature::DedupeNotMyVbucketClustermap,
          "DedupeNotMyVbucketClustermap"},
         {cb::mcbp::Feature::ClustermapChangeNotificationBrief,
          "ClustermapChangeNotificationBrief"},
         {cb::mcbp::Feature::SubdocAllowsAccessOnMultipleXattrKeys,
          "SubdocAllowsAccessOnMultipleXattrKeys"},
         {cb::mcbp::Feature::SubdocBinaryXattr, "SubdocBinaryXattr"},
         {cb::mcbp::Feature::RangeScanIncludeXattr, "RangeScanIncludeXattr"},
         {cb::mcbp::Feature::GetRandomKeyIncludeXattr,
          "GetRandomKeyIncludeXattr"},
         {cb::mcbp::Feature::SubdocAllowReplicaReadOnDeletedDocs,
          "SubdocAllowReplicaReadOnDeletedDocs"}}};

TEST(to_string, LegalValues) {
    for (const auto& [feature, name] : featureBlueprint) {
        EXPECT_EQ(name, fmt::format("{}", feature));
    }
}

// For some odd reason this test takes "forever" on our thread sanitizer
// commit validation before it crash (after 220sec, and on my mac it takes
// ~160ms). Given that this is a single-thread context lets just ignore
// the test under thread sanitizer
#ifndef THREAD_SANITIZER
TEST(to_string, IllegalValues) {
    auto end = uint32_t(std::numeric_limits<uint16_t>::max()) + 1;
    for (uint32_t ii = 0; ii < end; ++ii) {
        auto feature = static_cast<cb::mcbp::Feature>(ii);
        if (!featureBlueprint.contains(feature)) {
            EXPECT_EQ(fmt::format("unknown_{:#x}", ii),
                      fmt::format("{}", feature));
        }
    }
}
#endif
