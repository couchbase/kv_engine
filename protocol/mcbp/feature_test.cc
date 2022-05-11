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
#include <folly/portability/GTest.h>
#include <mcbp/protocol/feature.h>
#include <stdexcept>

const std::map<cb::mcbp::Feature, std::string> featureBlueprint = {
        {{cb::mcbp::Feature::Invalid, "Invalid"},
         {cb::mcbp::Feature::Invalid2, "Invalid2"},
         {cb::mcbp::Feature::TLS, "TLS"},
         {cb::mcbp::Feature::TCPNODELAY, "TCP nodelay"},
         {cb::mcbp::Feature::MUTATION_SEQNO, "Mutation seqno"},
         {cb::mcbp::Feature::TCPDELAY, "TCP delay"},
         {cb::mcbp::Feature::XATTR, "XATTR"},
         {cb::mcbp::Feature::XERROR, "XERROR"},
         {cb::mcbp::Feature::SELECT_BUCKET, "Select bucket"},
         {cb::mcbp::Feature::Collections, "Collections"},
         {cb::mcbp::Feature::SNAPPY, "Snappy"},
         {cb::mcbp::Feature::JSON, "JSON"},
         {cb::mcbp::Feature::Duplex, "Duplex"},
         {cb::mcbp::Feature::ClustermapChangeNotification,
          "Clustermap change notification"},
         {cb::mcbp::Feature::UnorderedExecution, "Unordered execution"},
         {cb::mcbp::Feature::Tracing, "Tracing"},
         {cb::mcbp::Feature::AltRequestSupport, "AltRequestSupport"},
         {cb::mcbp::Feature::SyncReplication, "SyncReplication"},
         {cb::mcbp::Feature::OpenTracing, "OpenTelemetry"},
         {cb::mcbp::Feature::PreserveTtl, "PreserveTtl"},
         {cb::mcbp::Feature::VAttr, "VAttr"},
         {cb::mcbp::Feature::PiTR, "PiTR"},
         {cb::mcbp::Feature::SubdocCreateAsDeleted, "SubdocCreateAsDeleted"},
         {cb::mcbp::Feature::SubdocDocumentMacroSupport,
          "SubdocDocumentMacroSupport"},
         {cb::mcbp::Feature::SubdocReplaceBodyWithXattr,
          "SubdocReplaceBodyWithXattr"},
         {cb::mcbp::Feature::ReportComputeUnitUsage,
          "ReportComputeUnitUsage"}}};

TEST(to_string, LegalValues) {
    for (const auto& entry : featureBlueprint) {
        EXPECT_EQ(entry.second, to_string(entry.first));
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
        auto feature = cb::mcbp::Feature(ii);
        if (featureBlueprint.find(feature) == featureBlueprint.end()) {
            EXPECT_THROW(to_string(feature), std::invalid_argument);
        }
    }
}
#endif
