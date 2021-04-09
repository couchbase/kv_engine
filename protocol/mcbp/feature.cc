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

#include <mcbp/protocol/feature.h>
#include <stdexcept>
#include <string>

std::string to_string(cb::mcbp::Feature feature) {
    switch (feature) {
    case cb::mcbp::Feature::Invalid:
        return "Invalid";
    case cb::mcbp::Feature::TLS:
        return "TLS";
    case cb::mcbp::Feature::TCPNODELAY:
        return "TCP nodelay";
    case cb::mcbp::Feature::MUTATION_SEQNO:
        return "Mutation seqno";
    case cb::mcbp::Feature::TCPDELAY:
        return "TCP delay";
    case cb::mcbp::Feature::XATTR:
        return "XATTR";
    case cb::mcbp::Feature::XERROR:
        return "XERROR";
    case cb::mcbp::Feature::SELECT_BUCKET:
        return "Select bucket";
    case cb::mcbp::Feature::Invalid2:
        return "Invalid2";
    case cb::mcbp::Feature::SNAPPY:
        return "Snappy";
    case cb::mcbp::Feature::JSON:
        return "JSON";
    case cb::mcbp::Feature::Duplex:
        return "Duplex";
    case cb::mcbp::Feature::ClustermapChangeNotification:
        return "Clustermap change notification";
    case cb::mcbp::Feature::UnorderedExecution:
        return "Unordered execution";
    case cb::mcbp::Feature::Tracing:
        return "Tracing";
    case cb::mcbp::Feature::AltRequestSupport:
        return "AltRequestSupport";
    case cb::mcbp::Feature::SyncReplication:
        return "SyncReplication";
    case cb::mcbp::Feature::Collections:
        return "Collections";
    case cb::mcbp::Feature::OpenTracing:
        return "OpenTelemetry";
    case cb::mcbp::Feature::PreserveTtl:
        return "PreserveTtl";
    case cb::mcbp::Feature::VAttr:
        return "VAttr";
    case cb::mcbp::Feature::PiTR:
        return "PiTR";
    case cb::mcbp::Feature::SubdocCreateAsDeleted:
        return "SubdocCreateAsDeleted";
    case cb::mcbp::Feature::SubdocDocumentMacroSupport:
        return "SubdocDocumentMacroSupport";
    case cb::mcbp::Feature::SubdocReplaceBodyWithXattr:
        return "SubdocReplaceBodyWithXattr";
    }

    throw std::invalid_argument(
            "to_string(cb::mcbp::Feature): unknown feature: " +
            std::to_string(uint16_t(feature)));
}
