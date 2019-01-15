/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
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
    }

    throw std::invalid_argument(
            "to_string(cb::mcbp::Feature): unknown feature: " +
            std::to_string(uint16_t(feature)));
}
