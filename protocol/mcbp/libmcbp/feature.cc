/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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
        return "TCP NODELAY";
    case cb::mcbp::Feature::MUTATION_SEQNO:
        return "Mutation seqno";
    case cb::mcbp::Feature::TCPDELAY:
        return "TCP DELAY";
    case cb::mcbp::Feature::XATTR:
        return "XATTR";
    case cb::mcbp::Feature::XERROR:
        return "XERROR";
    case cb::mcbp::Feature::SELECT_BUCKET:
        return "Select Bucket";
    case cb::mcbp::Feature::COLLECTIONS:
        return "COLLECTIONS";
    case cb::mcbp::Feature::SNAPPY:
        return "SNAPPY";
    case cb::mcbp::Feature::JSON:
        return "JSON";
    case cb::mcbp::Feature::Duplex:
        return "Duplex";
    }

    throw std::invalid_argument(
            "to_string(cb::mcbp::Feature): unknown feature: " +
            std::to_string(uint16_t(feature)));
}
