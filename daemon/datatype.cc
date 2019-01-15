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

#include "datatype.h"
#include "settings.h"

bool Datatype::isSupported(cb::mcbp::Feature feature) {
    switch (feature) {
    case cb::mcbp::Feature::XATTR:
        return settings.isXattrEnabled();
    case cb::mcbp::Feature::JSON:
        return settings.isDatatypeJsonEnabled();
    case cb::mcbp::Feature::SNAPPY:
        return settings.isDatatypeSnappyEnabled();
    case cb::mcbp::Feature::TLS:
    case cb::mcbp::Feature::TCPNODELAY:
    case cb::mcbp::Feature::MUTATION_SEQNO:
    case cb::mcbp::Feature::TCPDELAY:
    case cb::mcbp::Feature::XERROR:
    case cb::mcbp::Feature::SELECT_BUCKET:
    case cb::mcbp::Feature::Collections:
    case cb::mcbp::Feature::Invalid:
    case cb::mcbp::Feature::Invalid2:
    case cb::mcbp::Feature::Duplex:
    case cb::mcbp::Feature::ClustermapChangeNotification:
    case cb::mcbp::Feature::UnorderedExecution:
    case cb::mcbp::Feature::Tracing:
    case cb::mcbp::Feature::AltRequestSupport:
    case cb::mcbp::Feature::SyncReplication:
        throw std::invalid_argument("Datatype::isSupported invalid feature:" +
                                    std::to_string(int(feature)));
    }
    return false;
}

void Datatype::enable(cb::mcbp::Feature feature) {
    switch (feature) {
    case cb::mcbp::Feature::XATTR:
        enabled |= PROTOCOL_BINARY_DATATYPE_XATTR;
        break;
    case cb::mcbp::Feature::JSON:
        enabled |= PROTOCOL_BINARY_DATATYPE_JSON;
        break;
    case cb::mcbp::Feature::SNAPPY:
        enabled |= PROTOCOL_BINARY_DATATYPE_SNAPPY;
        break;
    case cb::mcbp::Feature::Duplex:
    case cb::mcbp::Feature::TLS:
    case cb::mcbp::Feature::TCPNODELAY:
    case cb::mcbp::Feature::MUTATION_SEQNO:
    case cb::mcbp::Feature::TCPDELAY:
    case cb::mcbp::Feature::XERROR:
    case cb::mcbp::Feature::SELECT_BUCKET:
    case cb::mcbp::Feature::Collections:
    case cb::mcbp::Feature::Invalid:
    case cb::mcbp::Feature::Invalid2:
    case cb::mcbp::Feature::ClustermapChangeNotification:
    case cb::mcbp::Feature::UnorderedExecution:
    case cb::mcbp::Feature::Tracing:
    case cb::mcbp::Feature::AltRequestSupport:
    case cb::mcbp::Feature::SyncReplication:
        throw std::invalid_argument("Datatype::enable invalid feature:" +
                                    std::to_string(int(feature)));
    }
}

void Datatype::enableAll() {
    enabled |= PROTOCOL_BINARY_DATATYPE_XATTR;
    enabled |= PROTOCOL_BINARY_DATATYPE_JSON;
    enabled |= PROTOCOL_BINARY_DATATYPE_SNAPPY;
}

void Datatype::disableAll() {
    enabled.reset();
}

protocol_binary_datatype_t Datatype::getIntersection(
        protocol_binary_datatype_t datatype) const {
    return protocol_binary_datatype_t(
            (enabled & DatatypeSet(datatype)).to_ulong());
}

bool Datatype::isEnabled(protocol_binary_datatype_t datatype) const {
    DatatypeSet in(datatype);
    return (enabled & in) == in;
}
