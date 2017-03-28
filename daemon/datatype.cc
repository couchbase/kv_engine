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

#include "datatype.h"
#include "settings.h"

bool Datatype::isSupported(mcbp::Feature feature) {
    switch (feature) {
    case mcbp::Feature::XATTR:
        return settings.isXattrEnabled();
    case mcbp::Feature::JSON:
        return settings.isDatatypeJsonEnabled();
    case mcbp::Feature::SNAPPY:
        return settings.isDatatypeSnappyEnabled();
    case mcbp::Feature::TLS:
    case mcbp::Feature::TCPNODELAY:
    case mcbp::Feature::MUTATION_SEQNO:
    case mcbp::Feature::TCPDELAY:
    case mcbp::Feature::XERROR:
    case mcbp::Feature::SELECT_BUCKET:
    case mcbp::Feature::COLLECTIONS:
    case mcbp::Feature::Invalid:
        throw std::invalid_argument("Datatype::isSupported invalid feature:" +
                                    std::to_string(int(feature)));
    }
    return false;
}

void Datatype::enable(mcbp::Feature feature) {
    switch (feature) {
    case mcbp::Feature::XATTR:
        enabled |= PROTOCOL_BINARY_DATATYPE_XATTR;
        break;
    case mcbp::Feature::JSON:
        enabled |= PROTOCOL_BINARY_DATATYPE_JSON;
        break;
    case mcbp::Feature::SNAPPY:
        enabled |= PROTOCOL_BINARY_DATATYPE_SNAPPY;
        break;
    case mcbp::Feature::TLS:
    case mcbp::Feature::TCPNODELAY:
    case mcbp::Feature::MUTATION_SEQNO:
    case mcbp::Feature::TCPDELAY:
    case mcbp::Feature::XERROR:
    case mcbp::Feature::SELECT_BUCKET:
    case mcbp::Feature::COLLECTIONS:
    case mcbp::Feature::Invalid:
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