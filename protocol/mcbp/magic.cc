/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <mcbp/protocol/magic.h>
#include <platform/string_hex.h>
#include <stdexcept>
#include <string>

std::string to_string(cb::mcbp::Magic magic) {
    switch (magic) {
    case cb::mcbp::Magic::ClientRequest:
        return "ClientRequest";
    case cb::mcbp::Magic::AltClientRequest:
        return "AltClientRequest";
    case cb::mcbp::Magic::ClientResponse:
        return "ClientResponse";
    case cb::mcbp::Magic::AltClientResponse:
        return "AltClientResponse";
    case cb::mcbp::Magic::ServerRequest:
        return "ServerRequest";
    case cb::mcbp::Magic::ServerResponse:
        return "ServerResponse";
    }

    throw std::invalid_argument(
            "to_string(cb::mcbp::Magic magic): Invalid value: " +
            std::to_string(uint8_t(magic)));
}

namespace cb::mcbp {

bool is_legal(Magic magic) {
    switch (magic) {
    case Magic::ClientRequest:
    case Magic::AltClientRequest:
    case Magic::ClientResponse:
    case Magic::AltClientResponse:
    case Magic::ServerRequest:
    case Magic::ServerResponse:
        return true;
    }

    return false;
}

bool is_request(Magic magic) {
    switch (magic) {
    case Magic::ClientRequest:
    case Magic::AltClientRequest:
    case Magic::ServerRequest:
        return true;
    case Magic::ClientResponse:
    case Magic::AltClientResponse:
    case Magic::ServerResponse:
        return false;
    }
    throw std::invalid_argument("cb::mcbp::is_request(): Invalid magic: " +
                                cb::to_hex(uint8_t(magic)));
}

bool is_client_magic(Magic magic) {
    switch (magic) {
    case Magic::ClientRequest:
    case Magic::AltClientRequest:
    case Magic::ClientResponse:
    case Magic::AltClientResponse:
        return true;
    case Magic::ServerRequest:
    case Magic::ServerResponse:
        return false;
    }
    throw std::invalid_argument("cb::mcbp::is_client_magic(): Invalid magic: " +
                                cb::to_hex(uint8_t(magic)));
}

bool is_alternative_encoding(Magic magic) {
    switch (magic) {
    case Magic::AltClientRequest:
    case Magic::AltClientResponse:
        return true;

    case Magic::ClientRequest:
    case Magic::ClientResponse:
    case Magic::ServerRequest:
    case Magic::ServerResponse:
        return false;
    }
    throw std::invalid_argument(
            "cb::mcbp::is_alternative_encoding(): Invalid magic: " +
            cb::to_hex(uint8_t(magic)));
}

} // namespace cb::mcbp
