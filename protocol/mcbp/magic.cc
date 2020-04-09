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
