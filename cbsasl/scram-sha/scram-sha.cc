/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "cbsasl/scram-sha/scram-sha.h"
#include "cbsasl/pwfile.h"
#include "cbsasl/scram-sha/stringutils.h"
#include "cbsasl/util.h"

#include <cbsasl/username_util.h>
#include <logger/logger.h>
#include <platform/base64.h>
#include <map>
#include <memory>
#include <sstream>
#include <string>

namespace cb::sasl::mechanism::scram {

bool ScramShaBackend::decodeAttributeList(Context& context,
                                          std::string_view list,
                                          AttributeMap& attributes) {
    size_t pos = 0;

    while (pos < list.length()) {
        auto equal = list.find('=', pos);
        if (equal == std::string::npos) {
            // syntax error!!
            LOG_ERROR("UUID:[{}] Decode attribute list [{}]] failed: no '='",
                      context.getUuid(),
                      list);
            return false;
        }

        if ((equal - pos) != 1) {
            LOG_ERROR(
                    "UUID:[{}] Decode attribute list [{}] failed: key is "
                    "multichar",
                    context.getUuid(),
                    list);
            return false;
        }

        char key = list.at(pos);
        pos = equal + 1;

        // Make sure we haven't seen this key before..
        if (attributes.find(key) != attributes.end()) {
            LOG_ERROR(
                    "UUID:[{}] Decode attribute list [{}] failed: key [{}] "
                    "already seen",
                    context.getUuid(),
                    list,
                    key);
            return false;
        }

        auto comma = list.find(',', pos);
        if (comma == std::string::npos) {
            attributes.insert(std::make_pair(key, list.substr(pos)));
            pos = list.length();
        } else {
            attributes.insert(
                    std::make_pair(key, list.substr(pos, comma - pos)));
            pos = comma + 1;
        }
    }

    return true;
}

std::string ScramShaBackend::getAuthMessage() {
    if (client_first_message_bare.empty()) {
        throw std::logic_error(
                "can't call getAuthMessage without client_first_message_bare "
                "is set");
    }
    if (server_first_message.empty()) {
        throw std::logic_error(
                "can't call getAuthMessage without server_first_message is "
                "set");
    }
    if (client_final_message_without_proof.empty()) {
        throw std::logic_error(
                "can't call getAuthMessage without "
                "client_final_message_without_proof is set");
    }
    return client_first_message_bare + "," + server_first_message + "," +
           client_final_message_without_proof;
}

void ScramShaBackend::addAttribute(std::ostream& out,
                                   char key,
                                   const std::string& value,
                                   bool more) {
    out << key << '=';

    switch (key) {
    case 'n': // username ..
        out << username::encode(SASLPrep(value));
        break;

    case 'r': // client nonce.. printable characters
        for (const auto& c : value) {
            if (c == ',' || !isprint(c)) {
                throw std::invalid_argument(
                        "ScramShaBackend::addAttribute: Invalid character in "
                        "client nonce");
            }
        }
        out << value;
        break;
    case 's': // base64 encoded salt
        out << value;
        break;
    case 'c': // base64 encoded GS2 header and channel binding data
    case 'p': // base64 encoded client proof
    case 'v': // base64 encoded server signature
        out << cb::base64::encode(value);
        break;

    case 'i': // iterator count
              // validate that it is an integer value
        if (!std::all_of(value.begin(), value.end(), ::isdigit)) {
            throw std::invalid_argument(
                    "ScramShaBackend::addAttribute: "
                    "Iteration count must be a numeric"
                    " value");
        }
        out << value;
        break;

    case 'e':
        for (const auto& c : value) {
            if (c == ',' || !isprint(c)) {
                throw std::invalid_argument(
                        "ScramShaBackend::addAttribute: Invalid character in "
                        "error message");
            }
        }
        out << value;
        break;

    default:
        throw std::invalid_argument(
                "ScramShaBackend::addAttribute:"
                " Invalid key");
    }

    if (more) {
        out << ',';
    }
}

void ScramShaBackend::addAttribute(std::ostream& out,
                                   char key,
                                   std::size_t value,
                                   bool more) {
    out << key << '=';

    std::string base64_encoded;

    switch (key) {
    case 'n': // username ..
    case 'r': // client nonce.. printable characters
    case 'c': // base64 encoded GS2 header and channel binding data
    case 's': // base64 encoded salt
    case 'p': // base64 encoded client proof
    case 'v': // base64 encoded server signature
    case 'e': // error message
        throw std::invalid_argument(
                "ScramShaBackend::addAttribute:"
                " Invalid value (should not be int)");

    case 'i': // iterator count
        out << value;
        break;

    default:
        throw std::invalid_argument(
                "ScramShaBackend::addAttribute:"
                " Invalid key");
    }

    if (more) {
        out << ',';
    }
}

std::string ScramShaBackend::getServerSignature(std::string_view server_key) {
    return cb::crypto::HMAC(algorithm, server_key, getAuthMessage());
}

std::string ScramShaBackend::getClientSignature(std::string_view stored_key) {
    return cb::crypto::HMAC(algorithm, stored_key, getAuthMessage());
}

} // namespace cb::sasl::mechanism::scram
