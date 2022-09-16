/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "cbsasl/pwfile.h"
#include "cbsasl/scram-sha/scram-sha.h"
#include <cbsasl/logging.h>
#include <fmt/format.h>
#include <gsl/gsl-lite.hpp>
#include <platform/base64.h>
#include <platform/random.h>
#include <platform/string_hex.h>
#include <map>
#include <memory>
#include <sstream>
#include <string>

namespace cb::sasl::mechanism::scram {

ClientBackend::ClientBackend(client::GetUsernameCallback& user_cb,
                             client::GetPasswordCallback& password_cb,
                             client::ClientContext& ctx,
                             Mechanism mechanism,
                             cb::crypto::Algorithm algo,
                             std::function<std::string()> generateNonceFunction)
    : MechanismBackend(user_cb, password_cb, ctx),
      ScramShaBackend(mechanism, algo) {
    if (generateNonceFunction) {
        clientNonce = generateNonceFunction();
    } else {
        cb::RandomGenerator randomGenerator;

        std::array<char, 8> nonce{};
        if (!randomGenerator.getBytes(nonce.data(), nonce.size())) {
            logging::log(&context,
                         logging::Level::Error,
                         "Failed to generate server nonce");
            throw std::bad_alloc();
        }

        clientNonce = cb::hex_encode({nonce.data(), nonce.size()});
    }
}

std::pair<Error, std::string_view> ClientBackend::start() {
    std::stringstream out;
    out << "n,,";
    addAttribute(out, 'n', usernameCallback(), true);
    addAttribute(out, 'r', clientNonce, false);

    client_first_message = out.str();
    client_first_message_bare = client_first_message.substr(3); // skip n,,

    return std::make_pair<Error, std::string_view>(Error::OK,
                                                   client_first_message);
}

std::pair<Error, std::string_view> ClientBackend::step(std::string_view input) {
    if (input.empty()) {
        return {Error::BAD_PARAM, {}};
    }

    if (server_first_message.empty()) {
        server_first_message.assign(input.data(), input.size());

        AttributeMap attributes;
        if (!decodeAttributeList(context, server_first_message, attributes)) {
            return {Error::BAD_PARAM, {}};
        }

        for (const auto& attribute : attributes) {
            switch (attribute.first) {
            case 'r': // combined nonce
                nonce = attribute.second;
                break;
            case 's':
                salt = cb::base64::decode(attribute.second);
                break;
            case 'i':
                try {
                    iterationCount = (unsigned int)std::stoul(attribute.second);
                } catch (...) {
                    return {Error::BAD_PARAM, {}};
                }
                break;
            default:
                return {Error::BAD_PARAM, {}};
            }
        }

        if (attributes.find('r') == attributes.end() ||
            attributes.find('s') == attributes.end() ||
            attributes.find('i') == attributes.end()) {
            errorMessage = "Missing r/s/i in server message";
            logging::log(&context, logging::Level::Error, errorMessage);
            return {Error::BAD_PARAM, errorMessage};
        }

        // I've got the SALT, lets generate the salted password
        if (!generateSaltedPassword(passwordCallback())) {
            errorMessage = "Failed to generate salted passwod";
            logging::log(&context, logging::Level::Error, errorMessage);
            return {Error::FAIL, errorMessage};
        }

        // Ok so we have salted hased password :D

        std::stringstream out;
        addAttribute(out, 'c', "n,,", true);
        addAttribute(out, 'r', nonce, false);
        client_final_message_without_proof = out.str();
        out << ",";

        addAttribute(out, 'p', getClientProof(), false);

        client_final_message = out.str();

        return {Error::CONTINUE, client_final_message};
    }

    if (server_final_message.empty()) {
        server_final_message.assign(input.data(), input.size());

        AttributeMap attributes;
        if (!decodeAttributeList(context, server_final_message, attributes)) {
            logging::log(&context,
                         logging::Level::Error,
                         "SCRAM: Failed to decode server-final-message");
            return {Error::BAD_PARAM, {}};
        }

        if (attributes.find('e') != attributes.end()) {
            errorMessage =
                    fmt::format("Failed to authenticate. Server reported: {}",
                                attributes['e']);
            logging::log(&context, logging::Level::Fail, errorMessage);
            return {Error::FAIL, errorMessage};
        }

        if (attributes.find('v') == attributes.end()) {
            errorMessage = "Syntax error server final message is missing 'v'";
            logging::log(&context, logging::Level::Trace, errorMessage);
            return {Error::BAD_PARAM, errorMessage};
        }

        auto encoded = cb::base64::encode(getServerSignature(cb::crypto::HMAC(
                algorithm, getSaltedPassword(), "Server Key")));
        if (encoded != attributes['v']) {
            errorMessage = fmt::format(
                    "Incorrect ServerKey received. Server reported: [{}]. "
                    "Mine: [{}]",
                    attributes['v'],
                    encoded);
            logging::log(&context, logging::Level::Trace, errorMessage);
            return {Error::FAIL, errorMessage};
        }

        return {Error::OK, {}};
    }

    return {Error::FAIL, {}};
}

bool ClientBackend::generateSaltedPassword(const std::string& secret) {
    try {
        saltedPassword = cb::crypto::PBKDF2_HMAC(
                algorithm, secret, salt, iterationCount);
        return true;
    } catch (...) {
        return false;
    }
}

std::string ClientBackend::getClientKey() {
    return cb::crypto::HMAC(algorithm, getSaltedPassword(), "Client Key");
}

/**
 * Generate the Client Proof. It is computed as:
 *
 * SaltedPassword  := Hi(Normalize(password), salt, i)
 * ClientKey       := HMAC(SaltedPassword, "Client Key")
 * StoredKey       := H(ClientKey)
 * AuthMessage     := client-first-message-bare + "," +
 *                    server-first-message + "," +
 *                    client-final-message-without-proof
 * ClientSignature := HMAC(StoredKey, AuthMessage)
 * ClientProof     := ClientKey XOR ClientSignature
 */
std::string ClientBackend::getClientProof() {
    auto clientSignature =
            getClientSignature(cb::crypto::digest(algorithm, getClientKey()));

    // Client Proof is ClientKey XOR ClientSignature
    const auto clientKey = getClientKey();
    const auto* ck = clientKey.data();
    const auto* cs = clientSignature.data();

    std::string proof;
    proof.resize(clientKey.size());

    auto total = proof.size();
    for (std::size_t ii = 0; ii < total; ++ii) {
        proof[ii] = ck[ii] ^ cs[ii];
    }

    return proof;
}

} // namespace cb::sasl::mechanism::scram
