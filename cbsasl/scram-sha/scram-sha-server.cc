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
#include "cbsasl/scram-sha/stringutils.h"
#include "cbsasl/util.h"
#include <cbsasl/logging.h>
#include <gsl/gsl-lite.hpp>
#include <platform/base64.h>
#include <platform/random.h>
#include <platform/string_hex.h>
#include <map>
#include <memory>
#include <sstream>
#include <string>

namespace cb::sasl::mechanism::scram {

ServerBackend::ServerBackend(server::ServerContext& ctx,
                             Mechanism mechanism,
                             cb::crypto::Algorithm algo,
                             std::function<std::string()> generateNonceFunction)
    : MechanismBackend(ctx), ScramShaBackend(mechanism, algo) {
    if (generateNonceFunction) {
        serverNonce = generateNonceFunction();
    } else {
        /* Generate a challenge */
        cb::RandomGenerator randomGenerator;

        std::array<char, 8> nonce{};
        if (!randomGenerator.getBytes(nonce.data(), nonce.size())) {
            logging::log(&context,
                         logging::Level::Error,
                         "Failed to generate server nonce");
            throw std::bad_alloc();
        }

        serverNonce = cb::hex_encode({nonce.data(), nonce.size()});
    }
}

std::pair<Error, std::string_view> ServerBackend::start(
        std::string_view input) {
    if (input.empty()) {
        logging::log(&context,
                     logging::Level::Error,
                     "Invalid arguments provided to "
                     "ScramShaServerBackend::start");
        return {Error::BAD_PARAM, {}};
    }

    // the "client-first-message" message should contain a gs2-header
    //   gs2-bind-flag,[authzid],client-first-message-bare
    client_first_message.assign(input.data(), input.size());

    // according to the RFC the client should not send 'y' unless the
    // server advertised SCRAM-SHA[n]-PLUS (which we don't)
    if (client_first_message.find("n,") != 0) {
        // We don't support the p= to do channel bindings (that should
        // be advertised with SCRAM-SHA[n]-PLUS)
        logging::log(&context,
                     logging::Level::Error,
                     "SCRAM: client should not try to ask for channel binding");
        return {Error::BAD_PARAM, {}};
    }

    // next up is an optional authzid which we completely ignore...
    auto idx = client_first_message.find(',', 2);
    if (idx == std::string::npos) {
        logging::log(&context,
                     logging::Level::Error,
                     "SCRAM: Format error on client-first-message");
        return {Error::BAD_PARAM, {}};
    }

    client_first_message_bare = client_first_message.substr(idx + 1);

    AttributeMap attributes;
    if (!decodeAttributeList(context, client_first_message_bare, attributes)) {
        logging::log(&context,
                     logging::Level::Error,
                     "SCRAM: Failed to decode client-first-message-bare");
        return {Error::BAD_PARAM, {}};
    }

    for (const auto& attribute : attributes) {
        switch (attribute.first) {
            // @todo at a later stage we might want to add support for the
            // @todo 'a' attribute that we'll use from n1ql/indexing etc
            // @todo note that they will then use n=@xdcr etc)
        case 'n':
            username = attribute.second;
            logging::log(&context,
                         logging::Level::Trace,
                         "Using username [" + username + "]");
            break;
        case 'r':
            clientNonce = attribute.second;
            logging::log(&context,
                         logging::Level::Trace,
                         "Using client nonce [" + clientNonce + "]");
            break;
        default:
            logging::log(&context,
                         logging::Level::Error,
                         "Unsupported key supplied");
            return {Error::BAD_PARAM, {}};
        }
    }

    if (username.empty() || clientNonce.empty()) {
        // mandatory fields!!!
        logging::log(
                &context, logging::Level::Error, "Unsupported key supplied");
        return {Error::BAD_PARAM, {}};
    }

    try {
        username = decodeUsername(username);
    } catch (std::runtime_error&) {
        logging::log(&context,
                     logging::Level::Error,
                     "Invalid character in username detected");
        return {Error::BAD_PARAM, {}};
    }

    if (!find_user(username, user)) {
        logging::log(&context,
                     logging::Level::Debug,
                     "User [" + username + "] doesn't exist.. using dummy");
        user = pwdb::UserFactory::createDummy(username, algorithm);
    }

    const auto& passwordMeta = user.getScramMetaData(algorithm);

    nonce = clientNonce + std::string(serverNonce.data(), serverNonce.size());

    // build up the server-first-message
    std::ostringstream out;
    addAttribute(out, 'r', nonce, true);
    addAttribute(out, 's', passwordMeta.salt, true);
    addAttribute(out, 'i', passwordMeta.iteration_count, false);
    server_first_message = out.str();

    return {Error::CONTINUE, server_first_message};
}

std::pair<Error, std::string_view> ServerBackend::step(std::string_view input) {
    if (input.empty()) {
        return {Error::BAD_PARAM, {}};
    }

    std::string client_final_message(input.data(), input.size());
    AttributeMap attributes;
    if (!decodeAttributeList(context, client_final_message, attributes)) {
        logging::log(&context,
                     logging::Level::Error,
                     "SCRAM: Failed to decode client_final_message");
        return {Error::BAD_PARAM, {}};
    }

    auto iter = attributes.find('p');
    if (iter == attributes.end()) {
        logging::log(
                &context,
                logging::Level::Error,
                "SCRAM: client_final_message does not contain client proof");
        return {Error::BAD_PARAM, {}};
    }

    auto idx = client_final_message.find(",p=");
    client_final_message_without_proof = client_final_message.substr(0, idx);

    int success = 0;
    for (const auto& key : user.getScramMetaData(algorithm).keys) {
        // Generate the server signature
        std::stringstream out;
        auto serverSignature = getServerSignature(key.server_key);
        addAttribute(out, 'v', serverSignature, false);
        server_final_message = out.str();

        const auto clientproof = cb::base64::decode(iter->second);
        const auto client_signature = getClientSignature(key.stored_key);
        if (clientproof.size() != client_signature.size()) {
            logging::log(
                    &context,
                    logging::Level::Error,
                    "SCRAM: client proof has a different width than client "
                    "signature");
            return {Error::BAD_PARAM, {}};
        }
        // ClientKey is Client Proof XOR ClientSignature
        const auto* cp = clientproof.data();
        const auto* cs = client_signature.data();
        std::string ck;
        ck.resize(clientproof.size());

        auto total = ck.size();
        for (std::size_t ii = 0; ii < total; ++ii) {
            ck[ii] = cp[ii] ^ cs[ii];
        }

        const auto sh = cb::crypto::digest(algorithm, ck);
        auto storedKey = key.stored_key;

        if ((cbsasl_secure_compare(
                     sh.data(), sh.size(), storedKey.data(), storedKey.size()) ^
             gsl::narrow_cast<int>(user.isDummy())) == 0) {
            success = 1;
        }
    }

    if (success) {
        logging::log(&context, logging::Level::Trace, server_final_message);
        return {Error::OK, server_final_message};
    }

    if (user.isDummy()) {
        logging::log(&context,
                     logging::Level::Fail,
                     "No such user [" + username + "]");
        return {Error::NO_USER, server_final_message};
    }
    logging::log(&context,
                 logging::Level::Fail,
                 "Authentication fail for [" + username + "]");
    return {Error::PASSWORD_ERROR, server_final_message};
}

} // namespace cb::sasl::mechanism::scram
