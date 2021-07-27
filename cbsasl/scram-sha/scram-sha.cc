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

using AttributeMap = std::map<char, std::string>;

/**
 * Decode the attribute list into a set. The attribute list looks like:
 * "k=value,y=value" etc
 *
 * @param list the list to parse
 * @param attributes where to store the attributes
 * @return true if success, false otherwise
 */
static bool decodeAttributeList(Context& context,
                                const std::string& list,
                                AttributeMap& attributes) {
    size_t pos = 0;

    logging::log(&context,
                 logging::Level::Debug,
                 "Decoding attribute list [" + list + "]");

    while (pos < list.length()) {
        auto equal = list.find('=', pos);
        if (equal == std::string::npos) {
            // syntax error!!
            logging::log(&context,
                         logging::Level::Error,
                         "Decode attribute list [" + list + "] failed: no '='");
            return false;
        }

        if ((equal - pos) != 1) {
            logging::log(&context,
                         logging::Level::Error,
                         "Decode attribute list [" + list +
                                 "] failed: " + "key is multichar");
            return false;
        }

        char key = list.at(pos);
        pos = equal + 1;

        // Make sure we haven't seen this key before..
        if (attributes.find(key) != attributes.end()) {
            logging::log(&context,
                         logging::Level::Error,
                         "Decode attribute list [" + list + "] failed: " +
                                 "key [" + key + "] is multichar");
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

/********************************************************************
 * Common API
 *******************************************************************/
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
        out << encodeUsername(SASLPrep(value));
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

    case 'c': // base64 encoded GS2 header and channel binding data
    case 's': // base64 encoded salt
    case 'p': // base64 encoded client proof
    case 'v': // base64 encoded server signature
        out << Couchbase::Base64::encode(value);
        break;

    case 'i': // iterator count
        // validate that it is an integer value
        try {
            std::stoi(value);
        } catch (...) {
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
                                   int value,
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

/**
 * Generate the Server Signature. It is computed as:
 *
 * SaltedPassword  := Hi(Normalize(password), salt, i)
 * ServerKey       := HMAC(SaltedPassword, "Server Key")
 * ServerSignature := HMAC(ServerKey, AuthMessage)
 */
std::string ScramShaBackend::getServerSignature() {
    auto serverKey =
            cb::crypto::HMAC(algorithm, getSaltedPassword(), "Server Key");

    return cb::crypto::HMAC(algorithm, serverKey, getAuthMessage());
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
std::string ScramShaBackend::getClientProof() {
    auto clientKey =
            cb::crypto::HMAC(algorithm, getSaltedPassword(), "Client Key");
    auto storedKey = cb::crypto::digest(algorithm, clientKey);
    std::string authMessage = getAuthMessage();
    auto clientSignature = cb::crypto::HMAC(algorithm, storedKey, authMessage);

    // Client Proof is ClientKey XOR ClientSignature
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

/********************************************************************
 * Generic SHA Server API
 *******************************************************************/
ServerBackend::ServerBackend(server::ServerContext& ctx,
                             Mechanism mechanism,
                             cb::crypto::Algorithm algo)
    : MechanismBackend(ctx), ScramShaBackend(mechanism, algo) {
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

std::pair<Error, std::string_view> ServerBackend::start(
        std::string_view input) {
    if (input.empty()) {
        logging::log(&context,
                     logging::Level::Error,
                     "Invalid arguments provided to "
                     "ScramShaServerBackend::start");
        return std::make_pair<Error, std::string_view>(Error::BAD_PARAM, {});
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
        return std::make_pair<Error, std::string_view>(Error::BAD_PARAM, {});
    }

    // next up is an optional authzid which we completely ignore...
    auto idx = client_first_message.find(',', 2);
    if (idx == std::string::npos) {
        logging::log(&context,
                     logging::Level::Error,
                     "SCRAM: Format error on client-first-message");
        return std::make_pair<Error, std::string_view>(Error::BAD_PARAM, {});
    }

    client_first_message_bare = client_first_message.substr(idx + 1);

    AttributeMap attributes;
    if (!decodeAttributeList(context, client_first_message_bare, attributes)) {
        logging::log(&context,
                     logging::Level::Error,
                     "SCRAM: Failed to decode client-first-message-bare");
        return std::make_pair<Error, std::string_view>(Error::BAD_PARAM, {});
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
            return std::make_pair<Error, std::string_view>(Error::BAD_PARAM,
                                                           {});
        }
    }

    if (username.empty() || clientNonce.empty()) {
        // mandatory fields!!!
        logging::log(
                &context, logging::Level::Error, "Unsupported key supplied");
        return std::make_pair<Error, std::string_view>(Error::BAD_PARAM, {});
    }

    try {
        username = decodeUsername(username);
    } catch (std::runtime_error&) {
        logging::log(&context,
                     logging::Level::Error,
                     "Invalid character in username detected");
        return std::make_pair<Error, std::string_view>(Error::BAD_PARAM, {});
    }

    if (!find_user(username, user)) {
        logging::log(&context,
                     logging::Level::Debug,
                     "User [" + username + "] doesn't exist.. using dummy");
        user = pwdb::UserFactory::createDummy(username, mechanism);
    }

    const auto& passwordMeta = user.getPassword(mechanism);

    nonce = clientNonce + std::string(serverNonce.data(), serverNonce.size());

    // build up the server-first-message
    std::ostringstream out;
    addAttribute(out, 'r', nonce, true);
    addAttribute(
            out, 's', Couchbase::Base64::decode(passwordMeta.getSalt()), true);
    addAttribute(out, 'i', passwordMeta.getIterationCount(), false);
    server_first_message = out.str();

    return std::make_pair<Error, std::string_view>(Error::CONTINUE,
                                                   server_first_message);
}

std::pair<Error, std::string_view> ServerBackend::step(std::string_view input) {
    if (input.empty()) {
        return std::make_pair<Error, std::string_view>(Error::BAD_PARAM, {});
    }

    std::string client_final_message(input.data(), input.size());
    AttributeMap attributes;
    if (!decodeAttributeList(context, client_final_message, attributes)) {
        logging::log(&context,
                     logging::Level::Error,
                     "SCRAM: Failed to decode client_final_message");
        return std::make_pair<Error, std::string_view>(Error::BAD_PARAM, {});
    }

    auto iter = attributes.find('p');
    if (iter == attributes.end()) {
        logging::log(
                &context,
                logging::Level::Error,
                "SCRAM: client_final_message does not contain client proof");
        return std::make_pair<Error, std::string_view>(Error::BAD_PARAM, {});
    }

    auto idx = client_final_message.find(",p=");
    client_final_message_without_proof = client_final_message.substr(0, idx);

    // Generate the server signature

    std::stringstream out;

    auto serverSignature = getServerSignature();
    addAttribute(out, 'v', serverSignature, false);
    server_final_message = out.str();

    std::string clientproof = iter->second;
    std::string my_clientproof = Couchbase::Base64::encode(getClientProof());

    int fail = cbsasl_secure_compare(clientproof.c_str(),
                                     clientproof.length(),
                                     my_clientproof.c_str(),
                                     my_clientproof.length()) ^
               gsl::narrow_cast<int>(user.isDummy());

    if (fail != 0) {
        if (user.isDummy()) {
            logging::log(&context,
                         logging::Level::Fail,
                         "No such user [" + username + "]");
            return std::make_pair<Error, std::string_view>(
                    Error::NO_USER, server_final_message);
        } else {
            logging::log(&context,
                         logging::Level::Fail,
                         "Authentication fail for [" + username + "]");
            return std::make_pair<Error, std::string_view>(
                    Error::PASSWORD_ERROR, server_final_message);
        }
    }

    logging::log(&context, logging::Level::Trace, server_final_message);
    return std::make_pair<Error, std::string_view>(Error::OK,
                                                   server_final_message);
}

/********************************************************************
 * Client API
 *******************************************************************/
ClientBackend::ClientBackend(client::GetUsernameCallback& user_cb,
                             client::GetPasswordCallback& password_cb,
                             client::ClientContext& ctx,
                             Mechanism mechanism,
                             cb::crypto::Algorithm algo)
    : MechanismBackend(user_cb, password_cb, ctx),
      ScramShaBackend(mechanism, algo) {
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
        return std::make_pair<Error, std::string_view>(Error::BAD_PARAM, {});
    }

    if (server_first_message.empty()) {
        server_first_message.assign(input.data(), input.size());

        AttributeMap attributes;
        if (!decodeAttributeList(context, server_first_message, attributes)) {
            return std::make_pair<Error, std::string_view>(Error::BAD_PARAM,
                                                           {});
        }

        for (const auto& attribute : attributes) {
            switch (attribute.first) {
            case 'r': // combined nonce
                nonce = attribute.second;
                break;
            case 's':
                salt = Couchbase::Base64::decode(attribute.second);
                break;
            case 'i':
                try {
                    iterationCount = (unsigned int)std::stoul(attribute.second);
                } catch (...) {
                    return std::make_pair<Error, std::string_view>(
                            Error::BAD_PARAM, {});
                }
                break;
            default:
                return std::make_pair<Error, std::string_view>(Error::BAD_PARAM,
                                                               {});
            }
        }

        if (attributes.find('r') == attributes.end() ||
            attributes.find('s') == attributes.end() ||
            attributes.find('i') == attributes.end()) {
            logging::log(&context,
                         logging::Level::Error,
                         "Missing r/s/i in server message");
            return std::make_pair<Error, std::string_view>(Error::BAD_PARAM,
                                                           {});
        }

        // I've got the SALT, lets generate the salted password
        if (!generateSaltedPassword(passwordCallback())) {
            logging::log(&context,
                         logging::Level::Error,
                         "Failed to generate salted passwod");
            return std::make_pair<Error, std::string_view>(Error::FAIL, {});
        }

        // Ok so we have salted hased password :D

        std::stringstream out;
        addAttribute(out, 'c', "n,,", true);
        addAttribute(out, 'r', nonce, false);
        client_final_message_without_proof = out.str();
        out << ",";

        addAttribute(out, 'p', getClientProof(), false);

        client_final_message = out.str();

        return std::make_pair<Error, std::string_view>(Error::CONTINUE,
                                                       client_final_message);
    } else {
        server_final_message.assign(input.data(), input.size());

        AttributeMap attributes;
        if (!decodeAttributeList(context, server_final_message, attributes)) {
            logging::log(&context,
                         logging::Level::Error,
                         "SCRAM: Failed to decode server-final-message");
            return std::make_pair<Error, std::string_view>(Error::BAD_PARAM,
                                                           {});
        }

        if (attributes.find('e') != attributes.end()) {
            logging::log(&context,
                         logging::Level::Fail,
                         "Failed to authenticate: " + attributes['e']);
            return std::make_pair<Error, std::string_view>(Error::FAIL, {});
        }

        if (attributes.find('v') == attributes.end()) {
            logging::log(&context,
                         logging::Level::Trace,
                         "Syntax error server final message is missing 'v'");
            return std::make_pair<Error, std::string_view>(Error::BAD_PARAM,
                                                           {});
        }

        auto encoded = Couchbase::Base64::encode(getServerSignature());
        if (encoded != attributes['v']) {
            logging::log(&context,
                         logging::Level::Trace,
                         "Incorrect ServerKey received");
            return std::make_pair<Error, std::string_view>(Error::FAIL, {});
        }

        return std::make_pair<Error, std::string_view>(Error::OK, {});
    }
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

} // namespace cb::sasl::mechanism::scram
