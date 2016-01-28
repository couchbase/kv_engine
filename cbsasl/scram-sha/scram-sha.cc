/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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
#include "config.h"
#include "cbsasl/scram-sha/scram-sha.h"
#include "cbsasl/scram-sha/stringutils.h"
#include "cbsasl/pwfile.h"
#include "cbsasl/cbsasl.h"
#include "cbsasl/util.h"

#include <cstring>
#include <iostream>
#include <map>
#include <memory>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/sha.h>
#include <platform/base64.h>
#include <platform/random.h>
#include <set>
#include <sstream>
#include <string>
#include <cbsasl/mechanismfactory.h>

typedef std::map<char, std::string> AttributeMap;

/**
 * Decode the attribute list into a set. The attribute list looks like:
 * "k=value,y=value" etc
 *
 * @param list the list to parse
 * @param attributes where to store the attributes
 * @return true if success, false otherwise
 */
static bool decodeAttributeList(cbsasl_conn_t* conn, const std::string& list,
                                AttributeMap& attributes) {
    unsigned long pos = 0;

    cbsasl_log(conn, cbsasl_loglevel_t::Debug,
               "Decoding attribute list [" + list + "]");


    while (pos < list.length()) {
        auto equal = list.find('=', pos);
        if (equal == std::string::npos) {
            // syntax error!!
            cbsasl_log(conn, cbsasl_loglevel_t::Error,
                       "Decode attribute list [" + list + "] failed: no '='");
            return false;
        }

        if ((equal - pos) != 1) {
            cbsasl_log(conn, cbsasl_loglevel_t::Error,
                       "Decode attribute list [" + list + "] failed: " +
                       "key is multichar");
            return false;
        }

        char key = list.at(pos);
        pos = equal + 1;

        // Make sure we haven't seen this key before..
        if (attributes.find(key) != attributes.end()) {
            cbsasl_log(conn, cbsasl_loglevel_t::Error,
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
            "can't call getAuthMessage without client_first_message_bare is set");
    }
    if (server_first_message.empty()) {
        throw std::logic_error(
            "can't call getAuthMessage without server_first_message is set");
    }
    if (client_final_message_without_proof.empty()) {
        throw std::logic_error(
            "can't call getAuthMessage without client_final_message_without_proof is set");
    }
    return client_first_message_bare + "," + server_first_message + "," +
           client_final_message_without_proof;
}

void ScramShaBackend::addAttribute(std::ostream& out, char key,
                                   const std::string& value, bool more) {
    out << key << '=';

    switch (key) {
    case 'n' : // username ..
        out << encodeUsername(SASLPrep(value));
        break;

    case 'r' : // client nonce.. printable characters
        for (auto iter = value.begin(); iter != value.end(); ++iter) {
            if (*iter == ',' || !isprint(*iter)) {
                throw std::invalid_argument("ScramShaBackend::addAttribute: "
                                                "Invalid character in client"
                                                " nonce");
            }
        }
        out << value;
        break;

    case 'c' : // base64 encoded GS2 header and channel binding data
    case 's' : // base64 encoded salt
    case 'p' : // base64 encoded client proof
    case 'v' : // base64 encoded server signature
        out << Couchbase::Base64::encode(value);
        break;

    case 'i' : // iterator count
        // validate that it is an integer value
        try {
            std::stoi(value);
        } catch (...) {
            throw std::invalid_argument("ScramShaBackend::addAttribute: "
                                            "Iteration count must be a numeric"
                                            " value");

        }
        out << value;
        break;

    case 'e':
        for (auto iter = value.begin(); iter != value.end(); ++iter) {
            if (*iter == ',' || !isprint(*iter)) {
                throw std::invalid_argument("ScramShaBackend::addAttribute: "
                                                "Invalid character in error"
                                                " message");
            }
        }
        out << value;
        break;

    default:
        throw std::invalid_argument("ScramShaBackend::addAttribute:"
                                        " Invalid key");
    }

    if (more) {
        out << ',';
    }
}

void ScramShaBackend::addAttribute(std::ostream& out, char key, int value,
                                   bool more) {
    out << key << '=';

    std::string base64_encoded;

    switch (key) {
    case 'n' : // username ..
    case 'r' : // client nonce.. printable characters
    case 'c' : // base64 encoded GS2 header and channel binding data
    case 's' : // base64 encoded salt
    case 'p' : // base64 encoded client proof
    case 'v' : // base64 encoded server signature
    case 'e' : // error message
        throw std::invalid_argument("ScramShaBackend::addAttribute:"
                                        " Invalid value (should not be int)");
        break;

    case 'i' : // iterator count
        out << value;
        break;

    default:
        throw std::invalid_argument("ScramShaBackend::addAttribute:"
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
    std::vector<uint8_t> serverKey(digestSize);
    std::vector<uint8_t> serverSignature(digestSize);
    std::vector<uint8_t> saltedPassword;

    getSaltedPassword(saltedPassword);
    const std::string data("Server Key");

    HMAC(digest, saltedPassword.data(), saltedPassword.size(),
         reinterpret_cast<const unsigned char*>(data.data()),
         data.length(), serverKey.data(), nullptr);

    std::string authMessage = getAuthMessage();

    HMAC(digest, serverKey.data(), serverKey.size(),
         reinterpret_cast<const unsigned char*>(authMessage.data()),
         authMessage.length(), serverSignature.data(), nullptr);

    return std::string(reinterpret_cast<const char*>(serverSignature.data()),
                       serverSignature.size());
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
    std::vector<uint8_t> clientKey(digestSize);
    std::vector<uint8_t> clientSignature(digestSize);
    std::vector<uint8_t> saltedPassword;

    getSaltedPassword(saltedPassword);

    const std::string data("Client Key");

    HMAC(digest, saltedPassword.data(), saltedPassword.size(),
         reinterpret_cast<const unsigned char*>(data.data()),
         data.length(), clientKey.data(), nullptr);

    std::vector<uint8_t> storedKey;
    calculateStoredKey(storedKey, clientKey);

    std::string authMessage = getAuthMessage();
    HMAC(digest, storedKey.data(), storedKey.size(),
         reinterpret_cast<const unsigned char*>(authMessage.data()),
         authMessage.length(), clientSignature.data(), nullptr);

    // Client Proof is ClientKey XOR ClientSignature
    uint8_t* ck = clientKey.data();
    uint8_t* cs = clientSignature.data();

    std::vector<uint8_t> proof(clientKey.size());
    uint8_t* d = proof.data();

    auto total = proof.size();
    for (unsigned int ii = 0; ii < total; ++ii) {
        d[ii] = ck[ii] ^ cs[ii];
    }

    return std::string(reinterpret_cast<const char*>(proof.data()),
                       proof.size());
}

void ScramShaBackend::calculateStoredKey(std::vector<uint8_t>& storedKey,
                                         const std::vector<uint8_t>& clientKey) {
    storedKey.resize(digestSize);
    switch (mechanism) {
    case Mechanism::SCRAM_SHA1:
        SHA1(clientKey.data(), clientKey.size(), storedKey.data());
        break;
    case Mechanism::SCRAM_SHA256:
        SHA256(clientKey.data(), clientKey.size(), storedKey.data());
        break;
    case Mechanism::SCRAM_SHA512:
        SHA512(clientKey.data(), clientKey.size(), storedKey.data());
        break;
    default:
        throw std::logic_error("No support for the requested mechanism");
    }
}


/********************************************************************
 * Generic SHA Server API
 *******************************************************************/
ScramShaServerBackend::ScramShaServerBackend(const std::string& mech_name,
                                             const Mechanism& mech,
                                             const EVP_MD* dig,
                                             const size_t digSize)
    : ScramShaBackend(mech_name, mech, dig, digSize) {
    /* Generate a challenge */
    Couchbase::RandomGenerator randomGenerator(true);

    std::array<char, 8> nonce;
    if (!randomGenerator.getBytes(nonce.data(), nonce.size())) {
        cbsasl_log(nullptr, cbsasl_loglevel_t::Error, "Failed to generate"
            " server nonce");
        throw std::bad_alloc();
    }

    serverNonce.resize(nonce.size() * 2);
    cbsasl_hex_encode((char*)serverNonce.data(), nonce.data(), nonce.size());
}

cbsasl_error_t ScramShaServerBackend::start(cbsasl_conn_t* conn,
                                            const char* input,
                                            unsigned inputlen,
                                            const char** output,
                                            unsigned* outputlen) {
    if (inputlen == 0 || output == nullptr || outputlen == nullptr) {
        cbsasl_log(conn, cbsasl_loglevel_t::Error,
                   "Invalid arguments provided to "
                       "ScramShaServerBackend::start");
        return CBSASL_BADPARAM;
    }

    cbsasl_log(conn, cbsasl_loglevel_t::Trace,
               "ScramShaServerBackend::start (" +
               MechanismFactory::toString(mechanism) + ")");

    if (conn->get_cnonce_fn != nullptr) {
        // Allow the user to override the nonce
        const char* nonce = nullptr;
        unsigned int len;

        if (conn->get_cnonce_fn(conn->get_cnonce_ctx, CBSASL_CB_CNONCE,
                                &nonce, &len) != 0) {
            cbsasl_log(conn, cbsasl_loglevel_t::Error,
                       "CBSASL_CB_CNONCE callback returned failure");
            return CBSASL_FAIL;
        }
        serverNonce.assign(nonce, len);

        // verify that the provided nonce consists of printable characters
        // and no ,
        for (const auto& c : serverNonce) {
            if (c == ',' || !isprint(c)) {
                cbsasl_log(conn, cbsasl_loglevel_t::Error,
                           "Invalid character specified in nonce");
                return CBSASL_BADPARAM;
            }
        }

        cbsasl_log(conn, cbsasl_loglevel_t::Trace, "Using provided "
                                                       "nonce [" + serverNonce +
                                                   "]");
    }

    // the "client-first-message" message should contain a gs2-header
    //   gs2-bind-flag,[authzid],client-first-message-bare
    client_first_message.assign(input, inputlen);

    // according to the RFC the client should not send 'y' unless the
    // server advertised SCRAM-SHA[n]-PLUS (which we don't)
    if (client_first_message.find("n,") != 0) {
        // We don't support the p= to do channel bindings (that should
        // be advertised with SCRAM-SHA[n]-PLUS)
        cbsasl_log(conn, cbsasl_loglevel_t::Error,
                   "SCRAM: client should not try to ask for channel binding");
        return CBSASL_BADPARAM;
    }

    // next up is an optional authzid which we completely ignore...
    auto idx = client_first_message.find(',', 2);
    if (idx == std::string::npos) {
        cbsasl_log(conn, cbsasl_loglevel_t::Error,
                   "SCRAM: Format error on client-first-message");
        return CBSASL_BADPARAM;
    }

    client_first_message_bare = client_first_message.substr(idx + 1);

    AttributeMap attributes;
    if (!decodeAttributeList(conn, client_first_message_bare, attributes)) {
        cbsasl_log(conn, cbsasl_loglevel_t::Error,
                   "SCRAM: Failed to decode client-first-message-bare");
        return CBSASL_BADPARAM;
    }

    for (const auto& attribute : attributes) {
        switch (attribute.first) {
            // @todo at a later stage we might want to add support for the
            // @todo 'a' attribute that we'll use from n1ql/indexing etc
            // @todo note that they will then use n=@xdcr etc)
        case 'n' :
            username = attribute.second;
            cbsasl_log(conn, cbsasl_loglevel_t::Trace,
                       "Using username [" + username + "]");
            break;
        case 'r' :
            clientNonce = attribute.second;
            cbsasl_log(conn, cbsasl_loglevel_t::Trace,
                       "Using client nonce [" + clientNonce + "]");
            break;
        default:
            cbsasl_log(conn, cbsasl_loglevel_t::Error,
                       "Unsupported key supplied");
            return CBSASL_BADPARAM;
        }
    }

    if (username.empty() || clientNonce.empty()) {
        // mandatory fields!!!
        cbsasl_log(conn, cbsasl_loglevel_t::Error, "Unsupported key supplied");
        return CBSASL_BADPARAM;
    }

    try {
        username = decodeUsername(username);
    } catch (std::runtime_error&) {
        cbsasl_log(conn, cbsasl_loglevel_t::Error,
                   "Invalid character in username detected");
        return CBSASL_BADPARAM;
    }

    if (!find_user(username, user)) {
        cbsasl_log(conn, cbsasl_loglevel_t::Debug,
                   "User [" + username + "] doesn't exist.. using dummy");
        user.generateSecrets(mechanism);
    }
    conn->server->username.assign(username);
    nonce = clientNonce + std::string(serverNonce.data(), serverNonce.size());

    // build up the server-first-message
    std::ostringstream out;
    addAttribute(out, 'r', nonce, true);
    addAttribute(out, 's', Couchbase::Base64::decode(getSalt()), true);
    addAttribute(out, 'i', user.getIterationCount(), false);
    server_first_message = out.str();

    *output = server_first_message.data();
    *outputlen = unsigned(server_first_message.size());

    cbsasl_log(conn, cbsasl_loglevel_t::Trace, server_first_message);

    return CBSASL_CONTINUE;
}

cbsasl_error_t ScramShaServerBackend::step(cbsasl_conn_t* conn,
                                           const char* input,
                                           unsigned inputlen,
                                           const char** output,
                                           unsigned* outputlen) {

    if (inputlen == 0) {
        cbsasl_log(conn, cbsasl_loglevel_t::Error, "Invalid input");
        return CBSASL_BADPARAM;
    }

    std::string client_final_message(input, inputlen);
    AttributeMap attributes;
    if (!decodeAttributeList(conn, client_final_message, attributes)) {
        cbsasl_log(conn, cbsasl_loglevel_t::Error,
                   "SCRAM: Failed to decode client_final_message");
        return CBSASL_BADPARAM;
    }

    auto iter = attributes.find('p');
    if (iter == attributes.end()) {
        cbsasl_log(conn, cbsasl_loglevel_t::Error,
                   "SCRAM: client_final_message does not contain client proof");
        return CBSASL_BADPARAM;
    }

    auto idx = client_final_message.find(",p=");
    client_final_message_without_proof = client_final_message.substr(0, idx);


    // Generate the server signature
    auto serverSignature = getServerSignature();

    std::stringstream out;
    addAttribute(out, 'v', serverSignature, false);
    server_final_message = out.str();
    (*output) = server_final_message.data();
    (*outputlen) = server_final_message.length();

    std::string clientproof = iter->second;
    std::string my_clientproof = Couchbase::Base64::encode(getClientProof());

    int fail = cbsasl_secure_compare(clientproof.c_str(), clientproof.length(),
                                     my_clientproof.c_str(),
                                     my_clientproof.length()) ^user.isDummy();

    if (fail != 0) {
        if (user.isDummy()) {
            cbsasl_log(conn, cbsasl_loglevel_t::Fail,
                       "No such user [" + username + "]");
            return CBSASL_NOUSER;
        } else {
            cbsasl_log(conn, cbsasl_loglevel_t::Fail,
                       "Authentication fail for [" + username + "]");
            return CBSASL_PWERR;
        }
    }

    cbsasl_log(conn, cbsasl_loglevel_t::Trace, server_final_message);
    return CBSASL_OK;
}

/********************************************************************
 * Client API
 *******************************************************************/
ScramShaClientBackend::ScramShaClientBackend(const std::string& mech_name,
                                             const Mechanism& mech,
                                             const EVP_MD* dig,
                                             const size_t digSize)
    : ScramShaBackend(mech_name, mech, dig, digSize) {
    Couchbase::RandomGenerator randomGenerator(true);

    std::array<char, 8> nonce;
    if (!randomGenerator.getBytes(nonce.data(), nonce.size())) {
        cbsasl_log(nullptr, cbsasl_loglevel_t::Error, "Failed to generate"
            " server nonce");
        throw std::bad_alloc();
    }

    clientNonce.resize(nonce.size() * 2);
    cbsasl_hex_encode((char*)clientNonce.data(), nonce.data(), nonce.size());
}

cbsasl_error_t ScramShaClientBackend::start(cbsasl_conn_t* conn,
                                            const char* input,
                                            unsigned inputlen,
                                            const char** output,
                                            unsigned* outputlen) {

    if (inputlen != 0 || output == nullptr || outputlen == nullptr) {
        cbsasl_log(conn, cbsasl_loglevel_t::Error,
                   "Invalid parameters provided");
        return CBSASL_BADPARAM;
    }

    cbsasl_log(conn, cbsasl_loglevel_t::Trace,
               "ScramShaClientBackend::start (" +
               MechanismFactory::toString(mechanism) + ")");

    if (conn->get_cnonce_fn != nullptr) {
        // Allow the user to override the nonce
        const char* nonce = nullptr;
        unsigned int len;

        if (conn->get_cnonce_fn(conn->get_cnonce_ctx, CBSASL_CB_CNONCE,
                                &nonce, &len) != 0) {
            cbsasl_log(conn, cbsasl_loglevel_t::Error,
                       "CBSASL_CB_CNONCE callback returned failure");
            return CBSASL_FAIL;
        }
        clientNonce.assign(nonce, len);

        // verify that the provided nonce consists of printable characters
        // and no ,
        for (const auto& c : clientNonce) {
            if (c == ',' || !isprint(c)) {
                cbsasl_log(conn, cbsasl_loglevel_t::Error,
                           "Invalid character specified in nonce");
                return CBSASL_BADPARAM;
            }
        }

        cbsasl_log(conn, cbsasl_loglevel_t::Trace, "Using provided "
                                                       "nonce [" + clientNonce +
                                                   "]");
    }

    const char* usernm = nullptr;
    unsigned int usernmlen;
    auto* client = conn->client.get();

    if (cbsasl_get_username(client->get_username, client->get_username_ctx,
                            &usernm, &usernmlen) != CBSASL_OK) {
        cbsasl_log(conn, cbsasl_loglevel_t::Error, "Failed to get username");
        return CBSASL_FAIL;
    }

    username.assign(usernm, usernmlen);

    std::stringstream out;
    out << "n,,";
    addAttribute(out, 'n', username, true);
    addAttribute(out, 'r', clientNonce, false);

    client_first_message = out.str();
    client_first_message_bare = client_first_message.substr(3); // skip n,,

    *output = client_first_message.data();
    *outputlen = unsigned(client_first_message.length());

    cbsasl_log(conn, cbsasl_loglevel_t::Trace, client_first_message);
    return CBSASL_OK;
}

cbsasl_error_t ScramShaClientBackend::step(cbsasl_conn_t* conn,
                                           const char* input,
                                           unsigned inputlen,
                                           const char** output,
                                           unsigned* outputlen) {
    if (inputlen == 0 || output == nullptr || outputlen == nullptr) {
        cbsasl_log(conn, cbsasl_loglevel_t::Error,
                   "Invalid parameters provided");
        return CBSASL_FAIL;
    }

    if (server_first_message.empty()) {
        server_first_message.assign(input, inputlen);

        AttributeMap attributes;
        if (!decodeAttributeList(conn, server_first_message, attributes)) {
            cbsasl_log(conn, cbsasl_loglevel_t::Error,
                       "SCRAM: Failed to decode server-first-message");
            return CBSASL_BADPARAM;
        }

        for (const auto& attribute : attributes) {
            switch (attribute.first) {
            case 'r' : // combined nonce
                nonce = attribute.second;
                break;
            case 's' :
                salt = Couchbase::Base64::decode(attribute.second);
                break;
            case 'i' :
                try {
                    iterationCount = (unsigned int)std::stoul(attribute.second);
                } catch (...) {
                    return CBSASL_BADPARAM;
                }
                break;
            default:
                return CBSASL_BADPARAM;
            }
        }

        if (attributes.find('r') == attributes.end() ||
            attributes.find('s') == attributes.end() ||
            attributes.find('i') == attributes.end()) {
            cbsasl_log(conn, cbsasl_loglevel_t::Error,
                       "Missing r/s/i in server message");
            return CBSASL_BADPARAM;
        }

        // I've got the SALT, lets generate the salted password
        cbsasl_secret_t* pass;
        if (cbsasl_get_password(conn->client->get_password,
                                conn,
                                conn->client->get_password_ctx,
                                &pass) != CBSASL_OK) {
            cbsasl_log(conn, cbsasl_loglevel_t::Error,
                       "Failed to get password");
            return CBSASL_FAIL;
        }

        if (!generateSaltedPassword(reinterpret_cast<const char*>(pass->data),
                                    static_cast<int>(pass->len))) {
            cbsasl_log(conn, cbsasl_loglevel_t::Error,
                       "Failed to generate salted passwod");
            return CBSASL_FAIL;
        }

        // Ok so we have salted hased password :D

        std::stringstream out;
        addAttribute(out, 'c', "n,,", true);
        addAttribute(out, 'r', nonce, false);
        client_final_message_without_proof = out.str();
        out << ",";

        addAttribute(out, 'p', getClientProof(), false);

        client_final_message = out.str();

        *output = client_final_message.data();
        *outputlen = client_final_message.length();
        cbsasl_log(conn, cbsasl_loglevel_t::Trace, client_final_message);
        return CBSASL_CONTINUE;
    } else {
        server_final_message.assign(input, inputlen);

        AttributeMap attributes;
        if (!decodeAttributeList(conn, server_final_message, attributes)) {
            cbsasl_log(conn, cbsasl_loglevel_t::Error,
                       "SCRAM: Failed to decode server-final-message");
            return CBSASL_BADPARAM;
        }

        if (attributes.find('e') != attributes.end()) {
            cbsasl_log(conn, cbsasl_loglevel_t::Fail,
                       "Failed to authenticate: " + attributes['e']);
            return CBSASL_FAIL;
        }

        if (attributes.find('v') == attributes.end()) {
            cbsasl_log(conn, cbsasl_loglevel_t::Trace,
                       "Syntax error server final message is missing 'v'");
            return CBSASL_BADPARAM;
        }

        auto encoded = Couchbase::Base64::encode(getServerSignature());
        if (encoded != attributes['v']) {
            cbsasl_log(conn, cbsasl_loglevel_t::Trace,
                       "Incorrect ServerKey received");
            return CBSASL_FAIL;
        }

        return CBSASL_OK;
    }
}

bool ScramSha1ClientBackend::generateSaltedPassword(const char* ptr, int len) {
#ifdef HAVE_PKCS5_PBKDF2_HMAC_SHA1
    saltedPassword.resize(Couchbase::Sha1DigestSize);
    if (PKCS5_PBKDF2_HMAC_SHA1(ptr, len,
                               reinterpret_cast<unsigned const char*>(salt.data()),
                               int(salt.size()),
                               iterationCount,
                               Couchbase::Sha1DigestSize,
                               saltedPassword.data()) != 1) {
        return false;
    }
    return true;
#else
    throw std::runtime_error("SHA-1 is not supported on this platform");
#endif
}

bool ScramSha256ClientBackend::generateSaltedPassword(const char* ptr,
                                                      int len) {
#ifdef HAVE_PKCS5_PBKDF2_HMAC
    saltedPassword.resize(Couchbase::Sha256DigestSize);
    if (PKCS5_PBKDF2_HMAC(ptr, len,
                          reinterpret_cast<unsigned const char*>(salt.data()),
                          int(salt.size()),
                          iterationCount,
                          EVP_sha256(),
                          Couchbase::Sha256DigestSize,
                          saltedPassword.data()) != 1) {
        return false;
    }
    return true;
#else
    throw std::runtime_error("SHA-256 is not supported on this platform");
#endif
}

bool ScramSha512ClientBackend::generateSaltedPassword(const char* ptr,
                                                      int len) {
#ifdef HAVE_PKCS5_PBKDF2_HMAC
    saltedPassword.resize(Couchbase::Sha512DigestSize);
    if (PKCS5_PBKDF2_HMAC(ptr, len,
                          reinterpret_cast<unsigned const char*>(salt.data()),
                          int(salt.size()),
                          iterationCount,
                          EVP_sha512(),
                          Couchbase::Sha512DigestSize,
                          saltedPassword.data()) != 1) {
        return false;
    }
    return true;
#else
    throw std::runtime_error("SHA-512 is not supported on this platform");
#endif
}
