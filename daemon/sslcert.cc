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

#include "config.h"
#include "sslcert.h"
#include "settings.h"

#include <platform/dirutils.h>
#include <platform/strerror.h>
#include <algorithm>
#include <cstring>
#include <fstream>
#include <system_error>

std::unique_ptr<ClientCertUser> ClientCertAuth::createCertUser(
        const std::string& field,
        const std::string& prefix,
        const std::string& delimiter) {
    if (field.empty()) {
        return std::make_unique<ClientCertUser>(prefix, delimiter);
    }
    std::size_t found = field.find_first_of(".");
    std::string root = field.substr(0, found);
    if (found != std::string::npos && found != 0) {
        std::string suffix = field.substr(found + 1, std::string::npos);
        if (root == "subject") {
            return std::make_unique<CertUserFromSubject>(
                    suffix, prefix, delimiter);
        } else if (root == "san") {
            return std::make_unique<CertUserFromSAN>(suffix, prefix, delimiter);
        }
    }
    std::ostringstream stringStream;
    stringStream << "createCertUser: Invalid field " << field;
    if (!root.empty()) {
        stringStream << "with root " << root;
    }
    throw std::invalid_argument(stringStream.str());
}

SslCertResult ClientCertUser::getUser(X509* cert) {
    return std::make_pair(ClientCertUser::Status::NotPresent, std::string(""));
}

std::string ClientCertUser::matchPattern(std::string input) {
    std::string ret = input;
    if (!prefix.empty()) {
        auto prefixLocation = input.find(prefix);
        if (prefixLocation != 0) {
            return "";
        }
        ret = input.substr(prefix.size());
    }
    if (!delimiter.empty()) {
        auto delimiterPos = ret.find_first_of(delimiter);
        return ret.substr(0, delimiterPos);
    }
    return ret;
}

SslCertResult CertUserFromSubject::getUser(X509* cert) {
    std::string userName;
    X509_NAME* name = X509_get_subject_name(cert);
    int idx = X509_NAME_get_index_by_NID(name, NID_commonName, -1);
    if (idx < 0) {
        std::string error = "Common name not found";
        return make_pair(Status::Error, error);
    }
    X509_NAME_ENTRY* entry = X509_NAME_get_entry(name, idx);
    if (entry) {
        ASN1_STRING* data = X509_NAME_ENTRY_get_data(entry);
        if (data) {
            unsigned char* utf8 = nullptr;
            int len = ASN1_STRING_to_UTF8(&utf8, data);
            if (len < 0) {
                std::string error = "Unable to read the common name";
                return make_pair(Status::Error, error);
            }
            std::string val(reinterpret_cast<char const*>(utf8), len);
            OPENSSL_free(utf8);
            userName = matchPattern(val);
            if (userName.empty()) {
                std::string error = "Not able to match prefix/delimiter";
                return make_pair(Status::Error, error);
            } else {
                return make_pair(Status::Success, userName);
            }
        }
    }
    std::string error = "Not able to find common name from cert";
    return make_pair(Status::Error, error);
}

CertUserFromSubject::CertUserFromSubject(const std::string& field,
                                         const std::string& prefix,
                                         const std::string& delimiter)
    : ClientCertUser(prefix, delimiter), field(field) {
    if (field.compare("cn")) {
        throw std::invalid_argument(
                std::string("CertUserFromSubject : Invalid field") + field);
    }
}

/*
 * Return the pointer which is owned by entry
 */
ASN1_IA5STRING* CertUserFromSAN::getValFromEntry(GENERAL_NAME* entry) {
    switch (field) {
    case GEN_DNS:
        return entry->d.dNSName;
    case GEN_EMAIL:
        return entry->d.rfc822Name;
    case GEN_URI:
        return entry->d.uniformResourceIdentifier;
    }
    throw std::invalid_argument(
            std::string("CertUserFromSAN : Invalid field type ") +
            std::to_string(field));
}

SslCertResult CertUserFromSAN::getUser(X509* cert) {
    Status status = Status::Error;
    GENERAL_NAMES* names = reinterpret_cast<GENERAL_NAMES*>(
            X509_get_ext_d2i(cert, NID_subject_alt_name, 0, 0));
    std::string userName;
    if (names) {
        unsigned char* utf8 = nullptr;
        for (int index = 0; index < sk_GENERAL_NAME_num(names); ++index) {
            GENERAL_NAME* entry = sk_GENERAL_NAME_value(names, index);
            if (!entry) {
                continue;
            }
            if (field == entry->type) {
                int len = ASN1_STRING_to_UTF8(&utf8, getValFromEntry(entry));
                if (len < 0) {
                    continue;
                }
                std::string val(reinterpret_cast<char const*>(utf8), len);
                OPENSSL_free(utf8);
                if ((int)val.size() == len) {
                    userName = matchPattern(val);
                    if (!userName.empty()) {
                        status = Status::Success;
                        break;
                    }
                }
            }
        }
        GENERAL_NAMES_free(names);
    }
    return std::make_pair(status, userName);
}

CertUserFromSAN::CertUserFromSAN(const std::string& field,
                                 const std::string& prefix,
                                 const std::string& delimiter)
    : ClientCertUser(prefix, delimiter) {
    auto it = sanStringToTypeMap.find(field);
    if (it == sanStringToTypeMap.end()) {
        throw std::invalid_argument(
                std::string("CertUserFromSAN : Invalid field") + field);
    }
    CertUserFromSAN::field = it->second;
}

ClientCertUser::~ClientCertUser() {
}
