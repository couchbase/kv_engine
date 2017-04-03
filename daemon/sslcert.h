/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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
#pragma once

#include "config.h"

#include <openssl/x509.h>
#include <openssl/x509v3.h>
#include <platform/make_unique.h>

#include <memory>
#include <string>
#include <unordered_map>

class ClientCertUser {
public:
    enum class Status {
        Error,
        NotPresent,
        Success
    };
    ClientCertUser(const std::string& prefix, const std::string& delimiter)
        : prefix(prefix), delimiter(delimiter) {
    }
    virtual ~ClientCertUser();
    virtual std::pair<ClientCertUser::Status, std::string> getUser(X509* cert);
    virtual std::unique_ptr<ClientCertUser> clone() {
        return std::make_unique<ClientCertUser>(*this);
    }

protected:
    std::string matchPattern(std::string input);
    const std::string prefix;
    const std::string delimiter;
};
using SslCertResult = std::pair<ClientCertUser::Status, std::string>;

class CertUserFromSubject : public ClientCertUser {
public:
    CertUserFromSubject(const std::string& field,
                        const std::string& prefix,
                        const std::string& delimiter);
    SslCertResult getUser(X509* cert);
    std::unique_ptr<ClientCertUser> clone() {
        return std::make_unique<CertUserFromSubject>(*this);
    }

private:
    const std::string field;
};

class CertUserFromSAN : public ClientCertUser {
public:
    CertUserFromSAN(const std::string& field,
                    const std::string& prefix,
                    const std::string& delimiter);
    ASN1_IA5STRING* getValFromEntry(GENERAL_NAME* entry);
    SslCertResult getUser(X509* cert);
    std::unique_ptr<ClientCertUser> clone() {
        return std::make_unique<CertUserFromSAN>(*this);
    }

private:
    int field;
    typedef std::unordered_map<std::string, int> StringToType;
    const StringToType sanStringToTypeMap = StringToType{
            {"dnsname", GEN_DNS}, {"email", GEN_EMAIL}, {"uri", GEN_URI}};
};
