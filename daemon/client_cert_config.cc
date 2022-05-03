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
#include "client_cert_config.h"

#include <nlohmann/json.hpp>
#include <openssl/x509.h>
#include <openssl/x509v3.h>
#include <utilities/json_utilities.h>
#include <optional>

namespace cb::x509 {

struct CommonNameMapping : public ClientCertConfig::Mapping {
    CommonNameMapping(std::string& path, const nlohmann::json& obj)
        : ClientCertConfig::Mapping(path, obj) {
    }

    std::pair<Status, std::string> match(X509* cert) const override {
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
                    return make_pair(Status::NoMatch, error);
                } else {
                    return make_pair(Status::Success, userName);
                }
            }
        }
        std::string error = "Not able to find common name from cert";
        return make_pair(Status::Error, error);
    };
};

struct SanMapping : public ClientCertConfig::Mapping {
    SanMapping(std::string& path, int field_, const nlohmann::json& obj)
        : ClientCertConfig::Mapping(path, obj), field(field_) {
    }

    ASN1_IA5STRING* getValFromEntry(GENERAL_NAME* entry) const {
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

    std::pair<Status, std::string> match(X509* cert) const override {
        Status status = Status::NoMatch;
        auto* names = reinterpret_cast<GENERAL_NAMES*>(
                X509_get_ext_d2i(cert, NID_subject_alt_name, nullptr, nullptr));
        std::string userName;
        if (names) {
            unsigned char* utf8 = nullptr;
            for (int index = 0; index < sk_GENERAL_NAME_num(names); ++index) {
                GENERAL_NAME* entry = sk_GENERAL_NAME_value(names, index);
                if (!entry) {
                    continue;
                }

                if (field == entry->type) {
                    int len =
                            ASN1_STRING_to_UTF8(&utf8, getValFromEntry(entry));
                    if (len < 0) {
                        continue;
                    }
                    std::string val(reinterpret_cast<char const*>(utf8), len);
                    OPENSSL_free(utf8);
                    if ((int)val.size() == len) {
                        userName = matchPattern(val);
                        if (userName.empty()) {
                            status = Status::NoMatch;
                        } else {
                            status = Status::Success;
                            break;
                        }
                    }
                }
            }
            GENERAL_NAMES_free(names);
        } else {
            status = Status::Error;
            userName = "X509_get_ext_d2i failed";
        }

        return std::make_pair(status, userName);
    };

    const int field;
};

static std::unique_ptr<ClientCertConfig::Mapping> createMapping(
        const nlohmann::json& obj) {
    auto path = cb::jsonGet<std::string>(obj, "path");

    if (path.empty()) {
        return std::make_unique<ClientCertConfig::Mapping>(path, obj);
    }

    if (path == "subject.cn") {
        return std::make_unique<CommonNameMapping>(path, obj);
    }

    if (path.find("san.dnsname") == 0) {
        return std::make_unique<SanMapping>(path, GEN_DNS, obj);
    }

    if (path.find("san.email") == 0) {
        return std::make_unique<SanMapping>(path, GEN_EMAIL, obj);
    }

    if (path.find("san.uri") == 0) {
        return std::make_unique<SanMapping>(path, GEN_URI, obj);
    }

    throw std::invalid_argument("createMapping: Unsupported path: " + path);
}

std::unique_ptr<cb::x509::ClientCertConfig> ClientCertConfig::create(
        const nlohmann::json& config) {
    return std::unique_ptr<cb::x509::ClientCertConfig>(
            new ClientCertConfig(config));
}

ClientCertConfig::ClientCertConfig(const nlohmann::json& config) {
    auto prefixes = cb::getOptionalJsonObject(config, "prefixes");
    if (!prefixes.has_value()) {
        // this is an old style configuration
        mappings.emplace_back(createMapping(config));
        return;
    }

    cb::throwIfWrongType("prefixes",
                         *prefixes,
                         nlohmann::json::value_t::array,
                         "ClientCertConfig");

    for (auto& prefix : *prefixes) {
        mappings.emplace_back(createMapping(prefix));
    }
}

const ClientCertConfig::Mapping& ClientCertConfig::getMapping(
        size_t index) const {
    return *mappings[index];
}

std::pair<Status, std::string> ClientCertConfig::lookupUser(X509* cert) const {
    for (const auto& mapping : mappings) {
        auto ret = mapping->match(cert);
        switch (ret.first) {
        case Status::Success:
        case Status::Error:
        case Status::NotPresent:
            return ret;
        case Status::NoMatch:
            // Try the next rule
            continue;
        }
        throw std::logic_error(
                "ClientCertConfig::lookupUser: mapping.match() returned "
                "illegal value");
    }

    return {Status::NoMatch, ""};
}

std::string ClientCertConfig::to_string() const {
    nlohmann::json root;
    if (mappings.size() == 1) {
        root = {{"path", mappings[0]->path},
                {"prefix", mappings[0]->prefix},
                {"suffix", mappings[0]->suffix},
                {"delimiter", mappings[0]->delimiter}};
    } else {
        nlohmann::json array;
        for (const auto& m : mappings) {
            array.emplace_back(nlohmann::json{{"path", m->path},
                                              {"prefix", m->prefix},
                                              {"suffix", m->suffix},
                                              {"delimiter", m->delimiter}});
        }
        root["prefixes"] = array;
    }

    return root.dump();
}

ClientCertConfig::Mapping::Mapping(std::string& path_,
                                   const nlohmann::json& obj)
    : path(std::move(path_)),
      prefix(obj.value("prefix", "")),
      suffix(obj.value("suffix", "")),
      delimiter(obj.value("delimiter", "")) {
}

std::string ClientCertConfig::Mapping::matchPattern(
        const std::string& input) const {
    std::string ret = input;
    if (!prefix.empty()) {
        auto prefixLocation = input.find(prefix);
        if (prefixLocation != 0) {
            return "";
        }
        ret = input.substr(prefix.size());
    }

    if (!suffix.empty()) {
        auto suffixLocation = ret.find(suffix);
        if (suffixLocation == std::string::npos ||
            suffixLocation + suffix.size() != ret.size()) {
            return "";
        }
        ret.resize(suffixLocation);
    }

    if (!delimiter.empty()) {
        auto delimiterPos = ret.find_first_of(delimiter);
        return ret.substr(0, delimiterPos);
    }
    return ret;
}

std::pair<Status, std::string> ClientCertConfig::Mapping::match(
        X509* cert) const {
    return std::make_pair(Status::NotPresent, "No mapping defined");
}

void ClientCertMapper::reconfigure(std::unique_ptr<ClientCertConfig> next) {
    config = std::move(next);
}

std::pair<Status, std::string> ClientCertMapper::lookupUser(X509* cert) const {
    if (cert == nullptr) {
        return std::make_pair(Status::NotPresent,
                              "certificate not presented by client");
    }

    return config.withRLock([cert](auto& c) -> std::pair<Status, std::string> {
        if (c) {
            return c->lookupUser(cert);
        } else {
            return std::make_pair(Status::Error, "No database configured");
        }
    });
}

std::string ClientCertMapper::to_string() const {
    return config.withRLock([](auto& c) {
        if (c) {
            return c->to_string();
        } else {
            return std::string{R"({})"};
        }
    });
}

} // namespace cb::x509
