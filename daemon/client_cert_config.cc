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
#include "client_cert_config.h"

#include <nlohmann/json.hpp>
#include <openssl/x509.h>
#include <openssl/x509v3.h>
#include <utilities/json_utilities.h>
#include <optional>

namespace cb {
namespace x509 {

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
        Status status = Status::Error;
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
    auto mode = cb::jsonGet<std::string>(config, "state");

    std::unique_ptr<cb::x509::ClientCertConfig> ret;
    if (mode == "disable") {
        return std::unique_ptr<cb::x509::ClientCertConfig>(
                new ClientCertConfig());
    }

    if (mode == "enable") {
        return std::unique_ptr<cb::x509::ClientCertConfig>(
                new ClientCertConfig(Mode::Enabled, config));
    }

    if (mode == "mandatory") {
        return std::unique_ptr<cb::x509::ClientCertConfig>(
                new ClientCertConfig(Mode::Mandatory, config));
    }

    throw std::invalid_argument(
            "ClientCertConfig::create: Invalid value for state");
}

ClientCertConfig::ClientCertConfig(Mode mode_, const nlohmann::json& config)
    : mode(mode_) {
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

    return std::pair<Status, std::string>(Status::NoMatch, "");
}

std::string ClientCertConfig::to_string() const {
    nlohmann::json root;
    switch (mode) {
    case Mode::Disabled:
        root["state"] = "disable";
        break;
    case Mode::Enabled:
        root["state"] = "enable";
        break;
    case Mode::Mandatory:
        root["state"] = "mandatory";
        break;
    }

    if (mappings.size() == 1) {
        root["path"] = mappings[0]->path.c_str();
        root["prefix"] = mappings[0]->prefix.c_str();
        root["delimiter"] = mappings[0]->delimiter.c_str();
    } else {
        nlohmann::json array;
        for (const auto& m : mappings) {
            nlohmann::json mapping;
            mapping["path"] = m->path.c_str();
            mapping["prefix"] = m->prefix.c_str();
            mapping["delimiter"] = m->delimiter.c_str();
            array.push_back(mapping);
        }
        root["prefixes"] = array;
    }

    return root.dump();
}

ClientCertConfig::Mapping::Mapping(std::string& path_,
                                   const nlohmann::json& obj)
    : path(std::move(path_)),
      prefix(obj.value("prefix", "")),
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

void ClientCertMapper::reconfigure(std::unique_ptr<ClientCertConfig>& next) {
    std::lock_guard<std::mutex> guard(mutex);
    config = std::move(next);
}

std::pair<Status, std::string> ClientCertMapper::lookupUser(X509* cert) const {
    if (cert == nullptr) {
        return std::make_pair(Status::NotPresent,
                              "certificate not presented by client");
    }
    std::lock_guard<std::mutex> guard(mutex);
    if (!config) {
        return std::make_pair(Status::Error, "No database configured");
    }
    return config->lookupUser(cert);
}

Mode ClientCertMapper::getMode() const {
    std::lock_guard<std::mutex> guard(mutex);
    if (!config) {
        return Mode::Disabled;
    }

    return config->getMode();
}
std::string ClientCertMapper::to_string() const {
    std::lock_guard<std::mutex> guard(mutex);
    if (!config) {
        return std::string(R"({"state":"disable"})");
    }

    return config->to_string();
}

} // namespace x509
} // namespace cb
