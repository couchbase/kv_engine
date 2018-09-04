/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
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

#include "collections/manifest.h"
#include "collections/collections_types.h"

#include <json_utilities.h>

#include <JSON_checker.h>
#include <nlohmann/json.hpp>
#include <gsl/gsl>

#include <cctype>
#include <cstring>
#include <iostream>
#include <sstream>

namespace Collections {

Manifest::Manifest(cb::const_char_buffer json, size_t maxNumberOfCollections)
    : defaultCollectionExists(false), uid(0) {
    nlohmann::json parsed;
    try {
        parsed = nlohmann::json::parse(json);
    } catch (const nlohmann::json::exception& e) {
        throw std::invalid_argument(
                "Manifest::Manifest nlohmann cannot parse json:" +
                cb::to_string(json) + ", e:" + e.what());
    }

    // Read the Manifest UID e.g. "uid" : "5fa1"
    auto jsonUid = getJsonObject(parsed, UidKey, UidType);
    uid = makeUid(jsonUid.get<std::string>());

    // Read the scopes within the Manifest
    auto scopes = getJsonObject(parsed, ScopesKey, ScopesType);
    for (const auto& scope : scopes) {
        throwIfWrongType(
                std::string(ScopesKey), scope, nlohmann::json::value_t::object);

        auto name = getJsonObject(scope, NameKey, NameType);
        auto uid = getJsonObject(scope, UidKey, UidType);

        auto nameValue = name.get<std::string>();
        ScopeID uidValue = makeScopeID(uid.get<std::string>());
        if (validCollection(nameValue) && validUid(uidValue)) {
            if (this->scopes.count(uidValue) > 0) {
                throw std::invalid_argument(
                        "Manifest::Manifest duplicate scope uid:" +
                        uidValue.to_string() + ", name:" + nameValue);
            }

            std::vector<CollectionID> scopeCollections = {};

            // Read the collections within this scope
            auto collections =
                    getJsonObject(scope, CollectionsKey, CollectionsType);

            // Check that the number of collections in this scope + the
            // number of already stored collections is not greater thatn
            if (collections.size() + this->collections.size() >
                maxNumberOfCollections) {
                throw std::invalid_argument(
                        "Manifest::Manifest too many collections count:" +
                        std::to_string(collections.size()));
            }

            for (const auto& collection : collections) {
                throwIfWrongType(std::string(CollectionsKey),
                                 collection,
                                 nlohmann::json::value_t::object);

                auto cname = getJsonObject(collection, NameKey, NameType);
                auto cuid = getJsonObject(collection, UidKey, UidType);

                auto cnameValue = cname.get<std::string>();
                CollectionID cuidValue =
                        makeCollectionID(cuid.get<std::string>());
                if (validCollection(cnameValue) && validUid(cuidValue)) {
                    if (this->collections.count(cuidValue) > 0) {
                        throw std::invalid_argument(
                                "Manifest::Manifest duplicate collection uid:" +
                                cuidValue.to_string() +
                                ", name: " + cnameValue);
                    }

                    // The default collection must be within the default scope
                    if (cuidValue.isDefaultCollection() &&
                        !uidValue.isDefaultCollection()) {
                        throw std::invalid_argument(
                                "Manifest::Manifest the default collection is"
                                " not in the default scope");
                    }

                    enableDefaultCollection(cuidValue);
                    this->collections.emplace(cuidValue, cnameValue);
                    scopeCollections.push_back(cuidValue);
                } else {
                    throw std::invalid_argument(
                            "Manifest::Manifest invalid collection entry:" +
                            cnameValue + ", uid: " + cuidValue.to_string());
                }
            }

            this->scopes.emplace(uidValue,
                                 Scope{nameValue, std::move(scopeCollections)});
        } else {
            throw std::invalid_argument(
                    "Manifest::Manifest duplicate scope entry:" + nameValue +
                    ", uid:" + uidValue.to_string());
        }
    }

    if (this->scopes.empty()) {
        throw std::invalid_argument(
                "Manifest::Manifest no scopes were defined in the manifest");
    } else if (this->scopes.count(ScopeID::Default) == 0) {
        throw std::invalid_argument(
                "Manifest::Manifest the default scope was"
                " not defined");
    }
}

nlohmann::json Manifest::getJsonObject(const nlohmann::json& object,
                                       const std::string& key,
                                       nlohmann::json::value_t expectedType) {
    return cb::getJsonObject(object, key, expectedType, "Manifest");
}

void Manifest::throwIfWrongType(const std::string& errorKey,
                                const nlohmann::json& object,
                                nlohmann::json::value_t expectedType) {
    cb::throwIfWrongType(errorKey, object, expectedType, "Manifest");
}

void Manifest::enableDefaultCollection(CollectionID identifier) {
    if (identifier == CollectionID::Default) {
        defaultCollectionExists = true;
    }
}

bool Manifest::validCollection(const std::string& collection) {
    // $ prefix is currently reserved for future use
    // Name cannot be empty
    if (collection.empty() || collection.size() > MaxCollectionNameSize ||
        collection[0] == '$') {
        return false;
    }
    // Check rest of the characters for validity
    for (const auto& c : collection) {
        // Collection names are allowed to contain
        // A-Z, a-z, 0-9 and . _ - % $
        // system collections are _ prefixed, but not enforced here
        if (!(std::isdigit(c) || std::isalpha(c) || c == '.' || c == '_' ||
              c == '-' || c == '%' || c == '$')) {
            return false;
        }
    }
    return true;
}

bool Manifest::validUid(CollectionID identifier) {
    // We reserve system
    return identifier != CollectionID::System;
}

std::string Manifest::toJson() const {
    std::stringstream json;
    json << R"({"uid":")" << std::hex << uid << R"(","scopes":[)";
    size_t nScopes = 0;
    for (const auto& scope : scopes) {
        json << R"({"name":")" << scope.second.name << R"(","uid":")"
             << std::hex << scope.first << R"(")";
        // Add the collections if this scope has any
        if (!scope.second.collections.empty()) {
            json << R"(,"collections":[)";
            // Add all collections
            size_t nCollections = 0;
            for (const auto& collection : collections) {
                json << R"({"name":")" << collection.second << R"(","uid":")"
                     << std::hex << collection.first << R"("})";
                if (nCollections != collections.size() - 1) {
                    json << ",";
                }
                nCollections++;
            }
            json << "]";
        }
        json << R"(})";
        if (nScopes != scopes.size() - 1) {
            json << ",";
        }
        nScopes++;
    }
    json << "]}";
    return json.str();
}

void Manifest::dump() const {
    std::cerr << *this << std::endl;
}

std::ostream& operator<<(std::ostream& os, const Manifest& manifest) {
    os << "Collections::Manifest"
       << ", defaultCollectionExists:" << manifest.defaultCollectionExists
       << ", collections.size:" << manifest.collections.size() << std::endl;
    for (const auto& entry : manifest.scopes) {
        os << "scope:{" << std::hex << entry.first << "," << entry.second.name
           << ",collections:{";
        for (const auto& entry : manifest.collections) {
            os << "collection:{" << std::hex << entry.first << ","
               << entry.second << "}\n";
        }
        os << "}\n";
    }
    return os;
}
}
