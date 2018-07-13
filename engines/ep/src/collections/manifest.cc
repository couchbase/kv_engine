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

#include <JSON_checker.h>
#include <nlohmann/json.hpp>
#include <gsl/gsl>

#include <cstring>
#include <iostream>
#include <sstream>

namespace Collections {

Manifest::Manifest(cb::const_char_buffer json, size_t maxNumberOfCollections)
    : defaultCollectionExists(false), uid(0) {
    if (!checkUTF8JSON(reinterpret_cast<const unsigned char*>(json.data()),
                       json.size())) {
        throw std::invalid_argument("Manifest::Manifest input not valid json:" +
                                    cb::to_string(json));
    }
    nlohmann::json parsed;
    try {
        parsed = nlohmann::json::parse(json);
    } catch (const nlohmann::json::exception& e) {
        throw std::invalid_argument(
                "Manifest::Manifest cJSON cannot parse json:" +
                cb::to_string(json) + ", e:" + e.what());
    }

    // Read the Manifest UID e.g. "uid" : "5fa1"
    auto jsonUid = getJsonObject(parsed, CollectionUidKey, CollectionUidType);
    uid = makeUid(jsonUid.get<std::string>());

    auto collections = getJsonObject(parsed, CollectionsKey, CollectionsType);

    if (collections.size() > maxNumberOfCollections) {
        throw std::invalid_argument(
                "Manifest::Manifest too many collections count:" +
                std::to_string(collections.size()));
    }

    for (const auto& collection : collections) {
        throwIfWrongType(std::string(CollectionsKey),
                         collection,
                         nlohmann::json::value_t::object);

        auto name = getJsonObject(
                collection, CollectionNameKey, CollectionNameType);
        auto uid =
                getJsonObject(collection, CollectionUidKey, CollectionUidType);

        if (validCollection(name.get<std::string>())) {
            if (find(name.get<std::string>()) != this->collections.end()) {
                throw std::invalid_argument(
                        "Manifest::Manifest duplicate collection name:" +
                        name.get<std::string>());
            }
            uid_t uidValue = makeUid(uid.get<std::string>());
            enableDefaultCollection(name.get<std::string>());
            this->collections.push_back({name.get<std::string>(), uidValue});
        } else {
            throw std::invalid_argument(
                    "Manifest::Manifest invalid collection name:" +
                    name.get<std::string>());
        }
    }
}

nlohmann::json Manifest::getJsonObject(const nlohmann::json& object,
                                       const std::string& key,
                                       nlohmann::json::value_t expectedType) {
    try {
        auto rv = object.at(key);
        throwIfWrongType(key, rv, expectedType);
        return rv;
    } catch (const nlohmann::json::exception& e) {
        throw std::invalid_argument("Manifest: cannot find key:" + key +
                                    ", e:" + e.what());
    }
}

void Manifest::throwIfWrongType(const std::string& errorKey,
                                const nlohmann::json& object,
                                nlohmann::json::value_t expectedType) {
    if (object.type() != expectedType) {
        throw std::invalid_argument("Manifest: wrong type for key:" + errorKey +
                                    ", " + object.dump());
    }
}

void Manifest::enableDefaultCollection(const std::string& name) {
    if (name.compare(DefaultCollectionIdentifier.data()) == 0) {
        defaultCollectionExists = true;
    }
}

bool Manifest::validCollection(const std::string& collection) {
    // Current validation is to just check the prefix to ensure
    // 1. $default is the only $ prefixed collection.
    // 2. _ is not allowed as the first character.

    if (collection[0] == '$' &&
        !(collection.compare(DefaultCollectionIdentifier.data()) == 0)) {
        return false;
    }
    return collection[0] != '_';
}

std::string Manifest::toJson() const {
    std::stringstream json;
    json << R"({"uid":")" << std::hex << uid << R"(","collections":[)";
    for (size_t ii = 0; ii < collections.size(); ii++) {
        json << R"({"name":")" << collections[ii].getName().data()
             << R"(","uid":")" << std::hex << collections[ii].getUid()
             << R"("})";
        if (ii != collections.size() - 1) {
            json << ",";
        }
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
    for (const auto& entry : manifest.collections) {
        os << "collection:{" << entry << "}\n";
    }
    return os;
}
}
