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
#include <cJSON_utils.h>

#include <cstring>
#include <iostream>

namespace Collections {

Manifest::Manifest(const std::string& json, size_t maxNumberOfCollections)
    : defaultCollectionExists(false) {
    if (!checkUTF8JSON(reinterpret_cast<const unsigned char*>(json.data()),
                       json.size())) {
        throw std::invalid_argument("Manifest::Manifest input not valid json:" +
                                    json);
    }

    unique_cJSON_ptr cjson(cJSON_Parse(json.c_str()));
    if (!cjson) {
        throw std::invalid_argument(
                "Manifest::Manifest cJSON cannot parse json:" + json);
    }

    auto* jsonSeparator =
            getJsonObject(cjson.get(), SeparatorKey, SeparatorType);

    if (validSeparator(jsonSeparator->valuestring)) {
        separator = jsonSeparator->valuestring;
    } else {
        throw std::invalid_argument("Manifest::Manifest invalid separator:" +
                                    std::string(jsonSeparator->valuestring));
    }

    auto jsonCollections =
            getJsonObject(cjson.get(), CollectionsKey, CollectionsType);

    size_t count = cJSON_GetArraySize(jsonCollections);
    if (count > maxNumberOfCollections) {
        throw std::invalid_argument(
                "Manifest::Manifest too many collections count:" +
                std::to_string(count));
    }

    for (size_t ii = 0; ii < count; ii++) {
        auto collection = cJSON_GetArrayItem(jsonCollections, ii);
        throwIfNullOrWrongType(
                std::string(CollectionsKey) + ":" + std::to_string(ii),
                collection,
                cJSON_Object);

        auto* name = getJsonObject(
                collection, CollectionNameKey, CollectionNameType);
        auto* uid =
                getJsonObject(collection, CollectionUidKey, CollectionUidType);

        if (validCollection(name->valuestring)) {
            if (find(name->valuestring) != collections.end()) {
                throw std::invalid_argument(
                        "Manifest::Manifest duplicate collection name:" +
                        std::string(name->valuestring));
            }
            uid_t uidValue = makeUid(uid->valuestring);
            enableDefaultCollection(name->valuestring);
            collections.push_back({name->valuestring, uidValue});
        } else {
            throw std::invalid_argument(
                    "Manifest::Manifest invalid collection name:" +
                    std::string(name->valuestring));
        }
    }
}

cJSON* Manifest::getJsonObject(cJSON* json, const char* key, int expectedType) {
    auto* rv = cJSON_GetObjectItem(json, key);
    throwIfNullOrWrongType(key, rv, expectedType);
    return rv;
}

void Manifest::throwIfNullOrWrongType(const std::string& errorKey,
                                      cJSON* cJsonHandle,
                                      int expectedType) {
    if (!cJsonHandle || cJsonHandle->type != expectedType) {
        throw std::invalid_argument(
                "Manifest: cannot find valid " + errorKey + ": " +
                (!cJsonHandle
                         ? "nullptr"
                         : "wrong type:" + std::to_string(cJsonHandle->type)));
    }
}

void Manifest::enableDefaultCollection(const char* name) {
    if (std::strncmp(name,
                     DefaultCollectionIdentifier.data(),
                     DefaultCollectionIdentifier.size()) == 0) {
        defaultCollectionExists = true;
    }
}

bool Manifest::validSeparator(const char* separator) {
    size_t size = std::strlen(separator);
    return size > 0 && size <= 16;
}

bool Manifest::validCollection(const char* collection) {
    // Current validation is to just check the prefix to ensure
    // 1. $default is the only $ prefixed collection.
    // 2. _ is not allowed as the first character.

    if (collection[0] == '$' && !(DefaultCollectionIdentifier == collection)) {
        return false;
    }
    return collection[0] != '_';
}

void Manifest::dump() const {
    std::cerr << *this << std::endl;
}

std::ostream& operator<<(std::ostream& os, const Manifest& manifest) {
    os << "Collections::Manifest"
       << ", defaultCollectionExists:" << manifest.defaultCollectionExists
       << ", separator:" << manifest.separator
       << ", collections.size:" << manifest.collections.size() << std::endl;
    for (const auto& entry : manifest.collections) {
        os << "collection:{" << entry << "}\n";
    }
    return os;
}
}
