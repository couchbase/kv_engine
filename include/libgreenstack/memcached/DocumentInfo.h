/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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

#include <libgreenstack/memcached/Cas.h>
#include <libgreenstack/memcached/Compression.h>
#include <libgreenstack/memcached/Datatype.h>

#include <string>

namespace Greenstack {

    /**
     * The DocumentInfo class contains all of the meta information available
     * for a Document to be stored in Couchbase
     */
    class DocumentInfo {
    public:
        DocumentInfo();

        const std::string& getId() const {
            return id;
        }

        void setId(const std::string& id) {
            DocumentInfo::id = id;
        }

        uint32_t getFlags() const {
            return flags;
        }

        void setFlags(uint32_t flags) {
            DocumentInfo::flags = flags;
        }

        const std::string& getExpiration() const {
            return expiration;
        }

        void setExpiration(const std::string& expiration) {
            DocumentInfo::expiration = expiration;
        }

        Compression getCompression() const {
            return compression;
        }

        void setCompression(const Compression& compression) {
            DocumentInfo::compression = compression;
        }

        Datatype getDatatype() const {
            return datatype;
        }

        void setDatatype(const Datatype& datatype) {
            DocumentInfo::datatype = datatype;
        }


        cas_t getCas() const {
            return cas;
        }

        void setCas(cas_t cas) {
            DocumentInfo::cas = cas;
        }

    protected:
        std::string id;
        uint32_t flags;
        std::string expiration;
        Compression compression;
        Datatype datatype;
        cas_t cas;
    };
}