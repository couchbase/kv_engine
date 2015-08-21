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

#include <libgreenstack/memcached/DocumentInfo.h>
#include <libgreenstack/Buffer.h>

namespace Greenstack {
    /**
     * The Document class represents a document as stored in Couchbase.
     * It consists of a DocumetInfo section containing the meta information
     * for the document, and a Buffer containing the value.
     */
    class Document {
    public:
        Document(DocumentInfo& info, Buffer& val)
            : documentInfo(info),
              value(val) { }

        const DocumentInfo& getDocumentInfo() const {
            return documentInfo;
        }

        const Buffer& getValue() const {
            return value;
        }

    private:
        DocumentInfo& documentInfo;
        Buffer& value;
    };
}