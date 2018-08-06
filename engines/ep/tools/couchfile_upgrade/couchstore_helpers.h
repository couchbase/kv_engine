/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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

#include <memory>

#include <libcouchstore/couch_db.h>

/**
 * RAII wrapper for working with couchstore Doc objects (returned from
 * couchstore_open_doc_with_docinfo).
 */
class DocPtr {
public:
    DocPtr();

    /// Calls couchstore_free_document if the doc address is set
    ~DocPtr();

    Doc** getDocAddress();
    Doc* getDoc();

private:
    Doc* doc;
};

struct LocalDocPtrDelete {
    void operator()(LocalDoc* ldoc);
};

using LocalDocPtr = std::unique_ptr<LocalDoc, LocalDocPtrDelete>;