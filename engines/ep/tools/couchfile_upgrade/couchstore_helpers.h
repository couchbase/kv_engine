/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
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