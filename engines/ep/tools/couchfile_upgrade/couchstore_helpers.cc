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

#include "couchstore_helpers.h"

DocPtr::DocPtr() : doc(nullptr) {
}

DocPtr::~DocPtr() {
    if (doc) {
        couchstore_free_document(doc);
    }
}

Doc** DocPtr::getDocAddress() {
    return &doc;
}

Doc* DocPtr::getDoc() {
    return doc;
}

void LocalDocPtrDelete::operator()(LocalDoc* ldoc) {
    couchstore_free_local_document(ldoc);
}