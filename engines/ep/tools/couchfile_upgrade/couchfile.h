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

#include "couchstore_helpers.h"
#include "options.h"

#include <libcouchstore/couch_db.h>

constexpr const char* NamespacesSupportedKey = "namespaces_supported";

/**
 * CouchFile manages access to a couchfile, hiding the couchstore API calls
 * for open/close/read/write.
 */
class CouchFile {
public:
    /**
     * Open filename using the couchstore flags
     * @throws runtime_error for any error returned from couchstore_open_db
     */
    CouchFile(OptionsSet options,
              std::string filename,
              couchstore_open_flags flags = 0);

    ~CouchFile();

    std::string getFilename() const;

protected:
    void open();

    void close() const;

    /**
     * Write the message to stdout along with a filename prefix, but only
     * if Options::Verbose is selected.
     */
    void verbose(const std::string& message) const;

    friend std::ostream& operator<<(std::ostream& os,
                                    const CouchFile& couchFile);

    Db* db;
    const std::string filename;
    couchstore_open_flags flags;
    OptionsSet options;
};

std::ostream& operator<<(std::ostream& os, const CouchFile& couchFile);