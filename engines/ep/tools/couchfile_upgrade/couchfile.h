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