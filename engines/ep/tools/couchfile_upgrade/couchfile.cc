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

#include "couchfile.h"

#include <iostream>
#include <utility>

CouchFile::CouchFile(OptionsSet options,
                     std::string filename,
                     couchstore_open_flags flags)
    : db(nullptr),
      filename(std::move(filename)),
      flags(flags),
      options(options) {
    open();
}

CouchFile::~CouchFile() {
    close();
}

std::string CouchFile::getFilename() const {
    return filename;
}

void CouchFile::open() {
    auto errcode = couchstore_open_db(filename.c_str(), flags, &db);

    if (errcode) {
        throw std::runtime_error(
                "CouchFile::open failed couchstore_open_db filename:" +
                filename + " errcode:" + std::to_string(errcode));
    }
    verbose("open flags:" + std::to_string(flags) + ", success");
}

void CouchFile::close() const {
    if (db) {
        auto errcode = couchstore_close_file(db);
        if (errcode) {
            throw std::runtime_error(
                    "CouchFile::close failed couchstore_close_file "
                    "filename:" +
                    filename + " errcode:" + std::to_string(errcode));
        }
        errcode = couchstore_free_db(db);
        if (errcode) {
            throw std::runtime_error(
                    "CouchFile::close failed couchstore_free_db "
                    "filename:" +
                    filename + " errcode:" + std::to_string(errcode));
        }
    }
    verbose("close success");
}

void CouchFile::verbose(const std::string& message) const {
    if (options.test(Options::Verbose)) {
        std::cout << *this << "::" << message << std::endl;
    }
}

std::ostream& operator<<(std::ostream& os, const CouchFile& couchFile) {
    os << "CouchFile(\"" << couchFile.filename << "\")";
    return os;
}
