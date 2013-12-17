/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2012 Couchbase, Inc
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

#ifndef SRC_COUCH_KVSTORE_DIRUTILS_H_
#define SRC_COUCH_KVSTORE_DIRUTILS_H_ 1

#include "config.h"

#include <string>
#include <vector>

namespace CouchKVStoreDirectoryUtilities
{
    /**
     * Return the directory part of an absolute path
     */
    std::string dirname(const std::string &dir);

    /**
     * Return the filename part of an absolute path
     */
    std::string basename(const std::string &name);

    /**
     * Return a vector containing all of the files starting with a given
     * name stored in a given directory
     */
    std::vector<std::string> findFilesWithPrefix(const std::string &dir, const std::string &name);

    /**
     * Return a vector containing all of the files starting with a given
     * name specified with this absolute path
     */
    std::vector<std::string> findFilesWithPrefix(const std::string &name);

    /**
     * Return a vector containing all of the files containing a given substring
     * located in a given directory
     */
    std::vector<std::string> findFilesContaining(const std::string &dir, const std::string &name);
}

#endif  // SRC_COUCH_KVSTORE_DIRUTILS_H_
