/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef SRC_COUCH_KVSTORE_DIRUTILS_H_
#define SRC_COUCH_KVSTORE_DIRUTILS_H_ 1

#include <string>
#include <vector>

namespace CouchKVStoreDirectoryUtilities
{
    using namespace std;

    /**
     * Return the directory part of an absolute path
     */
    string dirname(const string &dir);

    /**
     * Return the filename part of an absolute path
     */
    string basename(const string &name);

    /**
     * Return a vector containing all of the files starting with a given
     * name stored in a given directory
     */
    vector<string> findFilesWithPrefix(const string &dir, const string &name);

    /**
     * Return a vector containing all of the files starting with a given
     * name specified with this absolute path
     */
    vector<string> findFilesWithPrefix(const string &name);

    /**
     * Return a vector containing all of the files containing a given substring
     * located in a given directory
     */
    vector<string> findFilesContaining(const string &dir, const string &name);
}

#endif  // SRC_COUCH_KVSTORE_DIRUTILS_H_
