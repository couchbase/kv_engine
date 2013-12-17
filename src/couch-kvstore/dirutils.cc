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

#include "config.h"

#ifndef _MSC_VER
#include <dirent.h>
#endif

#include <string.h>

#include "couch-kvstore/dirutils.h"

namespace CouchKVStoreDirectoryUtilities
{
    using namespace std;

    static string split(const string &input, bool directory)
    {
        string::size_type path = input.find_last_of("\\/");
        string file;
        string dir;

        if (path == string::npos) {
            dir = ".";
            file = input;
        } else {
            dir = input.substr(0, path);
            if (dir.length() == 0) {
                dir = input.substr(0, 1);
            }
            file = input.substr(path + 1);
        }

        if (directory) {
            return dir;
        } else {
            return file;
        }
    }

    string dirname(const string &dir)
    {
        return split(dir, true);
    }

    string basename(const string &name)
    {
        return split(name, false);
    }

#ifdef _MSC_VER
    vector<string> findFilesWithPrefix(const string &dir, const string &name)
    {
        vector<string> files;
        std::string match = dir + "\\" + name + "*";
        WIN32_FIND_DATA FindFileData;

        HANDLE hFind = FindFirstFileEx(match.c_str(), FindExInfoStandard,
                                       &FindFileData, FindExSearchNameMatch,
                                       NULL, 0);

        if (hFind != INVALID_HANDLE_VALUE) {
            do {
                string entry = dir;
                entry.append("\\");
                entry.append(FindFileData.cFileName);
                files.push_back(entry);
            } while (FindNextFile(hFind, &FindFileData));

            FindClose(hFind);
        }
        return files;
    }
#else
    vector<string> findFilesWithPrefix(const string &dir, const string &name)
    {
        vector<string> files;
        DIR *dp = opendir(dir.c_str());
        if (dp != NULL) {
            struct dirent *de;
            while ((de = readdir(dp)) != NULL) {
                if (strncmp(de->d_name, name.c_str(), name.length()) == 0) {
                    string entry = dir;
                    entry.append("/");
                    entry.append(de->d_name);
                    files.push_back(entry);
                }
            }

            closedir(dp);
        }
        return files;
    }
#endif

    vector<string> findFilesWithPrefix(const string &name)
    {
        return findFilesWithPrefix(dirname(name), basename(name));
    }

#ifdef _MSC_VER
    vector<string> findFilesContaining(const string &dir, const string &name)
    {
        vector<string> files;
        std::string match = dir + "\\*" + name + "*";
        WIN32_FIND_DATA FindFileData;

        HANDLE hFind = FindFirstFileEx(match.c_str(), FindExInfoStandard,
                                       &FindFileData, FindExSearchNameMatch,
                                       NULL, 0);

        if (hFind != INVALID_HANDLE_VALUE) {
            do {
                string entry = dir;
                entry.append("\\");
                entry.append(FindFileData.cFileName);
                files.push_back(entry);
            } while (FindNextFile(hFind, &FindFileData));

            FindClose(hFind);
        }
        return files;
    }
#else
    vector<string> findFilesContaining(const string &dir, const string &name)
    {
        vector<string> files;
        DIR *dp = opendir(dir.c_str());
        if (dp != NULL) {
            struct dirent *de;
            while ((de = readdir(dp)) != NULL) {
                if (name.empty() || strstr(de->d_name, name.c_str()) != NULL) {
                    string entry = dir;
                    entry.append("/");
                    entry.append(de->d_name);
                    files.push_back(entry);
                }
            }

            closedir(dp);
        }

        return files;
    }
#endif
}
