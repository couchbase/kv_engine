/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <dirent.h>
#include "couch-kvstore/dirutils.h"
#include <string.h>

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

    vector<string> findFilesWithPrefix(const string &name)
    {
        return findFilesWithPrefix(dirname(name), basename(name));
    }

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
}
