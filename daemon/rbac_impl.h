/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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

/**
 * This file contains the private internals of the RBAC module
 */
#include <cstring>
#include <string>
#include <list>
#include <map>
#include <array>
#include <cJSON.h>
#include <atomic>
#include <ostream>
#include <mutex>

// Include public interface
#include "rbac.h"

class UserEntry;
class Profile;

typedef std::list<std::string> StringList;
typedef std::map<std::string, UserEntry> UserEntryMap;
typedef std::map<std::string, Profile> ProfileMap;

class UserEntry {
public:
    void initialize(cJSON *root, bool _role);

    const std::string &getName() const {
        return name;
    }

    const StringList &getBuckets() const {
        return buckets;
    }

    const StringList &getRoles() const {
        return roles;
    }

    const StringList &getProfiles() const {
        return profiles;
    }

private:
    void parseStringList(cJSON *root, const char *field, StringList &list);
    std::string getStringField(cJSON *root, const char *field);

    std::string name;

    // Is it a role or a user?
    bool role;

    // A user/role may contain roles
    StringList roles;
    // A user/role may contain a list of profiles
    StringList profiles;
    // A user/role may contain a list of buckets
    StringList buckets;
};

class Profile {
public:
    Profile() {
        cmd.fill(0);
    }

    const std::string &getName() const {
        return name;
    }

    const std::array<uint8_t, MAX_COMMANDS> &getCommands() const {
        return cmd;
    }

    void initialize(cJSON *root);

private:
    void parseCommands(cJSON *obj);
    uint8_t decodeOpcode(cJSON *c);
    void throwError(const std::string &prefix, int value);
    void throwError(const std::string &prefix, const std::string &value);

    std::array<uint8_t, MAX_COMMANDS> cmd;
    std::string name;
    std::string descr;
};


class RBACManager {
public:
    RBACManager();
    ~RBACManager();

    AuthContext *createAuthContext(const std::string name,
                                   const std::string &_connection);

    void initialize(cJSON *root);

    uint32_t getGeneration() {
        return generation.load();
    }

    bool isPrivilegeDebugging() {
        return privilegeDebugging.load();
    }

    void setPrivilegeDebugging(bool enable) {
        privilegeDebugging.store(enable);
    }

private:
    void initializeUserEntry(cJSON *root, bool role);
    void initializeProfiles(cJSON *root);

    void applyProfiles(AuthContext *ctx, const StringList &pf);

    std::atomic<bool> privilegeDebugging;
    std::atomic<uint32_t> generation;
    std::mutex mutex;
    UserEntryMap roles;
    UserEntryMap users;
    ProfileMap profiles;

};

cJSON *getDefaultRbacConfig(void);
