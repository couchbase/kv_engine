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
#include "config.h"
#include "rbac_impl.h"
#include "config_util.h"
#include "settings.h"
#include "utilities/protocol2text.h"

#include <strings.h>
#include <map>
#include <sstream>
#include <iostream>
#include <list>
#include <algorithm>

std::ostream& operator <<(std::ostream &out, const AuthContext &context)
{
    out << "name=\"" << context.name
        << "\" role=\"" << context.role
        << "\" connection=[" << context.connection << "]";
    return out;
}

void UserEntry::initialize(cJSON *root, bool _role) {
    role = _role;
    if (role) {
        name = getStringField(root, "name");
    } else {
        name = getStringField(root, "login");
    }

    parseStringList(root, "buckets", buckets);
    parseStringList(root, "profiles", profiles);
    parseStringList(root, "roles", roles);
}

void UserEntry::parseStringList(cJSON *root, const char *field, StringList &list) {
    cJSON *obj = cJSON_GetObjectItem(root, field);

    if (obj == nullptr) {
        // object not present. Not fatal
        return;
    }

    if (obj->type == cJSON_String) {
        list.push_back(obj->valuestring);
    } else if (obj->type != cJSON_Array && obj->type != cJSON_Object) {
        std::stringstream ss;
        ss << "FATAL: Invalid object for " << field << " in object: ";
        char *ptr = cJSON_PrintUnformatted(root);
        ss << ptr;
        cJSON_Free(ptr);
        throw ss.str();
    }

    obj = obj->child;
    while (obj) {
        if (obj->type == cJSON_String) {
            list.push_back(obj->valuestring);
        } else {
            std::stringstream ss;
            ss << "FATAL: Invalid object for " << field << " in object: ";
            char *ptr = cJSON_PrintUnformatted(root);
            ss << ptr;
            cJSON_Free(ptr);
            throw ss.str();
        }

        obj = obj->next;
    }
}

std::string UserEntry::getStringField(cJSON *root, const char *field) {
    cJSON *obj = cJSON_GetObjectItem(root, field);
    if (obj == nullptr) {
        std::stringstream ss;
        ss << "FATAL: failed to locate " << field << " in object: ";
        char *ptr = cJSON_PrintUnformatted(root);
        ss << ptr;
        cJSON_Free(ptr);
        throw ss.str();
    }

    if (obj->type != cJSON_String) {
        std::stringstream ss;
        ss << "FATAL: " << field << " is not a string value in object: ";
        char *ptr = cJSON_PrintUnformatted(root);
        ss << ptr;
        cJSON_Free(ptr);
        throw ss.str();
    }

    return obj->valuestring;
}

void Profile::throwError(const std::string &prefix, const std::string &value) {
    throw prefix + ": " + value;
}

void Profile::throwError(const std::string &prefix, int value) {
    throwError(prefix, std::to_string(value));
}

uint8_t Profile::decodeOpcode(cJSON *c) {
    uint8_t opcode = 0xff;

    switch (c->type) {
    case cJSON_String:
        opcode = memcached_text_2_opcode(c->valuestring);
        if (opcode == 0xff) {
            throwError("Invalid value specified for \"opcode\"", c->valuestring);
        }
        break;
    case cJSON_Number:
        if (c->valueint < 0 || c->valueint > 0xff) {
            throwError("Illegal value specified for \"opcode\"", c->valueint);
        } else {
            opcode = static_cast<uint8_t>(c->valueint);
        }
        break;
    default:
        throwError("Invalid type specified for \"opcode\"",
                   "Must be string or number");
    }

    return opcode;
}

void Profile::parseCommands(cJSON *c)
{
    if (c == nullptr) {
        return;
    }

    if (c->type == cJSON_String) {
        if (strcasecmp("all", c->valuestring) == 0) {
            cmd.fill(0xff);
        } else if (strcasecmp("none", c->valuestring) != 0) {
            cmd[decodeOpcode(c)] = 0xff;
        }
    } else if (c->type == cJSON_Number) {
        cmd[decodeOpcode(c)] = 0xff;
    } else if (c->type == cJSON_Array || c->type == cJSON_Object) {
        c = c->child;
        while (c) {
            cmd[decodeOpcode(c)] = 0xff;
            c = c->next;
        }
    } else {
        throw std::string("Invalid type for \"opcode\": Must be string or number");
    }
}

void Profile::initialize(cJSON *root) {
    cJSON *c = cJSON_GetObjectItem(root, "name");
    if (c == nullptr) {
        throw std::string("missing name");
    }
    name.assign(c->valuestring);

    c = cJSON_GetObjectItem(root, "description");
    if (c == nullptr) {
        throw std::string("missing description");
    }
    descr.assign(c->valuestring);

    root = cJSON_GetObjectItem(root, "memcached");
    if (root != nullptr) {
        parseCommands(cJSON_GetObjectItem(root, "opcode"));
    }
}

RBACManager::RBACManager() : privilegeDebugging(false), generation(0) {
}

RBACManager::~RBACManager() {
}

void RBACManager::applyProfiles(AuthContext *ctx, const StringList &pf) {
    ctx->clearCommands();
    StringList::const_iterator ii;
    for (ii = pf.begin(); ii != pf.end(); ++ii) {
        ProfileMap::const_iterator pi;
        pi = profiles.find(*ii);
        if (pi != profiles.end()) {
            ctx->mergeCommands(pi->second.getCommands());
        }
    }
}

AuthContext *RBACManager::createAuthContext(const std::string name,
                                            const std::string &_conn) {
    AuthContext *ret;

    std::lock_guard<std::mutex> lock(mutex);
    UserEntryMap::iterator iter = users.find(name);
    if (iter == users.end()) {
        ret = nullptr;
    } else {
        ret = new AuthContext(generation, name, _conn);
        applyProfiles(ret, iter->second.getProfiles());
    }

    return ret;
}

bool RBACManager::assumeRole(AuthContext *ctx, const std::string &role) {
    std::lock_guard<std::mutex> lock(mutex);
    if (ctx->getGeneration() != generation) {
        // this is an old generation of the config!
        // @todo add proper objects
        throw std::string("Stale configuration");
    }

    bool found = false;

    if (ctx->getRole().length() == 0) {
        // We're running as a user
        UserEntry &user = users[ctx->getName()];

        // search for the role in the users roles...
        StringList::const_iterator iter;
        iter = std::find(user.getRoles().begin(), user.getRoles().end(), role);
        if (iter != user.getRoles().end()) {
            found = true;
        }
    } else {
        // user didn't have it in his role list... search through
        // the current role..
        UserEntryMap::const_iterator ii = roles.find(ctx->getRole());
        if (ii != roles.end()) {
            StringList::const_iterator iter;
            iter = std::find(ii->second.getRoles().begin(),
                             ii->second.getRoles().end(), role);
            if (iter != ii->second.getRoles().end()) {
                found = true;
            }
        }
    }

    UserEntryMap::const_iterator iter = roles.find(role);
    if (found && iter != roles.end()) {
        // The effective user/role had the requested role listed..
        // apply the allowed commands to the context
        ctx->setRole(role);
        applyProfiles(ctx, iter->second.getProfiles());
    }

    return found;
}

void RBACManager::dropRole(AuthContext *ctx) {
    std::lock_guard<std::mutex> lock(mutex);
    if (ctx->getGeneration() != generation) {
        // this is an old generation of the config!
        // @todo add proper objects
        throw std::string("Stale configuration");
    }

    UserEntryMap::iterator iter = users.find(ctx->getName());
    cb_assert(iter != users.end());
    applyProfiles(ctx, iter->second.getProfiles());
    ctx->setRole("");
}

void RBACManager::initialize(cJSON *root) {
    roles.clear();
    profiles.clear();

    initializeUserEntry(cJSON_GetObjectItem(root, "roles"), true);
    initializeUserEntry(cJSON_GetObjectItem(root, "users"), false);
    initializeProfiles(cJSON_GetObjectItem(root, "profiles"));
    ++generation;
}

/**
 * Initialize all of the users/roles from the provided JSON
 *
 * @param root the root object of the JSON array
 * @param roles true if this is roles, false for users
 */
void RBACManager::initializeUserEntry(cJSON *root, bool role) {
    if (root == nullptr) {
        return;
    }

    if (root->type != cJSON_Array && root->type != cJSON_Object) {
        std::stringstream ss;
        ss << "FATAL: invalid object type provided for ";
        if (role) {
            ss << "roles";
        } else {
            ss << "users";
        }
        ss << ": ";
        char *ptr = cJSON_PrintUnformatted(root);
        ss << ptr;
        cJSON_Free(ptr);
        throw ss.str();
    }

    cJSON *obj = root->child;
    while (obj) {
        if (obj->type != cJSON_Array && obj->type != cJSON_Object) {
            std::stringstream ss;
            ss << "FATAL: invalid object type provided for ";
            if (role) {
                ss << "roles";
            } else {
                ss << "users";
            }
            ss << ": ";
            char *ptr = cJSON_PrintUnformatted(obj);
            ss << ptr;
            cJSON_Free(ptr);
            throw ss.str();
        }

        UserEntry entry;
        entry.initialize(obj, role);
        if (role) {
            roles[entry.getName()] = entry;
        } else {
            users[entry.getName()] = entry;
        }
        obj = obj->next;
    }
}

void RBACManager::initializeProfiles(cJSON *root) {
    if (root == nullptr) {
        return ;
    }
    if (root->type != cJSON_Array && root->type != cJSON_Object) {
        std::stringstream ss;
        ss << "FATAL: invalid object type provided for profile:";
        char *ptr = cJSON_PrintUnformatted(root);
        ss << ptr;
        cJSON_Free(ptr);
        throw ss.str();
    }

    cJSON *obj = root->child;
    while (obj) {
        if (obj->type != cJSON_Array && obj->type != cJSON_Object) {
            std::stringstream ss;
            ss << "FATAL: invalid object type provided for profile:";
            char *ptr = cJSON_PrintUnformatted(root);
            ss << ptr;
            cJSON_Free(ptr);
            throw ss.str();
            throw ss.str();
        }

        Profile entry;
        entry.initialize(obj);
        profiles[entry.getName()] = entry;
        obj = obj->next;
    }
}

static RBACManager rbac;

/* **********************************************************************
** **                                                                  **
** **                 public authentication api                        **
** **                                                                  **
** *********************************************************************/
AuthContext* auth_create(const char* user,
                         const char *peer,
                         const char *local)
{
    AuthContext *ret;
    std::string connection;
    if (peer) {
        std::stringstream ss;
        ss << peer << " => " << local;
        connection.assign(ss.str());
    }

    if (user == nullptr) {
        ret = rbac.createAuthContext("*", connection);
    } else {
        ret = rbac.createAuthContext(user, connection);
        if (ret == nullptr) {
            // No entry for the explicit user.. do we have a "wild card"
            // entry?
            ret = rbac.createAuthContext("*", connection);
        }
    }

    return ret;
}

void auth_destroy(AuthContext* context)
{
    delete context;
}

AuthResult auth_assume_role(AuthContext* ctx, const char *role)
{
    if (ctx == nullptr) {
        return AuthResult::FAIL;
    }

    AuthResult ret;
    try {
        if (rbac.assumeRole(ctx, role)) {
            ret = AuthResult::OK;
        } else {
            ret = AuthResult::FAIL;
        }
    } catch (std::string) {
        ret = AuthResult::STALE;
    }

    return ret;
}

AuthResult auth_drop_role(AuthContext* ctx)
{
    if (ctx == nullptr) {
        return AuthResult::FAIL;
    }

    AuthResult ret;
    try {
        rbac.dropRole(ctx);
        ret = AuthResult::OK;
    } catch (std::string) {
        ret = AuthResult::STALE;
    }

    return ret;
}

AuthResult auth_check_access(AuthContext* context, uint8_t opcode)
{
    if (context == nullptr) {
        return AuthResult::FAIL;
    }

    // sloppy check to avoid lock contention
    if (rbac.getGeneration() != context->getGeneration()) {
        return AuthResult::STALE;
    } else if (context->checkAccess(opcode)) {
        return AuthResult::OK;
    } else {
        if (rbac.isPrivilegeDebugging()) {
            std::stringstream ss;
            ss << "Missing privilege for " << *context << ". Need "
               << memcached_opcode_2_text(opcode) << std::endl;
            auto logger = settings.extensions.logger;
            logger->log(EXTENSION_LOG_NOTICE, context,
                        "%s", ss.str().c_str());
            return AuthResult::OK;
        } else {
            return AuthResult::FAIL;
        }
    }
}

void auth_set_privilege_debug(bool enable) {
    rbac.setPrivilegeDebugging(enable);
}

int load_rbac_from_file(const char *file)
{
    cJSON *root;
    if (file == nullptr) {
        root = getDefaultRbacConfig();
    } else {
        config_error_t err = config_load_file(file, &root);
        if (err != CONFIG_SUCCESS) {
            fprintf(stderr, "Failed to read profiles: %s\n",
                    config_strerror(file, err));
            return -1;
        }
    }

    try {
        rbac.initialize(root);
    } catch (std::string err) {
        std::cerr << err << std::endl;
    }
    cJSON_Delete(root);
    return 0;
}
