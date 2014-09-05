/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "config.h"
#include "rbac.h"
#include "config_util.h"
#include "utilities/protocol2text.h"

#include <strings.h>
#include <map>
#include <sstream>
#include <iostream>
#include <list>
#include <algorithm>

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

    if (obj == NULL) {
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
    if (obj == NULL) {
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

void Profile::throwError(const std::string &prefix,
                         uint8_t mask,
                         const char *value) {
    std::stringstream ss;
    ss << prefix << "\"";
    if (mask) {
        ss << "allow";
    } else {
        ss << "disallow";
    }

    ss << "\": \"" << value << "\"";
    throw ss.str();
}

void Profile::throwError(const std::string &prefix, uint8_t mask, int value) {
    std::stringstream ss;
    ss << value;
    throwError(prefix, mask, ss.str().c_str());
}

uint8_t Profile::decodeOpcode(cJSON *c, uint8_t mask) {
    uint8_t opcode = 0xff;

    switch (c->type) {
    case cJSON_String:
        opcode = memcached_text_2_opcode(c->valuestring);
        if (opcode == 0xff) {
            throwError("Invalid value specified for ", mask, c->valuestring);
        }
        break;
    case cJSON_Number:
        if (c->valueint < 0 || c->valueint > 0xff) {
            throwError("Illegal value specified for ", mask, c->valueint);
        } else {
            opcode = static_cast<uint8_t>(c->valueint);
        }
        break;
    default:
        throwError("Invalid value specified for ", mask, "not an opcode");
    }

    return opcode;
}

void Profile::parseCommands(cJSON *c, uint8_t mask)
{
    if (c == NULL) {
        return;
    }

    if (c->type == cJSON_String) {
        if (strcasecmp("all", c->valuestring) == 0) {
            cmd.fill(mask);
        } else if (strcasecmp("none", c->valuestring) != 0) {
            cmd[decodeOpcode(c, mask)] = mask;
        }
    } else if (c->type == cJSON_Number) {
        cmd[decodeOpcode(c, mask)] = mask;
    } else if (c->type == cJSON_Array || c->type == cJSON_Object) {
        c = c->child;
        while (c) {
            cmd[decodeOpcode(c, mask)] = mask;
            c = c->next;
        }
    } else {
        throw std::string("Invalid type for allow");
    }
}

void Profile::initialize(cJSON *root) {
    cJSON *c = cJSON_GetObjectItem(root, "name");
    if (c == NULL) {
        throw std::string("missing name");
    }
    name.assign(c->valuestring);

    c = cJSON_GetObjectItem(root, "description");
    if (c == NULL) {
        throw std::string("missing description");
    }
    descr.assign(c->valuestring);

    root = cJSON_GetObjectItem(root, "memcached");
    if (root != NULL) {
        parseCommands(cJSON_GetObjectItem(root, "allow"), 0xff);
        parseCommands(cJSON_GetObjectItem(root, "disallow"), 0x00);
    }
}

RBACManager::RBACManager() : generation(0) {
    cb_mutex_initialize(&mutex);
}

RBACManager::~RBACManager() {
    cb_mutex_destroy(&mutex);
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

AuthContext *RBACManager::createAuthContext(const std::string name) {
    AuthContext *ret;

    cb_mutex_enter(&mutex);
    UserEntryMap::iterator iter = users.find(name);
    if (iter == users.end()) {
        ret = NULL;
    } else {
        ret = new AuthContext(generation, name);
        applyProfiles(ret, iter->second.getProfiles());
    }
    cb_mutex_exit(&mutex);

    return ret;
}

bool RBACManager::assumeRole(AuthContext *ctx, const std::string &role) {
    cb_mutex_enter(&mutex);
    if (ctx->getGeneration() != generation) {
        // this is an old generation of the config!
        cb_mutex_exit(&mutex);
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

    cb_mutex_exit(&mutex);

    return found;
}

void RBACManager::dropRole(AuthContext *ctx) {
    cb_mutex_enter(&mutex);
    if (ctx->getGeneration() != generation) {
        // this is an old generation of the config!
        cb_mutex_exit(&mutex);
        // @todo add proper objects
        throw std::string("Stale configuration");
    }

    UserEntryMap::iterator iter = users.find(ctx->getName());
    assert(iter != users.end());
    applyProfiles(ctx, iter->second.getProfiles());
    ctx->setRole("");

    cb_mutex_exit(&mutex);
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
    if (root == NULL) {
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
    if (root == NULL) {
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
auth_context_t auth_create(const char* user)
{
    AuthContext *ret;

    if (user == NULL) {
        ret = rbac.createAuthContext("*");
    } else {
        ret = rbac.createAuthContext(user);
        if (ret == NULL) {
            // No entry for the explicit user.. do we have a "wild card"
            // entry?
            ret = rbac.createAuthContext("*");
        }
    }

    return reinterpret_cast<auth_context_t>(ret);
}

void auth_destroy(auth_context_t context)
{
    delete reinterpret_cast<AuthContext*>(context);
}

auth_error_t auth_assume_role(auth_context_t ctx, const char *role)
{
    if (ctx == NULL) {
        return AUTH_FAIL;
    }

    auth_error_t ret;
    try {
        if (rbac.assumeRole(reinterpret_cast<AuthContext*>(ctx), role)) {
            ret = AUTH_OK;
        } else {
            ret = AUTH_FAIL;
        }
    } catch (std::string) {
        ret = AUTH_STALE;
    }

    return ret;
}

auth_error_t auth_drop_role(auth_context_t ctx)
{
    if (ctx == NULL) {
        return AUTH_FAIL;
    }

    auth_error_t ret;
    try {
        rbac.dropRole(reinterpret_cast<AuthContext*>(ctx));
        ret = AUTH_OK;
    } catch (std::string) {
        ret = AUTH_STALE;
    }

    return ret;
}

auth_error_t auth_check_access(auth_context_t ctx, uint8_t opcode)
{
    if (ctx == NULL) {
        return AUTH_FAIL;
    }

    AuthContext *context = reinterpret_cast<AuthContext*>(ctx);

    // sloppy check to avoid lock contention
    if (rbac.getGeneration() != context->getGeneration()) {
        return AUTH_STALE;
    } else if (context->checkAccess(opcode)) {
        return AUTH_OK;
    } else {
        return AUTH_FAIL;
    }
}

int load_rbac_from_file(const char *file)
{
    if (file == NULL) {
        fprintf(stderr, "FATAL! No RBAC file specified");
        return -1;
    }

    cJSON *root;
    config_error_t err = config_load_file(file, &root);
    if (err != CONFIG_SUCCESS) {
        fprintf(stderr, "Failed to read profiles: %s\n",
                config_strerror(file, err));
        return -1;
    }

    try {
        rbac.initialize(root);
    } catch (std::string err) {
        std::cerr << err << std::endl;
    }
    cJSON_Delete(root);
    return 0;
}
