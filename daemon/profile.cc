/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "config.h"
#include "profile.h"
#include "config_util.h"
#include "utilities/protocol2text.h"

#include <map>
#include <sstream>

#include <iostream>

static cb_mutex_t mutex;
static std::map<std::string, profile_t*> profiles;

void initialize_profiles(void)
{
    cb_mutex_initialize(&mutex);
}

int load_profiles_from_file(const char *file)
{
    if (file == NULL) {
        return 0;
    }

    cJSON *root;
    config_error_t err = config_load_file(file, &root);
    if (err != CONFIG_SUCCESS) {
        fprintf(stderr, "Failed to read profiles: %s\n",
                config_strerror(file, err));
        return -1;
    }

    int ret = load_profiles_from_json(root);
    cJSON_Delete(root);
    return ret;
}

static void throwError(const std::string &prefix,
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

static void throwError(const std::string &prefix, uint8_t mask, int value) {
    std::stringstream ss;
    ss << value;
    throwError(prefix, mask, ss.str().c_str());
}

static uint8_t decodeOpcode(cJSON *c, uint8_t mask) {
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

void profile_st::parseCommands(cJSON *c, uint8_t mask)
{
    if (c == NULL) {
        throw std::string("could not find attribute");
        return;
    }

    if (c->type == cJSON_String) {
        if (strcasecmp("all", c->valuestring) == 0) {
            memset(cmd, mask, sizeof(cmd));
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

void profile_st::initialize(cJSON *root)
{
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
        parseCommands(cJSON_GetObjectItem(root, "allow"), true);
        parseCommands(cJSON_GetObjectItem(root, "disallow"), false);
    }
}

void loadProfilesFromJson(cJSON *root) {
    if (root == NULL) {
        throw std::string("root can't be null");
    }

    cJSON *obj = cJSON_GetObjectItem(root, "profiles");
    if (obj == NULL) {
        std::stringstream ss;
        ss << "No profile listed in JSON: \"" << cJSON_PrintUnformatted(root)
           << "\"";
        throw ss.str();
    }

    switch (obj->type) {
    case cJSON_Object:
    case cJSON_Array:
        break;
    default:
        {
            std::stringstream ss;
            ss << "Invalid object type for profiles: " << obj->type;
            throw ss.str();
        }
    }

    obj = obj->child;
    std::map<std::string, void*> names;

    while (obj != NULL) {
        profile_st *p = new profile_st;
        cb_assert(p != NULL);

        try {
            p->initialize(obj);
        } catch (std::string &str) {
            p->deleted = true;
            delete p;
            throw str;
        }

        std::map<std::string, profile_t*>::iterator iter;
        cb_mutex_enter(&mutex);
        iter = profiles.find(p->name);
        names[p->name] = NULL;
        if (iter == profiles.end()) {
            profiles[p->name] = p;
        } else {
            cb_mutex_enter(&iter->second->mutex);
            memcpy(iter->second->cmd, p->cmd, sizeof(p->cmd));
            iter->second->deleted = false;
            cb_mutex_exit(&iter->second->mutex);
            p->deleted = true;
            delete p;
        }
        cb_mutex_exit(&mutex);

        obj = obj->next;
    }

    // lets mark all thats no longer loaded as deleted..
    cb_mutex_enter(&mutex);
    std::map<std::string, profile_t*>::iterator iter;
    for (iter = profiles.begin(); iter != profiles.end(); ++iter) {
        if (names.find(iter->second->name) == names.end()) {
            cb_mutex_enter(&iter->second->mutex);
            iter->second->deleted = true;
            cb_mutex_exit(&iter->second->mutex);
        }
    }
    cb_mutex_exit(&mutex);
}


int load_profiles_from_json(cJSON *root) {
    try {
        loadProfilesFromJson(root);
    } catch (std::string &error) {
        std::cerr << "ERROR: " << error << std::endl;
        std::cerr.flush();
        return -1;
    }

    return 0;
}

/**
 * Destroy all profiles and release all resources.
 */
void destroy_profiles(void)
{
    std::map<std::string, profile_t*>::iterator iter;
    for (iter = profiles.begin(); iter != profiles.end(); ++iter) {
        (iter->second)->refcount = 0;
        (iter->second)->deleted = true;
        delete iter->second;
    }
    profiles.erase(profiles.begin(), profiles.end());
    cb_mutex_destroy(&mutex);
}

profile_t *find_profile(const char *name)
{
    profile_t *ret = NULL;
    profile_t *del = NULL;

    cb_mutex_enter(&mutex);
    std::map<std::string, profile_t*>::iterator iter;
    iter = profiles.find(name);
    if (iter != profiles.end()) {
        ret = iter->second;
        cb_mutex_enter(&ret->mutex);
        if (ret->deleted) {
            if (ret->refcount == 0) {
                del = ret;
            }
            ret = NULL;
        } else {
            ++ret->refcount;
        }
        cb_mutex_exit(&iter->second->mutex);
    }

    if (del != NULL) {
        profiles.erase(iter);
        delete del;
    }

    cb_mutex_exit(&mutex);
    return ret;
}

void release_profile(profile_t *profile)
{
    bool del = false;
    cb_mutex_enter(&mutex);
    cb_mutex_enter(&profile->mutex);
    --profile->refcount;
    if (profile->deleted && profile->refcount == 0) {
        del = true;
    }
    cb_mutex_exit(&profile->mutex);

    if (del) {
        std::map<std::string, profile_t*>::iterator iter;
        iter = profiles.find(profile->name);
        cb_assert(iter != profiles.end());
        profiles.erase(iter);

        delete profile;
    }

    cb_mutex_exit(&mutex);
}
