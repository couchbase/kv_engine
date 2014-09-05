/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#pragma once

#include <string>
#include <list>
#include <map>
#include <array>

#ifdef HAVE_ATOMIC
#include <atomic>
#else
#include <cstdatomic>
#endif

class UserEntry;
class AuthContext;
class Profile;

typedef std::list<std::string> StringList;
typedef std::map<std::string, UserEntry> UserEntryMap;
typedef std::map<std::string, Profile> ProfileMap;

#define MAX_COMMANDS 0x100

/**
 * The Authentication Context class is used as a "holder class" for the
 * authentication information available used for a connection in memcached.
 * It is created when the client connects (and recreated if the client
 * performs a SASL authentication to acquire another user identity). It
 * contains the available commands the client may perform, and the current
 * role it is running as.
 *
 * Clients may "assume" another role causing the effective privilege set
 * to be changed gaining access to additional buckets, roles and commands.
 */
class AuthContext {
public:
    AuthContext(uint32_t gen, const std::string &nm) :
        name(nm), generation(gen)
    {
        commands.fill(0);
    }

    const std::string &getName(void) const {
        return name;
    }

    void setRole(const std::string &_role) {
        role.assign(_role);
    }

    const std::string &getRole(void) const {
        return role;
    }

    uint32_t getGeneration(void) const {
        return generation;
    }

    void mergeCommands(const std::array<uint8_t, MAX_COMMANDS> &cmd) {
        for (int ii = 0; ii < MAX_COMMANDS; ++ii) {
            commands[ii] |= cmd[ii];
        }
    }

    void clearCommands(void) {
        commands.fill(0);
    }

    bool checkAccess(uint8_t opcode) {
        return commands[opcode] != 0;
    }

private:
    std::string name;
    std::string role; // if we've assumed a role, this is the current role
    uint32_t generation;
    std::array<uint8_t, MAX_COMMANDS> commands;
};

class UserEntry {
public:
    void initialize(cJSON *root, bool _role);

    const std::string &getName(void) const {
        return name;
    }

    const StringList &getBuckets(void) const {
        return buckets;
    }

    const StringList &getRoles(void) const {
        return roles;
    }

    const StringList &getProfiles(void) const {
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

    const std::string &getName(void) const {
        return name;
    }

    const std::array<uint8_t, MAX_COMMANDS> &getCommands(void) const {
        return cmd;
    }

    void initialize(cJSON *root);

private:
    void parseCommands(cJSON *obj, uint8_t mask);
    uint8_t decodeOpcode(cJSON *c, uint8_t mask);
    void throwError(const std::string &prefix, uint8_t mask, int value);
    void throwError(const std::string &prefix, uint8_t mask, const char *value);

    std::array<uint8_t, MAX_COMMANDS> cmd;
    std::string name;
    std::string descr;
};


class RBACManager {
public:
    RBACManager();
    ~RBACManager();

    AuthContext *createAuthContext(const std::string name);

    bool assumeRole(AuthContext *ctx, const std::string &role);
    void dropRole(AuthContext *ctx);

    void initialize(cJSON *root);

    uint32_t getGeneration(void) {
        return generation.load();
    }

private:
    void initializeUserEntry(cJSON *root, bool role);
    void initializeProfiles(cJSON *root);

    void applyProfiles(AuthContext *ctx, const StringList &pf);

    std::atomic<uint32_t> generation;
    cb_mutex_t mutex;
    UserEntryMap roles;
    UserEntryMap users;
    ProfileMap profiles;

};
