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

#include <platform/platform.h>
#include <cJSON.h>
#include <array>
#include <cstring>
#include <string>
#include <cstdint>

// @todo this needs to be refactored when we're opening up for Greenstack
#define MAX_COMMANDS 0x100

void loadProfilesFromJson(cJSON *root);

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
    AuthContext(uint32_t gen,
                const std::string &nm,
                const std::string &_connection)
        : name(nm), generation(gen), connection(_connection)
    {
        commands.fill(0);
    }

    const std::string &getName() const {
        return name;
    }

    void setRole(const std::string &_role) {
        role.assign(_role);
    }

    const std::string &getRole() const {
        return role;
    }

    uint32_t getGeneration() const {
        return generation;
    }

    void mergeCommands(const std::array<uint8_t, MAX_COMMANDS> &cmd) {
        for (int ii = 0; ii < MAX_COMMANDS; ++ii) {
            commands[ii] |= cmd[ii];
        }
    }

    void clearCommands() {
        commands.fill(0);
    }

    bool checkAccess(uint8_t opcode) {
        return commands[opcode] != 0;
    }

private:
    std::string name;
    std::string role; // if we've assumed a role, this is the current role
    uint32_t generation;
    std::string connection;
    std::array<uint8_t, MAX_COMMANDS> commands;

    friend std::ostream& operator<< (std::ostream& out,
                                     const AuthContext &context);
};

enum class AuthResult : uint8_t {
    /** authentication ok, please proceed */
    OK,
    /** authentication information is stale. Please refresh */
    STALE,
    /** authentication failed */
    FAIL
};

/**
 * Create authentication context for a given user
 *
 * @param user the name of the user to create the authentication
 *             context for.
 * @param peer the name of the peer connecting from
 * @param local the name of the local socket connecting to
 * @return pointer to the new authentication token if the user
 *                 exists, NULL otherwise
 */
AuthContext* auth_create(const char* user,
                         const char *peer,
                         const char *local);

/**
 * Destroy an authentication context and release all allocated
 * resources. The object is invalidated by calling this function.
 *
 * @param context the context to destroy
 */
void auth_destroy(AuthContext *context);

/**
 * Try to assume the given role
 *
 * @param ctx the current authentication context
 * @param role the role to assume
 * @return the status of the operation.
 */
AuthResult auth_assume_role(AuthContext *ctx, const char *role);

/**
 * Drop the current role
 *
 * @param ctx the current authentication context
 * @return the status of the operation.
 */
AuthResult auth_drop_role(AuthContext *ctx);

/**
 * check for access for a certain command
 *
 * @param ctx the application context to execute the operaiton
 * @param opcode the command to execute
 * @return the status of the operation
 */
AuthResult auth_check_access(AuthContext *, uint8_t opcode);

/**
 * Enable / disable privilege debugging
 *
 * privilege debugging allows you to run all commands no matter
 * what the authentication context defines, but it'll print out a
 * message to standard error containing the command it tried to
 * use (and the context)
 */
void auth_set_privilege_debug(bool enable);


int load_rbac_from_file(const char *file);
