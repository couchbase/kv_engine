/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#pragma once

#include <platform/platform.h>
#include <cJSON.h>

#include <stdint.h>

#ifdef __cplusplus
#include "rbac_impl.h"

extern "C" {

    void loadProfilesFromJson(cJSON *root);

#endif

    typedef void* auth_context_t;


    typedef enum {
        /** authentication ok, please proceed */
        AUTH_OK,
        /** authentication information is stale. Please refresh */
        AUTH_STALE,
        /** authentication failed */
        AUTH_FAIL
    } auth_error_t;

    /**
     * Create authentication context for a given user
     *
     * @param user the name of the user to create the authentication
     *             context for.
     * @return pointer to the new authentication token if the user
     *                 exists, NULL otherwise
     */
    auth_context_t auth_create(const char* user);

    /**
     * Destroy an authentication context and release all allocated
     * resources. The object is invalidated by calling this function.
     *
     * @param context the context to destroy
     */
    void auth_destroy(auth_context_t context);

    /**
     * Try to assume the given role
     *
     * @param ctx the current authentication context
     * @param role the role to assume
     * @return the status of the operation.
     */
    auth_error_t auth_assume_role(auth_context_t ctx, const char *role);

    /**
     * Drop the current role
     *
     * @param ctx the current authentication context
     * @return the status of the operation.
     */
    auth_error_t auth_drop_role(auth_context_t ctx);

    /**
     * check for access for a certain command
     *
     * @param ctx the application context to execute the operaiton
     * @param opcode the command to execute
     * @return the status of the operation
     */
    auth_error_t auth_check_access(auth_context_t ctx, uint8_t opcode);


    int load_rbac_from_file(const char *file);

#ifdef __cplusplus
} // extern "C"
#endif
