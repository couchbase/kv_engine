/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <memcached/engine_testapp.h>
#include <memcached/rbac.h>

void mock_init_alloc_hooks();

ServerApi* get_mock_server_api();

void init_mock_server();

void mock_time_travel(int by);

void mock_set_pre_link_function(PreLinkFunction function);

using CheckPrivilegeFunction =
        std::function<cb::rbac::PrivilegeAccess(gsl::not_null<const void*>,
                                                cb::rbac::Privilege,
                                                std::optional<ScopeID>,
                                                std::optional<CollectionID>)>;
void mock_set_check_privilege_function(CheckPrivilegeFunction function);
void mock_reset_check_privilege_function();
void mock_set_privilege_context_revision(uint32_t rev);
uint32_t mock_get_privilege_context_revision();
