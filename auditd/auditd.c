/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2014 Couchbase, Inc
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
#include "memcached/audit_interface.h"

AUDIT_ERROR_CODE initialize_auditdaemon(const char *config)
{
    return AUDIT_SUCCESS;
}

AUDIT_ERROR_CODE put_audit_event(const uint32_t audit_eventid, const void *payload, size_t length)
{
    return AUDIT_SUCCESS;
}

AUDIT_ERROR_CODE reload_auditdaemon_config(const char *config)
{
    return AUDIT_SUCCESS;
}

AUDIT_ERROR_CODE shutdown_auditdaemon(void)
{
    return AUDIT_SUCCESS;
}

