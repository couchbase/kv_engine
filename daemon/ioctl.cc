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

#include "ioctl.h"

#include "config.h"
#include "alloc_hooks.h"
#include "connections.h"

/*
 * Implement ioctl-style memcached commands (ioctl_get / ioctl_set).
 */

ENGINE_ERROR_CODE ioctl_get_property(const char* key, size_t keylen,
                                     size_t* value)
{
    return ENGINE_EINVAL;
}

ENGINE_ERROR_CODE ioctl_set_property(Connection* c,
                                     const char* key, size_t keylen,
                                     const char* value, size_t vallen) {
    std::string request_key(key, keylen);

    if (request_key == "release_free_memory") {
        AllocHooks::release_free_memory();
        LOG_NOTICE(c, "%u: IOCTL_SET: release_free_memory called", c->getId());
        return ENGINE_SUCCESS;
    } else if (request_key.find("trace.connection.") == 0) {
        return apply_connection_trace_mask(request_key,
                                           std::string(value, vallen));
    } else {
        return ENGINE_EINVAL;
    }
}
