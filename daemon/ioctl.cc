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

/*
 * Implement ioctl-style memcached commands (ioctl_get / ioctl_set).
 */

ENGINE_ERROR_CODE ioctl_get_property(const char* key, size_t keylen,
                                     size_t* value)
{
#if defined(HAVE_TCMALLOC)
    if (strncmp("tcmalloc.aggressive_memory_decommit", key, keylen) == 0 &&
        keylen == strlen("tcmalloc.aggressive_memory_decommit")) {
        if (mc_get_allocator_property("tcmalloc.aggressive_memory_decommit",
                                      value)) {
            return ENGINE_SUCCESS;
        } else {
            return ENGINE_EINVAL;
        }
    } else
#endif /* HAVE_TCMALLOC */
    {
        return ENGINE_EINVAL;
    }
}

ENGINE_ERROR_CODE ioctl_set_property(Connection * c, const char* key, size_t keylen,
                                     const char* value, size_t vallen)
{
    if (strncmp("release_free_memory", key, keylen) == 0 &&
        keylen == strlen("release_free_memory")) {
        mc_release_free_memory();
        LOG_NOTICE(c, "%u: IOCTL_SET: release_free_memory called", c->getId());
        return ENGINE_SUCCESS;
#if defined(HAVE_TCMALLOC)
    } else if (strncmp("tcmalloc.aggressive_memory_decommit", key, keylen) == 0 &&
               keylen == strlen("tcmalloc.aggressive_memory_decommit")) {

        /* null-terminate value */
        char val_buffer[IOCTL_VAL_LENGTH + 1]; /* +1 for terminating '\0' */
        long int intval;

        memcpy(val_buffer, value, vallen);
        val_buffer[vallen] = '\0';
        errno = 0;
        intval = strtol(val_buffer, NULL, 10);

        if (errno == 0 && mc_set_allocator_property("tcmalloc.aggressive_memory_decommit",
                                                    intval)) {
            LOG_NOTICE(c,
                "%u: IOCTL_SET: 'tcmalloc.aggressive_memory_decommit' set to %ld",
                c->getId(), intval);
            return ENGINE_SUCCESS;
        } else {
            return ENGINE_EINVAL;
        }
#endif /* HAVE_TCMALLOC */
    } else {
        return ENGINE_EINVAL;
    }
}
