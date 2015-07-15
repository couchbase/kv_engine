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

#ifndef DAEMON_IOCTL_H
#define DAEMON_IOCTL_H

#include "config.h"

#include <stddef.h>
#include <memcached/types.h>

#include "memcached.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Attempts to read the given property.
 * If the property could be read, return ENGINE_SUCCESS and writes the value
 * into address pointed to by {value}.
 * Otherwise returns a status code indicating why the read failed.
 */
ENGINE_ERROR_CODE ioctl_get_property(const char* key, size_t keylen,
                                     size_t* value);

/* Attempts to set property {key,keylen} to the value {value,vallen}.
 * If the property could be written, return ENGINE_SUCCESS.
 * Otherwise returns a status code indicating why the write failed.
 */
ENGINE_ERROR_CODE ioctl_set_property(Connection * c, const char* key, size_t keylen,
                                     const char* value, size_t vallen);
#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* DAEMON_IOCTL_H */
