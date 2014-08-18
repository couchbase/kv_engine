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

/*
 * Configuration file parsing and handling.
 */

#ifndef CONFIG_PARSE_H
#define CONFIG_PARSE_H

#include <cJSON.h>


#ifdef __cplusplus
extern "C" {
#endif

void load_config_file(const char *filename, struct settings *settings);

/* Frees all dynamic memory associated with the given settings struct */
void free_settings(struct settings* s);

#ifdef __cplusplus
}
#endif

#endif /* CONFIG_PARSE_H */
