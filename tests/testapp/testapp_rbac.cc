/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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

#include "testapp_rbac.h"

cJSON *generate_rbac_config(void)
{
    cJSON *root = cJSON_CreateObject();
    cJSON *prof;
    cJSON *obj;
    cJSON *array;
    cJSON *array2;

    /* profiles */
    array = cJSON_CreateArray();

    prof = cJSON_CreateObject();
    cJSON_AddStringToObject(prof, "name", "system");
    cJSON_AddStringToObject(prof, "description", "system internal");
    obj = cJSON_CreateObject();
    cJSON_AddStringToObject(obj, "opcode", "all");
    cJSON_AddItemToObject(prof, "memcached", obj);
    cJSON_AddItemToArray(array, prof);

    prof = cJSON_CreateObject();
    cJSON_AddStringToObject(prof, "name", "statistics");
    cJSON_AddStringToObject(prof, "description", "only stat and assume");
    obj = cJSON_CreateObject();

    array2 = cJSON_CreateArray();
    cJSON_AddItemToArray(array2, cJSON_CreateString("stat"));
    cJSON_AddItemToArray(array2, cJSON_CreateString("assume_role"));
    cJSON_AddItemToObject(obj, "opcode", array2);
    cJSON_AddItemToObject(prof, "memcached", obj);
    cJSON_AddItemToArray(array, prof);

    cJSON_AddItemToObject(root, "profiles", array);

    /* roles */
    array = cJSON_CreateArray();
    prof = cJSON_CreateObject();
    cJSON_AddStringToObject(prof, "name", "statistics");
    cJSON_AddStringToObject(prof, "profiles", "statistics");

    cJSON_AddItemToArray(array, prof);
    cJSON_AddItemToObject(root, "roles", array);

    /* users */
    array = cJSON_CreateArray();
    prof = cJSON_CreateObject();
    cJSON_AddStringToObject(prof, "login", "*");
    cJSON_AddStringToObject(prof, "profiles", "system");
    cJSON_AddStringToObject(prof, "roles", "statistics");

    cJSON_AddItemToArray(array, prof);
    cJSON_AddItemToObject(root, "users", array);

    return root;
}
