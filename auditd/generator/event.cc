/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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
#include "event.h"
#include "utilities.h"

#include <cJSON_utils.h>
#include <sstream>

Event::Event(gsl::not_null<const cJSON*> root) {
    cJSON* cId = getMandatoryObject(root, "id", cJSON_Number);
    cJSON* cName = getMandatoryObject(root, "name", cJSON_String);
    cJSON* cDescr = getMandatoryObject(root, "description", cJSON_String);
    cJSON* cSync = getMandatoryObject(root, "sync", -1);
    cJSON* cEnabled = getMandatoryObject(root, "enabled", -1);
    cJSON* cFilteringPermitted =
            getOptionalObject(root, "filtering_permitted", -1);
    cJSON* cMand = getMandatoryObject(root, "mandatory_fields", cJSON_Object);
    cJSON* cOpt = getMandatoryObject(root, "optional_fields", cJSON_Object);

    id = uint32_t(cId->valueint);
    name.assign(cName->valuestring);
    description.assign(cDescr->valuestring);
    sync = cSync->type == cJSON_True;
    enabled = cEnabled->type == cJSON_True;
    if (cFilteringPermitted != nullptr) {
        filtering_permitted = cFilteringPermitted->type == cJSON_True;
    } else {
        filtering_permitted = false;
    }
    mandatory_fields = to_string(cMand, false);
    optional_fields = to_string(cOpt, false);

    int num_elem = cJSON_GetArraySize(const_cast<cJSON*>(root.get()));
    if ((cFilteringPermitted == nullptr && num_elem != 7) ||
        (cFilteringPermitted != nullptr && num_elem != 8)) {
        std::stringstream ss;
        ss << "Unknown elements for " << name << ": " << std::endl
           << to_string(root.get()) << std::endl;
        throw std::runtime_error(ss.str());
    }
}