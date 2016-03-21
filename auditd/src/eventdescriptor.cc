/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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
#include "eventdescriptor.h"
#include <stdexcept>

EventDescriptor::EventDescriptor(const cJSON* root)
    : id(uint32_t(locate(root, "id", cJSON_Number)->valueint)),
      name(locate(root, "name", cJSON_String)->valuestring),
      description(locate(root, "description", cJSON_String)->valuestring),
      sync(locate(root, "sync", cJSON_True)->type == cJSON_True),
      enabled(locate(root, "enabled", cJSON_True)->type == cJSON_True) {
    int expected = 5;
    cJSON* obj;
    if ((obj = cJSON_GetObjectItem(const_cast<cJSON*>(root),
                                   "mandatory_fields")) != nullptr) {
        if (obj->type != cJSON_Array && obj->type != cJSON_Object) {
            throw std::logic_error(
                "EventDescriptor::EventDescriptor: Invalid type for mandatory_fields");
        }
        ++expected;
    }
    if ((obj = cJSON_GetObjectItem(const_cast<cJSON*>(root),
                                   "optional_fields")) != nullptr) {
        if (obj->type != cJSON_Array && obj->type != cJSON_Object) {
            throw std::logic_error(
                "EventDescriptor::EventDescriptor: Invalid type for optional_fields");
        }
        ++expected;
    }

    if (expected != cJSON_GetArraySize(const_cast<cJSON*>(root))) {
        throw std::logic_error(
            "EventDescriptor::EventDescriptor: Unknown elements specified");

    }
}

const cJSON* EventDescriptor::locate(const cJSON* root,
                                     const char* field,
                                     int type) {
    const cJSON* ret = cJSON_GetObjectItem(const_cast<cJSON*>(root), field);
    if (ret == nullptr) {
        std::string msg("EventDescriptor::locate: Element \"");
        msg.append(field);
        msg.append("\" not found");
        throw std::logic_error(msg);
    }

    if (type == cJSON_True) {
        if (ret->type != cJSON_True && ret->type != cJSON_False) {
            std::string msg("EventDescriptor::locate: Element \"");
            msg.append(field);
            msg.append("\" is not of the expected type");
            throw std::logic_error(msg);
        }
    } else {
        if (ret->type != type) {
            std::string msg("EventDescriptor::locate: Element \"");
            msg.append(field);
            msg.append("\" is not of the expected type");
            throw std::logic_error(msg);
        }
    }

    return ret;
}
