/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

#pragma once

#include <string>

#include "item.h"

/// underlying size of uint32_t as this is to be stored in the Item flags field.
enum class SystemEvent : uint32_t { CreateCollection, BeginDeleteCollection };

static inline std::string to_string(const SystemEvent se) {
    switch (se) {
    case SystemEvent::CreateCollection:
        return "CreateCollection";
    case SystemEvent::BeginDeleteCollection:
        return "BeginDeleteCollection";
    default:
        throw std::invalid_argument("to_string(SystemEvent) unknown " +
                                    std::to_string(int(se)));
        return "";
    }
}

class SystemEventFactory {
public:
    /**
     * Make an Item representing the SystemEvent
     * @param se The SystemEvent being created. The returned Item will have this
     *           value stored in the flags field.
     * @param keyExtra Every SystemEvent has defined key, keyExtra is appended
     *        to the defined key
     * @param itemSize The returned Item can be requested to allocate a value
     *        of itemSize. Some SystemEvents will update the value with data to
     *        be persisted/replicated.
     */
    static std::unique_ptr<Item> make(SystemEvent se,
                                      const std::string& keyExtra,
                                      size_t itemSize);
};
