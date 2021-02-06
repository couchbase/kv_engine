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

#include "callbacks.h"

#include "item.h"

GetValue::GetValue()
    : id(-1), status(cb::engine_errc::no_such_key), partial(false) {
}
GetValue::GetValue(GetValue&& other) = default;
GetValue& GetValue::operator=(GetValue&& other) = default;

GetValue::GetValue(std::unique_ptr<Item> v,
                   cb::engine_errc s,
                   uint64_t i,
                   bool incomplete)
    : item(std::move(v)), id(i), status(s), partial(incomplete) {
}

GetValue::~GetValue() = default;
