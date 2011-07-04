/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 NorthScale, Inc.
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
#include "item.hh"

Atomic<uint64_t> Item::casCounter(1);

bool Item::append(const Item &itm) {
    std::string newValue(value->getData(), value->length());
    newValue.append(itm.getValue()->to_s());
    value.reset(Blob::New(newValue));
    return true;
}

/**
 * Prepend another item to this item
 *
 * @param itm the item to prepend to this one
 * @return true if success
 */
bool Item::prepend(const Item &itm) {
    std::string newValue(itm.getValue()->to_s());
    newValue.append(value->to_s());
    value.reset(Blob::New(newValue));
    return true;
}
