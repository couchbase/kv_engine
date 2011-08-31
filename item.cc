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

bool Item::append(const Item &item) {
    size_t newSize = value->length() + item.getValue()->length();
    Blob *newData = Blob::New(newSize, '\0');
    char *newValue = (char *) newData->getData();
    std::memcpy(newValue, value->getData(), value->length());
    std::memcpy(newValue + value->length(), item.getValue()->getData(), item.getValue()->length());
    value.reset(newData);
    return true;
}

/**
 * Prepend another item to this item
 *
 * @param item the item to prepend to this one
 * @return true if success
 */
bool Item::prepend(const Item &item) {
    size_t newSize = value->length() + item.getValue()->length();
    Blob *newData = Blob::New(newSize, '\0');
    char *newValue = (char *) newData->getData();
    std::memcpy(newValue, item.getValue()->getData(), item.getValue()->length());
    std::memcpy(newValue + item.getValue()->length(), value->getData(), value->length());
    value.reset(newData);
    return true;
}
