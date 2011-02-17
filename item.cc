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

static void devnull(uint64_t)
{
}

Atomic<uint64_t> Item::casCounter(1);
uint64_t Item::casNotificationFrequency = 10000;
void (*Item::casNotifier)(uint64_t) = devnull;

bool Item::append(const Item &item) {
    std::string newValue(value->getData(), value->length());
    newValue.append(item.getValue()->to_s());
    value.reset(Blob::New(newValue));
    return true;
}

/**
 * Prepend another item to this item
 *
 * @param item the item to prepend to this one
 * @return true if success
 */
bool Item::prepend(const Item &item) {
    std::string newValue(item.getValue()->to_s());
    newValue.append(value->to_s());
    value.reset(Blob::New(newValue));
    return true;
}
