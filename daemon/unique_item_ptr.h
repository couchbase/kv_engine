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
#pragma once

#include "memcached.h"

namespace cb {
/**
 * Custom deleter for `item` objects returned over the engine API.
 * Used with std::unique_ptr (see unique_item_ptr) to allow smart pointers
 * for item.
 */
class ItemDeleter {
public:
    ItemDeleter() = delete;
    ItemDeleter(McbpConnection& connection_) : connection(connection_) {}
    void operator()(item* item) {
        bucket_release_item(&connection, item);
    }

private:
    McbpConnection& connection;
};

typedef std::unique_ptr<item, ItemDeleter> unique_item_ptr;
}
