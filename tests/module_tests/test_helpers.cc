/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
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

#include "test_helpers.h"

#include <thread>

Item make_item(uint16_t vbid,
               const StoredDocKey& key,
               const std::string& value,
               uint32_t exptime,
               protocol_binary_datatype_t datatype) {
    uint8_t ext_meta[EXT_META_LEN] = {datatype};
    Item item(key, /*flags*/0, /*exp*/exptime, value.c_str(), value.size(),
              ext_meta, sizeof(ext_meta));
    item.setVBucketId(vbid);
    return item;
}

std::chrono::microseconds decayingSleep(std::chrono::microseconds uSeconds) {
    /* Max sleep time is slightly over a second */
    static const std::chrono::microseconds maxSleepTime(0x1 << 20);
    std::this_thread::sleep_for(uSeconds);
    return std::min(uSeconds * 2, maxSleepTime);
}
