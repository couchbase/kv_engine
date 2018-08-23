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

#include <iomanip>
#include <iostream>

#include "storeddockey.h"

std::string StoredDocKey::to_string() const {
    std::stringstream ss;
    auto leb128 = cb::mcbp::decode_unsigned_leb128<CollectionIDType>(
            {reinterpret_cast<const uint8_t*>(keydata.data()), keydata.size()});
    ss << "cid:0x" << std::hex << leb128.first << ":"
       << std::string(reinterpret_cast<const char*>(leb128.second.data()),
                      leb128.second.size());
    ss << ", size:" << size();
    return ss.str();
}

std::ostream& operator<<(std::ostream& os, const StoredDocKey& key) {
    return os << key.to_string();
}

std::ostream& operator<<(std::ostream& os, const SerialisedDocKey& key) {
    auto leb128 = cb::mcbp::decode_unsigned_leb128<CollectionIDType>(
            {reinterpret_cast<const uint8_t*>(key.data()), key.size()});
    os << "cid:0x" << std::hex << leb128.first << ":"
       << std::string(reinterpret_cast<const char*>(leb128.second.data()),
                      leb128.second.size());
    os << ", size:" << key.size();
    return os;
}
