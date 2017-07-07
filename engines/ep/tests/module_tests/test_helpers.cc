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
#include <string_utilities.h>
#include <xattr/blob.h>

Item make_item(uint16_t vbid,
               const StoredDocKey& key,
               const std::string& value,
               uint32_t exptime,
               protocol_binary_datatype_t datatype) {
    Item item(key,
              /*flags*/ 0,
              /*exp*/ exptime,
              value.c_str(),
              value.size(),
              datatype);
    item.setVBucketId(vbid);
    return item;
}

std::chrono::microseconds decayingSleep(std::chrono::microseconds uSeconds) {
    /* Max sleep time is slightly over a second */
    static const std::chrono::microseconds maxSleepTime(0x1 << 20);
    std::this_thread::sleep_for(uSeconds);
    return std::min(uSeconds * 2, maxSleepTime);
}

std::string createXattrValue(const std::string& body) {
      cb::xattr::Blob blob;

      //Add a few XAttrs
      blob.set(to_const_byte_buffer("ABCuser"),
               to_const_byte_buffer("{\"author\":\"bubba\"}"));
      blob.set(to_const_byte_buffer("_sync"),
               to_const_byte_buffer("{\"cas\":\"0xdeadbeefcafefeed\"}"));
      blob.set(to_const_byte_buffer("meta"),
               to_const_byte_buffer("{\"content-type\":\"text\"}"));

      auto xattr_value = blob.finalize();

      // append body to the xattrs and store in data
      std::string data;
      std::copy(xattr_value.buf, xattr_value.buf + xattr_value.len,
                std::back_inserter(data));
      std::copy(body.c_str(), body.c_str() + body.size(),
                std::back_inserter(data));
      return data;
  }
