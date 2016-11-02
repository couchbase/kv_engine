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

#include "config.h"

#include "item.h"
#include "cJSON.h"

std::atomic<uint64_t> Item::casCounter(1);
const uint32_t Item::metaDataSize(2*sizeof(uint32_t) + 2*sizeof(uint64_t) + 2);


std::string to_string(queue_op op) {
    switch(op) {
        case queue_op::set: return "set";
        case queue_op::del: return "del";
        case queue_op::flush: return "flush";
        case queue_op::empty: return "empty";
        case queue_op::checkpoint_start: return "checkpoint_start";
        case queue_op::checkpoint_end: return "checkpoint_end";
        case queue_op::set_vbucket_state: return "set_vbucket_state";
    }
    return "<" +
            std::to_string(static_cast<std::underlying_type<queue_op>::type>(op)) +
            ">";

}
