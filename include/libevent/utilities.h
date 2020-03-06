/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020 Couchbase, Inc.
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

// It's easier to just include the two header files we need instead of
// trying to get the right declspec dllimport etc for windows..
#include <event2/bufferevent.h>
#include <event2/event.h>
#include <memory>

namespace cb {
namespace libevent {

struct EventBaseDeleter {
    void operator()(event_base* base) {
        event_base_free(base);
    }
};
using unique_event_base_ptr = std::unique_ptr<event_base, EventBaseDeleter>;

struct BuffereventDeleter {
    void operator()(bufferevent* bev) {
        bufferevent_free(bev);
    }
};
using unique_bufferevent_ptr = std::unique_ptr<bufferevent, BuffereventDeleter>;

struct EventDeleter {
    void operator()(struct event* e) {
        event_free(e);
    }
};
using unique_event_ptr = std::unique_ptr<event, EventDeleter>;

} // namespace libevent
} // namespace cb
