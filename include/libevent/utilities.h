/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

// It's easier to just include the two header files we need instead of
// trying to get the right declspec dllimport etc for windows..
#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/event.h>
#include <memory>

namespace cb::libevent {

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

struct EvbufferDeleter {
    void operator()(struct evbuffer* buf) {
        evbuffer_free(buf);
    }
};
using unique_evbuffer_ptr = std::unique_ptr<evbuffer, EvbufferDeleter>;

} // namespace cb::libevent
