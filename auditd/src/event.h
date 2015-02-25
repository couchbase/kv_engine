/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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
#ifndef EVENT_H
#define EVENT_H

#include <inttypes.h>
#include <string>

class Audit;

class Event {
public:
    const uint32_t id;
    const std::string payload;

    // Constructor required for ConfigureEvent
    Event()
        : id(0) {}

    Event(const uint32_t event_id, const char* p,
          size_t length)
        : id(event_id),
          payload(p,length) {}

    virtual bool process(Audit& audit);

    virtual ~Event() {}

};

#endif
