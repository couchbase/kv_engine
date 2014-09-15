/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2014 Couchbase, Inc
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

#ifndef EVENTS_H
#define    EVENTS_H

/*
 *   Defines all the type of events that can be raised.
 *   The types are outside of the syslog protocol standard.
 */

typedef enum {

    GENERIC_EVENT=0,
    STATE_CHANGE_EVENT=1,
    OUT_OF_MEMORY_EVENT=2

} EventID;

#endif    /*EVENTS_H */
