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
#pragma once

#include <vector>
#include <libgreenstack/FlexHeader.h>
#include <libgreenstack/Message.h>

namespace Greenstack {
    /**
     * The Request class is a concrete implementation of a request in the
     * Greenstack protocol.
     */
    class Request : public Greenstack::Message {
    public:
        /**
         * Initialize the Request object to the default values
         */
        Request();

        /**
         * Release all allocated resources from this Request object
         */
        virtual ~Request();

    protected:
        /**
         * Initialize the Request object and set the Opcode to a given
         * value.
         *
         * @param opcode the opcode for the object
         */
        Request(Opcode opcode)
            : Message(false) {
            setOpcode(opcode);
        }
    };
}
