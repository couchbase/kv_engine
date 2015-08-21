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
#include <libgreenstack/Message.h>
#include <libgreenstack/Status.h>

namespace Greenstack {
    /**
     * The Response class is a concrete implementation of a response in the
     * Greenstack protocol.
     */
    class Response : public Greenstack::Message {
    public:
        /**
         * Initialize the Response object to the default values
         */
        Response();

        /**
         * Release all allocated resources
         */
        virtual ~Response();

        /**
         * Set the response status code
         *
         * @param value the status code
         */
        void setStatus(const Greenstack::Status& value);

        /**
         * Get the status code
         */
        const Greenstack::Status getStatus() const;

    protected:

        /**
         * Initialize the response object with the given opcode and status
         *
         * @param opcode the opcode for the response
         * @param status the status code for the response
         */
        Response(Opcode opcode, const Greenstack::Status& status)
            : Message(true), status(status) {
            setOpcode(opcode);
        }

        /**
         * The status attribute
         */
        Greenstack::Status status;
    };
}
