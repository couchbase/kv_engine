/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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

#include <cbsasl/cbsasl.h>
#include "cbsasl_internal.h"
#include <memory>

class MechanismFactory {
public:
    /**
     * Lists all of the mechanisms this sasl server supports
     *
     * Currently all parameters except result and len is ignored, but provided
     * to maintain compatibility with other SASL implementations.
     *
     * @param conn the connection object that wants to call list mechs. May
     *             be null
     * @param user the user who wants to connect (may restrict the available
     *             mechs). May be null
     * @param prefix the prefix to insert to the resulting string (may be null)
     * @param sep the separator between each mechanism
     * @param suffix the suffix to append to the resulting string (may be null)
     * @param result pointer to where the result is to be stored (allocated
     *               and feed by the library)
     * @param len the length of the resulting string (may be null)
     * @param count the number of mechanisms in the resulting string (may be
     *              null)
     *
     * @return Whether or not an error occured while getting the mechanism list
     */
    static cbsasl_error_t list(cbsasl_conn_t* conn,
                               const char* user,
                               const char* prefix,
                               const char* sep,
                               const char* suffix,
                               const char** result,
                               unsigned* len,
                               int* count);

    /**
     * Try to look up a mechanism by name and return its enum value
     *
     * @param mech the textual mechanism name
     * @return the enum constant for the name, or UNKNOWN
     */
    static Mechanism toMechanism(const std::string mech);

    /**
     * Select a mechanism in the supplied list of mechanisms.
     *
     * The method will try to select "the most secure" mechanism
     * listed in the mechlist that is supported in the library
     *
     * @param mechlist the list of available mechanisms
     * @return the most secure mechanism the library supports, or Unknown
     *         none of the specified mechanisms is supported.
     */
    static Mechanism selectMechanism(const std::string& mechlist);

    /**
     * Create an instance of a mechanism implementation that provides the
     * server side backend for the requested mechanism
     *
     * @param mechanism the mechanism to create
     * @return a backend that implements the mechanism
     */
    static UniqueMechanismBackend createServerBackend(
        const Mechanism& mechanism);


    /**
     * Create an instance of a mechanism implementation that provides the
     * client side backend for the requested mechanism
     *
     * @param mechanism the mechanism to create
     * @return a backend that implements the mechanism
     */
    static UniqueMechanismBackend createClientBackend(
        const Mechanism& mechanism);
};
