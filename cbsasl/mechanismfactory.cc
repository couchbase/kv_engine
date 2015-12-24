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
#include "cram-md5/cram-md5.h"
#include "mechanismfactory.h"
#include "plain/plain.h"

#include <algorithm>
#include <cstring>
#include <stdexcept>

UniqueMechanismBackend MechanismFactory::createServerBackend(
    const Mechanism& mechanism) {
    switch (mechanism) {
    case Mechanism::PLAIN:
        return std::unique_ptr<MechanismBackend>(new PlainServerBackend);
    case Mechanism::CRAM_MD5:
        return std::unique_ptr<MechanismBackend>(new CramMd5ServerBackend);
    case Mechanism::UNKNOWN:
        throw std::invalid_argument("MechanismFactory::create() can't be "
                                        "called with an unknown mechanism");
    }
    throw std::invalid_argument("MechanismFactory::create() can't be "
                                    "called with an unknown mechanism");
}

UniqueMechanismBackend MechanismFactory::createClientBackend(
    const Mechanism& mechanism) {
    switch (mechanism) {
    case Mechanism::PLAIN:
        return std::unique_ptr<MechanismBackend>(new PlainClientBackend);
    case Mechanism::CRAM_MD5:
        return std::unique_ptr<MechanismBackend>(new CramMd5ClientBackend);
    case Mechanism::UNKNOWN:
        throw std::invalid_argument("MechanismFactory::create() can't be "
                                        "called with an unknown mechanism");
    }
    throw std::invalid_argument("MechanismFactory::create() can't be "
                                    "called with an unknown mechanism");
}


/**
 * Search to see if the mechlist contains the given name
 *
 * @param mechlist the list to search in
 * @param name the name to search for
 * @return true if it contains the given mechanism
 */
static bool containsMechanism(const std::string& mechlist,
                              const std::string& name) {
    size_t pos = 0;

    do {
        pos = mechlist.find(name, pos);
        if (pos == std::string::npos) {
            return false;
        }
        if (pos != 0) {
            if (mechlist.at(pos - 1) != ' ') {
                pos++;
                continue;
            }
        }

        if (mechlist.length() > (pos + name.length())) {
            if (mechlist.at(pos + name.length()) != ' ') {
                pos++;
                continue;
            }
        }
        return true;
    } while (true);
}

Mechanism MechanismFactory::selectMechanism(const std::string& mechlist) {
    std::string uppercase(mechlist);
    std::transform(uppercase.begin(), uppercase.end(), uppercase.begin(),
                   toupper);

    if (containsMechanism(uppercase, MECH_NAME_CRAM_MD5)) {
        return Mechanism::CRAM_MD5;
    } else if (containsMechanism(uppercase, MECH_NAME_PLAIN)) {
        return Mechanism::PLAIN;
    } else {
        return Mechanism::UNKNOWN;
    }
}

cbsasl_error_t MechanismFactory::list(cbsasl_conn_t* conn, const char* user,
                                      const char* prefix, const char* sep,
                                      const char* suffix, const char** result,
                                      unsigned* len, int* count) {
    if (result == nullptr) {
        return CBSASL_BADPARAM;
    }

    // Are we asking for the default string?
    if (user == nullptr && prefix == nullptr && suffix == nullptr &&
        strcmp(sep, " ") == 0) {
        *result = MECH_NAME_CRAM_MD5 " " MECH_NAME_PLAIN;

        if (len != nullptr) {
            *len = (unsigned)strlen(*result);
        }

        if (count != nullptr) {
            *count = 2;
        }
        return CBSASL_OK;
    }

    // We want a customized string
    if (conn == nullptr) {
        // You can't ask for a modified string without letting me get
        // a place to store the allocated resources
        return CBSASL_BADPARAM;
    }

    try {
        if (prefix != nullptr) {
            conn->server->list_mechs.append(prefix);
        }

        conn->server->list_mechs.append(MECH_NAME_CRAM_MD5);
        if (sep == nullptr) {
            conn->server->list_mechs.append(" ");
        } else {
            conn->server->list_mechs.append(sep);
        }
        conn->server->list_mechs.append(MECH_NAME_PLAIN);

        if (suffix != nullptr) {
            conn->server->list_mechs.append(suffix);
        }

        *result = conn->server->list_mechs.data();

        if (len != nullptr) {
            *len = (unsigned int)conn->server->list_mechs.length();
        }

        if (count != nullptr) {
            *count = 2;
        }
    } catch (std::bad_alloc) {
        conn->server->list_mechs.resize(0);
        return CBSASL_NOMEM;
    }

    return CBSASL_OK;
}

Mechanism MechanismFactory::toMechanism(const std::string mech) {
    std::string uppercase(mech);
    std::transform(uppercase.begin(), uppercase.end(), uppercase.begin(),
                   toupper);
    if (mech == MECH_NAME_PLAIN) {
        return Mechanism::PLAIN;
    } else if (mech == MECH_NAME_CRAM_MD5) {
        return Mechanism::CRAM_MD5;
    } else {
        return Mechanism::UNKNOWN;
    }
}
