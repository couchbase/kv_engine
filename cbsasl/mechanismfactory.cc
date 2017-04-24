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
#include "config.h"
#include "mechanismfactory.h"
#include "plain/plain.h"
#include "scram-sha/scram-sha.h"

#include <algorithm>
#include <cctype>
#include <cstring>
#include <iterator>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>

/**
 * In order to keep track of all of the avaliable mechanisms we support
 * we'll store them in a dynamic list so to easy add/remove support for
 * more mechanisms without having to change a ton of code.
 *
 * Each concrete implement of a mechanism should provide a subclass of
 * this class and insert itself into the available_mechs array.
 */
class MechInfo {
public:
    /**
     * Initialize a new mechanism
     *
     * @param nm the IANA registered name for the mechanism (should be
     *           all uppercase)
     * @param en Is the mechanism enabled or not
     * @param mech the Mechanism constant (to avoid the rest of the system
     *             to use string comparisons).
     */
    MechInfo(const char *nm, bool en, const Mechanism &mech) :
        name(nm), enabled(en), mechanism(mech) {
    }

    virtual UniqueMechanismBackend createServerBackend(cbsasl_conn_t& conn) = 0;
    virtual UniqueMechanismBackend createClientBackend(cbsasl_conn_t& conn) = 0;

    const std::string& getName() const {
        return name;
    }

    bool isEnabled() const {
        return enabled;
    }

    /**
     * Some of our backends require support from openssl, which seems
     * to be packaged up differently on our platforms so all mechanisms
     * isn't supported on all platforms.
     *
     * @return true if the mechanism is supported on this platform
     */
    virtual bool isMechanismSupported() = 0;

    void setEnabled(bool enabled) {
        if (isMechanismSupported()) {
            MechInfo::enabled = enabled;
        } else {
            // This mechanism can't be enabled on this platform
            MechInfo::enabled = false;
        }
    }

    const Mechanism& getMechanism() const {
        return mechanism;
    }

protected:

    const std::string name;
    bool enabled;
    Mechanism mechanism;
};

class Scram512MechInfo : public MechInfo {
public:
    Scram512MechInfo()
        : MechInfo(MECH_NAME_SCRAM_SHA512, true, Mechanism::SCRAM_SHA512) {
        setEnabled(isMechanismSupported());
    }

    virtual UniqueMechanismBackend createServerBackend(cbsasl_conn_t& conn) override {
        return UniqueMechanismBackend(new ScramSha512ServerBackend(conn));
    }

    virtual UniqueMechanismBackend createClientBackend(cbsasl_conn_t& conn) override {
        return UniqueMechanismBackend(new ScramSha512ClientBackend(conn));
    }

    virtual bool isMechanismSupported() override {
        return cb::crypto::isSupported(cb::crypto::Algorithm::SHA512);
    }
};

class Scram256MechInfo : public MechInfo {
public:
    Scram256MechInfo()
        : MechInfo(MECH_NAME_SCRAM_SHA256, true, Mechanism::SCRAM_SHA256) {
        setEnabled(isMechanismSupported());
    }

    virtual UniqueMechanismBackend createServerBackend(cbsasl_conn_t& conn) override {
        return UniqueMechanismBackend(new ScramSha256ServerBackend(conn));
    }

    virtual UniqueMechanismBackend createClientBackend(cbsasl_conn_t& conn) override {
        return UniqueMechanismBackend(new ScramSha256ClientBackend(conn));
    }

    virtual bool isMechanismSupported() override {
        return cb::crypto::isSupported(cb::crypto::Algorithm::SHA256);
    }
};

class Scram1MechInfo : public MechInfo {
public:
    Scram1MechInfo()
        : MechInfo(MECH_NAME_SCRAM_SHA1, true, Mechanism::SCRAM_SHA1) {
        setEnabled(isMechanismSupported());
    }

    virtual UniqueMechanismBackend createServerBackend(cbsasl_conn_t& conn) override {
        return UniqueMechanismBackend(new ScramSha1ServerBackend(conn));
    }

    virtual UniqueMechanismBackend createClientBackend(cbsasl_conn_t& conn) override {
        return UniqueMechanismBackend(new ScramSha1ClientBackend(conn));
    }

    virtual bool isMechanismSupported() override {
        return cb::crypto::isSupported(cb::crypto::Algorithm::SHA1);
    }
};

class PlainMechInfo : public MechInfo {
public:
    PlainMechInfo()
        : MechInfo(MECH_NAME_PLAIN, true, Mechanism::PLAIN) { }

    virtual UniqueMechanismBackend createServerBackend(cbsasl_conn_t& conn) override {
        return UniqueMechanismBackend(new PlainServerBackend(conn));
    }

    virtual UniqueMechanismBackend createClientBackend(cbsasl_conn_t& conn) override {
        return UniqueMechanismBackend(new PlainClientBackend(conn));
    }

    virtual bool isMechanismSupported() override {
        return true;
    }
};

static Scram512MechInfo scram512MechInfo;
static Scram256MechInfo scram256MechInfo;
static Scram1MechInfo scram1MechInfo;
static PlainMechInfo plainMechInfo;

static std::array<MechInfo*, 4> availableMechs = {
     &scram512MechInfo,
     &scram256MechInfo,
     &scram1MechInfo,
     &plainMechInfo
};

void cbsasl_set_available_mechanisms(cbsasl_getopt_fn getopt_fn,
                                     void* context) {
    const char* result = nullptr;
    unsigned int result_len;

    if (getopt_fn(context, nullptr, "sasl mechanisms", &result,
                  &result_len) != CBSASL_OK) {
        return;
    }

    // Disable all
    for (auto& mech : availableMechs) {
        mech->setEnabled(false);
    }

    std::string mechlist(result, result_len);
    std::transform(mechlist.begin(), mechlist.end(), mechlist.begin(), toupper);

    std::istringstream iss(mechlist);
    std::vector<std::string> tokens{std::istream_iterator<std::string>{iss},
                                    std::istream_iterator<std::string>{}};
    for (auto& token : tokens) {
        bool found = false;
        for (auto& mech : availableMechs) {
            if (mech->getName() == token) {
                mech->setEnabled(true);
                found = true;
            }
        }
        if (found) {
            logging::log(logging::Level::Error,
                         "Unknown mech [" + token + "] specified. Ignored");
        } else {
            logging::log(logging::Level::Debug, "Enable mech [" + token + "]");
        }
    }
}

UniqueMechanismBackend MechanismFactory::createServerBackend(cbsasl_conn_t& conn) {

    for (const auto& m : availableMechs) {
        if (m->getMechanism() == conn.mechanism) {
            if (m->isEnabled()) {
                return m->createServerBackend(conn);
            } else {
                logging::log(logging::Level::Debug,
                             "Requested disabled mechanism " + m->getName());
                return UniqueMechanismBackend();
            }
        }
    }

    throw std::invalid_argument("MechanismFactory::create() can't be "
                                    "called with an unknown mechanism");
}

UniqueMechanismBackend MechanismFactory::createClientBackend(
    cbsasl_conn_t& conn) {
    for (const auto& m : availableMechs) {
        if (m->getMechanism() == conn.mechanism) {
            if (m->isEnabled()) {
                return m->createClientBackend(conn);
            } else {
                logging::log(logging::Level::Debug,
                             "Requested disabled mechanism " + m->getName());
                return UniqueMechanismBackend();
            }
        }
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

static bool isSeparator(const char c) {
    if (c == '-' || std::ispunct(c) == 0) {
        return false;
    }

    return true;
}

Mechanism MechanismFactory::selectMechanism(const std::string& mechlist) {
    std::string uppercase(mechlist);
    std::transform(uppercase.begin(), uppercase.end(), uppercase.begin(),
                   toupper);
    std::replace_if(uppercase.begin(), uppercase.end(), isSeparator, ' ');

    for (const auto & m : availableMechs) {
        if (m->isEnabled() && containsMechanism(uppercase, m->getName())) {
            return m->getMechanism();
        }
    }

    return Mechanism::UNKNOWN;
}

cbsasl_error_t MechanismFactory::list(cbsasl_conn_t* conn, const char* user,
                                      const char* prefix, const char* sep,
                                      const char* suffix, const char** result,
                                      unsigned* len, int* count) {
    if (result == nullptr || conn == nullptr) {
        return CBSASL_BADPARAM;
    }

    try {
        int counter = 0;
        conn->server->list_mechs.clear();
        conn->server->list_mechs.reserve(80);

        if (prefix != nullptr) {
            conn->server->list_mechs.append(prefix);
        }

        bool needSep = false;
        for (const auto& mech : availableMechs) {
            if (mech->isEnabled()) {
                if (needSep) {
                    if (sep == nullptr) {
                        conn->server->list_mechs.append(" ");
                    } else {
                        conn->server->list_mechs.append(sep);
                    }
                } else {
                    needSep = true;
                }
                conn->server->list_mechs.append(mech->getName());
                ++counter;
            }
        }

        if (suffix != nullptr) {
            conn->server->list_mechs.append(suffix);
        }

        *result = conn->server->list_mechs.data();

        if (len != nullptr) {
            *len = (unsigned int)conn->server->list_mechs.length();
        }

        if (count != nullptr) {
            *count = counter;
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
    } else if (mech == MECH_NAME_SCRAM_SHA1) {
        return Mechanism::SCRAM_SHA1;
    } else if (mech == MECH_NAME_SCRAM_SHA256) {
        return Mechanism::SCRAM_SHA256;
    } else if (mech == MECH_NAME_SCRAM_SHA512) {
        return Mechanism::SCRAM_SHA512;
    } else {
        return Mechanism::UNKNOWN;
    }
}

std::string MechanismFactory::toString(const Mechanism& mech) {
    switch (mech) {
    case Mechanism::PLAIN:
        return MECH_NAME_PLAIN;
    case Mechanism::SCRAM_SHA1:
        return MECH_NAME_SCRAM_SHA1;
    case Mechanism::SCRAM_SHA256:
        return MECH_NAME_SCRAM_SHA256;
    case Mechanism::SCRAM_SHA512:
        return MECH_NAME_SCRAM_SHA512;
    case Mechanism::UNKNOWN:
        break;
    }
    throw std::invalid_argument("Provided mechanism does not exist");
}
