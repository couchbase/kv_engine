/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <cbsasl/mechanism.h>

#include <algorithm>
#include <vector>

namespace cb::sasl {

Mechanism selectMechanism(const std::string& mechanisms) {
    std::string avail;
    std::transform(mechanisms.begin(),
                   mechanisms.end(),
                   std::back_inserter(avail),
                   toupper);

    // Search what we've got backends for
    const std::vector<std::pair<std::string, Mechanism>> mechs = {
            {std::string{"SCRAM-SHA512"}, Mechanism::SCRAM_SHA512},
            {std::string{"SCRAM-SHA256"}, Mechanism::SCRAM_SHA256},
            {std::string{"SCRAM-SHA1"}, Mechanism::SCRAM_SHA1},
            {std::string{"PLAIN"}, Mechanism::PLAIN}};

    for (auto& mechanism : mechs) {
        const auto index = avail.find(mechanism.first);
        if (index != std::string::npos) {
            const auto offset = index + mechanism.first.length();

            if (offset == avail.length() || avail[offset] == ' ' ||
                avail[offset] == ',') {
                return mechanism.second;
            }
        }
    }

    throw unknown_mechanism(mechanisms);
}

Mechanism selectMechanism(const std::string& mechanism,
                          const std::string& available) {
    // Search what we've got backends for
    const std::vector<std::pair<std::string, Mechanism>> mechs = {
            {std::string{"SCRAM-SHA512"}, Mechanism::SCRAM_SHA512},
            {std::string{"SCRAM-SHA256"}, Mechanism::SCRAM_SHA256},
            {std::string{"SCRAM-SHA1"}, Mechanism::SCRAM_SHA1},
            {std::string{"PLAIN"}, Mechanism::PLAIN}};

    for (auto& m : mechs) {
        const auto index = available.find(m.first);
        if (index != std::string::npos) {
            const auto offset = index + m.first.length();

            if (offset == available.length() || available[offset] == ' ' ||
                available[offset] == ',') {
                if (m.first == mechanism) {
                    return m.second;
                }
            }
        }
    }

    throw unknown_mechanism(mechanism);
}

} // namespace cb::sasl
