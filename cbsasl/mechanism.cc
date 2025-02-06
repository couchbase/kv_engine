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
#include <platform/split_string.h>

#include <algorithm>
#include <array>

namespace cb::sasl {
using namespace std::string_view_literals;

/// The list of mechanisms we've got implementations for (in the "prefferred"
/// order for a client to pick from. OAUTHBEARER is listed last as most of our
/// command line tools allows username/password and don't have the tokens)
static constexpr std::array<std::pair<std::string_view, Mechanism>, 5> backends{
        {{"SCRAM-SHA512"sv, Mechanism::SCRAM_SHA512},
         {"SCRAM-SHA256"sv, Mechanism::SCRAM_SHA256},
         {"SCRAM-SHA1"sv, Mechanism::SCRAM_SHA1},
         {"PLAIN"sv, Mechanism::PLAIN},
         {"OAUTHBEARER"sv, Mechanism::OAUTHBEARER}}};

std::string_view format_as(Mechanism mechanism) {
    for (const auto& [name, m] : backends) {
        if (m == mechanism) {
            return name;
        }
    }
    throw unknown_mechanism("format_as(Mechanism): unknown mechanism " +
                            std::to_string(static_cast<int>(mechanism)));
}

Mechanism selectMechanism(const std::string_view mechanisms) {
    const auto avail = cb::string::split(mechanisms, ' ');

    // Search what we've got backends for (they're listed in preferred order)
    for (auto& [name, mechanism] : backends) {
        if (std::ranges::find(avail, name) != avail.end()) {
            return mechanism;
        }
    }

    throw unknown_mechanism(std::string{mechanisms});
}

Mechanism selectMechanism(const std::string_view mechanism,
                          const std::string_view available) {
    auto avail = cb::string::split(available, ' ');
    if (std::ranges::find(avail, mechanism) != avail.end()) {
        // The requested mechanism is listed within the available mechanisms,
        // but we might not have a backend for it:
        for (const auto& [name, m] : backends) {
            if (name == mechanism) {
                return m;
            }
        }
    }

    throw unknown_mechanism(std::string{mechanism});
}

} // namespace cb::sasl

std::string to_string(cb::sasl::Mechanism mechanism) {
    return std::string{format_as(mechanism)};
}
