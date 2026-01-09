/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <mcbp/codec/with_meta_options.h>
#include <memcached/protocol_binary.h>
#include <nlohmann/json.hpp>

namespace cb::mcbp {
WithMetaOptions::WithMetaOptions(uint32_t options) {
    if (options & (SKIP_CONFLICT_RESOLUTION_FLAG | FORCE_WITH_META_OP)) {
        // FORCE_WITH_META_OP used to change permittedVBStates to include
        // replica and pending, which is incredibly dangerous. This flag is
        // still supported but now only permits a change to the
        // checkConflicts flag. MB-67207
        check_conflicts = CheckConflicts::No;
    }

    if (options & FORCE_ACCEPT_WITH_META_OPS) {
        force_accept = ForceAcceptWithMetaOperation::Yes;
    }

    if (options & REGENERATE_CAS) {
        generate_cas = GenerateCas::Yes;
    }

    if (options & IS_EXPIRATION) {
        delete_source = DeleteSource::TTL;
    }
}

uint32_t WithMetaOptions::encode() {
    uint32_t options = 0;

    if (check_conflicts == CheckConflicts::No) {
        options |= SKIP_CONFLICT_RESOLUTION_FLAG;
    }

    if (force_accept == ForceAcceptWithMetaOperation::Yes) {
        options |= FORCE_ACCEPT_WITH_META_OPS;
    }

    if (generate_cas == GenerateCas::Yes) {
        options |= REGENERATE_CAS;
    }

    if (delete_source == DeleteSource::TTL) {
        options |= IS_EXPIRATION;
    }
    return options;
}
void to_json(nlohmann::json& j, const WithMetaOptions& opt) {
    j["generate_cas"] = opt.generate_cas == GenerateCas::Yes;
    j["check_conflicts"] = opt.check_conflicts == CheckConflicts::Yes;
    j["force_accept"] = opt.force_accept == ForceAcceptWithMetaOperation::Yes;
    j["is_expiration"] = opt.delete_source == DeleteSource::TTL;
}

void from_json(const nlohmann::json& j, WithMetaOptions& opt) {
    opt.generate_cas =
            j.value("generate_cas", false) ? GenerateCas::Yes : GenerateCas::No;
    opt.check_conflicts = j.value("check_conflicts", true) ? CheckConflicts::Yes
                                                           : CheckConflicts::No;
    opt.force_accept = j.value("force_accept", false)
                               ? ForceAcceptWithMetaOperation::Yes
                               : ForceAcceptWithMetaOperation::No;
    opt.delete_source = j.value("is_expiration", false)
                                ? DeleteSource::TTL
                                : DeleteSource::Explicit;
}

} // namespace cb::mcbp
