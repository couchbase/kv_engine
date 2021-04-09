/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "mutation_log_entry.h"

#include <iomanip>

std::string to_string(MutationLogType t) {
    switch (t) {
    case MutationLogType::New:
        return "new";
        break;
    case MutationLogType::Commit1:
        return "commit1";
        break;
    case MutationLogType::Commit2:
        return "commit2";
        break;
    case MutationLogType::NumberOfTypes: {
        // fall through
    }
    }
    throw std::invalid_argument("to_string(MutationLogType) unknown param " +
                                std::to_string(int(t)));
}

// ----------------------------------------------------------------------
// Output of entries
// ----------------------------------------------------------------------
std::ostream& operator<<(std::ostream& out, const MutationLogEntryV1& mle) {
    out << "{MutationLogEntryV1 rowid=" << mle.rowid()
        << ", vbucket=" << mle.vbucket().get() << ", magic=0x" << std::hex
        << static_cast<uint16_t>(mle.magic) << std::dec
        << ", type=" << to_string(mle.type()) << ", key=``" << mle.key()
        << "''";
    return out;
}

std::ostream& operator<<(std::ostream& out, const MutationLogEntryV2& mle) {
    out << "{MutationLogEntryV2 vbucket=" << mle.vbucket().get() << ", magic=0x"
        << std::hex << static_cast<uint16_t>(mle.magic) << std::dec
        << ", type=" << to_string(mle.type()) << ", key=``" << mle.key().data()
        << "''";
    return out;
}

std::ostream& operator<<(std::ostream& out, const MutationLogEntryV3& mle) {
    out << "{MutationLogEntryV3 vbucket=" << mle.vbucket().get() << ", magic=0x"
        << std::hex << static_cast<uint16_t>(mle.magic) << std::dec
        << ", type=" << to_string(mle.type()) << ", key=``" << mle.key().data()
        << "''";
    return out;
}
