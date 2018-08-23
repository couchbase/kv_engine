/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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
        << ", vbucket=" << mle.vbucket() << ", magic=0x" << std::hex
        << static_cast<uint16_t>(mle.magic) << std::dec
        << ", type=" << to_string(mle.type()) << ", key=``" << mle.key()
        << "''";
    return out;
}

std::ostream& operator<<(std::ostream& out, const MutationLogEntryV2& mle) {
    out << "{MutationLogEntryV2"
        << " vbucket=" << mle.vbucket() << ", magic=0x" << std::hex
        << static_cast<uint16_t>(mle.magic) << std::dec
        << ", type=" << to_string(mle.type()) << ", key=``" << mle.key().data()
        << "''";
    return out;
}

std::ostream& operator<<(std::ostream& out, const MutationLogEntryV3& mle) {
    out << "{MutationLogEntryV3"
        << " vbucket=" << mle.vbucket() << ", magic=0x" << std::hex
        << static_cast<uint16_t>(mle.magic) << std::dec
        << ", type=" << to_string(mle.type()) << ", key=``" << mle.key().data()
        << "''";
    return out;
}
