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

static const char* logType(uint8_t t) {
    switch (t) {
    case ML_NEW:
        return "new";
        break;
    case ML_COMMIT1:
        return "commit1";
        break;
    case ML_COMMIT2:
        return "commit2";
        break;
    }
    return "UNKNOWN";
}

uint64_t MutationLogEntry::rowid() const {
    return ntohll(_rowid);
}

// ----------------------------------------------------------------------
// Output of entries
// ----------------------------------------------------------------------

std::ostream& operator<<(std::ostream& out, const MutationLogEntry& mle) {
    out << "{MutationLogEntry rowid=" << mle.rowid()
        << ", vbucket=" << mle.vbucket() << ", magic=0x" << std::hex
        << static_cast<uint16_t>(mle.magic) << std::dec
        << ", type=" << logType(mle.type()) << ", key=``" << mle.key().data()
        << "''";
    return out;
}
