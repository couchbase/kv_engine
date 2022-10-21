/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "event.h"
#include "audit.h"
#include <logger/logger.h>
#include <memcached/isotime.h>
#include <nlohmann/json.hpp>
#include <utilities/logtags.h>

bool Event::process(AuditImpl& audit) {
    if (payload.find("timestamp") == payload.end()) {
        // the audit does not contain a timestamp, so the server
        // needs to insert one
        const auto timestamp = ISOTime::generatetimestamp();
        payload["timestamp"] = timestamp;
    }

    if (!audit.auditfile.ensure_open()) {
        LOG_WARNING("Audit: error opening audit file. Dropping event: {}",
                    cb::UserDataView(payload.dump()));
        return false;
    }

    if (audit.auditfile.write_event_to_disk(payload)) {
        return true;
    }

    LOG_WARNING("Audit: error writing event to disk. Dropping event: {}",
                cb::UserDataView(payload.dump()));

    // If the write_event_to_disk function returns false then it is
    // possible the audit file has been closed. Therefore, ensure
    // the file is open.
    audit.auditfile.ensure_open();
    return false;
}
