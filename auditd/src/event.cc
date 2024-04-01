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
#include <nlohmann/json.hpp>
#include <utilities/logtags.h>

bool Event::process(AuditImpl& audit) {
    return audit.write_to_audit_trail(payload);
}

bool Event::drop_if_audit_disabled() {
    return true;
}
