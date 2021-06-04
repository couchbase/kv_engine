/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "event.h"

class AuditImpl;
class CookieIface;

class ConfigureEvent : public Event {
public:
    const CookieIface* cookie;
    const std::string file;

    ConfigureEvent(std::string configfile, const CookieIface* c)
        : cookie(c), file(std::move(configfile)) {
    }

    bool process(AuditImpl& audit) override;
};
