/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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

#pragma once

#include "auditd/src/auditconfig.h"
#include <nlohmann/json_fwd.hpp>

class MockAuditConfig : public AuditConfig {
public:
    MockAuditConfig()
        : AuditConfig(){
                  // Empty
          };

    void public_set_disabled(const nlohmann::json& array) {
        AuditConfig::set_disabled(array);
    }

    void public_set_disabled_userids(const nlohmann::json& array) {
        AuditConfig::set_disabled_userids(array);
    }
};
