/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "testapp.h"

#include "testapp_client_test.h"
#include <mcbp/codec/frameinfo.h>
#include <nlohmann/json.hpp>
#include <platform/dirutils.h>
#include <string>

class AuditTest : public TestappClientTest {
public:
    void SetUp() override;

    void TearDown() override;

    void setEnabled(bool mode);

    static void reconfigureAudit();

    static std::vector<nlohmann::json> readAuditData();

    static std::vector<nlohmann::json> splitJsonData(const std::string& input);

    static bool searchAuditLogForID(int id,
                                    const std::string& username = "",
                                    const std::string& bucketname = "");

    /**
     * Iterate over all of the entries found in the log file(s) over and
     * over until the callback method returns false.
     *
     * @param callback the callback containing the audit event
     */
    static void iterate(
            const std::function<bool(const nlohmann::json&)>& callback);

    static int getAuditCount(const std::vector<nlohmann::json>& entries,
                             int id);
};
