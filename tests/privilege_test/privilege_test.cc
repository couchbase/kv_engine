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

#include <cJSON_utils.h>
#include <gtest/gtest.h>
#include "daemon/privilege_database.h"

TEST(PrivilegeDatabaseTest, ParseLegalConfig) {
    unique_cJSON_ptr root(cJSON_CreateObject());
    {
        cJSON* trond = cJSON_CreateObject();
        cJSON_AddItemToObject(root.get(), "trond", trond);

        cJSON* privileges = cJSON_CreateArray();
        cJSON_AddItemToArray(privileges, cJSON_CreateString("Audit"));
        cJSON_AddItemToArray(privileges,
                             cJSON_CreateString("BucketManagement"));
        cJSON_AddItemToObject(trond, "privileges", privileges);

        cJSON* buckets = cJSON_CreateObject();
        cJSON_AddItemToObject(trond, "buckets", buckets);

        privileges = cJSON_CreateArray();
        cJSON_AddItemToArray(privileges, cJSON_CreateString("Read"));
        cJSON_AddItemToArray(privileges, cJSON_CreateString("Write"));
        cJSON_AddItemToArray(privileges, cJSON_CreateString("SimpleStats"));
        cJSON_AddItemToObject(buckets, "bucket1", privileges);

        privileges = cJSON_CreateArray();
        cJSON_AddItemToArray(privileges, cJSON_CreateString("Read"));

        cJSON_AddItemToObject(buckets, "bucket2", privileges);
    }

    cb::rbac::PrivilegeDatabase db(root.get());
    const auto& ue = db.lookup("trond");

    {
        cb::rbac::PrivilegeMask privs{};
        privs[int(cb::rbac::Privilege::Audit)] = true;
        privs[int(cb::rbac::Privilege::BucketManagement)] = true;
        EXPECT_EQ(privs, ue.getPrivileges());
    }

    const auto& buckets = ue.getBuckets();
    EXPECT_EQ(2, buckets.size());
    auto it = buckets.find("bucket1");
    EXPECT_NE(buckets.cend(), it);

    {
        cb::rbac::PrivilegeMask privs{};
        privs[int(cb::rbac::Privilege::Read)] = true;
        privs[int(cb::rbac::Privilege::Write)] = true;
        privs[int(cb::rbac::Privilege::SimpleStats)] = true;

        EXPECT_EQ(privs, it->second);
    }

    it = buckets.find("bucket2");
    EXPECT_NE(buckets.cend(), it);
    {
        cb::rbac::PrivilegeMask privs{};
        privs[int(cb::rbac::Privilege::Read)] = true;
        EXPECT_EQ(privs, it->second);
    }
}

TEST(PrivilegeDatabaseTest, GenerationCounter) {
    cb::rbac::PrivilegeDatabase db1(nullptr);
    cb::rbac::PrivilegeDatabase db2(nullptr);
    EXPECT_GT(db2.generation, db1.generation);
}
