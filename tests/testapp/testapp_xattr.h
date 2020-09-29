/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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

#include "testapp_client_test.h"
#include "xattr/blob.h"

#include <platform/cb_malloc.h>

/**
 * Text fixture for XAttr tests which do not want to have an initial document
 * created.
 */
class XattrNoDocTest : public TestappXattrClientTest {
protected:
    void SetUp() override {
        TestappXattrClientTest::SetUp();
    }

    BinprotSubdocResponse subdoc_get(
            MemcachedConnection& conn,
            const std::string& path,
            protocol_binary_subdoc_flag flag = SUBDOC_FLAG_NONE,
            mcbp::subdoc::doc_flag docFlag = mcbp::subdoc::doc_flag::None) {
        return subdoc(conn,
                      cb::mcbp::ClientOpcode::SubdocGet,
                      name,
                      path,
                      {},
                      flag,
                      docFlag);
    }

    BinprotSubdocMultiLookupResponse subdoc_multi_lookup(
            MemcachedConnection& conn,
            std::vector<BinprotSubdocMultiLookupCommand::LookupSpecifier> specs,
            mcbp::subdoc::doc_flag docFlags = mcbp::subdoc::doc_flag::None);

    BinprotSubdocMultiMutationResponse subdoc_multi_mutation(
            MemcachedConnection& conn,
            std::vector<BinprotSubdocMultiMutationCommand::MutationSpecifier>
                    specs,
            mcbp::subdoc::doc_flag docFlags = mcbp::subdoc::doc_flag::None);

    GetMetaResponse get_meta(MemcachedConnection& conn);

    bool supportSyncRepl() const {
        return mcd_env->getTestBucket().supportsSyncWrites();
    }

    /// Constructs a Subdoc multi-mutation matching the style of an SDK
    // Transactions mutation.
    BinprotSubdocMultiMutationCommand makeSDKTxnMultiMutation() const;

    void testRequiresMkdocOrAdd();
    void testRequiresXattrPath();
    void testSinglePathDictAdd();
    void testMultipathDictAdd();
    void testMultipathDictUpsert();
    void testMultipathArrayPushLast();
    void testMultipathArrayPushFirst();
    void testMultipathArrayAddUnique();
    void testMultipathCounter();
    void testMultipathCombo();
    void testMultipathAccessDeletedCreateAsDeleted();

    std::optional<cb::durability::Requirements> durReqs = {};
};

class XattrNoDocDurabilityTest : public XattrNoDocTest {
protected:
    void SetUp() override {
        XattrNoDocTest::SetUp();
        // level:majority, timeout:default
        durReqs = cb::durability::Requirements();
    }
};
