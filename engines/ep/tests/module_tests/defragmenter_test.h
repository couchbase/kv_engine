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

#pragma once

#include "vbucket_test.h"

#include "objectregistry.h"

#include <platform/cb_arena_malloc.h>

#include <programs/engine_testapp/mock_server.h>

#include <folly/portability/GTest.h>

class DefragmenterTest
    : public VBucketTestBase,
      public ::testing::TestWithParam<std::tuple<EvictionPolicy, bool> > {
public:
    DefragmenterTest();
    ~DefragmenterTest();

protected:
    void SetUp() override {
        global_stats.arena = cb::ArenaMalloc::registerClient();
        cb::ArenaMalloc::switchToClient(global_stats.arena);
    }

    void TearDown() override {
        cb::ArenaMalloc::unregisterClient(global_stats.arena);
    }

    /**
     * Sets num_docs documents each with a doc size of docSize. This method
     * avoids polluting the heap to assist in measuring memory usage.
     *
     * @param docSize The size in bytes of each items blob
     * @param num_docs The number of docs to set
     */
    void setDocs(size_t docSize, size_t num_docs);

    /**
     * Remove all but one document or StoredValue in each page. This is to
     * create a situation where the defragmenter runs for every document.
     *
     * @param num_docs The number of docs that have been set
     * @param[out] num_remaining The resulting number documents that are left
     * after the fragmentation
     */
    void fragment(size_t num_docs, size_t &num_remaining);

    /// @return test param tuple element 1, true defrag stored value
    bool isModeStoredValue() const;

    /// @return the policy in use for the test
    EvictionPolicy getEvictionPolicy() const;

    /**
     * The value vs StoredValue variant of the defragger test uses different
     * length keys.
     * In SV mode we want lots of StoredValue memory allocated, so we pick the
     * largest key length we're allowed, which is 256 internally
     */
    const char* keyPattern1 = "%d";
    const char* keyPattern2 = "%0250d";
    const char* keyPattern{nullptr};
    char keyScratch[257];
};
