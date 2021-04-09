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

// Utility functions used to build up packets in the memcached binary protocol
#include <protocol/connection/client_connection.h>

#include <folly/portability/GTest.h>
#include <sys/types.h>
#include <algorithm>
#include <cstdlib>

#include <memcached/protocol_binary.h>

/* Validate the specified response header against the expected cmd and status.
 */
void mcbp_validate_response_header(protocol_binary_response_no_extras* response,
                                   cb::mcbp::ClientOpcode cmd,
                                   cb::mcbp::Status status);

void mcbp_validate_response_header(cb::mcbp::Response& response,
                                   cb::mcbp::ClientOpcode cmd,
                                   cb::mcbp::Status status);

::testing::AssertionResult mcbp_validate_response_header(
        const cb::mcbp::Response& response,
        cb::mcbp::ClientOpcode cmd,
        cb::mcbp::Status status,
        bool mutation_seqno_enabled);
