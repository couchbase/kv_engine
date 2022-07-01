/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/*
 * Helper functions to encode EWouldBlockEngine specific packets.
 */

#pragma once

#include <memcached/engine_error.h>
#include <string>
#include <vector>

namespace ewb {
/**
 * Helper function for EWBEngineMode::Sequence, encodes a vector of
 * status codes to inject into network order to be used as the key.
 *
 * Each call into the engine will return the next error code in the sequence.
 * In the case of engine_errc::would_block, the /next/ element will be used
 * for the value used for notifyIoComplete - i.e. a single engine call will
 * consume 2 codes in the sequence.
 */
std::string encodeSequence(const std::vector<cb::engine_errc>& sequence);

/**
 * Value to use for EWBEngineMode::Sequence when the original engine API error
 *  code should just be passed through.
 */
static constexpr cb::engine_errc Passthrough = cb::engine_errc(-1);
} // namespace ewb
