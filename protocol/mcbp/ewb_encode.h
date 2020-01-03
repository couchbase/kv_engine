/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc
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
 */
std::string encodeSequence(const std::vector<cb::engine_errc>& sequence);

/**
 * Value to use for EWBEngineMode::Sequence when the original engine API error
 *  code should just be passed through.
 */
static constexpr cb::engine_errc Passthrough = cb::engine_errc(-1);
} // namespace ewb
