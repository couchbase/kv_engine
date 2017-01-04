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
#include <memcached/protocol_binary.h>
#include <memcached/types.h>

namespace mcbp {
/**
 * Convert an engine error code to a status code as used in the memcached
 * binary protocol.
 *
 * @param code The engine error code specified
 * @return the response message for that engine error code
 * @throws std::logic_error for error codes it doesn't make any sense to
 *                          try to convert
 *         std::invalid_argument for unknown error codes
 */
protocol_binary_response_status to_status(cb::engine_errc code);
}

/**
 * Convert an error code generated from the storage engine to the corresponding
 * error code used by the protocol layer.
 * @param e the error code as used in the engine
 * @return the error code as used by the protocol layer
 */
static inline protocol_binary_response_status engine_error_2_mcbp_protocol_error(
    ENGINE_ERROR_CODE e) {
    return mcbp::to_status(cb::engine_errc(e));
}
