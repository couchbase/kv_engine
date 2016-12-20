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

#include <memcached/protocol_binary.h>
#include <platform/sized_buffer.h>

#include "steppable_command_context.h"

/**
 * The StatsCommandContext is responsible for implementing all of the
 * various stats commands (including the sub commands).
 */
class StatsCommandContext : public SteppableCommandContext {
public:

    StatsCommandContext(McbpConnection& c,
                        const protocol_binary_request_stats& req)
        : SteppableCommandContext(c),
          key{req.bytes + sizeof(req.bytes) + req.message.header.request.extlen,
              ntohs(req.message.header.request.keylen)} {
    }

protected:
    /**
     * In most cases we won't be returning EWOULDBLOCK, and there isn't any
     * complex logic in the implementation of the stats commands so we can
     * just do everything in a single state. This means that we could end
     * up "parsing" the key into the key and sub command multiple times.
     * If this ever turns up to be a bottleneck you should consider to
     * stop polling stats that often ;-)
     *
     * @return ENGINE_SUCCESS if the command completed successfully
     */
    ENGINE_ERROR_CODE step() override;

private:
    /**
     * Helper method to call into the engine API with the appropriate
     * parameters.
     *
     * @param k The key to pass on to the underlying engine
     * @return the return code from the engine
     */
    ENGINE_ERROR_CODE get_stats(const cb::const_char_buffer& k);

    /**
     * The key as specified in the input buffer (it may contain a sub command)
     */
    const cb::const_byte_buffer key;
};
