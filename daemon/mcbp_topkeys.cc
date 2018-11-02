/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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

#include "config.h"
#include "mcbp_topkeys.h"

#include <memcached/protocol_binary.h>

/**
 * Define valid commands to track operations on keys. True commands
 * will be tracked, false will not.
 */
std::array<bool, 0x100>& get_mcbp_topkeys() {
    static std::array<bool, 0x100> commands;

    commands[uint8_t(cb::mcbp::ClientOpcode::Setq)] = true;
    commands[uint8_t(cb::mcbp::ClientOpcode::Set)] = true;
    commands[uint8_t(cb::mcbp::ClientOpcode::Addq)] = true;
    commands[uint8_t(cb::mcbp::ClientOpcode::Add)] = true;
    commands[uint8_t(cb::mcbp::ClientOpcode::Replaceq)] = true;
    commands[uint8_t(cb::mcbp::ClientOpcode::Replace)] = true;
    commands[uint8_t(cb::mcbp::ClientOpcode::Appendq)] = true;
    commands[uint8_t(cb::mcbp::ClientOpcode::Append)] = true;
    commands[uint8_t(cb::mcbp::ClientOpcode::Prependq)] = true;
    commands[uint8_t(cb::mcbp::ClientOpcode::Prepend)] = true;
    commands[uint8_t(cb::mcbp::ClientOpcode::Get)] = true;
    commands[uint8_t(cb::mcbp::ClientOpcode::Getq)] = true;
    commands[uint8_t(cb::mcbp::ClientOpcode::Getk)] = true;
    commands[uint8_t(cb::mcbp::ClientOpcode::Getkq)] = true;
    commands[uint8_t(cb::mcbp::ClientOpcode::Delete)] = true;
    commands[uint8_t(cb::mcbp::ClientOpcode::Deleteq)] = true;
    commands[uint8_t(cb::mcbp::ClientOpcode::Increment)] = true;
    commands[uint8_t(cb::mcbp::ClientOpcode::Incrementq)] = true;
    commands[uint8_t(cb::mcbp::ClientOpcode::Decrement)] = true;
    commands[uint8_t(cb::mcbp::ClientOpcode::Decrementq)] = true;
    commands[uint8_t(cb::mcbp::ClientOpcode::SubdocGet)] = true;
    commands[uint8_t(cb::mcbp::ClientOpcode::SubdocExists)] = true;
    commands[uint8_t(cb::mcbp::ClientOpcode::SubdocDictAdd)] = true;
    commands[uint8_t(cb::mcbp::ClientOpcode::SubdocDictUpsert)] = true;
    commands[uint8_t(cb::mcbp::ClientOpcode::SubdocDelete)] = true;
    commands[uint8_t(cb::mcbp::ClientOpcode::SubdocReplace)] = true;
    commands[uint8_t(cb::mcbp::ClientOpcode::SubdocArrayPushLast)] = true;
    commands[uint8_t(cb::mcbp::ClientOpcode::SubdocArrayPushFirst)] = true;
    commands[uint8_t(cb::mcbp::ClientOpcode::SubdocArrayInsert)] = true;
    commands[uint8_t(cb::mcbp::ClientOpcode::SubdocArrayAddUnique)] = true;
    commands[uint8_t(cb::mcbp::ClientOpcode::GetReplica)] = true;
    commands[uint8_t(cb::mcbp::ClientOpcode::EvictKey)] = true;
    commands[uint8_t(cb::mcbp::ClientOpcode::GetLocked)] = true;
    commands[uint8_t(cb::mcbp::ClientOpcode::UnlockKey)] = true;
    commands[uint8_t(cb::mcbp::ClientOpcode::GetMeta)] = true;
    commands[uint8_t(cb::mcbp::ClientOpcode::GetqMeta)] = true;
    commands[uint8_t(cb::mcbp::ClientOpcode::SetWithMeta)] = true;
    commands[uint8_t(cb::mcbp::ClientOpcode::SetqWithMeta)] = true;
    commands[uint8_t(cb::mcbp::ClientOpcode::DelWithMeta)] = true;
    commands[uint8_t(cb::mcbp::ClientOpcode::DelqWithMeta)] = true;

    return commands;
}
