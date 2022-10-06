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

/*
 *                "ewouldblock_engine"
 *
 * The "ewouldblock_engine" allows one to test how memcached responds when the
 * engine returns EWOULDBLOCK instead of the correct response.
 */
#pragma once

#include <cstdint>

// The mode the engine is currently operating in. Determines when it will
// inject EWOULDBLOCK instead of the real return code.
enum class EWBEngineMode : uint32_t {
    // Make the next_N calls into engine return {inject_error}. N specified
    // by the {value} field.
    Next_N = 0,

    // Randomly return {inject_error}. Chance to return {inject_error} is
    // specified as an integer percentage (1,100) in the {value} field.
    Random = 1,

    // The first call to a given function from each connection will return
    // {inject_error}, with the next (and subsequent) calls to the *same*
    // function operating normally. Calling a different function will reset
    // back to failing again.  In other words, return {inject_error} if the
    // previous function was not this one.
    First = 2,

    // Make the next N calls return the specified status code (or their normal
    // value if -1 is specified).
    // The sequence is encoded in the request key (given it is of variable
    // length) as an array of cb::engine_errc codes (uint32_t).
    // If cb::engine_errc::wouldblock is specified, then the following element
    // in the array specifies the status code that notify_io_complete() should
    // return.
    Sequence = 3,

    // Simulate CAS mismatch - make the next N store operations return
    // KEY_EEXISTS. N specified by the {value} field.
    CasMismatch = 4,

    // Make a single call into engine and return {inject_error}.  In addition
    // do not add the operation to the processing queue and so a
    // notify_io_complete is never sent.
    No_Notify = 6,

    // Suspend a cookie with the provided id and return
    // cb::engine_errc::would_block.
    // The connection must be resumed with a call to Resume
    Suspend = 7,

    // Resume a cookie with the provided id
    Resume = 8,

    // Next time the connection invokes a call we'll start monitoring a file
    // for existence, and when the file goes away we'll notify the connection
    // with the {inject_error}.
    // The file to monitor is specified in the key for the packet.
    // This seems like an odd interface to have, but it is needed to be able
    // to test what happens with clients that is working inside the engine
    // while a bucket is deleted. Given that we're not instructing the
    // ewouldblock engine on a special channel there is no way to send
    // commmands to the engine whlie it is being deleted ;-)
    BlockMonitorFile = 9,

    // Set the CAS for an item.
    // Requires the CAS of the item. Bear in mind that we're limited to
    // 32 bits.
    SetItemCas = 10,

    // Make a single call to cb::logger::check_levels and return the result.
    // This allows us to verify that all of the registered logger instances are
    // set to the correct level.
    CheckLogLevels = 11,

    // Throw an exception
    ThrowException = 12,
};
