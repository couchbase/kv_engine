/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "steppable_command_context.h"

/**
 * The SessionValidatedCommandContext is a state machine used by all commands
 * which require the session control context to be locked during the duration
 * of their operation (compactDB, Set/DelVbucket to name a few).
 */
class SessionValidatedCommandContext : public SteppableCommandContext {
public:
    explicit SessionValidatedCommandContext(Cookie& cookie);
    ~SessionValidatedCommandContext() override;

protected:
    /// The step method is called from the SteppableCommandContext's drive()
    /// method. The step method will return an error if the requested CAS was
    /// illegal, otherwise it'll call the "sessionLockedStep()" method
    /// (implemented by the various subclasses) and send a response message
    /// back to the client iff sessionLockedStep returns
    /// cb::engine_errc::success (which indicates that the operation is done).
    /// SteppableCommandContext will send the appropriate error message and
    /// handle EWOULDBLOCK for us)
    cb::engine_errc step() override;

    /// Subclass of the SessionValidatedCommandContext must override this
    /// method to perform the action they're supposed to do
    virtual cb::engine_errc sessionLockedStep() = 0;

private:
    /// Set to true if the CAS for the connection is legal and we've registered
    /// the session control counter
    const bool valid;
};

/// Class to implement the SetParam command
class SetParameterCommandContext : public SessionValidatedCommandContext {
public:
    explicit SetParameterCommandContext(Cookie& cookie);

protected:
    cb::engine_errc sessionLockedStep() override;

private:
    const EngineParamCategory category;
};

/// Class to implement the CompactDB command
class CompactDatabaseCommandContext : public SessionValidatedCommandContext {
public:
    explicit CompactDatabaseCommandContext(Cookie& cookie)
        : SessionValidatedCommandContext(cookie) {
    }

protected:
    cb::engine_errc sessionLockedStep() override;
};

/// Class to implement the SetVBucket command
class SetVbucketCommandContext : public SessionValidatedCommandContext {
public:
    explicit SetVbucketCommandContext(Cookie& cookie);

protected:
    cb::engine_errc sessionLockedStep() override;

private:
    vbucket_state_t state;
    nlohmann::json meta;
    std::string error;
};

/// Class to implement the DelVBucket command
class DeleteVbucketCommandContext : public SessionValidatedCommandContext {
public:
    explicit DeleteVbucketCommandContext(Cookie& cookie)
        : SessionValidatedCommandContext(cookie) {
    }

protected:
    cb::engine_errc sessionLockedStep() override;
};

/// Class to implement the GetVBucket command.
///
/// The class don't really belong here as it don't need to lock the session
/// control counter, but it seemed weird that the engine interface had methods
/// to set and delete the vbucket state but not get.
class GetVbucketCommandContext : public SteppableCommandContext {
public:
    explicit GetVbucketCommandContext(Cookie& cookie)
        : SteppableCommandContext(cookie) {
    }

protected:
    cb::engine_errc step() override;
};
