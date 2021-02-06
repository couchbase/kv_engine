/*
 *     Copyright 2021 Couchbase, Inc.
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
    /// back to the client iff sessionLockedStep returns ENGINE_SUCCESS
    /// (which indicates that the operation is done). SteppableCommandContext
    /// will send the appropriate error message and handle EWOULDBLOCK for us)
    ENGINE_ERROR_CODE step() override;

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
    ENGINE_ERROR_CODE step() override;
};
