/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <memcached/engine_error.h>
#include <memcached/types.h>

/**
 *  A command may need to store command specific context during the duration
 *  of a command (you might for instance want to keep state between multiple
 *  calls that returns EWOULDBLOCK).
 *
 *  The implementation of such commands should subclass this class and
 *  allocate an instance and store in the commands commandContext member (which
 *  will be deleted and set to nullptr between each command being processed).
 */
class CommandContext {
public:
    virtual ~CommandContext(){};

    /**
     * The `pre_link_document()` is a hook called from the underlying engine
     * as part of the `store()` method in the engine API. See the `pre_link()`
     * method in the `SERVER_DOCUMENT_API` provided by the server for a
     * detailed description.
     */
    virtual cb::engine_errc pre_link_document(item_info&) {
        return cb::engine_errc::success;
    }
};
