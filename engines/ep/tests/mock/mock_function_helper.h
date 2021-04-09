/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <folly/portability/GMock.h>

#include <functional>

// MockFunction.AsStdFunction exists, but due to MB-37860 could not
// be used under windows. For now, this serves as a replacement
template <class Return, class... Args>
std::function<Return(Args...)> asStdFunction(
        testing::MockFunction<Return(Args...)>& mockFunction) {
    return [&mockFunction](Args&&... args) {
        return mockFunction.Call(std::forward<Args>(args)...);
    };
}