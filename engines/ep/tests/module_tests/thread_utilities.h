/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <folly/Function.h>
#include <folly/ScopeGuard.h>
#include <folly/synchronization/Baton.h>
#include <memory>
#include <thread>

using TypeErasedScopeGuard =
        decltype(folly::makeGuard(folly::Function<void()>()));

/**
 * Creates a std::thread and returns a scope guard which joins it.
 * TODO: Use std::jthread once it becomes available.
 */
template <typename Callable>
TypeErasedScopeGuard makeJoinableThread(Callable&& callable) {
    std::thread thread(std::forward<Callable>(callable));
    return folly::makeGuard(folly::Function<void()>(
            [thread = std::move(thread)]() mutable { thread.join(); }));
}

/**
 * Creates a std::thread and returns a scope guard which joins it.
 * The thread's execution is extended until the scope guard exists (the thread
 * waits to be joined).
 */
template <typename Callable>
TypeErasedScopeGuard makeLingeringThread(Callable&& callable) {
    auto join = std::make_unique<folly::Baton<>>();

    std::thread thread(
            [join = join.get(), callable = std::forward<Callable>(callable)]() {
                callable();
                join->wait();
            });

    return folly::makeGuard(folly::Function<void()>(
            [join = std::move(join), thread = std::move(thread)]() mutable {
                join->post();
                thread.join();
            }));
}
