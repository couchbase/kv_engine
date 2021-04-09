/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <gmock/gmock-generated-matchers.h>

/**
 * Implementation details for checkpoint_test and related subclasses.
 */

/**
 * Custom GTest parameterized matcher for checking that the argument (e.g. Item)
 * has the given operation.
 */
MATCHER_P(HasOperation, op, "") {
    return arg->getOperation() == op;
}
