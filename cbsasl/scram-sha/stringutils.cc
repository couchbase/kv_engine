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
#include "stringutils.h"

#include <stdexcept>
#include <cctype>

/**
 * According to the RFC:
 *
 * 2.3.  Prohibited Output
 *
 *    This profile specifies the following characters as prohibited input:
 *
 *       - Non-ASCII space characters [StringPrep, C.1.2]
 *       - ASCII control characters [StringPrep, C.2.1]
 *       - Non-ASCII control characters [StringPrep, C.2.2]
 *       - Private Use characters [StringPrep, C.3]
 *       - Non-character code points [StringPrep, C.4]
 *       - Surrogate code points [StringPrep, C.5]
 *       - Inappropriate for plain text characters [StringPrep, C.6]
 *       - Inappropriate for canonical representation characters
 *         [StringPrep, C.7]
 *       - Change display properties or deprecated characters
 *         [StringPrep, C.8]
 *       - Tagging characters [StringPrep, C.9]
 */
std::string SASLPrep(const std::string& string) {
    for (const auto& c : string) {
        if (c & 0x80) {
            throw std::runtime_error("SASLPrep: Multibyte UTF-8 is not"
                                         " implemented yet");
        }

        if (iscntrl(c)) {
            throw std::runtime_error("SASLPrep: control characters is not"
                                         " allowed");
        }
    }

    return string;
}
