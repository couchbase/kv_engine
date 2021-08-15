/*
 *    Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <ostream>

enum class TerminalColor {
    Black,
    Red,
    Green,
    Yellow,
    Blue,
    Magenta,
    Cyan,
    White,
    Reset
};

/// Enable/disable support for using colors. When enabled the << operator
/// will print out the escape sequence to set the font color. When disabled
/// the operator don't insert anything to the stream.
void setTerminalColorSupport(bool enable);

/// Insert the formatting code the requested color to the stream
std::ostream& operator<<(std::ostream& os, const TerminalColor& color);
