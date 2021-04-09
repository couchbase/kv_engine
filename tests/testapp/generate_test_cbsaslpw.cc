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

#include <cbsasl/user.h>
#include <fstream>
#include <iomanip>
#include <iostream>

int main() {
    std::ofstream isaslpw("isasl.pw");

    isaslpw << "@admin password" << std::endl
            << "bucket-1 1S|=,%#x1" << std::endl
            << "bucket-2 secret" << std::endl
            << "smith smithpassword" << std::endl
            << "jones jonespassword" << std::endl
            << "larry larrypassword" << std::endl
            << "legacy;legacy legacy" << std::endl
            << "legacy new" << std::endl
            << "default" << std::endl;

    isaslpw.close();

    cb::sasl::pwdb::UserFactory::setDefaultHmacIterationCount(10);

    int exitcode = EXIT_SUCCESS;

    try {
        cb::sasl::pwdb::convert("isasl.pw", "cbsaslpw.json");
    } catch (const std::exception &e) {
        std::cerr << "FATAL: Failed to write cbsaslpw.json: " << e.what()
                  << std::endl;
        exitcode = EXIT_FAILURE;
    }

    // Remove the input file
    std::remove("isasl.pw");

    return exitcode;
}
