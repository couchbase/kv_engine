/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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

#include <cbsasl/util.h>
#include "check_password.h"


namespace cb {
namespace sasl {
namespace plain {

cbsasl_error_t check_password(const Couchbase::User& user,
                              const std::string& userpw) {
    auto storedPassword = user.getPassword(Mechanism::PLAIN).getPassword();

    auto digest = cb::crypto::digest(cb::crypto::Algorithm::SHA1, userpw);
    if (cbsasl_secure_compare(storedPassword.data(), storedPassword.length(),
                              digest.data(), digest.length()) == 0 &&
        !user.isDummy()) {
        return CBSASL_OK;
    } else {
        return CBSASL_PWERR;
    }

}

}
}
}
