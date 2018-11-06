/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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

#include <daemon/mcbp_validators.h>
#include <mcbp/protocol/opcode.h>
#include <cstdlib>
#include <iostream>

/**
 * Small program to print out the opcodes we don't have a validator
 * for.
 *
 * @return EXIT_SUCCESS if we've got validators for all client opcodes
 *         EXIT_FAILURE otherwise
 */
int main() {
    class Validator : public McbpValidator {
    public:
        bool canValidate(ClientOpcode command) {
            const auto idx = std::underlying_type<ClientOpcode>::type(command);
            if (validators[idx]) {
                return true;
            }
            return false;
        }
    } validator;

    int code = EXIT_SUCCESS;

    for (int ii = 0; ii < 0x100; ++ii) {
        try {
            auto opcode = cb::mcbp::ClientOpcode(ii);
            to_string(opcode);
            if (!validator.canValidate(opcode)) {
                if (code == EXIT_SUCCESS) {
                    std::cerr << "Missing validator for:" << std::endl;
                }
                std::cerr << "\t" << to_string(opcode) << std::endl;
                code = EXIT_FAILURE;
            }
        } catch (const std::invalid_argument&) {
            // unknown command
        }
    }

    return code;
}
