/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2014 Couchbase, Inc.
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
#include <cbsasl/cbsasl.h>
#include <platform/platform.h>
#include "cbsasl/util.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>

int retval = EXIT_SUCCESS;

static void assert_equal(const char *input) {
    if (cbsasl_secure_compare(input, strlen(input), input, strlen(input)) != 0) {
        fprintf(stderr, "ERROR: assert_equal failed for [%s]", input);
        retval = EXIT_FAILURE;
    }
}

static void assert_different(const char *a, const char *b) {
    if (cbsasl_secure_compare(a, strlen(a), b, strlen(b)) == 0) {
        fprintf(stderr, "ERROR: Expected a difference between [%s] [%s]", a, b);
        retval = EXIT_FAILURE;
    }
}

int main(void)
{
    /* Check that it enforce the length without running outside the pointer */
    cb_assert(cbsasl_secure_compare(NULL, 0, NULL, 0) == 0);

    /* Check that it compares right with equal length and same input */
    assert_equal("");
    assert_equal("abcdef");
    assert_equal("@#$%^&*(&^%$#");
    assert_equal("jdfdsajk;14AFADSF517*(%(nasdfvlajk;ja''1345!%#!%$&%$&@$%@");

    /* check that it compares right with different lengths */
    assert_different("", "a");
    assert_different("abc", "");

    /* check that the case matter */
    assert_different("abcd", "ABCD");
    assert_different("ABCD", "abcd");
    assert_different("a", "A");

    /* check that it differs char from digits from special chars */
    assert_different("a", "1");
    assert_different("a", "!");
    assert_different("!", "a");
    assert_different("1", "a");

    return retval;
}
