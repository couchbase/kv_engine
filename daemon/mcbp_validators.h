/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
*     Copyright 2015 Couchbase, Inc
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

/*
 * memcached binary protocol packet validators
 */
#pragma once


#ifdef __cplusplus
extern "C" {
#endif

    typedef int (*mcbp_package_validate)(void *packet);

    /**
     * Get the memcached binary protocol validators
     *
     * @return the array of 0x100 entries for the package
     *         validators
     */
    mcbp_package_validate *get_mcbp_validators(void);

#ifdef __cplusplus
}
#endif
