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
#include "config.h"
#include <cJSON.h>
#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>
#include <gtest/gtest.h>

#include "daemon/config_util.h"

#ifdef WIN32
static void setenv(const char *key, const char *value, int overwrite)
{
    char obj[1024];
    sprintf(obj, "%s=%s", key, value);
    _putenv(_strdup(obj));
}

static void unsetenv(const char *key)
{
    char obj[1024];
    sprintf(obj, "%s=", key);
    _putenv(_strdup(obj));
}
#endif

TEST(ConfigUtil, Einval)
{
   char buff[1];
   char *fnm = buff;
   cJSON *json;
   EXPECT_EQ(CONFIG_INVALID_ARGUMENTS, config_load_file(NULL, NULL));
   EXPECT_EQ(CONFIG_INVALID_ARGUMENTS, config_load_file(fnm, NULL));
   EXPECT_EQ(CONFIG_INVALID_ARGUMENTS, config_load_file(NULL, &json));
}

TEST(ConfigUtil, NoSuchFile)
{
   cJSON *ptr;
   config_error_t err = config_load_file("/it/would/suck/if/this/file/exists",
                                         &ptr);
   EXPECT_EQ(CONFIG_NO_SUCH_FILE, err);
}

#ifndef WIN32
TEST(ConfigUtil, OpenFailed)
{
   /* @todo figure out how do chmod a file on windows ;) */
   cJSON *ptr;
   config_error_t err;
   FILE *fp = fopen("config_test", "w");

   ASSERT_NE(nullptr, fp)
       << "Failed to create file \"config_test\": " << strerror(errno);

   fclose(fp);
   chmod("config_test", S_IWUSR);

   err = config_load_file("config_test", &ptr);
   EXPECT_EQ(CONFIG_OPEN_FAILED, err);
   EXPECT_EQ(0, remove("config_test"));
}
#endif

TEST(ConfigUtil, MallocFailed)
{
   cJSON *ptr;
   config_error_t err;
   FILE *fp = fopen("config_test", "w");

   ASSERT_NE(nullptr, fp)
       << "Failed to create file \"config_test\": " << strerror(errno);

   fclose(fp);

   setenv("CONFIG_TEST_MOCK_MALLOC_FAILURE", "on", 1);
   err = config_load_file("config_test", &ptr);
   unsetenv("CONFIG_TEST_MOCK_MALLOC_FAILURE");

   EXPECT_EQ(CONFIG_MALLOC_FAILED, err);
   EXPECT_EQ(0, remove("config_test"));
}

TEST(ConfigUtil, IoError)
{
   cJSON *ptr;
   config_error_t err;
   FILE *fp = fopen("config_test", "w");

   ASSERT_NE(nullptr, fp)
       << "Failed to create file \"config_test\": " << strerror(errno);

   fclose(fp);

   setenv("CONFIG_TEST_MOCK_SPOOL_FAILURE", "on", 1);
   err = config_load_file("config_test", &ptr);
   unsetenv("CONFIG_TEST_MOCK_SPOOL_FAILURE");

   EXPECT_EQ(CONFIG_IO_ERROR, err);
   EXPECT_EQ(0, remove("config_test"));
}

TEST(ConfigUtil, ParseError)
{
   cJSON *ptr;
   config_error_t err;
   FILE *fp = fopen("config_test", "w");

   ASSERT_NE(nullptr, fp)
       << "Failed to create file \"config_test\": " << strerror(errno);

   fprintf(fp, "{ foo : bar }");
   fclose(fp);

   err = config_load_file("config_test", &ptr);

   EXPECT_EQ(CONFIG_PARSE_ERROR, err);
   EXPECT_EQ(0, remove("config_test"));
}

TEST(ConfigUtil, ParseErrorTrunctatedInput)
{
    cJSON *ptr;
    config_error_t err;
    FILE *fp = fopen("config_test", "w");

    ASSERT_NE(nullptr, fp)
        << "Failed to create file \"config_test\": " << strerror(errno);

    fprintf(fp, "{ \"foo\" : ");
    fclose(fp);

    err = config_load_file("config_test", &ptr);

    EXPECT_EQ(CONFIG_PARSE_ERROR, err);
    EXPECT_EQ(0, remove("config_test"));
}

TEST(ConfigUtil, ParseSuccess)
{
   cJSON *ptr = NULL;
   config_error_t err;
   FILE *fp = fopen("config_test", "w");

   ASSERT_NE(nullptr, fp)
       << "Failed to create file \"config_test\": " << strerror(errno);

   fprintf(fp, "{ \"string\" : \"stringval\", \"number\" : 123 }");
   fclose(fp);

   err = config_load_file("config_test", &ptr);

   EXPECT_NE(nullptr, ptr);
   cJSON_Delete(ptr);
   EXPECT_EQ(CONFIG_SUCCESS, err);
   EXPECT_EQ(0, remove("config_test"));
}

TEST(ConfigUtil, ConfigStrerror)
{
    int ii;
    errno = ENOENT;
    for (ii = 0; ii < 1000; ++ii) {
        std::string msg = config_strerror("foo", (config_error_t)ii);
        EXPECT_FALSE(msg.empty());
    }
}
