/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <cJSON.h>
#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>

#undef NDEBUG
#include <assert.h>

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

static void einval(void)
{
   char buff[1];
   char *fnm = buff;
   cJSON *json;
   assert(config_load_file(NULL, NULL) == CONFIG_INVALID_ARGUMENTS);
   assert(config_load_file(fnm, NULL) == CONFIG_INVALID_ARGUMENTS);
   assert(config_load_file(NULL, &json) == CONFIG_INVALID_ARGUMENTS);
}

static void no_such_file(void)
{
   cJSON *ptr;
   config_error_t err = config_load_file("/it/would/suck/if/this/file/exists",
                                         &ptr);
   assert(err == CONFIG_NO_SUCH_FILE);
}

static void open_failed(void)
{
#ifndef WIN32
   /* @todo figure out how do chmod a file on windows ;) */
   cJSON *ptr;
   config_error_t err;
   FILE *fp = fopen("config_test", "w");

   if (fp == NULL) {
      fprintf(stderr, "Failed to create file \"config_test\": %s\n",
              strerror(errno));
      exit(EXIT_FAILURE);
   }

   fclose(fp);
   chmod("config_test", S_IWUSR);

   err = config_load_file("config_test", &ptr);
   remove("config_test");
   assert(err == CONFIG_OPEN_FAILED);
#endif
}

static void malloc_failed(void)
{
   cJSON *ptr;
   config_error_t err;
   FILE *fp = fopen("config_test", "w");

   if (fp == NULL) {
      fprintf(stderr, "Failed to create file \"config_test\": %s\n",
              strerror(errno));
      exit(EXIT_FAILURE);
   }

   fclose(fp);

   setenv("CONFIG_TEST_MOCK_MALLOC_FAILURE", "on", 1);
   err = config_load_file("config_test", &ptr);
   unsetenv("CONFIG_TEST_MOCK_MALLOC_FAILURE");

   remove("config_test");

   assert(err == CONFIG_MALLOC_FAILED);
}

static void io_error(void)
{
   cJSON *ptr;
   config_error_t err;
   FILE *fp = fopen("config_test", "w");

   if (fp == NULL) {
      fprintf(stderr, "Failed to create file \"config_test\": %s\n",
              strerror(errno));
      exit(EXIT_FAILURE);
   }

   fclose(fp);

   setenv("CONFIG_TEST_MOCK_SPOOL_FAILURE", "on", 1);
   err = config_load_file("config_test", &ptr);
   unsetenv("CONFIG_TEST_MOCK_SPOOL_FAILURE");

   remove("config_test");

   assert(err == CONFIG_IO_ERROR);
}

static void parse_error(void)
{
   cJSON *ptr;
   config_error_t err;
   FILE *fp = fopen("config_test", "w");

   if (fp == NULL) {
      fprintf(stderr, "Failed to create file \"config_test\": %s\n",
              strerror(errno));
      exit(EXIT_FAILURE);
   }

   fprintf(fp, "{ foo : bar }");
   fclose(fp);

   err = config_load_file("config_test", &ptr);
   remove("config_test");

   assert(err == CONFIG_PARSE_ERROR);
}

static void parse_success(void)
{
   cJSON *ptr = NULL;
   config_error_t err;
   FILE *fp = fopen("config_test", "w");

   if (fp == NULL) {
      fprintf(stderr, "Failed to create file \"config_test\": %s\n",
              strerror(errno));
      exit(EXIT_FAILURE);
   }

   fprintf(fp, "{ \"string\" : \"stringval\", \"number\" : 123 }");
   fclose(fp);

   err = config_load_file("config_test", &ptr);
   remove("config_test");
   assert(ptr != NULL);
   cJSON_Delete(ptr);
   assert(err == CONFIG_SUCCESS);
}

static void test_config_strerror(void)
{
    int ii;
    errno = ENOENT;
    for (ii = 0; ii < 1000; ++ii) {
        char *msg = config_strerror("foo", (config_error_t)ii);
        assert(msg != NULL);
        free(msg);
    }
}

int main(void)
{
   einval();
   no_such_file();
   open_failed();
   malloc_failed();
   io_error();
   parse_error();
   parse_success();

   test_config_strerror();

   return 0;
}
