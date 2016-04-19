/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"

#include <cJSON.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <stdint.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <JSON_checker.h>

#include "config_util.h"

char *config_strerror(const char *file, config_error_t err)
{
    char buffer[1024];
    switch (err) {
    case CONFIG_SUCCESS:
        return strdup("success");
    case CONFIG_INVALID_ARGUMENTS:
        return strdup("Invalid arguments supplied to config_load_file");
    case CONFIG_NO_SUCH_FILE:
        snprintf(buffer, sizeof(buffer), "Failed to look up \"%s\": %s",
                 file, strerror(errno));
        return strdup(buffer);
    case CONFIG_OPEN_FAILED:
        snprintf(buffer, sizeof(buffer), "Failed to open \"%s\": %s",
                 file, strerror(errno));
        return strdup(buffer);
    case CONFIG_MALLOC_FAILED:
        return strdup("Failed to allocate memory");
    case CONFIG_IO_ERROR:
        snprintf(buffer, sizeof(buffer), "Failed to read \"%s\": %s",
                 file, strerror(errno));
        return strdup(buffer);
    case CONFIG_PARSE_ERROR:
        snprintf(buffer, sizeof(buffer),
                 "Failed to parse JSON in \"%s\"\nMost likely syntax "
                "error in the file.", file);
        return strdup(buffer);

    default:
        snprintf(buffer, sizeof(buffer), "Unknown error code %u", err);
        return strdup(buffer);
    }
}

static int spool(FILE *fp, char *dest, size_t size)
{
    size_t offset = 0;

    if (getenv("CONFIG_TEST_MOCK_SPOOL_FAILURE") != NULL) {
        return -1;
    }

    clearerr(fp);
    while (offset < size) {
        offset += fread(dest + offset, 1, size - offset, fp);
        if (ferror(fp)) {
            return -1;
        }
    }

    return 0;
}

static void *config_malloc(size_t size) {
    if (getenv("CONFIG_TEST_MOCK_MALLOC_FAILURE") != NULL) {
        return NULL;
    } else {
        return malloc(size);
    }
}

config_error_t config_load_file(const char *file, cJSON **json)
{
    FILE *fp;
    struct stat st;
    char *data;

    if (file == NULL || json == NULL) {
        return CONFIG_INVALID_ARGUMENTS;
    }

    if (stat(file, &st) == -1) {
        return CONFIG_NO_SUCH_FILE;
    }

    fp = fopen(file, "rb");
    if (fp == NULL) {
        return CONFIG_OPEN_FAILED;
    }

    data = reinterpret_cast<char*>(config_malloc(st.st_size + 1));
    if (data == NULL) {
        fclose(fp);
        return CONFIG_MALLOC_FAILED;
    }

    if (spool(fp, data, st.st_size) == -1) {
        free(data);
        fclose(fp);
        return CONFIG_IO_ERROR;
    }

    fclose(fp);
    data[st.st_size] = 0;

    *json = NULL;
    if (checkUTF8JSON((unsigned char*)data, st.st_size)) {
        *json = cJSON_Parse(data);
    }

    free(data);
    if (*json == NULL) {
        return CONFIG_PARSE_ERROR;
    }

    return CONFIG_SUCCESS;
}
