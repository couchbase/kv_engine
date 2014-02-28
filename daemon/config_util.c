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

#include "config_util.h"

cJSON *config_load_file(const char *file, bool mandatory)
{
    FILE *fp;
    struct stat st;
    char *data;
    cJSON *ret;

    if (file == NULL) {
        if (mandatory) {
            fprintf(stderr, "Mandatory configuration file not specified\n"
                    "Terminating\n");
            exit(EXIT_FAILURE);
        }
        return NULL;
    }

    if (stat(file, &st) == -1) {
        if (mandatory) {
            fprintf(stderr, "Failed to look up %s: %s\nTerminating\n",
                    file, strerror(errno));
            exit(EXIT_FAILURE);
        } else {
            return NULL;
        }
    }

    fp = fopen(file, "rb");
    if (fp == NULL) {
        fprintf(stderr, "Failed to open %s: %s\nTerminating\n",
                file, strerror(errno));
        exit(EXIT_FAILURE);
    }

    data = malloc(st.st_size + 1);
    if (data == NULL) {
        fprintf(stderr, "Failed to allocate memory to parse configuration file"
                "\nTerminating\n");
        exit(EXIT_FAILURE);
    }


    if (fread(data, 1, st.st_size, fp) != st.st_size) {
        fprintf(stderr, "Failed to read configuration file: %s"
                "\nTerminating\n", file);
        free(data);
        exit(EXIT_FAILURE);
    }
    data[st.st_size] = 0;

    ret = cJSON_Parse(data);
    free(data);
    if (ret == NULL) {
        fprintf(stderr, "Failed to parse JSON in \"%s\"\nMost likely syntax "
                "error in the file\nTerminating\n", file);
        exit(EXIT_FAILURE);
    }

    return ret;
}
