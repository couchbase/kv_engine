/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2012 Couchbase, Inc
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
#include <cerrno>
#include <cstdlib>
#include <string.h>
#include <strings.h>
#include <cerrno>
#include <getopt.h>
#include <sys/stat.h>
#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <ctype.h>

#include "cJSON.h"

using namespace std;

/**
 * Print the JSON object quoted
 */
static ostream& operator <<(ostream &out, cJSON *json)
{
    char *data = cJSON_PrintUnformatted(json);
    int ii = 0;

    out << '"';
    while (data[ii] != '\0') {
        if (data[ii] == '"') {
            out << '\\';
        }
        out << data[ii];
        ++ii;
    }

    out << '"';
    free(data);
    return out;
}

static void usage(void)
{
    cerr << "Usage: gencode -j JSON -c cfile -h headerfile -f function"
         << endl
         << "\tThe JSON file will be read to generate the c and h file."
         << endl;
    exit(EXIT_FAILURE);
}

static void str_replace_char(char* str, char toReplace, char replaceWith) {
    int i;
    for (i = 0; *(str + i) != '\0'; i++) {
        if (*(str + i) == toReplace) {
            *(str + i) = replaceWith;
        }
    }
}

static void all_caps(char* str) {
    int i;
    for (i = 0; *(str + i) != '\0'; i++) {
        if (*(str + i) >= 'a' && *(str + i) <= 'z') {
            *(str + i) -= 32;
        }
    }
}

int main(int argc, char **argv) {
    int cmd;
    const char *json = NULL;
    const char *hfile = NULL;
    const char *cfile = NULL;
    const char *function = NULL;

    while ((cmd = getopt(argc, argv, "j:c:h:f:")) != -1) {
        switch (cmd) {
        case 'j' :
            json = optarg;
            break;
        case 'c' :
            cfile = optarg;
            break;
        case 'h':
            hfile = optarg;
            break;
        case 'f':
            function = optarg;
            break;
        default:
            usage();
        }
    }

    if (json == NULL || hfile == NULL || cfile == NULL || function == NULL) {
        usage();
    }

    struct stat st;
    if (stat(json, &st) == -1) {
        cerr << "Failed to look up \"" << json << "\": "
             << strerror(errno) << endl;
        exit(EXIT_FAILURE);
    }

    char *data = new char[st.st_size + 1];
    data[st.st_size] = 0;
    ifstream input(json);
    input.read(data, st.st_size);
    input.close();

    cJSON *c = cJSON_Parse(data);
    if (c == NULL) {
        cerr << "Failed to parse JSON.. probably syntax error" << endl;
        exit(EXIT_FAILURE);
    }

    ofstream headerfile(hfile);
    char* macro = strdup(hfile);
    str_replace_char(macro, '/', '_');
    str_replace_char(macro, '-', '_');
    str_replace_char(macro, '.', '_');
    str_replace_char(macro, ':', '_');
    all_caps(macro);

    headerfile
        << "/*" << endl
        << " *     Copyright 2012 Couchbase, Inc" << endl
        << " *" << endl
        << " *   Licensed under the Apache License, Version 2.0 (the \"License\");" << endl
        << " *   you may not use this file except in compliance with the License." << endl
        << " *   You may obtain a copy of the License at" << endl
        << " *" << endl
        << " *       http://www.apache.org/licenses/LICENSE-2.0" << endl
        << " *" << endl
        << " *   Unless required by applicable law or agreed to in writing, software" << endl
        << " *   distributed under the License is distributed on an \"AS IS\" BASIS," << endl
        << " *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied." << endl
        << " *   See the License for the specific language governing permissions and" << endl
        << " *   limitations under the License." << endl
        << " */" << endl
        << endl
        << "/********************************" << endl
        << "** Generated file, do not edit **" << endl
        << "*********************************/" << endl
        << "#ifndef " << macro << endl
        << "#define " << macro << endl
        << endl
        << "#include \"config.h\"" << endl
        << endl
        << "#ifdef __cplusplus" << endl
        << "extern \"C\" {" << endl
        << "#endif" << endl
        << endl
        << "const char *" << function << "(void);" << endl
        << endl
        << "#ifdef __cplusplus" << endl
        << "}" << endl
        << "#endif" << endl
        << "#endif  /* " << macro << "*/" << endl;
    headerfile.close();

    ofstream sourcefile(cfile);
    sourcefile
        << "/*" << endl
        << " *     Copyright 2012 Couchbase, Inc" << endl
        << " *" << endl
        << " *   Licensed under the Apache License, Version 2.0 (the \"License\");" << endl
        << " *   you may not use this file except in compliance with the License." << endl
        << " *   You may obtain a copy of the License at" << endl
        << " *" << endl
        << " *       http://www.apache.org/licenses/LICENSE-2.0" << endl
        << " *" << endl
        << " *   Unless required by applicable law or agreed to in writing, software" << endl
        << " *   distributed under the License is distributed on an \"AS IS\" BASIS," << endl
        << " *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied." << endl
        << " *   See the License for the specific language governing permissions and" << endl
        << " *   limitations under the License." << endl
        << " */" << endl
        << endl
        << "/********************************" << endl
        << "** Generated file, do not edit **" << endl
        << "*********************************/" << endl
        << "#include \"config.h\"" << endl
        << "#include \"" << hfile << "\"" << endl
        << endl
        << "const char *" << function << "(void)" << endl
        << "{" << endl
        << "    return " << c << ";" << endl
        << "}" << endl;

    cJSON_Delete(c);

    return 0;
}
