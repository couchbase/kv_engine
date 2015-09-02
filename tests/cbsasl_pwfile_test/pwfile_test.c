#include <platform/platform.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include <cbsasl/cbsasl.h>

#include "cbsasl/pwfile.h"

const char *cbpwfile = "pwfile_test.pw";

const char *user1 = "mikewied";
const char *pass1 = "mikepw";
const char *user2 = "cseo";
const char *pass2 = "seopw";
const char *user3 = "jlim";
const char *pass3 = "limpw";

static void create_pw_file()
{
    FILE *fp = fopen(cbpwfile, "w");
    cb_assert(fp != NULL);

    fprintf(fp, "mikewied mikepw \ncseo seopw \njlim limpw \n");
    cb_assert(fclose(fp) == 0);

    putenv("ISASL_PWFILE=pwfile_test.pw");
}

static void remove_pw_file()
{
    cb_assert(remove(cbpwfile) == 0);
    free_user_ht();
}

static void test_pwfile()
{
    const char *password;

    create_pw_file();
    cb_assert(load_user_db() == CBSASL_OK);
    password = find_pw(user1);
    cb_assert(strncmp(password, pass1, strlen(pass1)) == 0);

    password = find_pw(user2);
    cb_assert(strncmp(password, pass2, strlen(pass2)) == 0);

    password = find_pw(user3);
    cb_assert(strncmp(password, pass3, strlen(pass3)) == 0);

    password = find_pw("non_existant_user");
    cb_assert(password == NULL);

    // Test with a username which is a superset of a valid user.
    password = find_pw("mikewied ");
    cb_assert(password == NULL);

    // Test with a username which is a subset of a valid user.
    password = find_pw("mikew");
    cb_assert(password == NULL);

    remove_pw_file();
}

int main()
{
    test_pwfile();
    return 0;
}
