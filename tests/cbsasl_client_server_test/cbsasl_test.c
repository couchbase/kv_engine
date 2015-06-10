#include <cbsasl/cbsasl.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

cbsasl_conn_t *server;
cbsasl_conn_t *client;

const char *cbpwfile = "cbsasl_test.pw";

static void create_pw_file()
{
    FILE *fp = fopen(cbpwfile, "w");

    fprintf(fp, "mikewied mikepw \n");
    fclose(fp);

    putenv("ISASL_PWFILE=cbsasl_test.pw");
}


struct my_sasl_ctx {
    char *username;
    cbsasl_secret_t *secret;
};

static int sasl_get_username(void *context, int id, const char **result,
                             unsigned int *len)
{
    struct my_sasl_ctx *ctx = context;
    if (!context || !result || (id != CBSASL_CB_USER && id != CBSASL_CB_AUTHNAME)) {
        return CBSASL_BADPARAM;
    }

    *result = ctx->username;
    if (len) {
        *len = (unsigned int)strlen(*result);
    }

    return CBSASL_OK;
}

static int sasl_get_password(cbsasl_conn_t *conn, void *context, int id,
                             cbsasl_secret_t **psecret)
{
    struct my_sasl_ctx *ctx = context;
    if (!conn || ! psecret || id != CBSASL_CB_PASS || ctx == NULL) {
        return CBSASL_BADPARAM;
    }

    *psecret = ctx->secret;
    return CBSASL_OK;
}

static void test_auth(const char *mech)
{
    cbsasl_error_t err;
    const char *data;
    unsigned int len;
    const char *serverdata;
    unsigned int serverlen;
    const char *chosenmech;
    struct my_sasl_ctx context;
    cbsasl_callback_t sasl_callbacks[4];

    sasl_callbacks[0].id = CBSASL_CB_USER;
    sasl_callbacks[0].proc = (int( *)(void)) &sasl_get_username;
    sasl_callbacks[0].context = &context;
    sasl_callbacks[1].id = CBSASL_CB_AUTHNAME;
    sasl_callbacks[1].proc = (int( *)(void)) &sasl_get_username;
    sasl_callbacks[1].context = &context;
    sasl_callbacks[2].id = CBSASL_CB_PASS;
    sasl_callbacks[2].proc = (int( *)(void)) &sasl_get_password;
    sasl_callbacks[2].context = &context;
    sasl_callbacks[3].id = CBSASL_CB_LIST_END;
    sasl_callbacks[3].proc = NULL;
    sasl_callbacks[3].context = NULL;

    context.username = "mikewied";
    context.secret = calloc(1, 100);
    memcpy(context.secret->data, "mikepw", 6);
    context.secret->len = 6;

    err = cbsasl_client_new(NULL, NULL, NULL, NULL, sasl_callbacks, 0, &client);
    if (err != CBSASL_OK) {
        fprintf(stderr, "cbsasl_client_new() failed: %d\n", err);
        exit(EXIT_FAILURE);
    }

    err = cbsasl_client_start(client, mech, NULL,
                              &data, &len, &chosenmech);
    if (err != CBSASL_OK) {
        fprintf(stderr, "cbsasl_client_start() failed: %d\n", err);
        exit(EXIT_FAILURE);
    }

    fprintf(stdout, "Trying to authenticate using %s ... ", chosenmech);
    fflush(stdout);

    err = cbsasl_server_start(&server, chosenmech, data, len,
                              (unsigned char**)&serverdata, &serverlen);
    if (err == CBSASL_OK) {
        fprintf(stdout, "Authenticated\n");
        free(context.secret);
        cbsasl_dispose(&client);
        cbsasl_dispose(&server);
        return;
    }

    if (err != CBSASL_CONTINUE) {
        fprintf(stderr, "cbsasl_server_start() failed: %d\n", err);
        exit(EXIT_FAILURE);
    }

    if (serverlen) {
        err = cbsasl_client_step(client, serverdata, serverlen,
                                 NULL, &data, &len);
        if (err != CBSASL_CONTINUE) {
            fprintf(stderr, "cbsasl_client_step() failed: %d\n", err);
            exit(EXIT_FAILURE);
        }
    }


    while ((err = cbsasl_server_step(server, data, len,
                                     &serverdata, &serverlen)) == CBSASL_CONTINUE) {

        err = cbsasl_client_step(client, serverdata, serverlen,
                                 NULL, &data, &len);
        if (err != CBSASL_CONTINUE) {
            fprintf(stderr, "cbsasl_client_step() failed: %d\n", err);
            exit(EXIT_FAILURE);
        }
    }

    free(context.secret);
    cbsasl_dispose(&client);
    cbsasl_dispose(&server);

    if (err == CBSASL_OK) {
        fprintf(stdout, "Authenticated\n");
    } else {
        fprintf(stderr, "cbsasl_server_step() failed: %d\n", err);
        exit(EXIT_FAILURE);
    }
}

int main(void)
{
    create_pw_file();
    cbsasl_server_init();

    test_auth("PLAIN");
    test_auth("CRAM-MD5");

    cbsasl_server_term();
    remove(cbpwfile);
    return 0;
}
