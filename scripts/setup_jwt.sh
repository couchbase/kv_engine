#! /bin/bash
# Copyright 2025-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

# Configure JWT for the provided cluster
auth=Administrator:asdfasdf
host=http://127.0.0.1:8091
keydir=${HOME}/.couchbase
signing_algorithm=HS256
issuer_name=couchbase

echo -n "Checking for key directory: "
if [ -d ${keydir} ]
then
    echo "Found"
else
    echo "Not found, creating"
    mkdir -p ${keydir} || {
        echo "Failed to create key directory ${keydir}"
        exit 1
    }
fi

if [ ! -f ${keydir}/token-skeleton.json  ]
then
    echo "Generate ${keydir}/token-skeleton.json (you might want to edit)"

    cat > ${keydir}/token-skeleton.json <<EOF
{
    "aud": "couchbase",
    "iss": "${issuer_name}",
    "sub": "${LOGNAME}",
    "name": "`id -F`"
}
EOF
fi

if [ "${signing_algorithm}" == "HS256" ]
then
    echo -n "Checking for shared secret: "
    if [ -f ${keydir}/shared_secret ]
    then
        echo "Found"
    else
        echo "Not found, generating"
        openssl rand -base64 -out ${keydir}/shared_secret 32 || {
            echo "Failed to generate shared secret"
            exit 1
        }
    fi
else
    if [ "${signing_algorithm}" == "RS256" ]
    then
        echo -n "Checking for private key: "
        if [ -f ${keydir}/private.pem ]
        then
            echo "Found"
        else
            echo "Not found, generating"
            openssl genpkey -algorithm RSA -out ${keydir}/private.pem -pkeyopt rsa_keygen_bits:2048 > /dev/null 2>&1 || {
                echo "Failed to generate private key"
                exit 1
            }
        fi

        echo -n "Checking for public key: "
        if  [ -f ${keydir}/public.pem ]
        then
            echo "Found"
        else
            echo "Not found, generating"
            openssl rsa -in ${keydir}/private.pem -pubout -out ${keydir}/public.pem > /dev/null 2>&1 || {
                echo "Failed to generate public key"
                exit 1
            }
        fi
    fi
fi

echo -n "Checking for developer preview: "
dev_preview=`curl -s -u ${auth} ${host}/pools | jq .isDeveloperPreview`
if [ "${dev_preview}" == "true" ]
then
    echo "Enabled"
else
    echo -n "Not enabled, enabling. "
    output=`curl -s -u ${auth} -X POST -d 'enabled=true' ${host}/settings/developerPreview`
    if [ "${output}" != "[]" ]
    then
        echo "Failed to enable dev preview: ${output}"
        exit 1
    fi
    echo
fi

echo -n "Checking for OAUTHBEARER: "
oauth_bearer=`curl -s -u ${auth} ${host}/settings/security | jq .oauthBearerEnabled`
if [ "${oauth_bearer}" == "true" ]
then
    echo "Enabled"
else
    echo -n "Not enabled, enabling. "
    output=`curl -s -u ${auth} -X POST -d 'oauthBearerEnabled=true' ${host}/settings/security`
    if [ "${output}" != "[]" ]
    then
        echo "Failed to enable OAUTHBEARER: ${output}"
        exit 1
    fi
    echo
fi

echo -n "Checking for JWT settings: "
output=`curl -s -u ${auth} ${host}/settings/jwt | jq .issuers[0].signingAlgorithm 2>/dev/null`
if [  "${output}" != \"${signing_algorithm}\" ]
then
    echo "creating with signing algorithm: ${signing_algorithm}"
    if [ "${signing_algorithm}" == "RS256" ]
    then
        PEM_CONTENT=$(cat ${keydir}/public.pem | awk 'NF {sub(/\r/, ""); printf "%s\\n", $0}')
        output=`curl -X PUT -u ${auth} \
                     -H "Content-Type: application/json" \
                     -d '{
               "enabled": true,
               "issuers": [{
                 "name": "'"${issuer_name}"'",
                 "audClaim": "aud",
                 "audienceHandling": "any",
                 "audiences": ["couchbase"],
                 "subClaim": "sub",
                 "signingAlgorithm": "RS256",
                 "publicKeySource": "pem",
                 "publicKey": "'"$PEM_CONTENT"'"
               }]
             }' \
                     -s \
                     ${host}/settings/jwt | jq .enabled 2>/dev/null`

    else
        if [ "${signing_algorithm}" == "HS256" ]
        then
            SHARED_SECRET=$(cat ${keydir}/shared_secret | tr -d '\n')
            output=`curl -X PUT -u ${auth} \
         -H "Content-Type: application/json" \
         -d '{
       "enabled": true,
       "issuers": [{
         "name": "'"${issuer_name}"'",
         "audClaim": "aud",
         "audienceHandling": "any",
         "audiences": ["couchbase"],
         "subClaim": "sub",
         "signingAlgorithm": "HS256",
         "sharedSecret": "'"${SHARED_SECRET}"'"
       }]
     }' \
         -s \
         ${host}/settings/jwt | jq .enabled 2>/dev/null`
        fi
    fi

    if [ "${output}" != "true" ]
    then
        echo "Failed to create JWT settings: ${output}"
        exit 1
    fi
    echo "Done"
else
    echo "Enabled"
fi
