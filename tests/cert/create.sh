#! /bin/bash
#
# The following script is used to generate the certificates we use
# in the unit tests.
#

set -e

openssl genrsa -out testapp.pem 1024
openssl rsa -passout file:passphrase.txt -des3 -in testapp.pem -out encrypted-testapp.pem
openssl req -new -x509  -days 3650 -sha256 -key testapp.pem -out testapp.cert \
        -subj '/C=NO/O=Couchbase Inc/OU=kv eng/CN=Root CA'
openssl genrsa -out client.key 1024
openssl req -new -key client.key -out int.csr \
	-subj '/C=NO/O=Couchbase Inc/OU=kv eng/G=Trond/S=Norbye/CN=Trond Norbye'
cat > v3_ca.ext <<EOF
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid:always,issuer:always
basicConstraints = CA:true
EOF
openssl x509 -req -in int.csr -CA testapp.cert \
	-CAkey testapp.pem -CAcreateserial \
        -CAserial rootCA.srl -extfile v3_ca.ext -out client.pem -days 3650

openssl req -new \
            -sha256 \
            -nodes \
            -out parse-test.csr \
            -newkey rsa:1024 \
            -keyout parse-test.key \
            -config <(
cat <<-EOF
[req]
default_bits = 1024
prompt = no
default_md = sha256
req_extensions = req_ext
distinguished_name = dn

[ dn ]
C=NO
O=Couchbase
OU=kv_eng
CN=testappname

[ req_ext ]
subjectAltName = @alt_names

[ alt_names ]
URI.1 = urn:li:testurl_1
URI.2 = email:testapp@example.com
URI.3 = couchbase://myuser@mycluster/mybucket
DNS.1 = MyServerName
EOF
)
cat > v3_ca.ext <<EOF
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid:always,issuer:always
basicConstraints = CA:true
subjectAltName = @alt_names

[ alt_names ]
URI.1 = urn:li:testurl_1
URI.2 = email:testapp@example.com
URI.3 = couchbase://myuser@mycluster/mybucket
DNS.1 = MyServerName
EOF

openssl x509 -req -in parse-test.csr -CA testapp.cert \
	-CAkey testapp.pem -CAcreateserial \
    -CAserial rootCA.srl -extfile v3_ca.ext -out parse-test.pem -days 3650

#openssl x509 -in parse-test.pem -text

rm -f rootCA.srl v3_ca.ext int.csr parse-test.csr

# generate a PCKS12 file
openssl pkcs12 -export -clcerts -inkey client.key -in client.pem -out client.p12 -passout pass:password -name "Trond Norbye"
