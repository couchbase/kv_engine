/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/// tls_test is a standalone binary to be used from memcached_testapp
/// to test that we can connect to the memcached server over TLS and
/// perform authentication with X.509 certificates from the go libraries
/// See MB-50033

package main

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"strings"
	"time"
)

var dialer *net.Dialer = &net.Dialer{Timeout: 30 * time.Second}

var options struct {
	rootCertFile   string
	clientCertFile string
	clientKeyFile  string
	nsServerUrl    string
	kvUrl          string
	skipCertVerify string
}

// Variables to be set after argsParse
var RootCA string
var ClientCert string
var ClientKey string
var kvConnStr string
var skipCertVerify bool

func argParse() {
	flag.StringVar(&options.rootCertFile, "rootCA", "",
		"root CA certificate file path")
	flag.StringVar(&options.clientCertFile, "clientCert", "",
		"client certificate file path")
	flag.StringVar(&options.clientKeyFile, "clientKey", "",
		"client private key file path")
	flag.StringVar(&options.kvUrl, "kv", "",
		"TLS enabled port for memcached. default 127.0.0.1:11207")
	flag.StringVar(&options.skipCertVerify, "skipCertVerify", "",
		"Set to true to disable certificate verification")
	flag.Parse()

	RootCA = options.rootCertFile
	ClientKey = options.clientKeyFile
	ClientCert = options.clientCertFile
	kvConnStr = options.kvUrl
	skipCertVerify = options.skipCertVerify == "true"

	if RootCA == "" || ClientCert == "" || ClientKey == "" {
		fmt.Printf("Required input are missing. Try \"-h\" to see flags\n")
		os.Exit(1)
	}
}

func GetHostName(hostAddr string) string {
	index := strings.LastIndex(hostAddr, ":")
	if index < 0 {
		// host addr does not contain ":". treat host addr as host name
		return hostAddr
	}
	return hostAddr[0:index]
}

func loadFile(filename string) []byte {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		panic(err)
	}
	if len(data) == 0 {
		panic(fmt.Errorf("no data in %s", filename))
	}
	return data
}

func MakeTLSConn(ssl_con_str string) (*tls.Conn, error) {
	certificates := loadFile(RootCA)
	caPool := x509.NewCertPool()
	ok := caPool.AppendCertsFromPEM(certificates)
	if !ok {
		panic(errors.New("invalid certificate"))
	}

	clientCert, err := tls.LoadX509KeyPair(ClientCert, ClientKey)
	if err != nil {
		panic(fmt.Errorf("failed to parse client certificate and client key. err=%v", err))
	}

	tlsConfig := &tls.Config{RootCAs: caPool, ClientAuth: tls.VerifyClientCertIfGiven}
	tlsConfig.Certificates = []tls.Certificate{clientCert}
	tlsConfig.InsecureSkipVerify = skipCertVerify
	if !skipCertVerify {
		tlsConfig.ServerName = GetHostName(ssl_con_str)
	}
	tlsConfig.CurvePreferences = []tls.CurveID{tls.CurveP256, tls.CurveP384, tls.CurveP521}

	// enforce timeout
	errChannel := make(chan error, 2)
	time.AfterFunc(dialer.Timeout, func() {
		errChannel <- errors.New("Exec timeout")
	})

	// get tcp connection
	rawConn, err := dialer.Dial("tcp", ssl_con_str)

	if err != nil {
		fmt.Printf("Failed to connect to %v, err=%v\n", ssl_con_str, err)
		return nil, err
	}

	tcpConn, ok := rawConn.(*net.TCPConn)
	if !ok {
		// should never get here
		_ = rawConn.Close()
		fmt.Printf("Failed to get tcp connection when connecting to %v\n", ssl_con_str)
		return nil, err
	}

	// wrap as tls connection
	tlsConn := tls.Client(tcpConn, tlsConfig)

	// spawn new routine to enforce timeout
	go func() {
		errChannel <- tlsConn.Handshake()
	}()

	err = <-errChannel

	if err != nil {
		_ = tlsConn.Close()
		fmt.Printf("TLS handshake failed when connecting to %v, err=%v\n", ssl_con_str, err)
		return nil, err
	}

	return tlsConn, nil
}

func main() {
	argParse()
	if kvConnStr == "" {
		kvConnStr = "127.0.0.1:11207"
	}

	conn, err := MakeTLSConn(kvConnStr)
	if err != nil {
		fmt.Printf("MakeTLSConn error: %v\n", err)
		os.Exit(1)
	}

	// We should have been authenticated with the content of the certificate.
	// That means that if we send a SASL AUTH message it should return NOT SUPPORTED
	saslAuthPacket := make([]byte, 33)
	saslAuthPacket[0] = 0x80
	saslAuthPacket[1] = 0x21  // sasl auth
	saslAuthPacket[3] = 5     // PLAIN
	saslAuthPacket[11] = 0x09 // PLAIN + '\0' + 'u' + '\0' + 'p'
	saslAuthPacket[24] = 'P'
	saslAuthPacket[25] = 'L'
	saslAuthPacket[26] = 'A'
	saslAuthPacket[27] = 'I'
	saslAuthPacket[28] = 'N'
	saslAuthPacket[30] = 'u'
	saslAuthPacket[32] = 'p'

	nw, err := conn.Write(saslAuthPacket)
	if err != nil {
		panic(err)
	}

	if nw != len(saslAuthPacket) {
		panic("Not enough bytes sent")
	}

	saslAuthResponse := make([]byte, 24)
	nr, err := conn.Read(saslAuthResponse)
	if nr != len(saslAuthResponse) {
		panic("Not enough bytes received")
	}

	if saslAuthResponse[0] != 0x81 {
		panic("Invalid magic received")
	}

	if saslAuthResponse[7] != 0x83 {
		panic(fmt.Errorf("expected not supported; server did not authenticate us: %v", saslAuthResponse))
	}

	if conn != nil {
		_ = conn.Close()
	}
}
