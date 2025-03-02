// Copyright 2019 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cliccl

import (
	gosql "database/sql"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/cli"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// TODO (rohany): change this once another endpoint is setup for getting licenses.
// This URL grants a license that is valid for 1 hour.
const licenseURL = "https://register.cockroachdb.com/api/prodtest"

func getLicense(clusterID uuid.UUID) (string, error) {
	client := &http.Client{
		Timeout: 5 * time.Second,
	}
	req, err := http.NewRequest("GET", licenseURL, nil)
	if err != nil {
		return "", err
	}
	// Send some extra information to the endpoint.
	q := req.URL.Query()
	q.Add("type", "demo")
	q.Add("version", build.VersionPrefix())
	q.Add("clusterid", clusterID.String())
	req.URL.RawQuery = q.Encode()

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", errors.New("unable to connect to licensing endpoint")
	}
	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(bodyBytes), nil
}

func getAndApplyLicense(db *gosql.DB, clusterID uuid.UUID, org string) (bool, error) {
	license, err := getLicense(clusterID)
	if err != nil {
		fmt.Fprintf(log.OrigStderr, "error when contacting licensing server: %+v\n", err)
		return false, nil
	}
	if _, err := db.Exec(`SET CLUSTER SETTING cluster.organization = $1`, org); err != nil {
		return false, err
	}
	if _, err := db.Exec(`SET CLUSTER SETTING enterprise.license = $1`, license); err != nil {
		return false, err
	}
	return true, nil
}

func init() {
	// Set the GetAndApplyLicense function within cockroach demo.
	// This separation is done to avoid using enterprise features in an OSS/BSL build.
	cli.GetAndApplyLicense = getAndApplyLicense
}
