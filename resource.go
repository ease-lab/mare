// Copyright (c) 2021 Mert Bora Alper and EASE Lab
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package mare

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func init() {
	err := os.Setenv("AWS_SDK_LOAD_CONFIG", "true")
	if err != nil {
		logrus.Fatal("Failed to set AWS_SDK_LOAD_CONFIG")
	}
}

func (x *Resource) Get(ctx context.Context) (string, error) {
	switch x.Backend {
	case ResourceBackend_FILE:
		return getFileResource(x.Locator)
	case ResourceBackend_S3:
		return getS3Resource(ctx, x.Locator)
	case ResourceBackend_XDT:
		panic("NOT IMPLEMENTED YET")
	}
	return "", fmt.Errorf("unknown backend: %d", x.Backend)
}

func getFileResource(path string) (string, error) {
	data, err := ioutil.ReadFile(path)
	return string(data), err
}

func getS3Resource(ctx context.Context, uri string) (string, error) {
	parsed, err := parseS3URI(uri)
	if err != nil {
		return "", errors.Wrap(err, "failed to parse S3 uri")
	}

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return "", errors.Wrap(err, "failed to load AWS config")
	}

	s3Client := s3.NewFromConfig(cfg)
	params := &s3.GetObjectInput{
		Bucket: aws.String(parsed.Hostname()),
		Key:    aws.String(parsed.Path),
	}
	resp, err := s3Client.GetObject(ctx, params)
	if err != nil {
		return "", errors.Wrapf(err, "failed to get object `%s`", uri)
	}

	data, err := ioutil.ReadAll(resp.Body)
	return string(data), err
}

func (x *ResourceHint) Put(ctx context.Context, data string) (*Resource, error) {
	switch x.Backend {
	case ResourceBackend_FILE:
		return putFileResource(x.Hint, data)
	case ResourceBackend_S3:
		return putS3Resource(ctx, x.Hint, data)
	case ResourceBackend_XDT:
		panic("NOT IMPLEMENTED YET")
	}
	return nil, fmt.Errorf("unknown backend: %d", x.Backend)
}

func putFileResource(dirname string, data string) (*Resource, error) {
	f, err := ioutil.TempFile(dirname, "mare-*.tsv")
	if err != nil {
		return nil, errors.Wrap(err, "failed to create a temp file")
	}
	defer f.Close()
	_, err = f.Write([]byte(data))
	if err != nil {
		return nil, errors.Wrap(err, "failed to write")
	}
	return &Resource{Backend: ResourceBackend_FILE, Locator: f.Name()}, nil
}

func putS3Resource(ctx context.Context, uri string, data string) (*Resource, error) {
	parsed, err := parseS3URI(uri)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse S3 uri")
	}
	bucket := parsed.Hostname()
	key := path.Join(parsed.Path, fmt.Sprintf("mare-%s.tsv", RandString(8)))

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to load AWS config")
	}

	s3Client := s3.NewFromConfig(cfg)
	params := &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   strings.NewReader(data),
	}
	_, err = s3Client.PutObject(ctx, params)
	if err != nil {
		return nil, errors.Wrap(err, "failed to put object")
	}

	return &Resource{Backend: ResourceBackend_S3, Locator: fmt.Sprintf("s3://%s/%s", bucket, key)}, nil
}

// parseS3URI is copied from Corral.
func parseS3URI(uri string) (*url.URL, error) {
	parsed, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("failed to parse S3URI: %s", err)
	}

	parsed.Path = strings.TrimPrefix(parsed.Path, "/")

	return parsed, err
}
