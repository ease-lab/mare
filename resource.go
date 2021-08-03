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
	"fmt"
	"io/ioutil"

	"github.com/pkg/errors"
)

func (x *Resource) Get() (string, error) {
	switch x.Backend {
	case ResourceBackend_FILE:
		return getFileResource(x.Locator)
	case ResourceBackend_S3:
		panic("NOT IMPLEMENTED YET")
	case ResourceBackend_XDT:
		panic("NOT IMPLEMENTED YET")
	}
	return "", fmt.Errorf("unknown backend: %d", x.Backend)
}

func getFileResource(path string) (string, error) {
	data, err := ioutil.ReadFile(path)
	return string(data), err
}

func (x *ResourceHint) Put(data string) (*Resource, error) {
	switch x.Backend {
	case ResourceBackend_FILE:
		return putFileResource(x.Hint, data)
	case ResourceBackend_S3:
		panic("NOT IMPLEMENTED YET")
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
