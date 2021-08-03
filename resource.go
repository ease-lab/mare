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
	f, err := ioutil.TempFile(dirname, "mare-*.json")
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


