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
	"encoding/json"

	"github.com/sirupsen/logrus"
)

func MarshalPairs(pairs []Pair) string {
	m, err := json.MarshalIndent(pairs, "", "\t")
	if err != nil {
		logrus.Fatal("Failed to marshal pairs: ", err)
	}
	return string(m)
}

func UnmarshalPairs(data string) (pairs []Pair) {
	if err := json.Unmarshal([]byte(data), &pairs); err != nil {
		logrus.Fatal("Failed to unmarshal pairs: ", err)
	}
	return
}

func MarshalValues(values []string) string {
	m, err := json.MarshalIndent(values, "", "\t")
	if err != nil {
		logrus.Fatal("Failed to marshal values: ", err)
	}
	return string(m)
}

func UnmarshalValues(data string) (values []string) {
	if err := json.Unmarshal([]byte(data), &values); err != nil {
		logrus.Fatal("Failed to unmarshal values: ", err)
	}
	return
}
