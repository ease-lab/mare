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
	"bytes"
	"fmt"
	"strings"
)

func MarshalPairs(pairs []Pair) string {
	buffer := new(bytes.Buffer)
	for _, pair := range pairs {
		buffer.WriteString(fmt.Sprintf("%s\t%s\n", pair.Key, pair.Value))
	}
	return buffer.String()
}

func UnmarshalPairs(data string) (pairs []Pair) {
	for _, line := range strings.Split(data, "\n") {
		if line == "" {
			continue
		}
		cells := strings.Split(line, "\t")
		// if there are no "columns", assume empty key and take data as values
		if len(cells) == 1 {
			pairs = append(pairs, Pair{Key: "", Value: cells[0]})
		} else {
			pairs = append(pairs, Pair{Key: cells[0], Value: cells[1]})
		}
	}
	return
}
