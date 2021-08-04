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

package main

import (
	"context"
	"flag"
	"fmt"

	"github.com/ease-lab/mare"
)

func main() {
	workerURL := flag.String("workerURL", "127.0.0.1:8080", "URL of the mapper/reducer workers including the port number")
	inputResourceBackend := flag.String("inputResourceBackend", "FILE", "Backend of the input resource. Either one of \"FILE\", \"S3\", or \"XDT\".")
	interBack := flag.String("interBack", "FILE", "Backend of the intermediate resources.")
	interHint := flag.String("interHint", "", "Hint for the intermediate resources.")
	outputBack := flag.String("outputBack", "FILE", "Backend of the final output resources.")
	outputHint := flag.String("outputHint", "", "Hint for the final output resources.")
	nReducers := flag.Int("nReducers", 5, "Number of reducer invocations.")
	flag.Parse()

	_, locator := mare.Drive(
		context.Background(),
		*workerURL,
		*inputResourceBackend,
		*interBack,
		*interHint,
		*outputBack,
		*outputHint,
		*nReducers,
		flag.Args(),
	)

	fmt.Println(locator)
}
