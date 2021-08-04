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

	tracing "github.com/ease-lab/vhive/utils/tracing/go"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"

	"github.com/ease-lab/mare"
)

var workerURL = flag.String("workerURL", "127.0.0.1:8080", "URL of the mapper/reducer workers including the port number")
var inputResourceBackend = flag.String("inputResourceBackend", "FILE", "Backend of the input resource. Either one of \"FILE\", \"S3\", or \"XDT\".")
var interBack = flag.String("interBack", "FILE", "Backend of the intermediate resources.")
var interHint = flag.String("interHint", "", "Hint for the intermediate resources.")
var outputBack = flag.String("outputBack", "FILE", "Backend of the final output resources.")
var outputHint = flag.String("outputHint", "", "Hint for the final output resources.")
var nReducers = flag.Int("nReducers", 5, "Number of reducer invocations.")

func main() {
	flag.Parse()
	var inputResources []*mare.Resource
	for _, locator := range flag.Args() {
		inputResources = append(inputResources, &mare.Resource{
			Backend: mare.ResourceBackend(mare.ResourceBackend_value[*inputResourceBackend]),
			Locator: locator,
		})
	}
	interResHint := mare.ResourceHint{
		Backend: mare.ResourceBackend(mare.ResourceBackend_value[*interBack]),
		Hint:    *interHint,
	}
	outputResHint := mare.ResourceHint{
		Backend: mare.ResourceBackend(mare.ResourceBackend_value[*outputBack]),
		Hint:    *outputHint,
	}

	ctx := context.Background()
	keys, values := runMappers(ctx, *workerURL, inputResources, &interResHint)
	finalOutput := runReducers(ctx, *workerURL, keys, values, &outputResHint)

	fmt.Println(finalOutput.Locator)
}

func runMappers(ctx context.Context, workerURL string, inputSlices []*mare.Resource, outputHint *mare.ResourceHint) (keys []string, values []*mare.Resource) {
	outputCh := make(chan *mare.MapBatchResponse)

	for _, inputSlice := range inputSlices {
		go invokeMapper(ctx, workerURL, inputSlice, outputHint, outputCh)
	}

	keysMap := make(map[string]interface{})
	for i := 0; i < len(inputSlices); i++ {
		mapBatchResponse := <-outputCh
		for _, key := range mapBatchResponse.Keys {
			keysMap[key] = nil
		}
		values = append(values, mapBatchResponse.Output)
	}
	keys = mare.Keys(keysMap)
	return
}

func invokeMapper(ctx context.Context, workerURL string, input *mare.Resource, outputHint *mare.ResourceHint, outputCh chan<- *mare.MapBatchResponse) {
	conn := getGrpcConn(workerURL)
	defer conn.Close()
	client := mare.NewMareClient(conn)

	resp, err := client.MapBatch(ctx, &mare.MapBatchRequest{
		Input:      input,
		OutputHint: outputHint,
	})
	if err != nil {
		logrus.Fatal("Failed to invoke map batch: ", err)
	}

	outputCh <- resp
}

func runReducers(ctx context.Context, workerURL string, keys []string, values []*mare.Resource, outputHint *mare.ResourceHint) *mare.Resource {
	outputCh := make(chan *mare.Resource)

	keysets := splitKeys(keys, *nReducers)
	for _, keyset := range keysets {
		go invokeReducer(ctx, workerURL, keyset, values, outputHint, outputCh)
	}

	var outputPairs []mare.Pair
	for i := 0; i < len(keysets); i++ {
		outputData, err := (<-outputCh).Get(ctx)
		if err != nil {
			logrus.Fatal("Failed to get reducer output: ", err)
		}
		outputPairs = append(outputPairs, mare.UnmarshalPairs(outputData)...)
	}

	output, err := outputHint.Put(ctx, mare.MarshalPairs(outputPairs))
	if err != nil {
		logrus.Fatal("Failed to put final output: ", err)
	}
	return output
}

func invokeReducer(ctx context.Context, workerURL string, keyset []string, values []*mare.Resource, outputHint *mare.ResourceHint, outputCh chan<- *mare.Resource) {
	conn := getGrpcConn(workerURL)
	defer conn.Close()
	client := mare.NewMareClient(conn)

	resp, err := client.ReduceBatch(ctx, &mare.ReduceBatchRequest{
		Keys:       keyset,
		Inputs:     values,
		OutputHint: outputHint,
	})
	if err != nil {
		logrus.Fatal("Failed to invoke reduce batch: ", err)
	}
	outputCh <- resp.Output
}

func getGrpcConn(workerURL string) *grpc.ClientConn {
	dialOptions := []grpc.DialOption{grpc.WithBlock(), grpc.WithInsecure()}
	if tracing.IsTracingEnabled() {
		dialOptions = append(dialOptions, grpc.WithUnaryInterceptor(otelgrpc.UnaryClientInterceptor()))
	}
	conn, err := grpc.Dial(workerURL, dialOptions...)
	if err != nil {
		logrus.Fatal("Failed to dial: ", err)
	}
	return conn
}

func splitKeys(keys []string, n int) [][]string {
	keySets := make([][]string, n)
	l := len(keys) / n
	for i := 0; i < n; i++ {
		if i == n - 1 {
			keySets[i] = keys[i*l:]
		} else {
			keySets[i] = keys[i*l : (i+1)*l]
		}
	}
	return keySets
}
