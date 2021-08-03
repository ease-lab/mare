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

	tracing "github.com/ease-lab/vhive/utils/tracing/go"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"

	"github.com/ease-lab/mare"
)

var workerURL = flag.String("workerURL", "127.0.0.1:8080", "URL of the mapper/reducer workers including the port number")
var inputResourceBackend = flag.String("inputResourceBackend", "FILE", "Backend of the input resource. Either one of \"FILE\", \"S3\", or \"XDT\".")
var inputResourceLocator = flag.String("inputResourceLocator", "", "Locator of the input resource")
var interBack = flag.String("interBack", "FILE", "Backend of the intermediate resources.")
var interHint = flag.String("interHint", "", "Hint for the intermediate resources.")
var outputBack = flag.String("outputBack", "FILE", "Backend of the final output resources.")
var outputHint = flag.String("outputHint", "", "Hint for the final output resources.")

func main() {
	flag.Parse()
	inputResource := mare.Resource{
		Backend: mare.ResourceBackend(mare.ResourceBackend_value[*inputResourceBackend]),
		Locator: *inputResourceLocator,
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
	interResources := runMappers(ctx, *workerURL, &inputResource, &interResHint)
	finalOutput := runReducers(ctx, *workerURL, interResources, &outputResHint)

	logrus.Info(finalOutput)
}

func runMappers(ctx context.Context, workerURL string, input *mare.Resource, outputHint *mare.ResourceHint) (outputs map[string][]*mare.Resource) {
	// TODO: slice inputs
	inputSlices := []*mare.Resource{input}

	outputCh := make(chan map[string]*mare.Resource)

	for _, inputSlice := range inputSlices {
		go invokeMapper(ctx, workerURL, inputSlice, outputHint, outputCh)
	}

	outputs = make(map[string][]*mare.Resource)
	for i := 0; i < len(inputSlices); i++ {
		for k, v := range <-outputCh {
			outputs[k] = append(outputs[k], v)
		}
	}
	return
}

func invokeMapper(ctx context.Context, workerURL string, input *mare.Resource, outputHint *mare.ResourceHint, outputCh chan<- map[string]*mare.Resource) {
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

	outputCh <- resp.Outputs
}

func runReducers(ctx context.Context, workerURL string, inputs map[string][]*mare.Resource, outputHint *mare.ResourceHint) *mare.Resource {
	outputCh := make(chan *mare.Resource)

	for key, values := range inputs {
		go invokeReducer(ctx, workerURL, key, values, outputHint, outputCh)
	}

	var outputPairs []mare.Pair
	for i := 0; i < len(inputs); i++ {
		outputData, err := (<-outputCh).Get()
		if err != nil {
			logrus.Fatal("Failed to get reducer output: ", err)
		}
		outputPairs = append(outputPairs, mare.UnmarshalPairs(outputData)...)
	}

	output, err := outputHint.Put(mare.MarshalPairs(outputPairs))
	if err != nil {
		logrus.Fatal("Failed to put final output: ", err)
	}
	return output
}

func invokeReducer(ctx context.Context, workerURL string, key string, values []*mare.Resource, outputHint *mare.ResourceHint, outputCh chan<- *mare.Resource) {
	conn := getGrpcConn(workerURL)
	defer conn.Close()
	client := mare.NewMareClient(conn)

	resp, err := client.ReduceBatch(ctx, &mare.ReduceBatchRequest{
		Key:        key,
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
