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

	tracing "github.com/ease-lab/vhive/utils/tracing/go"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
)


func Drive(
	ctx context.Context,
	workerURL,
	inputBack,
	interBack,
	interHint,
	outputBack,
	outputHint string,
	nReducers int,
	inputLocators []string) (backend string, locator string) {
	var inputResources []*Resource
	for _, locator := range inputLocators {
		inputResources = append(inputResources, &Resource{
			Backend: ResourceBackend(ResourceBackend_value[inputBack]),
			Locator: locator,
		})
	}
	interResHint := ResourceHint{
		Backend: ResourceBackend(ResourceBackend_value[interBack]),
		Hint:    interHint,
	}
	outputResHint := ResourceHint{
		Backend: ResourceBackend(ResourceBackend_value[outputBack]),
		Hint:    outputHint,
	}

	keys, values := runMappers(ctx, workerURL, inputResources, &interResHint)
	finalOutput := runReducers(ctx, workerURL, keys, nReducers, values, &outputResHint)

	return finalOutput.Backend.String(), finalOutput.Locator
}

func runMappers(ctx context.Context, workerURL string, inputSlices []*Resource, outputHint *ResourceHint) (keys []string, values []*Resource) {
	outputCh := make(chan *MapBatchResponse)

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
	keys = Keys(keysMap)
	return
}

func invokeMapper(ctx context.Context, workerURL string, input *Resource, outputHint *ResourceHint, outputCh chan<- *MapBatchResponse) {
	conn := getGrpcConn(workerURL)
	defer conn.Close()
	client := NewMareClient(conn)

	resp, err := client.MapBatch(ctx, &MapBatchRequest{
		Input:      input,
		OutputHint: outputHint,
	})
	if err != nil {
		logrus.Fatal("Failed to invoke map batch: ", err)
	}

	outputCh <- resp
}

func runReducers(ctx context.Context, workerURL string, keys []string, nReducers int, values []*Resource, outputHint *ResourceHint) *Resource {
	outputCh := make(chan *Resource)

	keysets := splitKeys(keys, nReducers)
	for _, keyset := range keysets {
		go invokeReducer(ctx, workerURL, keyset, values, outputHint, outputCh)
	}

	var outputPairs []Pair
	for i := 0; i < len(keysets); i++ {
		outputData, err := (<-outputCh).Get(ctx)
		if err != nil {
			logrus.Fatal("Failed to get reducer output: ", err)
		}
		outputPairs = append(outputPairs, UnmarshalPairs(outputData)...)
	}

	output, err := outputHint.Put(ctx, MarshalPairs(outputPairs))
	if err != nil {
		logrus.Fatal("Failed to put final output: ", err)
	}
	return output
}

func invokeReducer(ctx context.Context, workerURL string, keyset []string, values []*Resource, outputHint *ResourceHint, outputCh chan<- *Resource) {
	conn := getGrpcConn(workerURL)
	defer conn.Close()
	client := NewMareClient(conn)

	resp, err := client.ReduceBatch(ctx, &ReduceBatchRequest{
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
		if i == n-1 {
			keySets[i] = keys[i*l:]
		} else {
			keySets[i] = keys[i*l : (i+1)*l]
		}
	}
	return keySets
}
