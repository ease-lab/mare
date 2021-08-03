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
	"net"
	"os"

	tracing "github.com/ease-lab/vhive/utils/tracing/go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type mareServer struct {
	UnimplementedMareServer

	mapper  Mapper
	reducer Reducer
}

func Work(mapper Mapper, reducer Reducer) error {
	port := os.Getenv("PORT")
	if port == "" {
		logrus.Warn("PORT envvar is missing, defaulting to 80")
		port = "80"
	}

	var grpcServer *grpc.Server
	if tracing.IsTracingEnabled() {
		grpcServer = tracing.GetGRPCServerWithUnaryInterceptor()
	} else {
		grpcServer = grpc.NewServer()
	}

	mareServer := mareServer{
		mapper:  mapper,
		reducer: reducer,
	}

	RegisterMareServer(grpcServer, &mareServer)
	reflection.Register(grpcServer)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		return errors.Wrap(err, "failed to listen")
	}

	if err := grpcServer.Serve(lis); err != nil {
		return errors.Wrap(err, "failed to serve")
	}

	return nil
}

func (m *mareServer) MapBatch(ctx context.Context, request *MapBatchRequest) (*MapBatchResponse, error) {
	inputData, err := request.Input.Get()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get input")
	}
	inputPairs := UnmarshalPairs(inputData)

	logrus.Debugf("Mapping %d input pairs", len(inputPairs))

	results := make(map[string][]string)
	for _, pair := range inputPairs {
		curResults, err := m.mapper.Map(ctx, Pair{Key: pair.Key, Value: pair.Value})
		if err != nil {
			return nil, errors.Wrap(err, "mapper error")
		}
		for _, result := range curResults {
			results[result.Key] = append(results[result.Key], result.Value)
		}
	}

	logrus.Debugf("Mapper outputing %d groupped-values", len(results))

	outputs := make(map[string]*Resource)
	for key, values := range results {
		outputs[key], err = request.OutputHint.Put(MarshalValues(values))
		if err != nil {
			return nil, errors.Wrap(err, "failed to put output")
		}
	}

	return &MapBatchResponse{
		Outputs: outputs,
	}, nil
}

func (m *mareServer) ReduceBatch(ctx context.Context, request *ReduceBatchRequest) (*ReduceBatchResponse, error) {
	var inputValues []string

	logrus.Debugf("Concatenating %d inputs to be reduced", len(request.Inputs))

	for _, resource := range request.Inputs {
		inputData, err := resource.Get()
		if err != nil {
			return nil, errors.Wrap(err, "failed to get input")
		}
		inputValues = append(inputValues, UnmarshalValues(inputData)...)
	}

	logrus.Debugf("Reducing %d values", len(inputValues))

	results, err := m.reducer.Reduce(ctx, request.Key, inputValues)
	if err != nil {
		return nil, errors.Wrap(err, "reducer error")
	}

	logrus.Debugf("Reducer outputting %d pairs", len(results))

	output, err := request.OutputHint.Put(MarshalPairs(results))
	if err != nil {
		return nil, errors.Wrap(err, "failed to put output")
	}

	return &ReduceBatchResponse{Output: output}, nil
}
