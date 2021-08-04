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
	inputData, err := request.Input.Get(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get input")
	}
	inputPairs := UnmarshalPairs(inputData)

	logrus.Debugf("Mapper processing %d input pairs...", len(inputPairs))

	outputPairs := make([]Pair, 0)
	keys := make(map[string]interface{})
	for _, pair := range inputPairs {
		curOutputPairs, err := m.mapper.Map(ctx, Pair{Key: pair.Key, Value: pair.Value})
		if err != nil {
			return nil, errors.Wrap(err, "mapper error")
		}
		for _, pair := range curOutputPairs {
			keys[pair.Key] = nil
			outputPairs = append(outputPairs, pair)
		}
	}

	logrus.Debugf("Mapper uploading %d pairs with %d unique keys...", len(outputPairs), len(keys))

	output, err := request.OutputHint.Put(ctx, MarshalPairs(outputPairs))
	if err != nil {
			return nil, errors.Wrap(err, "failed to put output")
		}

	logrus.Debug("Mapper done.")

	return &MapBatchResponse{
		Output: output,
		Keys: Keys(keys),
	}, nil
}

func (m *mareServer) ReduceBatch(ctx context.Context, request *ReduceBatchRequest) (*ReduceBatchResponse, error) {
	logrus.Debugf("Reducer concatenating %d input partitions...", len(request.Inputs))

	values := make(map[string][]string)
	nValues := 0
	for _, resource := range request.Inputs {
		inputData, err := resource.Get(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "failed to get input")
		}
		for _, pair := range UnmarshalPairs(inputData) {
			values[pair.Key] = append(values[pair.Key], pair.Value)
			nValues++
		}
	}

	logrus.Debugf("Reducer processing %d keys...", len(request.Keys))

	var results []Pair
	for _, key := range request.Keys {
		curResults, err := m.reducer.Reduce(ctx, key, values[key])
		if err != nil {
			return nil, errors.Wrap(err, "reducer error")
		}
		results = append(results, curResults...)
	}

	logrus.Debugf("Reducer uploading %d pairs...", len(results))

	output, err := request.OutputHint.Put(ctx, MarshalPairs(results))
	if err != nil {
		return nil, errors.Wrap(err, "failed to put output")
	}

	logrus.Debug("Reducer done.")

	return &ReduceBatchResponse{Output: output}, nil
}
