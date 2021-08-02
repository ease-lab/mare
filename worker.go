package mare

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"

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

	results := make(map[string][]string)
	for _, line := range strings.Split(inputData, "\n") {
		columns := strings.Split(line, "\t")
		curResults, err := m.mapper.Map(ctx, Pair{Key: columns[0], Value: columns[1]})
		if err != nil {
			return nil, errors.Wrap(err, "mapper error")
		}
		for _, result := range curResults {
			results[result.Key] = append(results[result.Key], result.Value)
		}
	}

	outputs := make(map[string]*Resource)
	for key, values := range results {
		outputs[key], err = request.OutputHint.Put(serializeValues(values))
		if err != nil {
			return nil, errors.Wrap(err, "failed to put output")
		}
	}

	return &MapBatchResponse{
		Outputs: outputs,
	}, nil
}

func serializeValues(values []string) string {
	m, err := json.MarshalIndent(values, "", "\t")
	if err != nil {
		logrus.Fatal(err)
	}
	return string(m)
}

func (m *mareServer) ReduceBatch(ctx context.Context, request *ReduceBatchRequest) (*ReduceBatchResponse, error) {
	var inputValues []string
	for _, resource := range request.Inputs {
		val, err := resource.Get()
		if err != nil {
			return nil, errors.Wrap(err, "failed to get input")
		}
		inputValues = append(inputValues, val)
	}

	results, err := m.reducer.Reduce(ctx, request.Key, inputValues)
	if err != nil {
		return nil, errors.Wrap(err, "reducer error")
	}

	output, err := request.OutputHint.Put(serializePairs(results))
	if err != nil {
		return nil, errors.Wrap(err, "failed to put output")
	}

	return &ReduceBatchResponse{Output: output}, nil
}

func serializePairs(pairs []Pair) string {
	m, err := json.MarshalIndent(pairs, "", "\t")
	if err != nil {
		logrus.Fatal(err)
	}
	return string(m)
}
