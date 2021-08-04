module github.com/ease-lab/mare

go 1.16

require (
	github.com/aws/aws-sdk-go-v2 v1.7.1
	github.com/aws/aws-sdk-go-v2/config v1.5.0
	github.com/aws/aws-sdk-go-v2/service/s3 v1.11.1
	github.com/ease-lab/vhive/utils/tracing/go v0.0.0-20210802105725-6b277cd612ad
	github.com/pkg/errors v0.9.1
	github.com/sirupsen/logrus v1.8.1
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.20.0
	google.golang.org/grpc v1.39.0
	google.golang.org/protobuf v1.27.1
)
