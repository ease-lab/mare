package mare

import (
	"context"
)

type Pair struct {
	Key   string `json:"K"`
	Value string `json:"V"`
}

type Mapper interface {
	Map(ctx context.Context, pair Pair) ([]Pair, error)
}

type Reducer interface {
	Reduce(ctx context.Context, key string, values []string) ([]Pair, error)
}
