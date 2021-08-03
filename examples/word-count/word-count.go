package main

import (
	"context"
	"regexp"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/ease-lab/mare"
)

type counter struct {}

func (c *counter) Map(_ context.Context, pair mare.Pair) (outputs []mare.Pair, err error) {
	re := regexp.MustCompile("[^a-zA-Z0-9\\s]+")

	sanitized := strings.ToLower(re.ReplaceAllString(pair.Key, " "))
	for _, word := range strings.Fields(sanitized) {
		if len(word) == 0 {
			continue
		}
		outputs = append(outputs, mare.Pair{Key: word, Value: ""})
	}
	return
}

func (c *counter) Reduce(_ context.Context, key string, values []string) ([]mare.Pair, error) {
	output := mare.Pair{Key: key, Value: strconv.Itoa(len(values))}
	return []mare.Pair{output}, nil
}

func main() {
	// logrus.SetLevel(logrus.DebugLevel)
	counter := new(counter)
	if err := mare.Work(counter, counter); err != nil {
		logrus.Fatal("Failed to work: ", err)
	}
}
