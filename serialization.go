package mare

import (
	"encoding/json"

	"github.com/sirupsen/logrus"
)

func MarshalPairs(pairs []Pair) string {
	m, err := json.MarshalIndent(pairs, "", "\t")
	if err != nil {
		logrus.Fatal("Failed to marshal pairs: ", err)
	}
	return string(m)
}

func UnmarshalPairs(data string) (pairs []Pair) {
	if err := json.Unmarshal([]byte(data), &pairs); err != nil {
		logrus.Fatal("Failed to unmarshal pairs: ", err)
	}
	return
}

func MarshalValues(values []string) string {
	m, err := json.MarshalIndent(values, "", "\t")
	if err != nil {
		logrus.Fatal("Failed to marshal values: ", err)
	}
	return string(m)
}

func UnmarshalValues(data string) (values []string) {
	if err := json.Unmarshal([]byte(data), &values); err != nil {
		logrus.Fatal("Failed to unmarshal values: ", err)
	}
	return
}
