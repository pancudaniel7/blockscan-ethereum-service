package pkg

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// ConsumeOneKafkaRecord polls the given topic for a single record until timeout.
func ConsumeOneKafkaRecord(ctx context.Context, brokers []string, topic string, timeout time.Duration) (*kgo.Record, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(topic))
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		it := fetches.RecordIter()
		for !it.Done() {
			rec := it.Next()
			if rec != nil {
				return rec, nil
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	return nil, fmt.Errorf("no kafka record in time")
}
