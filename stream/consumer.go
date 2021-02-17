// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package stream

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ava-labs/ortelius/services/db"

	"github.com/ava-labs/ortelius/services/metrics"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/wrappers"
	"github.com/confluentinc/confluent-kafka-go/kafka"

	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services"
)

const (
	kafkaReadTimeout = 10 * time.Second

	ConsumerEventTypeDefault = EventTypeDecisions
	ConsumerMaxBytesDefault  = 10e8
)

type serviceConsumerFactory func(uint32, string, string) (services.Consumer, error)

// consumer takes events from Kafka and sends them to a service consumer
type consumer struct {
	id      string
	chainID string

	kafkaConsumer *kafka.Consumer
	consumer      services.Consumer
	conns         *services.Connections
	sc            *services.Control

	// metrics
	metricProcessedCountKey       string
	metricFailureCountKey         string
	metricProcessMillisCounterKey string
	metricSuccessCountKey         string

	groupName string
}

// NewConsumerFactory returns a processorFactory for the given service consumer
func NewConsumerFactory(factory serviceConsumerFactory) ProcessorFactory {
	return func(sc *services.Control, conf cfg.Config, chainVM string, chainID string) (Processor, error) {
		conns, err := sc.DatabaseOnly()
		if err != nil {
			return nil, err
		}

		kafkaConsumer, err := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers":  strings.Join(conf.Stream.Kafka.Brokers, ","),
			"group.id":           conf.Stream.Consumer.GroupName,
			"session.timeout.ms": 6000,
			"auto.offset.reset":  "earliest"})
		if err != nil {
			return nil, err
		}

		c := &consumer{
			kafkaConsumer: kafkaConsumer,

			chainID:                       chainID,
			conns:                         conns,
			sc:                            sc,
			metricProcessedCountKey:       fmt.Sprintf("consume_records_processed_%s", chainID),
			metricProcessMillisCounterKey: fmt.Sprintf("consume_records_process_millis_%s", chainID),
			metricSuccessCountKey:         fmt.Sprintf("consume_records_success_%s", chainID),
			metricFailureCountKey:         fmt.Sprintf("consume_records_failure_%s", chainID),
			id:                            fmt.Sprintf("consumer %d %s %s", conf.NetworkID, chainVM, chainID),
		}
		metrics.Prometheus.CounterInit(c.metricProcessedCountKey, "records processed")
		metrics.Prometheus.CounterInit(c.metricProcessMillisCounterKey, "records processed millis")
		metrics.Prometheus.CounterInit(c.metricSuccessCountKey, "records success")
		metrics.Prometheus.CounterInit(c.metricFailureCountKey, "records failure")
		sc.InitConsumeMetrics()

		// Create consumer backend
		c.consumer, err = factory(conf.NetworkID, chainVM, chainID)
		if err != nil {
			if err := c.Close(); err != nil {
				return nil, err
			}
			return nil, err
		}

		// Setup config
		c.groupName = conf.Consumer.GroupName
		if c.groupName == "" {
			c.groupName = c.consumer.Name()
		}
		if !conf.Consumer.StartTime.IsZero() {
			c.groupName = ""
		}

		return c, nil
	}
}

func (c *consumer) ID() string {
	return c.id
}

// Close closes the consumer
func (c *consumer) Close() error {
	c.sc.Log.Info("close %s", c.id)
	errs := wrappers.Errs{}
	errs.Add(c.kafkaConsumer.Close())
	if c.conns != nil {
		errs.Add(c.conns.Close())
	}
	return errs.Err
}

// ProcessNextMessage waits for a new Message and adds it to the services
func (c *consumer) ProcessNextMessage() error {
	msg, err := c.nextMessage()
	if err != nil {
		if err != context.DeadlineExceeded {
			c.sc.Log.Error("consumer.getNextMessage: %s", err.Error())
		}
		return err
	}

	collectors := metrics.NewCollectors(
		metrics.NewCounterIncCollect(c.metricProcessedCountKey),
		metrics.NewCounterObserveMillisCollect(c.metricProcessMillisCounterKey),
		metrics.NewCounterIncCollect(services.MetricConsumeProcessedCountKey),
		metrics.NewCounterObserveMillisCollect(services.MetricConsumeProcessMillisCounterKey),
	)
	defer func() {
		err := collectors.Collect()
		if err != nil {
			c.sc.Log.Error("collectors.Collect: %s", err)
		}
	}()

	for {
		err = c.persistConsume(msg)
		if !db.ErrIsLockError(err) {
			break
		}
	}
	if err != nil {
		collectors.Error()
		c.sc.Log.Error("consumer.Consume: %s", err)
		return err
	}

	c.sc.BalanceAccumulatorManager.Run(c.sc.Persist, c.sc)

	return c.commitMessage(msg)
}

func (c *consumer) persistConsume(msg *Message) error {
	ctx, cancelFn := context.WithTimeout(context.Background(), cfg.DefaultConsumeProcessWriteTimeout)
	defer cancelFn()
	return c.consumer.Consume(ctx, c.conns, msg, c.sc.Persist)
}

func (c *consumer) nextMessage() (*Message, error) {
	ctx, cancelFn := context.WithTimeout(context.Background(), kafkaReadTimeout)
	defer cancelFn()

	return c.getNextMessage(ctx)
}

func (c *consumer) Failure() {
	_ = metrics.Prometheus.CounterInc(c.metricFailureCountKey)
	_ = metrics.Prometheus.CounterInc(services.MetricConsumeFailureCountKey)
}

func (c *consumer) Success() {
	_ = metrics.Prometheus.CounterInc(c.metricSuccessCountKey)
	_ = metrics.Prometheus.CounterInc(services.MetricConsumeSuccessCountKey)
}

func (c *consumer) commitMessage(msg *Message) error {
	if _, err := c.kafkaConsumer.CommitMessage(msg.kafkaMessage); err != nil {
		return err
	}
	return nil
}

// getNextMessage gets the next Message from the Kafka Indexer
func (c *consumer) getNextMessage(ctx context.Context) (*Message, error) {
	// Get raw Message from Kafka
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(5 * time.Second)
	}

	ev := c.kafkaConsumer.Poll(int(time.Until(deadline).Milliseconds()))
	if ev == nil {
		return nil, nil
	}

	var msg *Message
	switch e := ev.(type) {
	case *kafka.Message:
		msg = &Message{
			chainID:      c.chainID,
			body:         e.Value,
			timestamp:    e.Timestamp.UTC().Unix(),
			nanosecond:   int64(e.Timestamp.UTC().Nanosecond()),
			kafkaMessage: e,
		}

		// Extract Message ID from key
		id, err := ids.ToID(e.Key)
		if err != nil {
			msg.id = string(e.Key)
		} else {
			msg.id = id.String()
		}

		return msg, nil
	case kafka.Error:
		return nil, errors.New(e.Error())
	default:
		return nil, nil
	}
}
